/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.dledger;

import io.openmessaging.storage.dledger.AppendFuture;
import io.openmessaging.storage.dledger.BatchAppendFuture;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.BatchAppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.store.file.MmapFile;
import io.openmessaging.storage.dledger.store.file.MmapFileList;
import io.openmessaging.storage.dledger.store.file.SelectMmapBufferResult;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.StoreStatsService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

/**
 * Store all metadata downtime for recovery, data protection reliability
 *
 * DLedger 在整合时，使用 DLedger 条目包裹 RocketMQ 中的 commitlog 条目，即在DLedger 条目的 body 字段来存储整条 commitlog 条目
 * 引入 dividedCommitlogOffset 变量，表示物理偏移量小于该值的消息存在于旧的 commitlog 文件中，实现 升级 DLedger 集群后能访问到旧的数据
 *
 * 新 DLedger 集群启动后，会将最后一个 commitlog 填充，即新的数据不会再写入到原先的 commitlog 文件
 * 消息追加到 DLedger 数据日志文件中，返回的偏移量不是 DLedger 条目的起始偏移量，而是 DLedger 条目中 body 字段的起始偏移量，即真实消息的起始偏移量，
 * 保证消息物理偏移量的语义与 RocketMQ Commitlog 一样
 */
public class DLedgerCommitLog extends CommitLog {
    /**
     * 基于 raft 协议实现的集群内的一个节点，用 DLedgerServer 实例表示
     */
    private final DLedgerServer dLedgerServer;
    /**
     * DLedger 的配置信息
     */
    private final DLedgerConfig dLedgerConfig;
    /**
     * DLedger 基于文件映射的存储实现
     */
    private final DLedgerMmapFileStore dLedgerFileStore;
    /**
     * DLedger 所管理的存储文件集合，对比 RocketMQ 中的 MappedFileQueue
     */
    private final MmapFileList dLedgerFileList;

    //The id identifies the broker role, 0 means master, others means slave
    //节点 ID，0 表示主节点，非 0 表示从节点
    private final int id;

    /**
     * 消息序列器
     */
    private final MessageSerializer messageSerializer;
    /**
     * 用于记录 消息追加的时耗(日志追加所持有锁时间)
     */
    private volatile long beginTimeInDledgerLock = 0;

    //This offset separate the old commitlog from dledger commitlog
    /**
     * 记录的旧 commitlog 文件中的最大偏移量，如果访问的偏移量大于它，则访问 dledger 管理的文件
     */
    private long dividedCommitlogOffset = -1;

    /**
     * 是否正在恢复旧的 commitlog 文件
     */
    private boolean isInrecoveringOldCommitlog = false;

    private final StringBuilder msgIdBuilder = new StringBuilder();

    public DLedgerCommitLog(final DefaultMessageStore defaultMessageStore) {
        // 调用父类 即 CommitLog 的构造函数，加载 ${ROCKETMQ_HOME}/store/commitlog 下的 commitlog 文件，以便兼容升级 DLedger 的消息
        super(defaultMessageStore);
        dLedgerConfig = new DLedgerConfig();
        dLedgerConfig.setEnableDiskForceClean(defaultMessageStore.getMessageStoreConfig().isCleanFileForciblyEnable());
        dLedgerConfig.setStoreType(DLedgerConfig.FILE);
        dLedgerConfig.setSelfId(defaultMessageStore.getMessageStoreConfig().getdLegerSelfId());
        dLedgerConfig.setGroup(defaultMessageStore.getMessageStoreConfig().getdLegerGroup());
        dLedgerConfig.setPeers(defaultMessageStore.getMessageStoreConfig().getdLegerPeers());
        dLedgerConfig.setStoreBaseDir(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        dLedgerConfig.setMappedFileSizeForEntryData(defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog());
        dLedgerConfig.setDeleteWhen(defaultMessageStore.getMessageStoreConfig().getDeleteWhen());
        dLedgerConfig.setFileReservedHours(defaultMessageStore.getMessageStoreConfig().getFileReservedTime() + 1);
        dLedgerConfig.setPreferredLeaderId(defaultMessageStore.getMessageStoreConfig().getPreferredLeaderId());
        dLedgerConfig.setEnableBatchPush(defaultMessageStore.getMessageStoreConfig().isEnableBatchPush());

        id = Integer.parseInt(dLedgerConfig.getSelfId().substring(1)) + 1;
        dLedgerServer = new DLedgerServer(dLedgerConfig);
        dLedgerFileStore = (DLedgerMmapFileStore) dLedgerServer.getdLedgerStore();
        DLedgerMmapFileStore.AppendHook appendHook = (entry, buffer, bodyOffset) -> {
            assert bodyOffset == DLedgerEntry.BODY_OFFSET;
            buffer.position(buffer.position() + bodyOffset + MessageDecoder.PHY_POS_POSITION);
            buffer.putLong(entry.getPos() + bodyOffset);
        };
        dLedgerFileStore.addAppendHook(appendHook);
        dLedgerFileList = dLedgerFileStore.getDataFileList();
        this.messageSerializer = new MessageSerializer(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());

    }

    @Override
    public boolean load() {
        return super.load();
    }

    private void refreshConfig() {
        dLedgerConfig.setEnableDiskForceClean(defaultMessageStore.getMessageStoreConfig().isCleanFileForciblyEnable());
        dLedgerConfig.setDeleteWhen(defaultMessageStore.getMessageStoreConfig().getDeleteWhen());
        dLedgerConfig.setFileReservedHours(defaultMessageStore.getMessageStoreConfig().getFileReservedTime() + 1);
    }

    private void disableDeleteDledger() {
        dLedgerConfig.setEnableDiskForceClean(false);
        dLedgerConfig.setFileReservedHours(24 * 365 * 10);
    }

    @Override
    public void start() {
        dLedgerServer.startup();
    }

    @Override
    public void shutdown() {
        dLedgerServer.shutdown();
    }

    @Override
    public long flush() {
        dLedgerFileStore.flush();
        return dLedgerFileList.getFlushedWhere();
    }

    @Override
    public long getMaxOffset() {
        if (dLedgerFileStore.getCommittedPos() > 0) {
            return dLedgerFileStore.getCommittedPos();
        }
        if (dLedgerFileList.getMinOffset() > 0) {
            return dLedgerFileList.getMinOffset();
        }
        return 0;
    }

    @Override
    public long getMinOffset() {
        if (!mappedFileQueue.getMappedFiles().isEmpty()) {
            return mappedFileQueue.getMinOffset();
        }
        return dLedgerFileList.getMinOffset();
    }

    @Override
    public long getConfirmOffset() {
        return this.getMaxOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        log.warn("Should not set confirm offset {} for dleger commitlog", phyOffset);
    }

    @Override
    public long remainHowManyDataToCommit() {
        return dLedgerFileList.remainHowManyDataToCommit();
    }

    @Override
    public long remainHowManyDataToFlush() {
        return dLedgerFileList.remainHowManyDataToFlush();
    }

    @Override
    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately
    ) {
        if (mappedFileQueue.getMappedFiles().isEmpty()) {
            refreshConfig();
            //To prevent too much log in defaultMessageStore
            return Integer.MAX_VALUE;
        } else {
            disableDeleteDledger();
        }
        int count = super.deleteExpiredFile(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
        if (count > 0 || mappedFileQueue.getMappedFiles().size() != 1) {
            return count;
        }
        //the old logic will keep the last file, here to delete it
        MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
        log.info("Try to delete the last old commitlog file {}", mappedFile.getFileName());
        long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
        if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
            while (!mappedFile.destroy(10 * 1000)) {
                DLedgerUtils.sleep(1000);
            }
            mappedFileQueue.getMappedFiles().remove(mappedFile);
        }
        return 1;
    }

    public SelectMappedBufferResult convertSbr(SelectMmapBufferResult sbr) {
        if (sbr == null) {
            return null;
        } else {
            return new DLedgerSelectMappedBufferResult(sbr);
        }

    }

    public SelectMmapBufferResult truncate(SelectMmapBufferResult sbr) {
        long committedPos = dLedgerFileStore.getCommittedPos();
        if (sbr == null || sbr.getStartOffset() == committedPos) {
            return null;
        }
        if (sbr.getStartOffset() + sbr.getSize() <= committedPos) {
            return sbr;
        } else {
            sbr.setSize((int) (committedPos - sbr.getStartOffset()));
            return sbr;
        }
    }

    @Override
    public SelectMappedBufferResult getData(final long offset) {
        if (offset < dividedCommitlogOffset) {
            return super.getData(offset);
        }
        return this.getData(offset, offset == 0);
    }

    @Override
    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        if (offset < dividedCommitlogOffset) {
            return super.getData(offset, returnFirstOnNotFound);
        }
        if (offset >= dLedgerFileStore.getCommittedPos()) {
            return null;
        }
        int mappedFileSize = this.dLedgerServer.getdLedgerConfig().getMappedFileSizeForEntryData();
        MmapFile mappedFile = this.dLedgerFileList.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            SelectMmapBufferResult sbr = mappedFile.selectMappedBuffer(pos);
            return convertSbr(truncate(sbr));
        }

        return null;
    }

    private void recover(long maxPhyOffsetOfConsumeQueue) {
        // 加载 DLedger相关的存储文件，并一一构建对应的 MmapFile，其初始化三个重要的指针 wrotePosition、flushedPosition、committedPosition 三个指针为文件的大小
        dLedgerFileStore.load();
        if (dLedgerFileList.getMappedFiles().size() > 0) {
            // 如果已存在 DLedger 的数据文件，则只需要恢复 DLedger 相关数据文件，因为在加载旧的 commitlog 文件时已经将其重要的数据指针设置为最大值
            dLedgerFileStore.recover();
            dividedCommitlogOffset = dLedgerFileList.getFirstMappedFile().getFileFromOffset();
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                // 如果启用了 DLedger 并且是初次启动(还未生成 DLedger 相关的日志文件)，则需要恢复 旧的 commitlog 文件
                disableDeleteDledger();
            }
            long maxPhyOffset = dLedgerFileList.getMaxWrotePosition();
            // Clear ConsumeQueue redundant data
            if (maxPhyOffsetOfConsumeQueue >= maxPhyOffset) {
                log.warn("[TruncateCQ]maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, maxPhyOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(maxPhyOffset);
            }
            return;
        }
        //Indicate that, it is the first time to load mixed commitlog, need to recover the old commitlog
        isInrecoveringOldCommitlog = true;
        //No need the abnormal recover
        super.recoverNormally(maxPhyOffsetOfConsumeQueue);
        isInrecoveringOldCommitlog = false;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile == null) {
            return;
        }
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        byteBuffer.position(mappedFile.getWrotePosition());
        boolean needWriteMagicCode = true;
        // 1 TOTAL SIZE
        byteBuffer.getInt(); //size
        int magicCode = byteBuffer.getInt();
        if (magicCode == CommitLog.BLANK_MAGIC_CODE) {
            needWriteMagicCode = false;
        } else {
            log.info("Recover old commitlog found a illegal magic code={}", magicCode);
        }
        dLedgerConfig.setEnableDiskForceClean(false);
        dividedCommitlogOffset = mappedFile.getFileFromOffset() + mappedFile.getFileSize();
        log.info("Recover old commitlog needWriteMagicCode={} pos={} file={} dividedCommitlogOffset={}", needWriteMagicCode, mappedFile.getFileFromOffset() + mappedFile.getWrotePosition(), mappedFile.getFileName(), dividedCommitlogOffset);
        if (needWriteMagicCode) {
            // 如果存在旧的 commitlog 文件，需要将最后的文件剩余部分全部填充，即不再接受新的数据写入，新的数据全部写入到 DLedger 的数据文件中, 其中的关键点是：
            //1. 尝试查找最后一个 commitlog 文件，如果未找到，则结束。
            //2. 从最后一个文件的最后写入点(原 commitlog 文件的 待写入位点)尝试去查找写入的魔数，如果存在魔数并等于 CommitLog.BLANK_MAGIC_CODE，则无需再写入魔数，在升级 DLedger 第一次启动时，魔数为空，故需要写入魔数。
            //3. 初始化 dividedCommitlogOffset ，等于最后一个文件的起始偏移量加上文件的大小，即该指针指向最后一个文件的结束位置。
            //4. 将最后一个 commitlog 未写满的数据全部写入，其方法为 设置消息体的 size 与魔数即可。
            //5. 设置最后一个文件的 wrotePosition、flushedPosition、committedPosition为文件的大小，同样有意味者最后一个文件已经写满，下一条消息将写入 DLedger 中
            byteBuffer.position(mappedFile.getWrotePosition());
            byteBuffer.putInt(mappedFile.getFileSize() - mappedFile.getWrotePosition());
            byteBuffer.putInt(BLANK_MAGIC_CODE);
            mappedFile.flush(0);
        }
        mappedFile.setWrotePosition(mappedFile.getFileSize());
        mappedFile.setCommittedPosition(mappedFile.getFileSize());
        mappedFile.setFlushedPosition(mappedFile.getFileSize());
        dLedgerFileList.getLastMappedFile(dividedCommitlogOffset);
        log.info("Will set the initial commitlog offset={} for dledger", dividedCommitlogOffset);
    }

    @Override
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
        recover(maxPhyOffsetOfConsumeQueue);
    }

    @Override
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {
        recover(maxPhyOffsetOfConsumeQueue);
    }

    @Override
    public DispatchRequest checkMessageAndReturnSize(ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    @Override
    public DispatchRequest checkMessageAndReturnSize(ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean readBody) {
        if (isInrecoveringOldCommitlog) {
            return super.checkMessageAndReturnSize(byteBuffer, checkCRC, readBody);
        }
        try {
            int bodyOffset = DLedgerEntry.BODY_OFFSET;
            int pos = byteBuffer.position();
            int magic = byteBuffer.getInt();
            //In dledger, this field is size, it must be gt 0, so it could prevent collision
            int magicOld = byteBuffer.getInt();
            if (magicOld == CommitLog.BLANK_MAGIC_CODE || magicOld == CommitLog.MESSAGE_MAGIC_CODE) {
                byteBuffer.position(pos);
                return super.checkMessageAndReturnSize(byteBuffer, checkCRC, readBody);
            }
            if (magic == MmapFileList.BLANK_MAGIC_CODE) {
                return new DispatchRequest(0, true);
            }
            byteBuffer.position(pos + bodyOffset);
            DispatchRequest dispatchRequest = super.checkMessageAndReturnSize(byteBuffer, checkCRC, readBody);
            if (dispatchRequest.isSuccess()) {
                dispatchRequest.setBufferSize(dispatchRequest.getMsgSize() + bodyOffset);
            } else if (dispatchRequest.getMsgSize() > 0) {
                dispatchRequest.setBufferSize(dispatchRequest.getMsgSize() + bodyOffset);
            }
            return dispatchRequest;
        } catch (Throwable ignored) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    @Override
    public boolean resetOffset(long offset) {
        //currently, it seems resetOffset has no use
        return false;
    }

    @Override
    public long getBeginTimeInLock() {
        return beginTimeInDledgerLock;
    }

    private void setMessageInfo(MessageExtBrokerInner msg, int tranType) {
        // Set the storage time
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));

        //should be consistent with the old version
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
            || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            if (msg.getDelayTimeLevel() > 0) {
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }


                String topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
                int queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // Backup real topic, queueId
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }
    }

    /**
     * 关键点一：消息追加时，则不再写入到原先的 commitlog 文件中，而是调用 DLedgerServer 的 handleAppend 进行消息追加
     * 该方法会由集群内的 Leader 节点负责消息追加以及在消息复制，只有超过集群内的半数节点成功写入消息后，才会返回写入成功
     * 如果追加成功，将会返回本次追加成功后的起始偏移量，即 pos 属性，即类似于 rocketmq 中 commitlog 的偏移量，即物理偏移量
     *
     * 关键点二：根据 DLedger 的起始偏移量计算真正的消息的物理偏移量，从开头部分得知，DLedger 自身有其存储协议，
     * 其 body 字段存储真实的消息，即 commitlog 条目的存储结构，返回给客户端的消息偏移量为 body字段的开始偏移量，
     * 即通过 putMessage 返回的物理偏移量与不使用 Dledger 方式返回的物理偏移量的含义是一样的，即从开偏移量开始，可以正确读取消息
     * 这样 DLedger 完美的兼容了 RocketMQ Commitlog
     */
    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());

        setMessageInfo(msg, tranType);

        final String finalTopic = msg.getTopic();

        // Back to Results
        AppendMessageResult appendResult;
        AppendFuture<AppendEntryResponse> dledgerFuture;
        EncodeResult encodeResult;

        encodeResult = this.messageSerializer.serialize(msg);
        if (encodeResult.status != AppendMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, new AppendMessageResult(encodeResult.status)));
        }
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        long elapsedTimeInLock;
        long queueOffset;
        try {
            beginTimeInDledgerLock = this.defaultMessageStore.getSystemClock().now();
            queueOffset = getQueueOffsetByKey(encodeResult.queueOffsetKey, tranType);
            encodeResult.setQueueOffsetKey(queueOffset, false);
            AppendEntryRequest request = new AppendEntryRequest();
            request.setGroup(dLedgerConfig.getGroup());
            request.setRemoteId(dLedgerServer.getMemberState().getSelfId());
            request.setBody(encodeResult.getData());
            dledgerFuture = (AppendFuture<AppendEntryResponse>) dLedgerServer.handleAppend(request);
            if (dledgerFuture.getPos() == -1) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
            }
            long wroteOffset = dledgerFuture.getPos() + DLedgerEntry.BODY_OFFSET;

            int msgIdLength = (msg.getSysFlag() & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
            ByteBuffer buffer = ByteBuffer.allocate(msgIdLength);

            String msgId = MessageDecoder.createMessageId(buffer, msg.getStoreHostBytes(), wroteOffset);
            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginTimeInDledgerLock;
            appendResult = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, encodeResult.getData().length, msgId, System.currentTimeMillis(), queueOffset, elapsedTimeInLock);
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    DLedgerCommitLog.this.topicQueueTable.put(encodeResult.queueOffsetKey, queueOffset + 1);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("Put message error", e);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        } finally {
            beginTimeInDledgerLock = 0;
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, appendResult);
        }

        return dledgerFuture.thenApply(appendEntryResponse -> {
            PutMessageStatus putMessageStatus = PutMessageStatus.UNKNOWN_ERROR;
            switch (DLedgerResponseCode.valueOf(appendEntryResponse.getCode())) {
                case SUCCESS:
                    putMessageStatus = PutMessageStatus.PUT_OK;
                    break;
                case INCONSISTENT_LEADER:
                case NOT_LEADER:
                case LEADER_NOT_READY:
                case DISK_FULL:
                    putMessageStatus = PutMessageStatus.SERVICE_NOT_AVAILABLE;
                    break;
                case WAIT_QUORUM_ACK_TIMEOUT:
                    //Do not return flush_slave_timeout to the client, for the ons client will ignore it.
                    putMessageStatus = PutMessageStatus.OS_PAGECACHE_BUSY;
                    break;
                case LEADER_PENDING_FULL:
                    putMessageStatus = PutMessageStatus.OS_PAGECACHE_BUSY;
                    break;
            }
            PutMessageResult putMessageResult = new PutMessageResult(putMessageStatus, appendResult);
            if (putMessageStatus == PutMessageStatus.PUT_OK) {
                // Statistics
                storeStatsService.getSinglePutMessageTopicTimesTotal(finalTopic).add(1);
                storeStatsService.getSinglePutMessageTopicSizeTotal(msg.getTopic()).add(appendResult.getWroteBytes());
            }
            return putMessageResult;
        });
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        // Set the storage time
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        // Back to Results
        AppendMessageResult appendResult;
        BatchAppendFuture<AppendEntryResponse> dledgerFuture;
        EncodeResult encodeResult;

        encodeResult = this.messageSerializer.serialize(messageExtBatch);
        if (encodeResult.status != AppendMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, new AppendMessageResult(encodeResult
                    .status)));
        }

        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        msgIdBuilder.setLength(0);
        long elapsedTimeInLock;
        long queueOffset;
        int msgNum = 0;
        try {
            beginTimeInDledgerLock = this.defaultMessageStore.getSystemClock().now();
            queueOffset = getQueueOffsetByKey(encodeResult.queueOffsetKey, tranType);
            encodeResult.setQueueOffsetKey(queueOffset, true);
            BatchAppendEntryRequest request = new BatchAppendEntryRequest();
            request.setGroup(dLedgerConfig.getGroup());
            request.setRemoteId(dLedgerServer.getMemberState().getSelfId());
            request.setBatchMsgs(encodeResult.batchData);
            AppendFuture<AppendEntryResponse> appendFuture = (AppendFuture<AppendEntryResponse>) dLedgerServer.handleAppend(request);
            if (appendFuture.getPos() == -1) {
                log.warn("HandleAppend return false due to error code {}", appendFuture.get().getCode());
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
            }
            dledgerFuture = (BatchAppendFuture<AppendEntryResponse>) appendFuture;

            long wroteOffset = 0;

            int msgIdLength = (messageExtBatch.getSysFlag() & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
            ByteBuffer buffer = ByteBuffer.allocate(msgIdLength);

            boolean isFirstOffset = true;
            long firstWroteOffset = 0;
            for (long pos : dledgerFuture.getPositions()) {
                wroteOffset = pos + DLedgerEntry.BODY_OFFSET;
                if (isFirstOffset) {
                    firstWroteOffset = wroteOffset;
                    isFirstOffset = false;
                }
                String msgId = MessageDecoder.createMessageId(buffer, messageExtBatch.getStoreHostBytes(), wroteOffset);
                if (msgIdBuilder.length() > 0) {
                    msgIdBuilder.append(',').append(msgId);
                } else {
                    msgIdBuilder.append(msgId);
                }
                msgNum++;
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginTimeInDledgerLock;
            appendResult = new AppendMessageResult(AppendMessageStatus.PUT_OK, firstWroteOffset, encodeResult.totalMsgLen,
                    msgIdBuilder.toString(), System.currentTimeMillis(), queueOffset, elapsedTimeInLock);
            appendResult.setMsgNum(msgNum);
            DLedgerCommitLog.this.topicQueueTable.put(encodeResult.queueOffsetKey, queueOffset + msgNum);
        } catch (Exception e) {
            log.error("Put message error", e);
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        } finally {
            beginTimeInDledgerLock = 0;
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}",
                    elapsedTimeInLock, messageExtBatch.getBody().length, appendResult);
        }

        return dledgerFuture.thenApply(appendEntryResponse -> {
            PutMessageStatus putMessageStatus = PutMessageStatus.UNKNOWN_ERROR;
            switch (DLedgerResponseCode.valueOf(appendEntryResponse.getCode())) {
                case SUCCESS:
                    putMessageStatus = PutMessageStatus.PUT_OK;
                    break;
                case INCONSISTENT_LEADER:
                case NOT_LEADER:
                case LEADER_NOT_READY:
                case DISK_FULL:
                    putMessageStatus = PutMessageStatus.SERVICE_NOT_AVAILABLE;
                    break;
                case WAIT_QUORUM_ACK_TIMEOUT:
                    //Do not return flush_slave_timeout to the client, for the ons client will ignore it.
                    putMessageStatus = PutMessageStatus.OS_PAGECACHE_BUSY;
                    break;
                case LEADER_PENDING_FULL:
                    putMessageStatus = PutMessageStatus.OS_PAGECACHE_BUSY;
                    break;
            }
            PutMessageResult putMessageResult = new PutMessageResult(putMessageStatus, appendResult);
            if (putMessageStatus == PutMessageStatus.PUT_OK) {
                // Statistics
                storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).add(appendResult.getMsgNum());
                storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).add(appendResult.getWroteBytes());
            }
            return putMessageResult;
        });
    }

    /**
     * 消息查找比较简单，因为返回给客户端消息，转发给 consumequeue 的消息物理偏移量并不是DLedger 条目的偏移量，而是真实消息的起始偏移量
     * 其实现关键点如下：
     * 1. 如果查找的物理偏移量小于 dividedCommitlogOffset，则从原先的 commitlog 文件中查找
     * 2. 然后根据物理偏移量按照二分方找到具体的物理文件
     * 3. 对物理偏移量取模，得出在该物理文件中中的绝对偏移量，进行消息查找即可，因为只有知道其物理偏移量，从该处先将消息的长度读取出来，然后即可读出一条完整的消息
     */
    @Override
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        if (offset < dividedCommitlogOffset) {
            return super.getMessage(offset, size);
        }
        int mappedFileSize = this.dLedgerServer.getdLedgerConfig().getMappedFileSizeForEntryData();
        MmapFile mappedFile = this.dLedgerFileList.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return convertSbr(mappedFile.selectMappedBuffer(pos, size));
        }
        return null;
    }

    @Override
    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    @Override
    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    @Override
    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    @Override
    public void destroy() {
        super.destroy();
        dLedgerFileList.destroy();
    }

    @Override
    public boolean appendData(long startOffset, byte[] data, int dataStart, int dataLength) {
        //the old ha service will invoke method, here to prevent it
        return false;
    }

    @Override
    public void checkSelf() {
        dLedgerFileList.checkSelf();
    }

    @Override
    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInDledgerLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    private long getQueueOffsetByKey(String key, int tranType) {
        Long queueOffset = DLedgerCommitLog.this.topicQueueTable.get(key);
        if (null == queueOffset) {
            queueOffset = 0L;
            DLedgerCommitLog.this.topicQueueTable.put(key, queueOffset);
        }

        // Transaction messages that require special handling
        switch (tranType) {
            // Prepared and Rollback message is not consumed, will not enter the
            // consumer queuec
            case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                queueOffset = 0L;
                break;
            case MessageSysFlag.TRANSACTION_NOT_TYPE:
            case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
            default:
                break;
        }
        return queueOffset;
    }


    class EncodeResult {
        private String queueOffsetKey;
        private ByteBuffer data;
        private List<byte[]> batchData;
        private AppendMessageStatus status;
        private int totalMsgLen;

        public EncodeResult(AppendMessageStatus status, ByteBuffer data, String queueOffsetKey) {
            this.data = data;
            this.status = status;
            this.queueOffsetKey = queueOffsetKey;
        }

        public void setQueueOffsetKey(long offset, boolean isBatch) {
            if (!isBatch) {
                this.data.putLong(MessageDecoder.QUEUE_OFFSET_POSITION, offset);
                return;
            }

            for (byte[] data : batchData) {
                ByteBuffer.wrap(data).putLong(MessageDecoder.QUEUE_OFFSET_POSITION, offset++);
            }
        }

        public byte[] getData() {
            return data.array();
        }

        public EncodeResult(AppendMessageStatus status, String queueOffsetKey, List<byte[]> batchData, int totalMsgLen) {
            this.batchData = batchData;
            this.status = status;
            this.queueOffsetKey = queueOffsetKey;
            this.totalMsgLen = totalMsgLen;
        }
    }

    class MessageSerializer {

        // The maximum length of the message
        private final int maxMessageSize;

        MessageSerializer(final int size) {
            this.maxMessageSize = size;
        }

        public EncodeResult serialize(final MessageExtBrokerInner msgInner) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            long wroteOffset = 0;

            long queueOffset = 0;

            int sysflag = msgInner.getSysFlag();

            int bornHostLength = (sysflag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            String key = msgInner.getTopic() + "-" + msgInner.getQueueId();

            /**
             * Serialize message
             */
            final byte[] propertiesData =
                msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new EncodeResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED, null, key);
            }

            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;

            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            final int msgLen = calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

            ByteBuffer msgStoreItemMemory = ByteBuffer.allocate(msgLen);

            // Exceeds the maximum message
            if (msgLen > this.maxMessageSize) {
                DLedgerCommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageSize: " + this.maxMessageSize);
                return new EncodeResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED, null, key);
            }
            // Initialization of storage space
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 TOTALSIZE
            msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            msgStoreItemMemory.putInt(DLedgerCommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC
            msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG
            msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET
            msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET
            msgStoreItemMemory.putLong(wroteOffset);
            // 8 SYSFLAG
            msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST
            resetByteBuffer(bornHostHolder, bornHostLength);
            msgStoreItemMemory.put(msgInner.getBornHostBytes(bornHostHolder));
            // 11 STORETIMESTAMP
            msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            resetByteBuffer(storeHostHolder, storeHostLength);
            msgStoreItemMemory.put(msgInner.getStoreHostBytes(storeHostHolder));
            //this.msgBatchMemory.put(msgInner.getStoreHostBytes());
            // 13 RECONSUMETIMES
            msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0) {
                msgStoreItemMemory.put(msgInner.getBody());
            }
            // 16 TOPIC
            msgStoreItemMemory.put((byte) topicLength);
            msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES
            msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0) {
                msgStoreItemMemory.put(propertiesData);
            }
            return new EncodeResult(AppendMessageStatus.PUT_OK, msgStoreItemMemory, key);
        }

        public EncodeResult serialize(final MessageExtBatch messageExtBatch) {
            String key = messageExtBatch.getTopic() + "-" + messageExtBatch.getQueueId();

            int totalMsgLen = 0;
            ByteBuffer messagesByteBuff = messageExtBatch.wrap();
            List<byte[]> batchBody = new LinkedList<>();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

                final int topicLength = topicData.length;

                final int msgLen = calMsgLength(messageExtBatch.getSysFlag(), bodyLen, topicLength, propertiesLen);
                ByteBuffer msgStoreItemMemory = ByteBuffer.allocate(msgLen);

                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " +
                            bodyLen
                            + ", maxMessageSize: " + this.maxMessageSize);
                    throw new RuntimeException("message size exceeded");
                }

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if (totalMsgLen > maxMessageSize) {
                    throw new RuntimeException("message size exceeded");
                }

                // Initialization of storage space
                this.resetByteBuffer(msgStoreItemMemory, msgLen);
                // 1 TOTALSIZE
                msgStoreItemMemory.putInt(msgLen);
                // 2 MAGICCODE
                msgStoreItemMemory.putInt(DLedgerCommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                msgStoreItemMemory.putInt(bodyCrc);
                // 4 QUEUEID
                msgStoreItemMemory.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                msgStoreItemMemory.putInt(flag);
                // 6 QUEUEOFFSET
                msgStoreItemMemory.putLong(0L);
                // 7 PHYSICALOFFSET
                msgStoreItemMemory.putLong(0);
                // 8 SYSFLAG
                msgStoreItemMemory.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                msgStoreItemMemory.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                resetByteBuffer(bornHostHolder, bornHostLength);
                msgStoreItemMemory.put(messageExtBatch.getBornHostBytes(bornHostHolder));
                // 11 STORETIMESTAMP
                msgStoreItemMemory.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                resetByteBuffer(storeHostHolder, storeHostLength);
                msgStoreItemMemory.put(messageExtBatch.getStoreHostBytes(storeHostHolder));
                // 13 RECONSUMETIMES
                msgStoreItemMemory.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset
                msgStoreItemMemory.putLong(0);
                // 15 BODY
                msgStoreItemMemory.putInt(bodyLen);
                if (bodyLen > 0) {
                    msgStoreItemMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
                }
                // 16 TOPIC
                msgStoreItemMemory.put((byte) topicLength);
                msgStoreItemMemory.put(topicData);
                // 17 PROPERTIES
                msgStoreItemMemory.putShort(propertiesLen);
                if (propertiesLen > 0) {
                    msgStoreItemMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
                }
                byte[] data = new byte[msgLen];
                msgStoreItemMemory.clear();
                msgStoreItemMemory.get(data);
                batchBody.add(data);
            }

            return new EncodeResult(AppendMessageStatus.PUT_OK, key, batchBody, totalMsgLen);
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }
    }

    public static class DLedgerSelectMappedBufferResult extends SelectMappedBufferResult {

        private SelectMmapBufferResult sbr;

        public DLedgerSelectMappedBufferResult(SelectMmapBufferResult sbr) {
            super(sbr.getStartOffset(), sbr.getByteBuffer(), sbr.getSize(), null);
            this.sbr = sbr;
        }

        @Override
        public synchronized void release() {
            super.release();
            if (sbr != null) {
                sbr.release();
            }
        }

    }

    public DLedgerServer getdLedgerServer() {
        return dLedgerServer;
    }

    public int getId() {
        return id;
    }

    public long getDividedCommitlogOffset() {
        return dividedCommitlogOffset;
    }
}
