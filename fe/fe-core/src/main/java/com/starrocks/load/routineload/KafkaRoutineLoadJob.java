// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/KafkaRoutineLoadJob.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.routineload;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.RoutineLoadDataSourceProperties;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.KafkaUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.SmallFileMgr.SmallFile;
import com.starrocks.load.Load;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * KafkaRoutineLoadJob is a kind of RoutineLoadJob which fetch data from kafka.
 * The progress which is super class property is seems like "{"partition1": offset1, "partition2": offset2}"
 */
public class KafkaRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJob.class);

    public static final String KAFKA_FILE_CATALOG = "kafka";

    private static final String PROPERTY_KAFKA_GROUP_ID = "group.id";

    @SerializedName("bkl")
    private String brokerList;
    @SerializedName("tpc")
    private String topic;

    // optional, user want to load partitions.
    @SerializedName("ckp")
    private List<Integer> customKafkaPartitions = Lists.newArrayList();
    // current kafka partitions is the actually partition which will be fetched
    @SerializedName("ctkp")
    private List<Integer> currentKafkaPartitions = Lists.newArrayList();
    // optional, user want to set default offset when new partition add or offset not set.
    private Long kafkaDefaultOffSet = null;
    // kafka properties, property prefix will be mapped to kafka custom parameters, which can be extended in the future
    @SerializedName("cpr")
    private Map<String, String> customProperties = Maps.newHashMap();
    private Map<String, String> convertedCustomProperties = Maps.newHashMap();
    private String confluentSchemaRegistryUrl = null;
    @SerializedName("ckpo")
    private List<Pair<Integer, Long>> customeKafkaPartitionOffsets = null;
    boolean useDefaultGroupId = true;

    private Map<Integer, Long> latestPartitionOffsets = Maps.newHashMap();

    public KafkaRoutineLoadJob() {
        // for serialization, id is dummy
        super(-1, LoadDataSourceType.KAFKA);
        this.progress = new KafkaProgress();
        this.timestampProgress = new KafkaProgress();
    }

    public KafkaRoutineLoadJob(Long id, String name,
                               long dbId, long tableId, String brokerList, String topic) {
        super(id, name, dbId, tableId, LoadDataSourceType.KAFKA);
        this.brokerList = brokerList;
        this.topic = topic;
        this.progress = new KafkaProgress();
        this.timestampProgress = new KafkaProgress();
    }

    public String getConfluentSchemaRegistryUrl() {
        return confluentSchemaRegistryUrl;
    }

    public void setConfluentSchemaRegistryUrl(String confluentSchemaRegistryUrl) {
        this.confluentSchemaRegistryUrl = confluentSchemaRegistryUrl;
    }

    public String getTopic() {
        return topic;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public Map<String, String> getConvertedCustomProperties() {
        return convertedCustomProperties;
    }

    @Override
    protected String getSourceProgressString() {
        // To be compatible with progress format, we convert the Map<Integer, Long> to Map<String, String>
        Map<String, String> partitionOffsets = Maps.newHashMap();
        for (Map.Entry<Integer, Long> entry : latestPartitionOffsets.entrySet()) {
            partitionOffsets.put(entry.getKey().toString(), entry.getValue().toString());
        }

        Gson gson = new Gson();
        return gson.toJson(partitionOffsets);
    }

    public void setPartitionOffset(int partition, long offset) {
        latestPartitionOffsets.put(Integer.valueOf(partition), Long.valueOf(offset));
    }

    public Long getPartitionOffset(int partition) {
        return latestPartitionOffsets.get(Integer.valueOf(partition));
    }

    @Override
    public void prepare() throws UserException {
        super.prepare();
        checkCustomPartition(customKafkaPartitions);
        // should reset converted properties each time the job being prepared.
        // because the file info can be changed anytime.
        convertCustomProperties(true);

        ((KafkaProgress) progress).convertOffset(brokerList, topic, convertedCustomProperties);
    }

    public synchronized void convertCustomProperties(boolean rebuild) throws DdlException {
        if (customProperties.isEmpty()) {
            return;
        }

        if (!rebuild && !convertedCustomProperties.isEmpty()) {
            return;
        }

        if (rebuild) {
            convertedCustomProperties.clear();
        }

        SmallFileMgr smallFileMgr = GlobalStateMgr.getCurrentState().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                // convert FILE:file_name -> FILE:file_id:md5
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                SmallFile smallFile = smallFileMgr.getSmallFile(dbId, KAFKA_FILE_CATALOG, file, true);
                convertedCustomProperties.put(entry.getKey(), "FILE:" + smallFile.id + ":" + smallFile.md5);
            } else {
                convertedCustomProperties.put(entry.getKey(), entry.getValue());
            }
        }
        if (convertedCustomProperties.containsKey(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS)) {
            try {
                kafkaDefaultOffSet = CreateRoutineLoadStmt.getKafkaOffset(
                        convertedCustomProperties.remove(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS));
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }
    }

    @Override
    public void divideRoutineLoadJob(int currentConcurrentTaskNum) throws UserException {
        List<RoutineLoadTaskInfo> result = new ArrayList<>();
        writeLock();
        try {
            if (state == JobState.NEED_SCHEDULE) {
                // divide kafkaPartitions into tasks
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    Map<Integer, Long> taskKafkaProgress = Maps.newHashMap();
                    for (int j = 0; j < currentKafkaPartitions.size(); j++) {
                        if (j % currentConcurrentTaskNum == i) {
                            int kafkaPartition = currentKafkaPartitions.get(j);
                            taskKafkaProgress.put(kafkaPartition,
                                    ((KafkaProgress) progress).getOffsetByPartition(kafkaPartition));
                        }
                    }
                    long timeToExecuteMs = System.currentTimeMillis() + taskSchedIntervalS * 1000;
                    KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(UUID.randomUUID(), id,
                            taskSchedIntervalS * 1000,
                            timeToExecuteMs, taskKafkaProgress, taskTimeoutSecond * 1000);
                    routineLoadTaskInfoList.add(kafkaTaskInfo);
                    result.add(kafkaTaskInfo);
                }
                // change job state to running
                if (result.size() != 0) {
                    unprotectUpdateState(JobState.RUNNING, null, false);
                }
            } else {
                LOG.debug("Ignore to divide routine load job while job state {}", state);
            }
            // save task into queue of needScheduleTasks
            GlobalStateMgr.getCurrentState().getRoutineLoadTaskScheduler().addTasksInQueue(result);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public int calculateCurrentConcurrentTaskNum() throws MetaNotFoundException {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        // TODO: need to refactor after be split into cn + dn
        int aliveNodeNum = systemInfoService.getAliveBackendNumber();
        if (RunMode.isSharedDataMode()) {
            Warehouse warehouse = GlobalStateMgr.getCurrentWarehouseMgr().getDefaultWarehouse();
            aliveNodeNum = 0;
            for (long nodeId : warehouse.getAnyAvailableCluster().getComputeNodeIds()) {
                ComputeNode node = GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(nodeId);
                if (node != null && node.isAlive()) {
                    ++aliveNodeNum;
                }
            }
        }
        int partitionNum = currentKafkaPartitions.size();
        if (partitionNum == 0) {
            // In non-stop states (NEED_SCHEDULE/RUNNING), having `partitionNum` as 0 is equivalent 
            // to `currentKafkaPartitions` being uninitialized. When `currentKafkaPartitions` is 
            // uninitialized, it indicates that the job has just been created and hasn't been scheduled yet. 
            // At this point, the user-specified number of partitions is used.
            partitionNum = customKafkaPartitions.size();
            if (partitionNum == 0) {
                // If the user hasn't specified partition information, then we no longer take the `partition` 
                // variable into account when calculating concurrency.
                partitionNum = Integer.MAX_VALUE;
            }
        }

        if (desireTaskConcurrentNum == 0) {
            desireTaskConcurrentNum = Config.max_routine_load_task_concurrent_num;
        }

        LOG.debug("current concurrent task number is min"
                        + "(partition num: {}, desire task concurrent num: {}, alive be num: {}, config: {})",
                partitionNum, desireTaskConcurrentNum, aliveNodeNum, Config.max_routine_load_task_concurrent_num);
        currentTaskConcurrentNum = Math.min(Math.min(partitionNum, Math.min(desireTaskConcurrentNum, aliveNodeNum)),
                Config.max_routine_load_task_concurrent_num);
        return currentTaskConcurrentNum;
    }

    // Through the transaction status and attachment information, to determine whether the progress needs to be updated.
    @Override
    protected boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment,
                                      TransactionState txnState,
                                      TransactionState.TxnStatusChangeReason txnStatusChangeReason) {
        if (txnState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            // For committed txn, update the progress.
            return true;
        }

        // For compatible reason, the default behavior of empty load is still returning "all partitions have no load data" and abort transaction.
        // In this situation, we also need update commit info.
        if (txnStatusChangeReason != null &&
                txnStatusChangeReason == TransactionState.TxnStatusChangeReason.NO_PARTITIONS) {
            // Because the max_filter_ratio of routine load task is always 1.
            // Therefore, under normal circumstances, routine load task will not return the error "too many filtered rows".
            // If no data is imported, the error "all partitions have no load data" may only be returned.
            // In this case, the status of the transaction is ABORTED,
            // but we still need to update the offset to skip these error lines.
            Preconditions.checkState(txnState.getTransactionStatus() == TransactionStatus.ABORTED,
                    txnState.getTransactionStatus());
            return true;
        }

        // Running here, the status of the transaction should be ABORTED,
        // and it is caused by other errors. In this case, we should not update the offset.
        LOG.debug("no need to update the progress of kafka routine load. txn status: {}, " +
                        "txnStatusChangeReason: {}, task: {}, job: {}",
                txnState.getTransactionStatus(), txnStatusChangeReason,
                DebugUtil.printId(rlTaskTxnCommitAttachment.getTaskId()), id);
        return false;
    }

    @Override
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        super.updateProgress(attachment);
        this.progress.update(attachment.getProgress());
        this.timestampProgress.update(attachment.getTimestampProgress());
    }

    @Override
    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        super.replayUpdateProgress(attachment);
        this.progress.update(attachment.getProgress());
    }

    @Override
    protected RoutineLoadTaskInfo unprotectRenewTask(long timeToExecuteMs, RoutineLoadTaskInfo routineLoadTaskInfo) {
        KafkaTaskInfo oldKafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
        // add new task
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(timeToExecuteMs, oldKafkaTaskInfo,
                ((KafkaProgress) progress).getPartitionIdToOffset(oldKafkaTaskInfo.getPartitions()),
                ((KafkaTaskInfo) routineLoadTaskInfo).getLatestOffset());
        // remove old task
        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
        // add new task
        routineLoadTaskInfoList.add(kafkaTaskInfo);
        return kafkaTaskInfo;
    }

    @Override
    protected void unprotectUpdateProgress() {
        updateNewPartitionProgress();
    }

    // if customKafkaPartition is not null, then return false immediately
    // else if kafka partitions of topic has been changed, return true.
    // else return false
    // update current kafka partition at the same time
    // current kafka partitions = customKafkaPartitions == 0 ? all of partition of kafka topic : customKafkaPartitions
    @Override
    protected boolean unprotectNeedReschedule() throws UserException {
        // only running and need_schedule job need to be changed current kafka partitions
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE) {
            if (customKafkaPartitions != null && customKafkaPartitions.size() != 0) {
                currentKafkaPartitions = customKafkaPartitions;
                return false;
            } else {
                List<Integer> newCurrentKafkaPartition;
                try {
                    newCurrentKafkaPartition = getAllKafkaPartitions();
                } catch (Exception e) {
                    String msg = "Job failed to fetch all current partition with error [" + e.getMessage() + "]";
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                            .add("error_msg", msg)
                            .build(), e);
                    if (this.state == JobState.NEED_SCHEDULE) {
                        unprotectUpdateState(JobState.PAUSED,
                                new ErrorReason(InternalErrorCode.PARTITIONS_ERR, msg),
                                false /* not replay */);
                    }
                    return false;
                }
                if (currentKafkaPartitions.containsAll(newCurrentKafkaPartition)) {
                    if (currentKafkaPartitions.size() > newCurrentKafkaPartition.size()) {
                        currentKafkaPartitions = newCurrentKafkaPartition;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                    .add("current_kafka_partitions", Joiner.on(",").join(currentKafkaPartitions))
                                    .add("msg", "current kafka partitions has been change")
                                    .build());
                        }
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    currentKafkaPartitions = newCurrentKafkaPartition;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                .add("current_kafka_partitions", Joiner.on(",").join(currentKafkaPartitions))
                                .add("msg", "current kafka partitions has been change")
                                .build());
                    }
                    return true;
                }
            }
        } else if (this.state == JobState.PAUSED) {
            boolean autoSchedule = ScheduleRule.isNeedAutoSchedule(this);
            if (autoSchedule) {
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, name)
                        .add("current_state", this.state)
                        .add("msg", "should be rescheduled")
                        .build());
            }
            return autoSchedule;
        } else {
            return false;
        }
    }
    
    @Override
    protected String getStatistic() {
        Map<String, Object> summary = Maps.newHashMap();
        summary.put("totalRows", Long.valueOf(totalRows));
        summary.put("loadedRows", Long.valueOf(totalRows - errorRows - unselectedRows));
        summary.put("errorRows", Long.valueOf(errorRows));
        summary.put("unselectedRows", Long.valueOf(unselectedRows));
        summary.put("receivedBytes", Long.valueOf(receivedBytes));
        summary.put("taskExecuteTimeMs", Long.valueOf(totalTaskExcutionTimeMs));
        summary.put("receivedBytesRate", Long.valueOf(receivedBytes / totalTaskExcutionTimeMs * 1000));
        summary.put("loadRowsRate",
                Long.valueOf((totalRows - errorRows - unselectedRows) / totalTaskExcutionTimeMs * 1000));
        summary.put("committedTaskNum", Long.valueOf(committedTaskNum));
        summary.put("abortedTaskNum", Long.valueOf(abortedTaskNum));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(summary);
    }

    private List<Integer> getAllKafkaPartitions() throws UserException {
        convertCustomProperties(false);
        return KafkaUtil.getAllKafkaPartitions(brokerList, topic, ImmutableMap.copyOf(convertedCustomProperties));
    }

    public static KafkaRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) throws UserException {
        // check db and table
        Database db = GlobalStateMgr.getCurrentState().getDb(stmt.getDBName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, stmt.getDBName());
        }

        long tableId = -1L;
        db.readLock();
        try {
            unprotectedCheckMeta(db, stmt.getTableName(), stmt.getRoutineLoadDesc());
            Table table = db.getTable(stmt.getTableName());
            Load.checkMergeCondition(stmt.getMergeConditionStr(), (OlapTable) table, table.getFullSchema(), false);
            tableId = table.getId();
        } finally {
            db.readUnlock();
        }

        // init kafka routine load job
        long id = GlobalStateMgr.getCurrentState().getNextId();
        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(id, stmt.getName(),
                db.getId(), tableId, stmt.getKafkaBrokerList(), stmt.getKafkaTopic());
        kafkaRoutineLoadJob.setOptional(stmt);
        kafkaRoutineLoadJob.checkCustomProperties();

        return kafkaRoutineLoadJob;
    }

    private void checkCustomPartition(List<Integer> customKafkaPartitions) throws UserException {
        if (customKafkaPartitions.isEmpty()) {
            return;
        }
        List<Integer> allKafkaPartitions = getAllKafkaPartitions();
        for (Integer customPartition : customKafkaPartitions) {
            if (!allKafkaPartitions.contains(customPartition)) {
                throw new LoadException("there is a custom kafka partition " + customPartition
                        + " which is invalid for topic " + topic);
            }
        }
    }

    private void checkCustomProperties() throws DdlException {
        SmallFileMgr smallFileMgr = GlobalStateMgr.getCurrentState().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                // check file
                if (!smallFileMgr.containsFile(dbId, KAFKA_FILE_CATALOG, file)) {
                    throw new DdlException("File " + file + " does not exist in db "
                            + dbId + " with globalStateMgr: " + KAFKA_FILE_CATALOG);
                }
            }
        }
    }

    private void updateNewPartitionProgress() {
        // update the progress of new partitions
        for (Integer kafkaPartition : currentKafkaPartitions) {
            if (!((KafkaProgress) progress).containsPartition(kafkaPartition)) {
                // if offset is not assigned, start from OFFSET_END
                long beginOffSet = kafkaDefaultOffSet == null ? KafkaProgress.OFFSET_END_VAL : kafkaDefaultOffSet;
                ((KafkaProgress) progress).addPartitionOffset(Pair.create(kafkaPartition, beginOffSet));
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                            .add("kafka_partition_id", kafkaPartition)
                            .add("begin_offset", beginOffSet)
                            .add("msg", "The new partition has been added in job"));
                }
            }
        }
    }

    @Override
    protected void setOptional(CreateRoutineLoadStmt stmt) throws UserException {
        super.setOptional(stmt);

        if (!stmt.getKafkaPartitionOffsets().isEmpty()) {
            setCustomKafkaPartitions(stmt.getKafkaPartitionOffsets());
        }
        if (!stmt.getCustomKafkaProperties().isEmpty()) {
            setCustomKafkaProperties(stmt.getCustomKafkaProperties());
        }

        if (stmt.getConfluentSchemaRegistryUrl() != null) {
            setConfluentSchemaRegistryUrl(stmt.getConfluentSchemaRegistryUrl());
        }

        setDefaultKafkaGroupID();
    }

    // this is a unprotected method which is called in the initialization function
    private void setCustomKafkaPartitions(List<Pair<Integer, Long>> kafkaPartitionOffsets) throws LoadException {
        if (kafkaPartitionOffsets != null) {
            customeKafkaPartitionOffsets = kafkaPartitionOffsets;
        }
        for (Pair<Integer, Long> partitionOffset : kafkaPartitionOffsets) {
            this.customKafkaPartitions.add(partitionOffset.first);
            ((KafkaProgress) progress).addPartitionOffset(partitionOffset);
        }
    }

    private void setCustomKafkaProperties(Map<String, String> kafkaProperties) {
        this.customProperties = kafkaProperties;
    }

    private void setDefaultKafkaGroupID() {
        if (this.customProperties.containsKey(PROPERTY_KAFKA_GROUP_ID)) {
            useDefaultGroupId = false;
            return;
        }
        this.customProperties.put(PROPERTY_KAFKA_GROUP_ID, name + "_" + UUID.randomUUID());
    }

    @Override
    public String dataSourcePropertiesToSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("(\n");
        sb.append("\"").append(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY).append("\"=\"");
        sb.append(brokerList).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY).append("\"=\"");
        sb.append(topic).append("\",\n");

        sb.append("\"").append(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY).append("\"=\"");
        if (customeKafkaPartitionOffsets == null) {
            List<Integer> sortedPartitions = Lists.newArrayList(currentKafkaPartitions);
            Collections.sort(sortedPartitions);
            sb.append(Joiner.on(",").join(sortedPartitions)).append("\",\n");
        } else {
            class PairComparator implements Comparator<Pair<Integer, Long>> {
                @Override
                public int compare(Pair<Integer, Long> p1, Pair<Integer, Long> p2) {
                    return Integer.compare(p1.first, p2.first);
                }
            }
            Collections.sort(customeKafkaPartitionOffsets, new PairComparator());
            List<Integer> customeKafkaPartitions = new ArrayList<>();
            List<Long> customeKakfaOffsets = new ArrayList<>();
            for (Pair<Integer, Long> partitionOffset : customeKafkaPartitionOffsets) {
                customeKafkaPartitions.add(partitionOffset.first);
                customeKakfaOffsets.add(partitionOffset.second);
            }
            sb.append(Joiner.on(",").join(customeKafkaPartitions)).append("\",\n");

            sb.append("\"").append(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY).append("\"=\"");
            for (int i = 0; i < customeKakfaOffsets.size(); i++) {
                if (customeKakfaOffsets.get(i) == KafkaProgress.OFFSET_BEGINNING_VAL) {
                    sb.append("OFFSET_BEGINNING");
                } else if (customeKakfaOffsets.get(i) == KafkaProgress.OFFSET_END_VAL) {
                    sb.append("OFFSET_END");
                } else {
                    sb.append(Long.toString(customeKakfaOffsets.get(i)));
                }
                if (i != customeKakfaOffsets.size() - 1) {
                    sb.append(",");
                }
            }
            sb.append("\",\n");
        }
        if (confluentSchemaRegistryUrl != null) {
            sb.append("\"").append(CreateRoutineLoadStmt.CONFLUENT_SCHEMA_REGISTRY_URL).append("\"=\"");
            sb.append(getPrintableConfluentSchemaRegistryUrl()).append("\",\n");
        }

        Map<String, String> maskedProperties = getMaskedCustomProperties();
        if (useDefaultGroupId) {
            maskedProperties.remove(PROPERTY_KAFKA_GROUP_ID);
        }
        Iterator<Map.Entry<String, String>> iterator = maskedProperties.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            String key = entry.getKey();
            String value = entry.getValue();
            sb.append("\"").append("property.").append(key).append("\"=\"");
            sb.append(value).append("\",\n");
        }
        if (sb.length() >= 2) {
            sb.delete(sb.length() - 2, sb.length());
            sb.append("\n");
        }
        sb.append(")");
        return sb.toString();
    }

    private String getPrintableConfluentSchemaRegistryUrl() {
        String confluentSchemaRegistryUrl = getConfluentSchemaRegistryUrl();
        if (confluentSchemaRegistryUrl != null) {
            // confluentSchemaRegistryUrl have three patterns:
            // 1. https or http://key:password@addr
            // 2. https://key:password@addr
            // 3. http://key:password@addr
            // https://IAP4CQUET7L243C7:QEiI9CwV1szDViwIaBXiow2zXicQ1MY5/PLrmaaJE/FolDCCFmf2KPUkvv+UJozo@psrc-0kywq.us-east-2.aws.confluent.cloud
            String[] fragments = confluentSchemaRegistryUrl.split("@");
            if (fragments.length != 2) {
                // case 1
                return confluentSchemaRegistryUrl;
            } else {
                if (fragments[0].length() < 5) {
                    return confluentSchemaRegistryUrl;
                } else {
                    // case 2
                    String addr;
                    if ("https".equals(fragments[0].substring(0, 5))) {
                        addr = "https://" + fragments[1];
                    } else {
                        addr = "http://" + fragments[1];
                    }
                    return addr;
                }
            }
        } else {
            return null;
        }
    }

    @VisibleForTesting
    @Override
    protected String dataSourcePropertiesJsonToString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("brokerList", brokerList);
        dataSourceProperties.put("topic", topic);
        String confluentSchemaRegistryUrl = getConfluentSchemaRegistryUrl();
        if (confluentSchemaRegistryUrl != null) {
            dataSourceProperties.put("confluent.schema.registry.url", getPrintableConfluentSchemaRegistryUrl());
        }
        List<Integer> sortedPartitions = Lists.newArrayList(currentKafkaPartitions);
        Collections.sort(sortedPartitions);
        dataSourceProperties.put("currentKafkaPartitions", Joiner.on(",").join(sortedPartitions));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dataSourceProperties);
    }

    protected Map<String, String> getMaskedCustomProperties() {
        Map<String, String> maskedProperties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getKey().contains("password") || entry.getKey().contains("secret")) {
                maskedProperties.put(entry.getKey(), "******");
            } else {
                maskedProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return maskedProperties;
    }

    @Override
    protected String customPropertiesJsonToString() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        Map<String, String> maskedProperties = getMaskedCustomProperties();
        return gson.toJson(maskedProperties);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, brokerList);
        Text.writeString(out, topic);

        out.writeInt(customKafkaPartitions.size());
        for (Integer partitionId : customKafkaPartitions) {
            out.writeInt(partitionId);
        }

        out.writeInt(customProperties.size());
        for (Map.Entry<String, String> property : customProperties.entrySet()) {
            Text.writeString(out, "property." + property.getKey());
            Text.writeString(out, property.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        brokerList = Text.readString(in);
        topic = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            customKafkaPartitions.add(in.readInt());
        }

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String propertyKey = Text.readString(in);
            String propertyValue = Text.readString(in);
            if (propertyKey.startsWith("property.")) {
                this.customProperties.put(propertyKey.substring(propertyKey.indexOf(".") + 1), propertyValue);
            }
        }
    }

    /**
     * add extra parameter check for changing kafka offset
     * 1. if customKafkaParition is specified, only the specific partitions can be modified
     * 2. otherwise, will check if partition is validated by actually reading kafka meta from kafka proxy
     */
    @Override
    public void modifyJob(RoutineLoadDesc routineLoadDesc, Map<String, String> jobProperties,
                          RoutineLoadDataSourceProperties dataSourceProperties, OriginStatement originStatement,
                          boolean isReplay) throws DdlException {
        if (!isReplay && dataSourceProperties != null && dataSourceProperties.hasAnalyzedProperties()) {
            List<Pair<Integer, Long>> kafkaPartitionOffsets = dataSourceProperties.getKafkaPartitionOffsets();
            if (customKafkaPartitions != null && customKafkaPartitions.size() != 0) {
                for (Pair<Integer, Long> pair : kafkaPartitionOffsets) {
                    if (! customKafkaPartitions.contains(pair.first)) {
                        throw new DdlException("The specified partition " + pair.first + " is not in the custom partitions");
                    }
                }
            } else {
                // check if partition is validate
                try {
                    checkCustomPartition(kafkaPartitionOffsets.stream().map(k -> k.first).collect(Collectors.toList()));
                } catch (UserException e) {
                    throw new DdlException("The specified partition is not in the consumed partitions ", e);
                }
            }
        }
        super.modifyJob(routineLoadDesc, jobProperties, dataSourceProperties, originStatement, isReplay);
    }

    @Override
    public void modifyDataSourceProperties(RoutineLoadDataSourceProperties dataSourceProperties) throws DdlException {
        List<Pair<Integer, Long>> kafkaPartitionOffsets = Lists.newArrayList();
        Map<String, String> customKafkaProperties = Maps.newHashMap();

        if (dataSourceProperties.hasAnalyzedProperties()) {
            kafkaPartitionOffsets = dataSourceProperties.getKafkaPartitionOffsets();
            customKafkaProperties = dataSourceProperties.getCustomKafkaProperties();
        }

        // modify partition offset first
        if (!kafkaPartitionOffsets.isEmpty()) {
            // we can only modify the partition that is being consumed
            ((KafkaProgress) progress).modifyOffset(kafkaPartitionOffsets);
        }

        if (!customKafkaProperties.isEmpty()) {
            this.customProperties.putAll(customKafkaProperties);
            convertCustomProperties(true);
        }

        if (dataSourceProperties.getConfluentSchemaRegistryUrl() != null) {
            confluentSchemaRegistryUrl = dataSourceProperties.getConfluentSchemaRegistryUrl();
        }

        LOG.info("modify the data source properties of kafka routine load job: {}, datasource properties: {}",
                this.id, dataSourceProperties);
    }

    // update substate according to the lag.
    @Override
    public void updateSubstate() throws UserException {
        KafkaProgress progress = (KafkaProgress) getTimestampProgress();
        Map<Integer, Long> partitionTimestamps = progress.getPartitionIdToOffset();
        long now = System.currentTimeMillis();

        for (Map.Entry<Integer, Long> entry : partitionTimestamps.entrySet()) {
            int partition = entry.getKey();
            long lag = (now - entry.getValue().longValue()) / 1000;
            if (lag > Config.routine_load_unstable_threshold_second) {
                updateSubstate(JobSubstate.UNSTABLE, new ErrorReason(InternalErrorCode.SLOW_RUNNING_ERR,
                        String.format("The lag [%d] of partition [%d] exceeds " +
                                        "Config.routine_load_unstable_threshold_second [%d]",
                                lag,  partition, Config.routine_load_unstable_threshold_second)));
                return;
            }
        }
        updateSubstate(JobSubstate.STABLE, null);
    }
}
