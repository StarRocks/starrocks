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


package com.starrocks.load.routineload;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.PulsarUtil;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.SmallFileMgr.SmallFile;
import com.starrocks.load.Load;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * PulsarRoutineLoadJob is a kind of RoutineLoadJob which fetch data from pulsar.
 * The progress which is super class property is seems like "{"partition1": backlognum1, "partition2": backlognum2}"
 */
public class PulsarRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(PulsarRoutineLoadJob.class);

    public static final String PULSAR_FILE_CATALOG = "pulsar";

    private String serviceUrl;
    private String topic;
    private String subscription;
    // optional, user want to load partitions.
    private List<String> customPulsarPartitions = Lists.newArrayList();
    // current pulsar partitions is the actually partition which will be fetched
    private List<String> currentPulsarPartitions = Lists.newArrayList();
    // pulsar properties, property prefix will be mapped to pulsar custom parameters, which can be extended in the future
    private Map<String, String> customProperties = Maps.newHashMap();
    private Map<String, String> convertedCustomProperties = Maps.newHashMap();

    public static final String POSITION_EARLIEST = "POSITION_EARLIEST"; // 1
    public static final String POSITION_LATEST = "POSITION_LATEST"; // 0
    public static final long POSITION_LATEST_VAL = 0;
    public static final long POSITION_EARLIEST_VAL = 1;

    private Long defaultInitialPosition = null;

    public PulsarRoutineLoadJob() {
        // for serialization, id is dummy
        super(-1, LoadDataSourceType.PULSAR);
    }

    public PulsarRoutineLoadJob(Long id, String name, long dbId, long tableId,
                                String serviceUrl, String topic, String subscription) {
        super(id, name, dbId, tableId, LoadDataSourceType.PULSAR);
        this.serviceUrl = serviceUrl;
        this.topic = topic;
        this.subscription = subscription;
        this.progress = new PulsarProgress();
    }

    public String getTopic() {
        return topic;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public String getSubscription() {
        return subscription;
    }

    public Map<String, String> getConvertedCustomProperties() {
        return convertedCustomProperties;
    }

    @Override
    public void prepare() throws UserException {
        super.prepare();
        // should reset converted properties each time the job being prepared.
        // because the file info can be changed anytime.
        convertCustomProperties(true);
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
                SmallFile smallFile = smallFileMgr.getSmallFile(dbId, PULSAR_FILE_CATALOG, file, true);
                convertedCustomProperties.put(entry.getKey(), "FILE:" + smallFile.id + ":" + smallFile.md5);
            } else {
                convertedCustomProperties.put(entry.getKey(), entry.getValue());
            }
        }

        if (convertedCustomProperties.containsKey(CreateRoutineLoadStmt.PULSAR_DEFAULT_INITIAL_POSITION)) {
            try {
                this.defaultInitialPosition = CreateRoutineLoadStmt.getPulsarPosition(
                        convertedCustomProperties.remove(CreateRoutineLoadStmt.PULSAR_DEFAULT_INITIAL_POSITION));
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
                // divide pulsarPartitions into tasks
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    List<String> partitions = Lists.newArrayList();
                    Map<String, Long> initialPositions = Maps.newHashMap();
                    for (int j = 0; j < currentPulsarPartitions.size(); j++) {
                        if (j % currentConcurrentTaskNum == i) {
                            String partition = currentPulsarPartitions.get(j);
                            partitions.add(partition);
                            // initial position was set, need to be added into PulsarTaskInfo
                            Long initialPosition = ((PulsarProgress) progress).getInitialPosition(partition);
                            if (initialPosition != -1L) {
                                initialPositions.put(partition, initialPosition);
                            }
                        }
                    }
                    long timeToExecuteMs = System.currentTimeMillis() + taskSchedIntervalS * 1000;
                    PulsarTaskInfo pulsarTaskInfo = new PulsarTaskInfo(UUID.randomUUID(), id,
                            taskSchedIntervalS * 1000, timeToExecuteMs, partitions, initialPositions);
                    LOG.debug("pulsar routine load task created: " + pulsarTaskInfo);
                    routineLoadTaskInfoList.add(pulsarTaskInfo);
                    result.add(pulsarTaskInfo);
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
        int aliveBeNum = systemInfoService.getAliveBackendNumber();
        int partitionNum = currentPulsarPartitions.size();
        if (desireTaskConcurrentNum == 0) {
            desireTaskConcurrentNum = Config.max_routine_load_task_concurrent_num;
        }

        LOG.debug("current concurrent task number is min"
                        + "(partition num: {}, desire task concurrent num: {}, alive be num: {}, config: {})",
                partitionNum, desireTaskConcurrentNum, aliveBeNum, Config.max_routine_load_task_concurrent_num);
        currentTaskConcurrentNum = Math.min(Math.min(partitionNum, Math.min(desireTaskConcurrentNum, aliveBeNum)),
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
            // but we still need to update the position to skip these error lines.
            Preconditions.checkState(txnState.getTransactionStatus() == TransactionStatus.ABORTED,
                    txnState.getTransactionStatus());
            return true;
        }

        // Running here, the status of the transaction should be ABORTED,
        // and it is caused by other errors. In this case, we should not update the position.
        LOG.debug("no need to update the progress of pulsar routine load. txn status: {}, " +
                        "txnStatusChangeReason: {}, task: {}, job: {}",
                txnState.getTransactionStatus(), txnStatusChangeReason,
                DebugUtil.printId(rlTaskTxnCommitAttachment.getTaskId()), id);
        return false;
    }

    @Override
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        super.updateProgress(attachment);
        this.progress.update(attachment);
    }

    @Override
    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        super.replayUpdateProgress(attachment);
        this.progress.update(attachment);
    }

    @Override
    protected RoutineLoadTaskInfo unprotectRenewTask(long timeToExecuteMs, RoutineLoadTaskInfo routineLoadTaskInfo) {
        PulsarTaskInfo oldPulsarTaskInfo = (PulsarTaskInfo) routineLoadTaskInfo;
        // add new task
        PulsarTaskInfo pulsarTaskInfo = new PulsarTaskInfo(timeToExecuteMs, oldPulsarTaskInfo,
                ((PulsarProgress) progress).getPartitionToInitialPosition(oldPulsarTaskInfo.getPartitions()));
        // remove old task
        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
        // add new task
        routineLoadTaskInfoList.add(pulsarTaskInfo);
        LOG.debug("pulsar routine load task renewed: " + pulsarTaskInfo);
        return pulsarTaskInfo;
    }

    @Override
    protected void unprotectUpdateProgress() {
        ((PulsarProgress) progress).unprotectUpdate(currentPulsarPartitions, defaultInitialPosition);
    }

    // if customPulsarPartition is not null, then return false immediately
    // else if pulsar partitions of topic has been changed, return true.
    // else return false
    // update current pulsar partition at the same time
    // current pulsar partitions = customPulsarPartitions == 0 ? all of partition of pulsar topic : customPulsarPartitions
    @Override
    protected boolean unprotectNeedReschedule() throws UserException {
        // only running and need_schedule job need to be changed current pulsar partitions
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE) {
            if (customPulsarPartitions != null && customPulsarPartitions.size() != 0) {
                currentPulsarPartitions = customPulsarPartitions;
                return false;
            } else {
                List<String> newCurrentPulsarPartition;
                try {
                    newCurrentPulsarPartition = getAllPulsarPartitions();
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
                if (currentPulsarPartitions.containsAll(newCurrentPulsarPartition)) {
                    if (currentPulsarPartitions.size() > newCurrentPulsarPartition.size()) {
                        unprotectUpdateCurrentPartitions(newCurrentPulsarPartition);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    unprotectUpdateCurrentPartitions(newCurrentPulsarPartition);
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

    protected void unprotectUpdateCurrentPartitions(List<String> newCurrentPartitions) {
        currentPulsarPartitions = newCurrentPartitions;
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("current_pulsar_partitions", Joiner.on(",").join(currentPulsarPartitions))
                    .add("msg", "current pulsar partitions has been change")
                    .build());
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

    private List<String> getAllPulsarPartitions() throws UserException {
        // Get custom properties like tokens
        convertCustomProperties(false);
        return PulsarUtil.getAllPulsarPartitions(serviceUrl, topic,
                subscription, ImmutableMap.copyOf(convertedCustomProperties));
    }

    public static PulsarRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) throws UserException {
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
            Load.checkMergeCondition(stmt.getMergeConditionStr(), (OlapTable) table, false);
            tableId = table.getId();
        } finally {
            db.readUnlock();
        }

        // init pulsar routine load job
        long id = GlobalStateMgr.getCurrentState().getNextId();
        PulsarRoutineLoadJob pulsarRoutineLoadJob = new PulsarRoutineLoadJob(id, stmt.getName(),
                db.getId(), tableId, stmt.getPulsarServiceUrl(), stmt.getPulsarTopic(),
                stmt.getPulsarSubscription());
        pulsarRoutineLoadJob.setOptional(stmt);
        pulsarRoutineLoadJob.checkCustomProperties();
        pulsarRoutineLoadJob.checkCustomPartition();

        return pulsarRoutineLoadJob;
    }

    private void checkCustomPartition() throws UserException {
        if (customPulsarPartitions.isEmpty()) {
            return;
        }
        List<String> allPulsarPartitions = getAllPulsarPartitions();
        for (String customPartition : customPulsarPartitions) {
            if (!allPulsarPartitions.contains(customPartition)) {
                throw new LoadException("there is a custom pulsar partition " + customPartition
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
                if (!smallFileMgr.containsFile(dbId, PULSAR_FILE_CATALOG, file)) {
                    throw new DdlException("File " + file + " does not exist in db "
                            + dbId + " with globalStateMgr: " + PULSAR_FILE_CATALOG);
                }
            }
        }
    }

    @Override
    protected void setOptional(CreateRoutineLoadStmt stmt) throws UserException {
        super.setOptional(stmt);

        if (!stmt.getPulsarPartitions().isEmpty()) {
            setCustomPulsarPartitions(stmt.getPulsarPartitions());
        }

        if (!stmt.getPulsarPartitionInitialPositions().isEmpty()) {
            setPulsarPatitionInitialPositions(stmt.getPulsarPartitionInitialPositions());
        }

        if (!stmt.getCustomPulsarProperties().isEmpty()) {
            setCustomPulsarProperties(stmt.getCustomPulsarProperties());
        }
    }

    // this is an unprotected method which is called in the initialization function
    private void setCustomPulsarPartitions(List<String> pulsarPartitions) {
        this.customPulsarPartitions = pulsarPartitions;
    }

    private void setPulsarPatitionInitialPositions(List<Pair<String, Long>> patitionToInitialPositions) {
        for (Pair<String, Long> entry : patitionToInitialPositions) {
            ((PulsarProgress) progress).addPartitionToInitialPosition(entry);
        }
    }

    private void setCustomPulsarProperties(Map<String, String> pulsarProperties) {
        this.customProperties = pulsarProperties;
    }

    @Override
    protected String dataSourcePropertiesJsonToString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("serviceUrl", serviceUrl);
        dataSourceProperties.put("topic", topic);
        dataSourceProperties.put("subscription", subscription);
        List<String> sortedPartitions = Lists.newArrayList(currentPulsarPartitions);
        Collections.sort(sortedPartitions);
        dataSourceProperties.put("currentPulsarPartitions", Joiner.on(",").join(sortedPartitions));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dataSourceProperties);
    }

    @Override
    protected String customPropertiesJsonToString() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(customProperties);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, serviceUrl);
        Text.writeString(out, topic);
        Text.writeString(out, subscription);

        out.writeInt(customPulsarPartitions.size());
        for (String partition : customPulsarPartitions) {
            Text.writeString(out, partition);
        }

        out.writeInt(customProperties.size());
        for (Map.Entry<String, String> property : customProperties.entrySet()) {
            Text.writeString(out, "property." + property.getKey());
            Text.writeString(out, property.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        serviceUrl = Text.readString(in);
        topic = Text.readString(in);
        subscription = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            customPulsarPartitions.add(Text.readString(in));
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

    @Override
    public void modifyDataSourceProperties(RoutineLoadDataSourceProperties dataSourceProperties) throws DdlException {
        List<Pair<String, Long>> partitionInitialPositions = Lists.newArrayList();
        Map<String, String> customPulsarProperties = Maps.newHashMap();

        if (dataSourceProperties.hasAnalyzedProperties()) {
            partitionInitialPositions = dataSourceProperties.getPulsarPartitionInitialPositions();
            customPulsarProperties = dataSourceProperties.getCustomPulsarProperties();
        }

        if (!customPulsarProperties.isEmpty()) {
            this.customProperties.putAll(customPulsarProperties);
            convertCustomProperties(true);

            if (customPulsarProperties.containsKey(CreateRoutineLoadStmt.PULSAR_DEFAULT_INITIAL_POSITION)) {
                // defaultInitialPosition should be updated by convertCustomProperties()
                List<Pair<String, Long>> initialPositions = new ArrayList<>();
                // defaultInitialPosition can only update currentPulsarPartitions
                currentPulsarPartitions.forEach(
                        entry -> initialPositions.add(Pair.create(entry, this.defaultInitialPosition)));
                ((PulsarProgress) progress).modifyInitialPositions(initialPositions);
            }
        }

        // modify partition positions
        if (!partitionInitialPositions.isEmpty()) {
            // we can only modify the partition if it's specified in the create statement
            for (Pair<String, Long> pair : partitionInitialPositions) {
                if (!customPulsarPartitions.contains(pair.first)) {
                    throw new DdlException("The partition " +
                            pair.first + " is not specified in the create statement");
                }
            }

            ((PulsarProgress) progress).modifyInitialPositions(partitionInitialPositions);
        }

        LOG.info("modify the data source properties of pulsar routine load job: {}, datasource properties: {}",
                this.id, dataSourceProperties);
    }
}
