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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/LoadManager.java

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

package com.starrocks.load.loadv2;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.DataQualityException;
import com.starrocks.common.DdlException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.TimeoutException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.FailMsg;
import com.starrocks.load.FailMsg.CancelType;
import com.starrocks.load.Load;
import com.starrocks.persist.AlterLoadJobOperationLog;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.CancelLoadStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.warehouse.WarehouseLoadInfoBuilder;
import com.starrocks.warehouse.WarehouseLoadStatusInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * The broker and spark load jobs are included in this class.
 * <p>
 * The lock sequence:
 * Database.lock
 * LoadManager.lock
 * LoadJob.lock
 */
public class LoadMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(LoadMgr.class);

    private final Map<Long, LoadJob> idToLoadJob = Maps.newConcurrentMap();
    private final Map<Long, Map<String, List<LoadJob>>> dbIdToLabelToLoadJobs = Maps.newConcurrentMap();
    private final LoadJobScheduler loadJobScheduler;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final WarehouseLoadInfoBuilder warehouseLoadInfoBuilder =
            new WarehouseLoadInfoBuilder();

    public LoadMgr(LoadJobScheduler loadJobScheduler) {
        this.loadJobScheduler = loadJobScheduler;
    }

    /**
     * This method will be invoked by the broker load(v2) now.
     *
     * @param stmt
     * @throws DdlException
     */
    public void createLoadJobFromStmt(LoadStmt stmt, ConnectContext context) throws DdlException {
        Database database = checkDb(stmt.getLabel().getDbName());
        long dbId = database.getId();
        LoadJob loadJob = null;
        writeLock();
        try {
            checkLabelUsed(dbId, stmt.getLabel().getLabelName());
            if (stmt.getBrokerDesc() == null && stmt.getResourceDesc() == null) {
                throw new DdlException("LoadManager only support the broker and spark load.");
            }
            if (loadJobScheduler.isQueueFull()) {
                throw new DdlException(
                        "There are more than " + Config.desired_max_waiting_jobs + " load jobs in waiting queue, "
                                + "please retry later.");
            }
            loadJob = BulkLoadJob.fromLoadStmt(stmt, context);
            createLoadJob(loadJob);
        } finally {
            writeUnlock();
        }
        GlobalStateMgr.getCurrentState().getEditLog().logCreateLoadJob(loadJob);

        // The job must be submitted after edit log.
        // It guarantee that load job has not been changed before edit log.
        loadJobScheduler.submitJob(loadJob);
    }

    public void alterLoadJob(AlterLoadStmt stmt) throws DdlException {
        Database db = GlobalStateMgr.getCurrentState().getDb(stmt.getDbName());
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + stmt.getDbName());
        }

        LoadJob loadJob = null;
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(db.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("Load job does not exist");
            }
            List<LoadJob> loadJobList = labelToLoadJobs.get(stmt.getLabel());
            if (loadJobList == null) {
                throw new DdlException("Load job does not exist");
            }
            Optional<LoadJob> loadJobOptional = loadJobList.stream().filter(entity -> !entity.isTxnDone()).findFirst();
            if (!loadJobOptional.isPresent()) {
                throw new DdlException("There is no uncompleted job which label is " + stmt.getLabel());
            }
            loadJob = loadJobOptional.get();

        } finally {
            readUnlock();
        }

        loadJob.alterJob(stmt);
    }

    public void replayCreateLoadJob(LoadJob loadJob) {
        createLoadJob(loadJob);
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId())
                .add("msg", "replay create load job")
                .build());
    }

    // add load job and also add to to callback factory
    private void createLoadJob(LoadJob loadJob) {
        addLoadJob(loadJob);
        // add callback before txn created, because callback will be performed on replay without txn begin
        // register txn state listener
        if (!loadJob.isCompleted()) {
            GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(loadJob);
        }
    }

    private void addLoadJob(LoadJob loadJob) {
        idToLoadJob.put(loadJob.getId(), loadJob);
        long dbId = loadJob.getDbId();
        if (!dbIdToLabelToLoadJobs.containsKey(dbId)) {
            dbIdToLabelToLoadJobs.put(loadJob.getDbId(), new ConcurrentHashMap<>());
        }
        Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
        if (!labelToLoadJobs.containsKey(loadJob.getLabel())) {
            labelToLoadJobs.put(loadJob.getLabel(), new ArrayList<>());
        }
        labelToLoadJobs.get(loadJob.getLabel()).add(loadJob);
    }

    public void recordFinishedOrCacnelledLoadJob(long jobId, EtlJobType jobType, String failMsg, String trackingUrl)
            throws UserException {
        LoadJob loadJob = getLoadJob(jobId);
        if (loadJob.isTxnDone() && !Strings.isNullOrEmpty(failMsg)) {
            throw new LoadException("LoadJob " + jobId + " state " + loadJob.getState().name() + ", can not be cancal");
        }
        if (loadJob.isCompleted()) {
            throw new LoadException(
                    "LoadJob " + jobId + " state " + loadJob.getState().name() + ", can not be cancal/publish");
        }
        if (Objects.requireNonNull(jobType) == EtlJobType.INSERT) {
            InsertLoadJob insertLoadJob = (InsertLoadJob) loadJob;
            insertLoadJob.setLoadFinishOrCancel(failMsg, trackingUrl);
        } else {
            throw new LoadException("Unknown job type [" + jobType.name() + "]");
        }
    }

    public long registerLoadJob(String label, String dbName, long tableId, EtlJobType jobType,
                                long createTimestamp, long estimateScanRows, TLoadJobType type, long timeout,
                                String warehouse, boolean isStatisticsJob)
            throws UserException {

        // get db id
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new MetaNotFoundException("Database[" + dbName + "] does not exist");
        }

        LoadJob loadJob;
        if (Objects.requireNonNull(jobType) == EtlJobType.INSERT) {
            loadJob = new InsertLoadJob(
                    label, db.getId(), tableId, createTimestamp, estimateScanRows, type, timeout, warehouse,
                    isStatisticsJob);
        } else {
            throw new LoadException("Unknown job type [" + jobType.name() + "]");
        }
        addLoadJob(loadJob);
        // persistent
        GlobalStateMgr.getCurrentState().getEditLog().logCreateLoadJob(loadJob);
        return loadJob.getId();
    }

    public void cancelLoadJob(CancelLoadStmt stmt) throws DdlException {
        Database db = GlobalStateMgr.getCurrentState().getDb(stmt.getDbName());
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + stmt.getDbName());
        }

        LoadJob loadJob = null;
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(db.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("Load job does not exist");
            }
            List<LoadJob> loadJobList = labelToLoadJobs.get(stmt.getLabel());
            if (loadJobList == null) {
                throw new DdlException("Load job does not exist");
            }
            Optional<LoadJob> loadJobOptional = loadJobList.stream().filter(entity -> !entity.isTxnDone()).findFirst();
            if (!loadJobOptional.isPresent()) {
                throw new DdlException("There is no uncompleted job which label is " + stmt.getLabel());
            }
            loadJob = loadJobOptional.get();
        } finally {
            readUnlock();
        }

        loadJob.cancelJob(new FailMsg(FailMsg.CancelType.USER_CANCEL, "user cancel"));
    }

    public void replayEndLoadJob(LoadJobFinalOperation operation) {
        LoadJob job = idToLoadJob.get(operation.getId());
        if (job == null) {
            // This should not happen.
            // Last time I found that when user submit a job with already used label, an END_LOAD_JOB edit log
            // will be wrote but the job is not added to 'idToLoadJob', so this job here we got will be null.
            // And this bug has been fixed.
            // Just add a log here to observe.
            LOG.warn("job does not exist when replaying end load job edit log: {}", operation);
            return;
        }
        job.unprotectReadEndOperation(operation, true);

        GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(job.id);

        LOG.info(new LogBuilder(LogKey.LOAD_JOB, operation.getId())
                .add("operation", operation)
                .add("msg", "replay end load job")
                .build());
        if (isJobExpired(job, System.currentTimeMillis())) {
            LOG.info("remove expired job: {}", job);
            unprotectedRemoveJobReleatedMeta(job);
        }
    }

    public void replayUpdateLoadJobStateInfo(LoadJob.LoadJobStateUpdateInfo info) {
        long jobId = info.getJobId();
        LoadJob job = idToLoadJob.get(jobId);
        if (job == null) {
            LOG.warn("replay update load job state failed. error: job not found, id: {}", jobId);
            return;
        }

        job.replayUpdateStateInfo(info);

        if (isJobExpired(job, System.currentTimeMillis())) {
            LOG.info("remove expired job: {}", job);
            unprotectedRemoveJobReleatedMeta(job);
        }
    }

    public void replayAlterLoadJob(AlterLoadJobOperationLog op) {
        long jobId = op.getJobId();
        LoadJob job = idToLoadJob.get(jobId);
        if (job == null) {
            LOG.warn("replay alter load job state failed. error: job not found, id: {}", jobId);
            return;
        }

        job.replayAlterJob(op);
    }

    public long getLoadJobNum(JobState jobState, long dbId) {
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
            if (labelToLoadJobs == null) {
                return 0;
            }
            List<LoadJob> loadJobList = labelToLoadJobs.values().stream()
                    .flatMap(entity -> entity.stream()).collect(Collectors.toList());
            return (int) loadJobList.stream().filter(entity -> entity.getState() == jobState).count();
        } finally {
            readUnlock();
        }
    }

    public long getLoadJobNum(JobState jobState, EtlJobType jobType) {
        readLock();
        try {
            return idToLoadJob.values().stream().filter(j -> j.getState() == jobState && j.getJobType() == jobType)
                    .count();
        } finally {
            readUnlock();
        }
    }

    private void unprotectedRemoveJobReleatedMeta(LoadJob job) {
        long dbId = job.getDbId();
        String label = job.getLabel();

        // 1. remove from idToLoadJob
        idToLoadJob.remove(job.getId());

        // 2. remove from dbIdToLabelToLoadJobs
        Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
        List<LoadJob> sameLabelJobs = labelToLoadJobs.get(label);
        sameLabelJobs.remove(job);
        if (sameLabelJobs.isEmpty()) {
            labelToLoadJobs.remove(label);
        }
        if (labelToLoadJobs.isEmpty()) {
            dbIdToLabelToLoadJobs.remove(dbId);
        }

        // 3. remove spark launcher log
        if (job instanceof SparkLoadJob) {
            ((SparkLoadJob) job).clearSparkLauncherLog();
        }

        warehouseLoadInfoBuilder.withRemovedJob(job);
    }

    private boolean isJobExpired(LoadJob job, long currentTimeMs) {
        if (!job.isCompleted()) {
            return false;
        }
        return (currentTimeMs - job.getFinishTimestamp()) / 1000 > Config.label_keep_max_second;
    }

    public void cleanResidualJob() {
        // clean residual insert job
        readLock();
        List<LoadJob> insertJobs;
        try {
            insertJobs = idToLoadJob.values().stream()
                    .filter(job -> job.getJobType() == EtlJobType.INSERT && !job.isCompleted())
                    .collect(Collectors.toList());
        } finally {
            readUnlock();
        }

        insertJobs.forEach(job -> {
            TransactionStatus st = GlobalStateMgr.getCurrentGlobalTransactionMgr().getLabelState(
                    job.getDbId(), job.getLabel());
            if (st == TransactionStatus.UNKNOWN) {
                try {
                    recordFinishedOrCacnelledLoadJob(
                            job.getId(), EtlJobType.INSERT, "Cancelled since transaction status unknown", "");
                    LOG.info("abort job: {} since transaction status unknown", job.getLabel());
                } catch (UserException e) {
                    LOG.warn("failed to abort job: {}", job.getLabel(), e);
                }
            }
        });
    }

    public void removeOldLoadJob() {
        // clean expired load job
        long currentTimeMs = System.currentTimeMillis();

        writeLock();
        try {
            // add load job to a sorted tree set
            Set<LoadJob> jobs = new TreeSet<>(new Comparator<LoadJob>() {
                @Override
                public int compare(LoadJob o1, LoadJob o2) {
                    // sort by finish time desc
                    return Long.signum(o1.getFinishTimestamp() - o2.getFinishTimestamp());
                }
            });

            for (Map.Entry<Long, LoadJob> entry : idToLoadJob.entrySet()) {
                LoadJob job = entry.getValue();
                if (!job.isCompleted()) {
                    continue;
                }
                if (isJobExpired(job, currentTimeMs)) {
                    // remove expired job
                    LOG.info("remove expired job: {}", job.getLabel());
                    unprotectedRemoveJobReleatedMeta(job);
                } else {
                    jobs.add(job);
                }
            }

            // if there are still more jobs than LABEL_KEEP_MAX_NUM
            // remove the ones that finished earlier
            int numJobsToRemove = idToLoadJob.size() - Config.label_keep_max_num;
            if (numJobsToRemove > 0) {
                LOG.info("remove {} jobs from {}", numJobsToRemove, jobs.size());
                Iterator<LoadJob> iterator = jobs.iterator();
                for (int i = 0; i != numJobsToRemove && iterator.hasNext(); ++i) {
                    LoadJob job = iterator.next();
                    LOG.info("remove redundant job: {}", job.getLabel());
                    unprotectedRemoveJobReleatedMeta(job);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    // only for those jobs which transaction is not started
    public void processTimeoutJobs() {
        idToLoadJob.values().stream().forEach(entity -> entity.processTimeout());
    }

    // only for those jobs which have etl state, like SparkLoadJob
    public void processEtlStateJobs() {
        idToLoadJob.values().stream().filter(job -> (job.jobType == EtlJobType.SPARK && job.state == JobState.ETL))
                .forEach(job -> {
                    try {
                        ((SparkLoadJob) job).updateEtlStatus();
                    } catch (DataQualityException e) {
                        LOG.info("update load job etl status failed. job id: {}", job.getId(), e);
                        job.cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED,
                                        DataQualityException.QUALITY_FAIL_MSG),
                                true, true);
                    } catch (TimeoutException e) {
                        // timeout, retry next time
                        LOG.warn("update load job etl status failed. job id: {}", job.getId(), e);
                    } catch (UserException e) {
                        LOG.warn("update load job etl status failed. job id: {}", job.getId(), e);
                        job.cancelJobWithoutCheck(new FailMsg(CancelType.ETL_RUN_FAIL, e.getMessage()), true, true);
                    } catch (Exception e) {
                        LOG.warn("update load job etl status failed. job id: {}", job.getId(), e);
                    }
                });
    }

    // only for those jobs which load by PushTask
    public void processLoadingStateJobs() {
        idToLoadJob.values().stream().filter(job -> (job.jobType == EtlJobType.SPARK && job.state == JobState.LOADING))
                .forEach(job -> {
                    try {
                        ((SparkLoadJob) job).updateLoadingStatus();
                    } catch (UserException e) {
                        LOG.warn("update load job loading status failed. job id: {}", job.getId(), e);
                        job.cancelJobWithoutCheck(new FailMsg(CancelType.LOAD_RUN_FAIL, e.getMessage()), true, true);
                    } catch (Exception e) {
                        LOG.warn("update load job loading status failed. job id: {}", job.getId(), e);
                    }
                });
    }

    /**
     * This method will return the jobs info which can meet the condition of input param.
     *
     * @param dbId          used to filter jobs which belong to this db
     * @param labelValue    used to filter jobs which's label is or like labelValue.
     * @param accurateMatch true: filter jobs which's label is labelValue. false: filter jobs which's label like itself.
     * @param statesValue   used to filter jobs which's state within the statesValue set.
     * @return The result is the list of jobInfo.
     * JobInfo is a List<Comparable> which includes the comparable object: jobId, label, state etc.
     * The result is unordered.
     */
    public List<List<Comparable>> getLoadJobInfosByDb(long dbId, String labelValue,
                                                      boolean accurateMatch, Set<String> statesValue) {

        LinkedList<List<Comparable>> loadJobInfos = new LinkedList<>();

        Set<JobState> states = Sets.newHashSet();
        if (statesValue == null || statesValue.size() == 0) {
            states.addAll(EnumSet.allOf(JobState.class));
        } else {
            for (String stateValue : statesValue) {
                try {
                    states.add(JobState.valueOf(stateValue));
                } catch (IllegalArgumentException e) {
                    // ignore this state
                }
            }
        }

        List<LoadJob> loadJobList = getLoadJobsByDb(dbId, labelValue, accurateMatch);
        // check state
        for (LoadJob loadJob : loadJobList) {
            try {
                if (!states.contains(loadJob.getAcutalState())) {
                    continue;
                }
                // add load job info
                loadJobInfos.add(loadJob.getShowInfo());
            } catch (DdlException ignored) {
                // ignored
            }
        }
        return loadJobInfos;
    }

    public List<LoadJob> getLoadJobs(String labelValue) {
        List<LoadJob> loadJobList = Lists.newArrayList();
        readLock();
        try {
            if (Strings.isNullOrEmpty(labelValue)) {
                loadJobList.addAll(dbIdToLabelToLoadJobs.values().stream().flatMap(it -> it.values().stream())
                        .flatMap(Collection::stream).collect(Collectors.toList()));
            } else {
                loadJobList.addAll(dbIdToLabelToLoadJobs.values().stream().flatMap(it -> it.values().stream())
                        .flatMap(Collection::stream).filter(it -> it.getLabel().equals(labelValue))
                        .collect(Collectors.toList()));
            }
            return loadJobList;
        } finally {
            readUnlock();
        }
    }

    public List<LoadJob> getLoadJobsByDb(long dbId, String labelValue, boolean accurateMatch) {

        List<LoadJob> loadJobList = Lists.newArrayList();
        if (dbId != -1 && !dbIdToLabelToLoadJobs.containsKey(dbId)) {
            return loadJobList;
        }
        readLock();
        try {
            for (Map<String, List<LoadJob>> dbJobs : dbIdToLabelToLoadJobs.values()) {
                Map<String, List<LoadJob>> labelToLoadJobs = dbId == -1 ? dbJobs : dbIdToLabelToLoadJobs.get(dbId);

                if (Strings.isNullOrEmpty(labelValue)) {
                    loadJobList.addAll(labelToLoadJobs.values()
                            .stream().flatMap(Collection::stream).collect(Collectors.toList()));
                } else {
                    // check label value
                    if (accurateMatch) {
                        if (!labelToLoadJobs.containsKey(labelValue)) {
                            return loadJobList;
                        }
                        loadJobList.addAll(labelToLoadJobs.get(labelValue));
                    } else {
                        // non-accurate match
                        for (Map.Entry<String, List<LoadJob>> entry : labelToLoadJobs.entrySet()) {
                            if (entry.getKey().contains(labelValue)) {
                                loadJobList.addAll(entry.getValue());
                            }
                        }
                    }
                }
                if (dbId != -1) {
                    break;
                }
            }
            return loadJobList;
        } finally {
            readUnlock();
        }
    }

    public void getLoadJobInfo(Load.JobInfo info) throws DdlException {
        Database database = checkDb(info.dbName);
        readLock();
        try {
            // find the latest load job by info
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(database.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("No jobs belong to database(" + info.dbName + ")");
            }
            List<LoadJob> loadJobList = labelToLoadJobs.get(info.label);
            if (loadJobList == null || loadJobList.isEmpty()) {
                throw new DdlException("Unknown job(" + info.label + ")");
            }

            LoadJob loadJob = loadJobList.get(loadJobList.size() - 1);
            loadJob.getJobInfo(info);
        } finally {
            readUnlock();
        }
    }

    public LoadJob getLoadJob(long jobId) {
        readLock();
        try {
            return idToLoadJob.get(jobId);
        } finally {
            readUnlock();
        }
    }

    public void prepareJobs() {
        analyzeLoadJobs();
        submitJobs();
    }

    private void submitJobs() {
        loadJobScheduler.submitJob(idToLoadJob.values().stream().filter(
                loadJob -> loadJob.state == JobState.PENDING).collect(Collectors.toList()));
    }

    private void analyzeLoadJobs() {
        for (LoadJob loadJob : idToLoadJob.values()) {
            if (loadJob.getState() == JobState.PENDING) {
                loadJob.analyze();
            }
        }
    }

    private Database checkDb(String dbName) throws DdlException {
        // get db
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            LOG.warn("Database {} does not exist", dbName);
            throw new DdlException("Database[" + dbName + "] does not exist");
        }
        return db;
    }

    /**
     * step1: if label has been used in old load jobs which belong to load class
     * step2: if label has been used in v2 load jobs
     * step2.1: if label has been user in v2 load jobs, the create timestamp will be checked
     *
     * @param dbId
     * @param label
     * @throws LabelAlreadyUsedException throw exception when label has been used by an unfinished job.
     */
    private void checkLabelUsed(long dbId, String label)
            throws DdlException {
        // if label has been used in v2 of load jobs
        if (dbIdToLabelToLoadJobs.containsKey(dbId)) {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
            if (labelToLoadJobs.containsKey(label)) {
                List<LoadJob> labelLoadJobs = labelToLoadJobs.get(label);
                Optional<LoadJob> loadJobOptional =
                        labelLoadJobs.stream().filter(entity -> entity.getState() != JobState.CANCELLED).findFirst();
                if (loadJobOptional.isPresent()) {
                    LOG.warn("Failed to add load job when label {} has been used.", label);
                    throw new LabelAlreadyUsedException(label);
                }
            }
        }
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    // If load job will be removed by cleaner later, it will not be saved in image.
    private boolean needSave(LoadJob loadJob) {
        if (!loadJob.isCompleted()) {
            return true;
        }

        long currentTimeMs = System.currentTimeMillis();
        return loadJob.isCompleted() &&
                ((currentTimeMs - loadJob.getFinishTimestamp()) / 1000 <= Config.label_keep_max_second);
    }

    public void initJobProgress(Long jobId, TUniqueId loadId, Set<TUniqueId> fragmentIds,
                                List<Long> relatedBackendIds) {
        LoadJob job = idToLoadJob.get(jobId);
        if (job != null) {
            job.initLoadProgress(loadId, fragmentIds, relatedBackendIds);
        }
    }

    public void updateJobPrgress(Long jobId, TReportExecStatusParams params) {
        LoadJob job = idToLoadJob.get(jobId);
        if (job != null) {
            job.updateProgress(params);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        List<LoadJob> loadJobs = idToLoadJob.values().stream().filter(this::needSave).collect(Collectors.toList());

        out.writeInt(loadJobs.size());
        for (LoadJob loadJob : loadJobs) {
            loadJob.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        long now = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            LoadJob loadJob = LoadJob.read(in);
            // discard expired job right away
            if (isJobExpired(loadJob, now)) {
                LOG.info("discard expired job: {}", loadJob);
                continue;
            }

            putLoadJob(loadJob);
        }
    }

    public long loadLoadJobsV2(DataInputStream in, long checksum) throws IOException {
        readFields(in);
        LOG.info("finished replay loadJobsV2 from image");
        return checksum;
    }

    public long saveLoadJobsV2(DataOutputStream out, long checksum) throws IOException {
        write(out);
        return checksum;
    }

    public void loadLoadJobsV2JsonFormat(SRMetaBlockReader reader)
            throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        int size = reader.readInt();
        long now = System.currentTimeMillis();
        while (size-- > 0) {
            LoadJob loadJob = reader.readJson(LoadJob.class);
            // discard expired job right away
            if (isJobExpired(loadJob, now)) {
                LOG.info("discard expired job: {}", loadJob);
                continue;
            }

            putLoadJob(loadJob);
        }
    }

    public Map<String, WarehouseLoadStatusInfo> getWarehouseLoadInfo() {
        readLock();
        try {
            return warehouseLoadInfoBuilder.buildFromJobs(idToLoadJob.values());
        } finally {
            readUnlock();
        }
    }

    private void putLoadJob(LoadJob loadJob) {
        idToLoadJob.put(loadJob.getId(), loadJob);
        Map<String, List<LoadJob>> map =
                dbIdToLabelToLoadJobs.computeIfAbsent(loadJob.getDbId(), k -> Maps.newConcurrentMap());

        List<LoadJob> jobs = map.computeIfAbsent(loadJob.getLabel(), k -> Lists.newArrayList());
        jobs.add(loadJob);
        // The callback of load job which is replayed by image need to be registered in callback factory.
        // The commit and visible txn will callback the unfinished load job.
        // Otherwise, the load job always does not be completed while the txn is visible.
        if (!loadJob.isCompleted()) {
            GlobalStateMgr.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(loadJob);
        }
    }

    public void saveLoadJobsV2JsonFormat(DataOutputStream out) throws IOException, SRMetaBlockException {
        List<LoadJob> loadJobs = idToLoadJob.values().stream().filter(this::needSave).collect(Collectors.toList());
        // 1 json for number of jobs, size of idToLoadJob for jobs
        final int cnt = 1 + loadJobs.size();
        SRMetaBlockWriter writer = new SRMetaBlockWriter(out, SRMetaBlockID.LOAD_MGR, cnt);
        writer.writeJson(loadJobs.size());
        for (LoadJob loadJob : loadJobs) {
            writer.writeJson(loadJob);
        }
        writer.close();
    }
}
