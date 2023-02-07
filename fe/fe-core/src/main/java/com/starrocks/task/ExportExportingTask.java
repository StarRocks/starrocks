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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/ExportExportingTask.java

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

package com.starrocks.task;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.Version;
import com.starrocks.common.util.BrokerUtil;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.ExportChecker;
import com.starrocks.load.ExportFailMsg;
import com.starrocks.load.ExportJob;
import com.starrocks.qe.Coordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ExportExportingTask extends PriorityLeaderTask {
    private static final Logger LOG = LogManager.getLogger(ExportExportingTask.class);
    private static final int RETRY_NUM = 2;

    protected final ExportJob job;

    private RuntimeProfile profile = new RuntimeProfile("Export");
    private final List<RuntimeProfile> fragmentProfiles = Lists.newArrayList();

    // task index -> dummy value
    private final MarkedCountDownLatch<Integer, Integer> subTasksDoneSignal;

    public ExportExportingTask(ExportJob job) {
        this.job = job;
        this.signature = job.getId();
        this.subTasksDoneSignal = new MarkedCountDownLatch<Integer, Integer>(job.getCoordList().size());
    }

    @Override
    protected void exec() {
        if (job.getState() != ExportJob.JobState.EXPORTING) {
            return;
        }
        LOG.info("begin execute export job in exporting state. job: {}", job);

        // check timeout
        if (getLeftTimeSecond() < 0) {
            job.cancelInternal(ExportFailMsg.CancelType.TIMEOUT, "timeout");
            return;
        }

        synchronized (job) {
            if (job.getDoExportingThread() != null) {
                LOG.warn("export job is already being executed");
                return;
            }
            job.setDoExportingThread(Thread.currentThread());
        }

        if (job.isReplayed()) {
            // If the job is created from replay thread, all plan info will be lost.
            // so the job has to be cancelled.
            String failMsg = "FE restarted or Leader changed during exporting. Job must be cancelled";
            job.cancelInternal(ExportFailMsg.CancelType.RUN_FAIL, failMsg);
            return;
        }

        // sub tasks execute in parallel
        List<Coordinator> coords = job.getCoordList();
        int coordSize = coords.size();
        List<ExportExportingSubTask> subTasks = Lists.newArrayList();
        for (int i = 0; i < coordSize; i++) {
            Coordinator coord = coords.get(i);
            ExportExportingSubTask subTask = new ExportExportingSubTask(coord, i, coordSize, job);
            subTasks.add(subTask);
            subTasksDoneSignal.addMark(i, -1);
        }
        for (ExportExportingSubTask subTask : subTasks) {
            if (!submitSubTask(subTask)) {
                job.cancelInternal(ExportFailMsg.CancelType.RUN_FAIL, "submit exporting task failed");
                return;
            }
            LOG.info("submit export sub task success. task idx: {}, task query id: {}",
                    subTask.getTaskIdx(), subTask.getQueryId());
        }

        boolean success = false;
        try {
            success = subTasksDoneSignal.await(getLeftTimeSecond(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("export sub task signal await error", e);
        }

        Status status = subTasksDoneSignal.getStatus();
        if (!success || !status.ok()) {
            if (!success) {
                job.cancelInternal(ExportFailMsg.CancelType.TIMEOUT, "timeout");
            } else {
                job.cancelInternal(ExportFailMsg.CancelType.RUN_FAIL, status.getErrorMsg());
            }
            registerProfile();
            return;
        }

        // move tmp file to final destination
        Status mvStatus = moveTmpFiles();
        if (!mvStatus.ok()) {
            String failMsg = "move tmp file to final destination fail, ";
            failMsg += mvStatus.getErrorMsg();
            job.cancelInternal(ExportFailMsg.CancelType.RUN_FAIL, failMsg);
            LOG.warn("move tmp file to final destination fail. job:{}", job);
            registerProfile();
            return;
        }

        // finish job
        job.finish();
        registerProfile();

        synchronized (this) {
            job.setDoExportingThread(null);
        }
    }

    private boolean submitSubTask(ExportExportingSubTask subTask) {
        int retryNum = 0;
        while (!ExportChecker.getExportingSubTaskExecutor().submit(subTask)) {
            LOG.warn("submit export sub task failed. try to resubmit. task idx {}, task query id: {}, retry: {}",
                    subTask.getTaskIdx(), subTask.getQueryId(), retryNum);
            if (++retryNum > RETRY_NUM) {
                return false;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.warn(e);
            }
        }
        return true;
    }

    private int getLeftTimeSecond() {
        return (int) (job.getTimeoutSecond() - (System.currentTimeMillis() - job.getCreateTimeMs()) / 1000);
    }

    private void initProfile() {
        profile = new RuntimeProfile("Query");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, String.valueOf(job.getId()));
        summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(job.getStartTimeMs()));

        long currentTimestamp = System.currentTimeMillis();
        long totalTimeMs = currentTimestamp - job.getStartTimeMs();
        summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(currentTimestamp));
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(totalTimeMs));

        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, job.getState().toString());
        summaryProfile.addInfoString("StarRocks Version",
                String.format("%s-%s", Version.STARROCKS_VERSION, Version.STARROCKS_COMMIT_HASH));
        summaryProfile.addInfoString(ProfileManager.USER, "xxx");
        summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, String.valueOf(job.getDbId()));
        summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, job.getSql());
        profile.addChild(summaryProfile);
    }

    private void registerProfile() {
        initProfile();
        synchronized (fragmentProfiles) {
            for (RuntimeProfile p : fragmentProfiles) {
                profile.addChild(p);
            }
        }
        ProfileManager.getInstance().pushProfile(profile);
    }

    private Status moveTmpFiles() {
        Set<String> exportedTempFiles = job.getExportedTempFiles();
        String exportPath = job.getExportPath();
        for (String exportedTempFile : exportedTempFiles) {
            // move exportPath/__starrocks_tmp/file to exportPath/file
            // data_f8d0f324-83b3-11eb-9e09-02425ee98b69_0_0_0.csv.1615609467311
            String exportedFile = exportedTempFile.substring(exportedTempFile.lastIndexOf("/") + 1);
            // remove timestamp suffix
            // data_f8d0f324-83b3-11eb-9e09-02425ee98b69_0_0_0.csv
            exportedFile = exportedFile.substring(0, exportedFile.lastIndexOf("."));
            exportedFile = exportPath + exportedFile;
            boolean success = false;
            String failMsg = null;

            for (int i = 0; i < RETRY_NUM; ++i) {
                try {
                    // check export file exist
                    if (!job.getBrokerDesc().hasBroker()) {
                        if (HdfsUtil.checkPathExist(exportedFile, job.getBrokerDesc())) {
                            failMsg = exportedFile + " already exist";
                            LOG.warn("move {} to {} fail. job id: {}, retry: {}, msg: {}",
                                    exportedTempFile, exportedFile, job.getId(), i, failMsg);
                            break;
                        }
                        if (!HdfsUtil.checkPathExist(exportedTempFile, job.getBrokerDesc())) {
                            failMsg = exportedFile + " temp file not exist";
                            LOG.warn("move {} to {} fail. job id: {}, retry: {}, msg: {}",
                                    exportedTempFile, exportedFile, job.getId(), i, failMsg);
                            break;
                        }
                    } else {
                        if (BrokerUtil.checkPathExist(exportedFile, job.getBrokerDesc())) {
                            failMsg = exportedFile + " already exist";
                            LOG.warn("move {} to {} fail. job id: {}, retry: {}, msg: {}",
                                    exportedTempFile, exportedFile, job.getId(), i, failMsg);
                            break;
                        }
                        if (!BrokerUtil.checkPathExist(exportedTempFile, job.getBrokerDesc())) {
                            failMsg = exportedFile + " temp file not exist";
                            LOG.warn("move {} to {} fail. job id: {}, retry: {}, msg: {}",
                                    exportedTempFile, exportedFile, job.getId(), i, failMsg);
                            break;
                        }
                    }

                    // move
                    int timeoutMs = Math.min(Math.max(1, getLeftTimeSecond()), 3600) * 1000;
                    if (!job.getBrokerDesc().hasBroker()) {
                        HdfsUtil.rename(exportedTempFile, exportedFile, job.getBrokerDesc(), timeoutMs);
                    } else {
                        BrokerUtil.rename(exportedTempFile, exportedFile, job.getBrokerDesc(), timeoutMs);
                    }
                    job.addExportedFile(exportedFile);
                    success = true;
                    LOG.info("move {} to {} success. job id: {}", exportedTempFile, exportedFile, job.getId());
                    break;
                } catch (UserException e) {
                    failMsg = e.getMessage();
                    LOG.warn("move {} to {} fail. job id: {}, retry: {}, msg: {}",
                            exportedTempFile, exportedFile, job.getId(), i, failMsg);
                }
            }

            if (!success) {
                return new Status(TStatusCode.INTERNAL_ERROR, failMsg);
            }
        }

        job.clearExportedTempFiles();
        return Status.OK;
    }

    private class ExportExportingSubTask extends PriorityLeaderTask {
        private Coordinator coord;
        private final int taskIdx;
        private final int coordSize;
        private final ExportJob exportJob;

        public ExportExportingSubTask(Coordinator coord, int taskIdx, int coordSize, ExportJob exportJob) {
            this.coord = coord;
            this.taskIdx = taskIdx;
            this.coordSize = coordSize;
            this.exportJob = exportJob;
            this.signature = GlobalStateMgr.getCurrentState().getNextId();
        }

        public int getTaskIdx() {
            return taskIdx;
        }

        public String getQueryId() {
            return DebugUtil.printId(coord.getQueryId());
        }

        @Override
        protected void exec() {
            LOG.info("begin execute sub task, task idx: {}, task query id: {}", taskIdx, getQueryId());

            boolean success = false;
            String failMsg = null;

            for (int i = 0; i < RETRY_NUM; ++i) {
                // maybe job is cancelled by user
                if (job.isExportDone()) {
                    break;
                }

                try {
                    execOneCoord(coord);
                    if (coord.getExecStatus().ok()) {
                        success = true;
                        break;
                    }
                } catch (Exception e) {
                    failMsg = e.getMessage();
                    TUniqueId queryId = coord.getQueryId();
                    LOG.warn("export sub task internal error. task idx: {}, task query id: {}",
                            taskIdx, getQueryId(), e);
                }

                if (i < RETRY_NUM - 1) {
                    TUniqueId oldQueryId = coord.getQueryId();
                    UUID uuid = UUID.randomUUID();
                    // generate one new queryId here, to avoid being rejected by BE,
                    // because the request is considered as a repeat request.
                    // we make the high part of query id unchanged to facilitate tracing problem by log.
                    TUniqueId newQueryId = new TUniqueId(oldQueryId.hi, uuid.getLeastSignificantBits());
                    if (exportJob.needResetCoord()) {
                        try {
                            Coordinator newCoord = exportJob.resetCoord(taskIdx, newQueryId);
                            coord = newCoord;
                        } catch (UserException e) {
                            // still use old coord if there are any problems when reseting Coord
                            LOG.warn("fail to reset coord for task idx: {}, task query id: {}, reason: {}", taskIdx, 
                                    getQueryId(), e.getMessage());
                            coord.clearExportStatus();
                        }
                    } else {
                        coord.clearExportStatus();
                    }
                    coord.setQueryId(newQueryId);
                    LOG.warn(
                            "export sub task fail. err: {}. task idx: {}, task query id: {}. retry: {}, new query id: {}",
                            coord.getExecStatus().getErrorMsg(), taskIdx, DebugUtil.printId(oldQueryId), i,
                            DebugUtil.printId(coord.getQueryId()));
                }
            }

            if (!success) {
                onSubTaskFailed(coord, failMsg);
            }

            coord.getQueryProfile().getCounterTotalTime().setValue(TimeUtils.getEstimatedTime(job.getStartTimeMs()));
            coord.endProfile();
            synchronized (fragmentProfiles) {
                fragmentProfiles.add(coord.getQueryProfile());
            }
        }

        private void execOneCoord(Coordinator coord) throws Exception {
            TUniqueId queryId = coord.getQueryId();
            QeProcessorImpl.INSTANCE.registerQuery(queryId, coord);
            try {
                actualExecCoord(coord);
            } finally {
                QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
            }
        }

        private void actualExecCoord(Coordinator coord) throws Exception {
            int leftTimeSecond = getLeftTimeSecond();
            if (leftTimeSecond <= 0) {
                throw new UserException("timeout");
            }

            coord.setTimeout(leftTimeSecond);
            coord.exec();

            if (coord.join(leftTimeSecond)) {
                Status status = coord.getExecStatus();
                if (status.ok()) {
                    onSubTaskFinished(coord.getExportFiles());
                } else {
                    throw new UserException(status.getErrorMsg());
                }
            } else {
                throw new UserException("timeout");
            }
        }

        private void onSubTaskFinished(List<String> exportFiles) {
            job.addExportedTempFiles(exportFiles);
            synchronized (subTasksDoneSignal) {
                subTasksDoneSignal.markedCountDown(taskIdx, -1 /* dummy value */);
                job.setProgress((int) (coordSize - subTasksDoneSignal.getCount()) * 100 / coordSize);
            }
            LOG.info("export sub task finish. task idx: {}, task query id: {}", taskIdx, getQueryId());
        }

        private void onSubTaskFailed(Coordinator coordinator, String failMsg) {
            Status coordStatus = coordinator.getExecStatus();
            String taskFailMsg = "export job fail. query id: " + DebugUtil.printId(coordinator.getQueryId())
                    + ", fail msg: ";
            if (!Strings.isNullOrEmpty(coordStatus.getErrorMsg())) {
                taskFailMsg += coordStatus.getErrorMsg();
            } else {
                taskFailMsg += failMsg;
            }
            Status failStatus = new Status(TStatusCode.INTERNAL_ERROR, taskFailMsg);
            synchronized (subTasksDoneSignal) {
                subTasksDoneSignal.countDownToZero(failStatus);
            }
            LOG.warn("export sub task fail. task idx: {}, task query id: {}, err: {}",
                    taskIdx, getQueryId(), taskFailMsg);
        }
    }
}
