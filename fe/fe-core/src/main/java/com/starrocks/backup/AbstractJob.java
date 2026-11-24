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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/backup/AbstractJob.java

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

package com.starrocks.backup;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.backup.LakeBackupJob;
import com.starrocks.lake.backup.LakeRestoreJob;
import com.starrocks.persist.image.ProtoKinds;
import com.starrocks.persist.proto.AbstractJobCorePB;
import com.starrocks.persist.proto.AbstractJobPB;
import com.starrocks.persist.proto.BackupTaskErrorsPB;
import com.starrocks.server.GlobalStateMgr;

import java.util.HashMap;
import java.util.Map;

/*
 * The design of JobI is as follows
 * 1. Here are only two methods: run() and cancel() that can modify the internal state of a Job.
 *    And each method is implemented as synchronized to avoid handling concurrent modify things.
 *
 * 2. isDone() method is used to check whether we can submit the next job.
 */
public abstract class AbstractJob implements Writable {

    public enum JobType {
        BACKUP, RESTORE, LAKE_BACKUP, LAKE_RESTORE
    }

    @SerializedName(value = "type")
    protected JobType type;

    // must be set right before job's running
    protected GlobalStateMgr globalStateMgr;
    // repo will be set at first run()
    protected Repository repo;
    @SerializedName(value = "repoId")
    protected long repoId;

    /*
     * In BackupJob, jobId will be generated every time before we call prepareAndSendSnapshotTask();
     * Because prepareAndSendSnapshotTask() may be called several times due to FE restart.
     * And each time this method is called, the snapshot tasks will be sent with (maybe) different
     * version. So we have to use different job id to identify the tasks in different batches.
     */
    @SerializedName(value = "jobId")
    protected long jobId = -1;

    @SerializedName(value = "label")
    protected String label;
    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "dbName")
    protected String dbName;

    @SerializedName("status")
    protected Status status = Status.OK;

    @SerializedName(value = "createTime")
    protected long createTime = -1;
    @SerializedName(value = "finishedTime")
    protected long finishedTime = -1;
    @SerializedName(value = "timeoutMs")
    protected long timeoutMs;

    // task signature -> <finished num / total num>
    protected Map<Long, Pair<Integer, Integer>> taskProgress = Maps.newConcurrentMap();

    // save err msg of tasks
    @SerializedName(value = "taskErrMsg")
    protected Map<Long, String> taskErrMsg = Maps.newHashMap();

    protected AbstractJob(JobType type) {
        this.type = type;
    }

    protected AbstractJob(JobType type, String label, long dbId, String dbName,
                          long timeoutMs, GlobalStateMgr globalStateMgr, long repoId) {
        this.type = type;
        this.label = label;
        this.dbId = dbId;
        this.dbName = dbName;
        this.createTime = System.currentTimeMillis();
        this.timeoutMs = timeoutMs;
        this.globalStateMgr = globalStateMgr;
        this.repoId = repoId;
    }

    protected AbstractJob(AbstractJob job) {
        this.type = job.type;
        this.repoId = job.repoId;
        this.jobId = job.jobId;
        this.label = job.label;
        this.dbId = job.dbId;
        this.dbName = job.dbName;
        this.status = job.status;
        this.createTime = job.createTime;
        this.finishedTime = job.finishedTime;
        this.timeoutMs = job.timeoutMs;
        this.taskErrMsg = job.taskErrMsg;
    }

    public JobType getType() {
        return type;
    }

    public long getJobId() {
        return jobId;
    }

    public String getLabel() {
        return label;
    }

    public long getDbId() {
        return dbId;
    }

    public String getDbName() {
        return dbName;
    }

    public Status getStatus() {
        return status;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getFinishedTime() {
        return finishedTime;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setGlobalStateMgr(GlobalStateMgr globalStateMgr) {
        this.globalStateMgr = globalStateMgr;
    }

    public long getRepoId() {
        return repoId;
    }

    public abstract void run();

    public abstract Status cancel();

    public abstract void replayRun();

    public abstract void replayCancel();

    public abstract boolean isDone();

    public abstract boolean isPending();

    public abstract boolean isCancelled();




    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(type.name());
        sb.append(" repo id: ").append(repoId).append(", label: ").append(label);
        sb.append(", job id: ").append(jobId).append(", db id: ").append(dbId).append(", db name: ").append(dbName);
        sb.append(", status: ").append(status);
        sb.append(", timeout: ").append(timeoutMs);
        return sb.toString();
    }

    protected AbstractJobCorePB toAbstractJobCoreProto() {
        AbstractJobCorePB.Builder builder = AbstractJobCorePB.newBuilder();
        builder.setRepoId(repoId);
        builder.setJobId(jobId);
        if (label != null) {
            builder.setLabel(label);
        }
        builder.setDbId(dbId);
        if (dbName != null) {
            builder.setDbName(dbName);
        }
        if (status != null) {
            builder.setStatus(status.toProto());
        }
        builder.setCreateTime(createTime);
        builder.setFinishedTime(finishedTime);
        builder.setTimeoutMs(timeoutMs);
        if (taskErrMsg != null) {
            builder.setTaskErrMsg(BackupTaskErrorsPB.newBuilder().putAllValues(taskErrMsg).build());
        }
        return builder.build();
    }

    protected void fillFromAbstractJobCoreProto(AbstractJobCorePB pb) {
        if (pb.hasRepoId()) {
            repoId = pb.getRepoId();
        }
        if (pb.hasJobId()) {
            jobId = pb.getJobId();
        }
        if (pb.hasLabel()) {
            label = pb.getLabel();
        } else {
            label = null;
        }
        if (pb.hasDbId()) {
            dbId = pb.getDbId();
        }
        if (pb.hasDbName()) {
            dbName = pb.getDbName();
        } else {
            dbName = null;
        }
        if (pb.hasStatus()) {
            status = Status.fromProto(pb.getStatus());
        }
        if (pb.hasCreateTime()) {
            createTime = pb.getCreateTime();
        }
        if (pb.hasFinishedTime()) {
            finishedTime = pb.getFinishedTime();
        }
        if (pb.hasTimeoutMs()) {
            timeoutMs = pb.getTimeoutMs();
        }
        if (pb.hasTaskErrMsg()) {
            taskErrMsg = new HashMap<>(pb.getTaskErrMsg().getValuesMap());
        } else {
            taskErrMsg = null;
        }
    }

    public AbstractJobPB toProto() {
        AbstractJobPB.Builder builder = AbstractJobPB.newBuilder();
        builder.setCore(toAbstractJobCoreProto());
        fillProtoVariant(builder);
        return builder.build();
    }

    protected void fillProtoVariant(AbstractJobPB.Builder builder) {
        throw new IllegalStateException("cannot persist " + getClass().getName());
    }

    protected void fillFromVariantProto(AbstractJobPB pb) {
        throw new IllegalStateException("cannot load " + getClass().getName());
    }

    public static AbstractJob fromProto(AbstractJobPB pb) {
        ProtoKinds.checkKnown(pb.getKind(), pb.getKindValue());
        AbstractJob value;
        switch (pb.getKind()) {
            case BACKUP_JOB:
                if (!pb.hasBackupJob()) {
                    throw new IllegalArgumentException("missing backupJob variant");
                }
                value = new BackupJob();
                value.type = JobType.BACKUP;
                break;
            case RESTORE_JOB:
                if (!pb.hasRestoreJob()) {
                    throw new IllegalArgumentException("missing restoreJob variant");
                }
                value = new RestoreJob();
                value.type = JobType.RESTORE;
                break;
            case LAKE_BACKUP_JOB:
                if (!pb.hasLakeBackupJob()) {
                    throw new IllegalArgumentException("missing lakeBackupJob variant");
                }
                value = new LakeBackupJob();
                value.type = JobType.LAKE_BACKUP;
                break;
            case LAKE_RESTORE_JOB:
                if (!pb.hasLakeRestoreJob()) {
                    throw new IllegalArgumentException("missing lakeRestoreJob variant");
                }
                value = new LakeRestoreJob();
                value.type = JobType.LAKE_RESTORE;
                break;
            default:
                throw new IllegalArgumentException("unknown AbstractJob kind " + pb.getKind());
        }
        value.fillFromAbstractJobCoreProto(pb.getCore());
        value.fillFromVariantProto(pb);
        return value;
    }
}
