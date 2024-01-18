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

package com.starrocks.replication;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.AlreadyExistsException;
import com.starrocks.common.exception.UserException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.RemoteSnapshotTask;
import com.starrocks.task.ReplicateSnapshotTask;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TTableReplicationRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ReplicationMgr extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(ReplicationMgr.class);

    @SerializedName(value = "runningJobs")
    private final Map<Long, ReplicationJob> runningJobs = Maps.newConcurrentMap(); // Running jobs

    @SerializedName(value = "committedJobs")
    private final Map<Long, ReplicationJob> committedJobs = Maps.newConcurrentMap(); // Committed jobs

    @SerializedName(value = "abortedJobs")
    private final Map<Long, ReplicationJob> abortedJobs = Maps.newConcurrentMap(); // Aborted jobs, will retry later

    public ReplicationMgr() {
        super("ReplicationMgr", Config.replication_interval_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        List<ReplicationJob> toRemovedJobs = Lists.newArrayList();
        for (ReplicationJob job : runningJobs.values()) {
            job.run();

            ReplicationJobState state = job.getState();
            if (state.equals(ReplicationJobState.COMMITTED)) {
                toRemovedJobs.add(job);
                committedJobs.put(job.getTableId(), job);
            } else if (state.equals(ReplicationJobState.ABORTED)) {
                toRemovedJobs.add(job);
                abortedJobs.put(job.getTableId(), job);
            }
        }

        for (ReplicationJob job : toRemovedJobs) {
            runningJobs.remove(job.getTableId(), job);
        }
    }

    public void addReplicationJob(TTableReplicationRequest request) throws UserException {
        ReplicationJob job = new ReplicationJob(request);
        addReplicationJob(job);
    }

    public void addReplicationJob(ReplicationJob job) throws AlreadyExistsException {
        // Limit replication job size
        if (runningJobs.size() >= Config.replication_max_parallel_table_count) {
            throw new RuntimeException(
                    "The replication jobs exceeds the replication_max_parallel_table_count: "
                            + Config.replication_max_parallel_table_count);
        }

        // Limit replication data size
        long replicatingDataSizeMB = getReplicatingDataSize() / 1048576;
        if (replicatingDataSizeMB >= Config.replication_max_parallel_data_size_mb) {
            throw new RuntimeException("The replicating data size in all running replication jobs "
                    + replicatingDataSizeMB
                    + "(MB) exceeds replication_max_parallel_data_size_mb: "
                    + Config.replication_max_parallel_data_size_mb);
        }

        if (runningJobs.putIfAbsent(job.getTableId(), job) != null) {
            throw new AlreadyExistsException("Replication job of table " + job.getTableId() + " is already running");
        }

        committedJobs.remove(job.getTableId()); // If the job is committed before, remove it from committed jobs
        abortedJobs.remove(job.getTableId()); // If the job is aborted before, remove it from aborted jobs

        LOG.info("Added replication job, database id: {}, table id: {}, "
                + "replication data size: {}, current replicating data size: {}(MB)",
                job.getDatabaseId(), job.getTableId(), job.getReplicationDataSize(), replicatingDataSizeMB);
    }

    public boolean hasRunningJobs() {
        return !runningJobs.isEmpty();
    }

    public boolean hasFailedJobs() {
        return !abortedJobs.isEmpty();
    }

    public void clearFinishedJobs() {
        committedJobs.clear();
        abortedJobs.clear();
        GlobalStateMgr.getServingState().getEditLog().logReplicationJob(null);
    }

    public void cancelRunningJobs() {
        List<ReplicationJob> toRemovedJobs = Lists.newArrayList();
        for (ReplicationJob job : runningJobs.values()) {
            job.cancel();

            if (job.getState().equals(ReplicationJobState.ABORTED)) {
                toRemovedJobs.add(job);
                abortedJobs.put(job.getTableId(), job);
            }
        }

        for (ReplicationJob job : toRemovedJobs) {
            runningJobs.remove(job.getTableId(), job);
        }
    }

    public void finishRemoteSnapshotTask(RemoteSnapshotTask task, TFinishTaskRequest request) {
        ReplicationJob job = runningJobs.get(task.getTableId());
        if (job == null) {
            LOG.warn("Remote snapshot task {} is finished, but cannot find it in replication jobs", task);
            return;
        }

        job.finishRemoteSnapshotTask(task, request);
    }

    public void finishReplicateSnapshotTask(ReplicateSnapshotTask task, TFinishTaskRequest request) {
        ReplicationJob job = runningJobs.get(task.getTableId());
        if (job == null) {
            LOG.warn("Replicate snapshot task {} is finished, but cannot find it in replication jobs", task);
            return;
        }

        job.finishReplicateSnapshotTask(task, request);
    }

    public void replayReplicationJob(ReplicationJob replicationJob) {
        if (replicationJob == null) {
            committedJobs.clear();
            abortedJobs.clear();
            return;
        }

        if (replicationJob.getState().equals(ReplicationJobState.COMMITTED)) {
            committedJobs.put(replicationJob.getTableId(), replicationJob);
            runningJobs.remove(replicationJob.getTableId());
        } else if (replicationJob.getState().equals(ReplicationJobState.ABORTED)) {
            abortedJobs.put(replicationJob.getTableId(), replicationJob);
            runningJobs.remove(replicationJob.getTableId());
        } else {
            runningJobs.put(replicationJob.getTableId(), replicationJob);
        }
    }

    private long getReplicatingDataSize() {
        long replicatingDataSize = 0;
        for (ReplicationJob job : runningJobs.values()) {
            replicatingDataSize += job.getReplicationDataSize();
        }
        return replicatingDataSize;
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.REPLICATION_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        ReplicationMgr replicationMgr = reader.readJson(ReplicationMgr.class);
        runningJobs.putAll(replicationMgr.runningJobs);
        committedJobs.putAll(replicationMgr.committedJobs);
        abortedJobs.putAll(replicationMgr.abortedJobs);
    }
}
