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


package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.CreateInsertOverwriteJobLog;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InsertOverwriteJobMgr implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteJobMgr.class);

    @SerializedName(value = "overwriteJobMap")
    private Map<Long, InsertOverwriteJob> overwriteJobMap;

    // tableId -> insert overwrite job id list
    @SerializedName(value = "tableToOverwriteJobs")
    private Map<Long, List<Long>> tableToOverwriteJobs;

    private transient ExecutorService cancelJobExecutorService;

    // store the jobs which are still running after FE restart
    // it is used when replay, so no need to add concurrent control for it
    private transient List<InsertOverwriteJob> runningJobs;

    private transient ReentrantReadWriteLock lock;

    public InsertOverwriteJobMgr() {
        this.overwriteJobMap = Maps.newConcurrentMap();
        this.tableToOverwriteJobs = Maps.newHashMap();
        ThreadFactory threadFactory = new DefaultThreadFactory("cancel-thread");
        this.cancelJobExecutorService = Executors.newSingleThreadExecutor(threadFactory);
        this.runningJobs = Lists.newArrayList();
        this.lock = new ReentrantReadWriteLock();
    }

    public void executeJob(ConnectContext context, StmtExecutor stmtExecutor, InsertOverwriteJob job) throws Exception {
        boolean registered = registerOverwriteJob(job);
        if (!registered) {
            LOG.warn("register insert overwrite job:{} failed", job.getJobId());
            throw new RuntimeException("register insert overwrite job failed");
        }
        try {
            InsertOverwriteJobRunner jobRunner =
                    new InsertOverwriteJobRunner(job, context, stmtExecutor);
            jobRunner.run();
        } finally {
            deregisterOverwriteJob(job.getJobId());
        }
    }

    public boolean registerOverwriteJob(InsertOverwriteJob job) {
        lock.writeLock().lock();
        try {
            if (overwriteJobMap.containsKey(job.getJobId())) {
                LOG.warn("insert overwrite job:{} is running", job.getJobId());
                return false;
            }
            overwriteJobMap.put(job.getJobId(), job);
            List<Long> tableJobs = tableToOverwriteJobs.get(job.getTargetTableId());
            if (tableJobs == null) {
                tableJobs = Lists.newArrayList();
                tableToOverwriteJobs.put(job.getTargetTableId(), tableJobs);
            }
            tableJobs.add(job.getJobId());
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean deregisterOverwriteJob(long jobid) {
        lock.writeLock().lock();
        try {
            if (!overwriteJobMap.containsKey(jobid)) {
                return true;
            }
            InsertOverwriteJob job = overwriteJobMap.get(jobid);
            List<Long> tableJobs = tableToOverwriteJobs.get(job.getTargetTableId());
            if (tableJobs != null) {
                tableJobs.remove(job.getJobId());
                if (tableJobs.isEmpty()) {
                    tableToOverwriteJobs.remove(job.getTargetTableId());
                }
            }
            overwriteJobMap.remove(jobid);
            return true;
        } catch (Exception e) {
            LOG.warn("deregister overwrite job failed", e);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean hasRunningOverwriteJob(long tableId) {
        lock.readLock().lock();
        try {
            return tableToOverwriteJobs.containsKey(tableId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void replayCreateInsertOverwrite(CreateInsertOverwriteJobLog jobInfo) {
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(jobInfo.getJobId(),
                jobInfo.getDbId(), jobInfo.getTableId(), jobInfo.getTargetPartitionIds());
        boolean registered = registerOverwriteJob(insertOverwriteJob);
        if (!registered) {
            LOG.warn("register insert overwrite job failed. jobId:{}", insertOverwriteJob.getJobId());
            return;
        }
        if (runningJobs == null) {
            runningJobs = Lists.newArrayList();
        }
        runningJobs.add(insertOverwriteJob);
    }

    public void replayInsertOverwriteStateChange(InsertOverwriteStateChangeInfo info) {
        InsertOverwriteJob job = getInsertOverwriteJob(info.getJobId());
        if (job == null) {
            LOG.info("cannot find job: {}, ignore", info);
            return;
        }
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
        runner.replayStateChange(info);
        if (job.isFinished()) {
            deregisterOverwriteJob(job.getJobId());
            if (runningJobs != null) {
                runningJobs.remove(job);
            }
        }
    }

    public void cancelRunningJobs() {
        // resubmit running insert overwrite jobs
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        cancelJobExecutorService.submit(() -> {
            try {
                // wait until serving catalog is ready
                while (!GlobalStateMgr.getServingState().isReady()) {
                    try {
                        // not return, but sleep a while. to avoid some thread with large running interval will
                        // wait for a long time to start again.
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOG.warn("InsertOverwriteJobManager runAfterCatalogReady interrupted exception.", e);
                    }
                }
                if (runningJobs != null) {
                    for (InsertOverwriteJob job : runningJobs) {
                        LOG.info("start to cancel unfinished insert overwrite job:{}", job.getJobId());
                        try {
                            InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job);
                            runner.cancel();
                        } catch (Exception e) {
                            LOG.warn("cancel insert overwrite job:{} failed.", job.getJobId(), e);
                        } finally {
                            deregisterOverwriteJob(job.getJobId());
                        }
                    }
                    runningJobs.clear();
                }
            } catch (Exception e) {
                LOG.warn("cancel running jobs failed. cancel thread will exit", e);
            }
        });
    }

    public long getJobNum() {
        return overwriteJobMap.size();
    }

    public long getRunningJobSize() {
        return runningJobs.size();
    }

    public InsertOverwriteJob getInsertOverwriteJob(long jobId) {
        lock.readLock().lock();
        try {
            return overwriteJobMap.get(jobId);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static InsertOverwriteJobMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        InsertOverwriteJobMgr jobManager = GsonUtils.GSON.fromJson(json, InsertOverwriteJobMgr.class);
        return jobManager;
    }

    @Override
    public void gsonPostProcess() {
        if (!GlobalStateMgr.isCheckpointThread()) {
            if (runningJobs == null) {
                runningJobs = Lists.newArrayList();
            }
            for (InsertOverwriteJob job : overwriteJobMap.values()) {
                if (!job.isFinished()) {
                    LOG.info("add insert overwrite job:{} to runningJobs, state:{}",
                            job.getJobId(), job.getJobState());
                    runningJobs.add(job);
                }
            }
        }
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.INSERT_OVERWRITE_JOB_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        InsertOverwriteJobMgr catalog = reader.readJson(InsertOverwriteJobMgr.class);
        overwriteJobMap = catalog.overwriteJobMap;
        tableToOverwriteJobs = catalog.tableToOverwriteJobs;
        runningJobs = catalog.runningJobs;
    }
}
