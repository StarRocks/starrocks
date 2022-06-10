// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.CreateInsertOverwriteJobInfo;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InsertOverwriteJobManager implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteJobManager.class);

    @SerializedName(value = "overwriteJobMap")
    private Map<Long, InsertOverwriteJob> overwriteJobMap;

    // tableId -> partitionId list
    @SerializedName(value = "partitionsWithOverwrite")
    private Map<Long, List<Long>> partitionsWithOverwrite;

    @SerializedName(value = "jobNum")
    private transient long jobNum;

    private ExecutorService cancelJobExecutorService;

    private List<InsertOverwriteJob> runningJobs;

    // private Map<Long, Long> jobToTxnId;

    private ReentrantReadWriteLock lock;

    public InsertOverwriteJobManager() {
        this.overwriteJobMap = Maps.newHashMap();
        this.partitionsWithOverwrite = Maps.newHashMap();
        ThreadFactory threadFactory = new DefaultThreadFactory("cancel-thread");
        this.cancelJobExecutorService = Executors.newSingleThreadExecutor(threadFactory);
        this.runningJobs = Lists.newArrayList();
        this.lock = new ReentrantReadWriteLock();
        this.jobNum = 0;
    }

    public void submitJob(ConnectContext context, StmtExecutor stmtExecutor, InsertOverwriteJob job) throws Exception {
        boolean registered = registerOverwriteJob(job);
        if (!registered) {
            LOG.warn("register insert overwrite job:{} failed", job.getJobId());
            throw new RuntimeException("register insert overwrite job failed");
        }
        try {
            // get db and table
            Database database = MetaUtils.getDatabase(context, job.getTargetDbId());
            OlapTable table = (OlapTable) MetaUtils.getTable(context, database.getId(), job.getTargetTableId());
            InsertOverwriteJobRunner jobRunner = new InsertOverwriteJobRunner(job, context, stmtExecutor, database, table);
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
            List<Long> runningPartitions = partitionsWithOverwrite.getOrDefault(job.getTargetTableId(), Lists.newArrayList());
            if (job.getOriginalTargetPartitionIds() != null) {
                runningPartitions.addAll(job.getOriginalTargetPartitionIds());
            }
            partitionsWithOverwrite.put(job.getTargetTableId(), runningPartitions);
            jobNum++;
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
            List<Long> partitionIds = partitionsWithOverwrite.get(job.getTargetTableId());
            if (partitionIds != null) {
                partitionIds.removeAll(job.getOriginalTargetPartitionIds());
                if (partitionIds.isEmpty()) {
                    partitionsWithOverwrite.remove(job.getTargetTableId());
                }
            } else {
                partitionsWithOverwrite.remove(job.getTargetTableId());
            }
            overwriteJobMap.remove(jobid);
            jobNum--;
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
            return partitionsWithOverwrite.containsKey(tableId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void replayCreateInsertOverwrite(CreateInsertOverwriteJobInfo jobInfo) {
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
        if (!GlobalStateMgr.isCheckpointThread()) {
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
    }

    public long getJobNum() {
        return jobNum;
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

    public static InsertOverwriteJobManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        InsertOverwriteJobManager jobManager = GsonUtils.GSON.fromJson(json, InsertOverwriteJobManager.class);
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
}
