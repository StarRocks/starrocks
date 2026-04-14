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

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manage FE-side MV compatibility metadata such as legacy maintenance job replay/state
 * and MV timeliness tracking.
 */
public class MaterializedViewMgr {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewMgr.class);

    // MV's global timeliness info manager
    private final MVTimelinessMgr mvTimelinessMgr = new MVTimelinessMgr();
    // MV's maintenance job
    private final Map<MvId, MVMaintenanceJob> jobMap = new ConcurrentHashMap<>();

    /**
     * Reload jobs from meta store
     */
    public long reload(DataInputStream input, long checksum) throws IOException {
        Preconditions.checkState(jobMap.isEmpty());

        try {
            String str = Text.readString(input);
            SerializedJobs data = GsonUtils.GSON.fromJson(str, SerializedJobs.class);
            if (CollectionUtils.isNotEmpty(data.jobList)) {
                for (MVMaintenanceJob job : data.jobList) {
                    // NOTE: job's view is not serialized, cannot use it directly!
                    MvId mvId = new MvId(job.getDbId(), job.getViewId());
                    job.restore();
                    jobMap.put(mvId, job);
                }
                LOG.info("reload MV maintenance jobs: {}", data.jobList);
                LOG.debug("reload MV maintenance job details: {}", str);
            }
            checksum ^= data.jobList.size();
        } catch (EOFException ignored) {
        }
        return checksum;
    }

    /**
     * Replay from journal
     */
    public void replay(MVMaintenanceJob job) throws IOException {
        try {
            job.restore();
            // NOTE: job's view is not serialized, cannot use it directly!
            MvId mvId = new MvId(job.getDbId(), job.getViewId());
            jobMap.put(mvId, job);
            LOG.info("Replay MV maintenance jobs: {}", job);
        } catch (Exception e) {
            LOG.warn("Replay MV maintenance job failed: {}", job);
            LOG.warn("Failed to replay MV maintenance job", e);
        }
    }

    /**
     * Replay epoch from journal
     */
    public void replayEpoch(MVEpoch epoch) throws IOException {
        // TODO: Make it works.
        try {
            MvId mvId = new MvId(epoch.getDbId(), epoch.getMvId());
            Preconditions.checkState(jobMap.containsKey(mvId));
            MVMaintenanceJob job = jobMap.get(mvId);
            job.setEpoch(epoch);
            LOG.info("Replay MV epoch: {}", job);
        } catch (Exception e) {
            LOG.warn("Replay MV epoch failed: {}", epoch);
            LOG.warn("Failed to replay MV epoch", e);
        }
    }

    /**
     * Store jobs in meta store
     */
    public long store(DataOutputStream output, long checksum) throws IOException {
        SerializedJobs data = new SerializedJobs();
        data.jobList = new ArrayList<>(jobMap.values());
        String json = GsonUtils.GSON.toJson(data);
        Text.writeString(output, json);
        checksum ^= data.jobList.size();
        return checksum;
    }

    /**
     * Prepare maintenance work for materialized view
     * 1. Create Intermediate Materialized Table(IMT)
     * 2. Store metadata of Job in metastore
     * 3. Create job for MV maintenance
     * 4. Compute physical topology of MV job
     */
    void prepareMaintenanceWork(CreateMaterializedViewStatement stmt,
                                MaterializedView view,
                                ExecPlan execPlan,
                                ColumnRefFactory columnRefFactory)
            throws DdlException {
        if (!view.getRefreshScheme().isIncremental()) {
            return;
        }

        try {
            if (jobMap.get(view.getMvId()) != null) {
                throw new DdlException("MV already existed");
            }
            // Prepare the table sink for exec-plan
            PlanFragment sinkFragment = execPlan.getTopFragment();
            OlapTableSink tableSink = (OlapTableSink) sinkFragment.getSink();
            tableSink.setDstTable(view);
            tableSink.setPartitionIds(view.getAllPartitionIds());

            // Create the job but not execute it
            MVMaintenanceJob job = new MVMaintenanceJob(view, execPlan);
            // TODO(murphy) atomic persist the meta of MV (IMT, MaintenancePlan) along with materialized view
            AtomicBoolean jobExist = new AtomicBoolean(false);
            GlobalStateMgr.getCurrentState().getEditLog().logMVJobState(job, wal -> {
                if (jobMap.putIfAbsent(view.getMvId(), job) != null) {
                    jobExist.set(true);
                }
            });
            if (!jobExist.get()) {
                IMTCreator.createIMT(stmt, view, execPlan, columnRefFactory);
            }
            LOG.info("create the maintenance job for MV: {}", view.getName());
        } catch (Exception e) {
            jobMap.remove(view.getMvId());
            LOG.warn("prepare MV {} failed, ", view.getName(), e);
        }
    }

    /**
     * Stop the maintenance job for MV after dropped
     */
    public void stopMaintainMV(MaterializedView view) {
        MaterializedView.MvRefreshScheme refreshScheme = view.getRefreshScheme();
        if (refreshScheme != null && !refreshScheme.isIncremental()) {
            return;
        }
        MVMaintenanceJob job = getJob(view.getMvId());
        if (job == null) {
            LOG.warn("MV job not exists {}", view.getName());
            return;
        }
        job.stopJob();
        jobMap.remove(view.getMvId());
        LOG.info("Remove maintenance job for mv: {}", view.getName());
    }

    /**
     * Re-Schedule the MV maintenance job
     * 1. Tablet migration
     * 2. Cluster rescale
     * 3. Temporary failure happens(FE or BE)
     */
    public void reScheduleMaintainMV(MvId mvId) {
        throw UnsupportedException.unsupportedException("re-schedule mv job is not supported");
    }

    /**
     * Rebuild the maintenance job for critical changes
     * 1. Schema change of MV (should be optimized later)
     * 2. Binlog lost
     * 3. Base table schema change (should be optimized later)
     */
    public void rebuildMaintainMV(MaterializedView view) {
        if (!view.getRefreshScheme().isIncremental()) {
            return;
        }
        throw UnsupportedException.unsupportedException("rebuild mv job is not supported");
    }

    protected MVMaintenanceJob getJob(MvId mvId) {
        return jobMap.get(mvId);
    }

    // fot test
    protected void clearJobs() {
        jobMap.clear();
    }

    static class SerializedJobs {
        @SerializedName("jobList")
        List<MVMaintenanceJob> jobList;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        int numJson = 1 + jobMap.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.MATERIALIZED_VIEW_MGR, numJson);
        writer.writeInt(jobMap.size());
        for (MVMaintenanceJob mvMaintenanceJob : jobMap.values()) {
            writer.writeJson(mvMaintenanceJob);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        reader.readCollection(MVMaintenanceJob.class, mvMaintenanceJob -> {
            // NOTE: job's view is not serialized, cannot use it directly!
            MvId mvId = new MvId(mvMaintenanceJob.getDbId(), mvMaintenanceJob.getViewId());
            mvMaintenanceJob.restore();
            jobMap.put(mvId, mvMaintenanceJob);
        });
    }

    public MVTimelinessMgr getMvTimelinessMgr() {
        return mvTimelinessMgr;
    }

    public void triggerTimelessInfoEvent(MaterializedView mv, MVTimelinessMgr.MVChangeEvent event) {
        mvTimelinessMgr.triggerEvent(mv, event);
    }
}
