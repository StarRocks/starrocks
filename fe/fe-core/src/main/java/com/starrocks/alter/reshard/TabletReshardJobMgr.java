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

package com.starrocks.alter.reshard;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletReshardJobsResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class TabletReshardJobMgr extends FrontendDaemon implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(TabletReshardJobMgr.class);

    @SerializedName(value = "tabletReshardJobs")
    protected final Map<Long, TabletReshardJob> tabletReshardJobs = Maps.newConcurrentMap();

    // Original tablet id -> resharding tablet info
    protected final Map<Long, ReshardingTabletInfo> reshardingTabletInfos = Maps.newConcurrentMap();

    public TabletReshardJobMgr() {
        super("tablet-reshard-job-mgr", Config.tablet_reshard_job_scheduler_interval_ms);
    }

    public TabletReshardJob getTabletReshardJob(long jobId) {
        return tabletReshardJobs.get(jobId);
    }

    public Map<Long, TabletReshardJob> getTabletReshardJobs() {
        return tabletReshardJobs;
    }

    public ReshardingTablet getReshardingTablet(long tabletId, long visibleVersion) {
        ReshardingTabletInfo reshardingTabletInfo = reshardingTabletInfos.get(tabletId);
        if (reshardingTabletInfo == null) {
            return null;
        }

        if (visibleVersion < reshardingTabletInfo.getVisibleVersion()) {
            return null;
        }

        return reshardingTabletInfo.getReshardingTablet();
    }

    public void createTabletReshardJob(Database db, OlapTable table, SplitTabletClause splitTabletClause)
            throws StarRocksException {
        TabletReshardJob job = new SplitTabletJobFactory(db, table, splitTabletClause).createTabletReshardJob();
        addTabletReshardJob(job);
    }

    public void addTabletReshardJob(TabletReshardJob tabletReshardJob) throws StarRocksException {
        checkTabletReshardJob(tabletReshardJob);

        TabletReshardJob existingJob = tabletReshardJobs.putIfAbsent(tabletReshardJob.getJobId(), tabletReshardJob);
        if (existingJob != null) {
            throw new StarRocksException("Tablet reshard job is already existed. " + existingJob);
        }

        GlobalStateMgr.getCurrentState().getEditLog().logUpdateTabletReshardJob(tabletReshardJob);

        LOG.info("Added tablet reshard job. {}", tabletReshardJob);
    }

    public long getTotalParallelTablets() {
        long totalParallelTablets = 0;
        for (TabletReshardJob job : tabletReshardJobs.values()) {
            if (job.isDone()) {
                continue;
            }
            totalParallelTablets += job.getParallelTablets();
        }
        return totalParallelTablets;
    }

    public void replayUpdateTabletReshardJob(TabletReshardJob tabletReshardJob) {
        tabletReshardJob.replay();
        tabletReshardJobs.put(tabletReshardJob.getJobId(), tabletReshardJob);
    }

    public void replayRemoveTabletReshardJob(long tabletReshardJobId) {
        if (tabletReshardJobs.remove(tabletReshardJobId) == null) {
            // Should not happen, just add a warning log
            LOG.warn("Failed to find tablet reshard job {} when replaying remove tablet reshard job",
                    tabletReshardJobId);
        }
    }

    public TTabletReshardJobsResponse getAllJobsInfo() {
        TTabletReshardJobsResponse response = new TTabletReshardJobsResponse();
        response.status = new TStatus();
        response.status.setStatus_code(TStatusCode.OK);
        for (TabletReshardJob job : tabletReshardJobs.values()) {
            try {
                response.addToItems(job.getInfo());
            } catch (Exception e) {
                if (response.status.getStatus_code() == TStatusCode.OK) {
                    // if encouter any unexpected exception, set error status for response
                    response.status.setStatus_code(TStatusCode.INTERNAL_ERROR);
                    response.status.addToError_msgs(Strings.nullToEmpty(e.getMessage()));
                    LOG.warn("Encounter unexpected exception when getting tablet reshard jobs info. ", e);
                }
            }
        }
        return response;
    }

    protected void registerReshardingTablet(long tabletId, ReshardingTablet reshardingTablet, long visibleVersion) {
        reshardingTabletInfos.put(tabletId, new ReshardingTabletInfo(reshardingTablet, visibleVersion));
    }

    protected void unregisterReshardingTablet(long tabletId) {
        reshardingTabletInfos.remove(tabletId);
    }

    @Override
    protected void runAfterCatalogReady() {
        runTabletReshardJobs();
    }

    private void checkTabletReshardJob(TabletReshardJob tabletReshardJob) throws StarRocksException {
        if (tabletReshardJob.getJobState() != TabletReshardJob.JobState.PENDING) {
            throw new StarRocksException("Tablet reshard job state is not pending. " + tabletReshardJob);
        }

        long currentParallelTablets = getTotalParallelTablets();
        if (currentParallelTablets <= 0) { // No running jobs
            return;
        }

        long newParallelTablets = tabletReshardJob.getParallelTablets() + currentParallelTablets;
        if (newParallelTablets > Config.tablet_reshard_max_parallel_tablets) {
            throw new StarRocksException("Total parallel tablets exceed tablet_reshard_max_parallel_tablets: "
                    + Config.tablet_reshard_max_parallel_tablets);
        }
    }

    private void runTabletReshardJobs() {
        for (var iterator = tabletReshardJobs.entrySet().iterator(); iterator.hasNext(); /* */) {
            TabletReshardJob job = iterator.next().getValue();
            // Job is not done, run it
            if (!job.isDone()) {
                job.run();
                continue;
            }

            // Job is done, remove expired job
            if (job.isExpired()) {
                iterator.remove();
                GlobalStateMgr.getCurrentState().getEditLog().logRemoveTabletReshardJob(job.getJobId());
                LOG.info("Removed expired tablet reshard job. {}", job);
            }
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        for (TabletReshardJob job : tabletReshardJobs.values()) {
            if (job.isDone()) {
                continue;
            }

            job.registerReshardingTabletsOnRestart();
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.TABLET_RESHARD_JOB_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        TabletReshardJobMgr tabletReshardJobMgr = reader.readJson(TabletReshardJobMgr.class);
        tabletReshardJobs.putAll(tabletReshardJobMgr.tabletReshardJobs);
        reshardingTabletInfos.putAll(tabletReshardJobMgr.reshardingTabletInfos);
    }
}
