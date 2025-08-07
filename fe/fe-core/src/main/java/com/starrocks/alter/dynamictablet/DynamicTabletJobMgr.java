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

package com.starrocks.alter.dynamictablet;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.thrift.TDynamicTabletJobsResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class DynamicTabletJobMgr extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(DynamicTabletJobMgr.class);

    @SerializedName(value = "dynamicTabletJobs")
    protected final Map<Long, DynamicTabletJob> dynamicTabletJobs = Maps.newConcurrentMap();

    protected final Map<Long, DynamicTablet> dynamicTablets = Maps.newConcurrentMap();

    public DynamicTabletJobMgr() {
        super("DynamicTabletJobMgr", Config.dynamic_tablet_job_scheduler_interval_ms);
    }

    public DynamicTabletJob getDynamicTabletJob(long dynamicJobId) {
        return dynamicTabletJobs.get(dynamicJobId);
    }

    public Map<Long, DynamicTabletJob> getDynamicTabletJobs() {
        return dynamicTabletJobs;
    }

    public DynamicTablet getDynamicTablet(long tabletId) {
        return dynamicTablets.get(tabletId);
    }

    public void createDynamicTabletJob(Database db, OlapTable table, SplitTabletClause splitTabletClause)
            throws StarRocksException {
        DynamicTabletJob job = new SplitTabletJobFactory(db, table, splitTabletClause).createDynamicTabletJob();
        addDynamicTabletJob(job);
    }

    public void addDynamicTabletJob(DynamicTabletJob dynamicTabletJob) throws StarRocksException {
        synchronized (this) {
            checkDynamicTabletJob(dynamicTabletJob);

            DynamicTabletJob existingJob = dynamicTabletJobs.putIfAbsent(dynamicTabletJob.getJobId(), dynamicTabletJob);
            if (existingJob != null) {
                throw new StarRocksException("Dynamic tablet job is already existed. " + existingJob);
            }
        }

        GlobalStateMgr.getCurrentState().getEditLog().logUpdateDynamicTabletJob(dynamicTabletJob);

        LOG.info("Added dynamic tablet job. {}", dynamicTabletJob);
    }

    public long getTotalParalelTablets() {
        long totalParallelTablets = 0;
        for (DynamicTabletJob job : dynamicTabletJobs.values()) {
            if (job.isDone()) {
                continue;
            }
            totalParallelTablets += job.getParallelTablets();
        }
        return totalParallelTablets;
    }

    public void replayUpdateDynamicTabletJob(DynamicTabletJob dynamicTabletJob) {
        dynamicTabletJob.replay();
        dynamicTabletJobs.put(dynamicTabletJob.getJobId(), dynamicTabletJob);
    }

    public void replayRemoveDynamicTabletJob(long dynamicTabletJobId) {
        if (dynamicTabletJobs.remove(dynamicTabletJobId) == null) {
            // Should not happen, just add a warnning log
            LOG.warn("Failed to find dynamic tablet job {} when replaying remove dynamic tablet job",
                    dynamicTabletJobId);

        }
    }

    public TDynamicTabletJobsResponse getAllJobsInfo() {
        TDynamicTabletJobsResponse response = new TDynamicTabletJobsResponse();
        response.status = new TStatus();
        response.status.setStatus_code(TStatusCode.OK);
        for (DynamicTabletJob job : dynamicTabletJobs.values()) {
            try {
                response.addToItems(job.getInfo());
            } catch (Exception e) {
                if (response.status.getStatus_code() == TStatusCode.OK) {
                    // if encouter any unexpected exception, set error status for response
                    response.status.setStatus_code(TStatusCode.INTERNAL_ERROR);
                    response.status.addToError_msgs(Strings.nullToEmpty(e.getMessage()));
                    LOG.warn("Encounter unexpected exception when getting dynamic tablet jobs info. ", e);
                }
            }
        }
        return response;
    }

    @Override
    protected void runAfterCatalogReady() {
        runDynamicTabletJobs();
    }

    private void checkDynamicTabletJob(DynamicTabletJob dynamicTabletJob) throws StarRocksException {
        if (dynamicTabletJob.getJobState() != DynamicTabletJob.JobState.PENDING) {
            throw new StarRocksException("Dynamic tablet job state is not pending. " + dynamicTabletJob);
        }

        long totalParallelTablets = dynamicTabletJob.getParallelTablets();
        for (DynamicTabletJob job : dynamicTabletJobs.values()) {
            if (job.isDone()) {
                continue;
            }

            if (job.getDbId() == dynamicTabletJob.getDbId() && job.getTableId() == dynamicTabletJob.getTableId()) {
                throw new StarRocksException("Dynamic tablet job is not done. " + job);
            }
            totalParallelTablets += job.getParallelTablets();
        }

        if (totalParallelTablets > Config.dynamic_tablet_max_parallel_tablets) {
            throw new StarRocksException("Total parallel tablets exceed dynamic_tablet_max_parallel_tablets: "
                    + Config.dynamic_tablet_max_parallel_tablets);
        }
    }

    private void runDynamicTabletJobs() {
        for (var iterator = dynamicTabletJobs.entrySet().iterator(); iterator.hasNext(); /* */) {
            DynamicTabletJob job = iterator.next().getValue();
            // Job is not done, run it
            if (!job.isDone()) {
                job.run();
                continue;
            }

            // Job is done, remove expired job
            if (job.isExpired()) {
                iterator.remove();
                GlobalStateMgr.getCurrentState().getEditLog().logRemoveDynamicTabletJob(job.getJobId());
                LOG.info("Removed expired dynamic tablet job. {}", job);
            }
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.DYNAMIC_TABLET_JOB_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        DynamicTabletJobMgr dynamicTabletJobMgr = reader.readJson(DynamicTabletJobMgr.class);
        dynamicTabletJobs.putAll(dynamicTabletJobMgr.dynamicTabletJobs);
    }
}
