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

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class DynamicTabletJobMgr extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(DynamicTabletJobMgr.class);

    @SerializedName(value = "jobs")
    protected final Map<Long, DynamicTabletJob> jobs = Maps.newConcurrentMap();

    public DynamicTabletJobMgr() {
        super("DynamicTabletJobMgr", Config.dynamic_tablet_job_scheduler_interval_ms);
    }

    public Map<Long, DynamicTabletJob> getJobs() {
        return jobs;
    }

    public boolean addDynamicTabletJob(DynamicTabletJob dynamicTabletJob) {
        if (dynamicTabletJob.getJobState() != DynamicTabletJob.JobState.PENDING) {
            throw new RuntimeException("Dynamic tablet job state is not pending. " + dynamicTabletJob);
        }

        if (getTotalParalelTablets()
                + dynamicTabletJob.getParallelTablets() > Config.dynamic_tablet_max_parallel_tablets) {
            return false;
        }

        DynamicTabletJob existingJob = jobs.putIfAbsent(dynamicTabletJob.getJobId(), dynamicTabletJob);
        if (existingJob != null) {
            throw new RuntimeException("Running dynamic tablet job is already existed. " + existingJob);
        }

        GlobalStateMgr.getCurrentState().getEditLog().logUpdateDynamicTabletJob(dynamicTabletJob);

        LOG.info("Added running dynamic tablet job. {}", dynamicTabletJob);
        return true;
    }

    public long getTotalParalelTablets() {
        long totalParallelTablets = 0;
        for (DynamicTabletJob job : jobs.values()) {
            if (job.isDone()) {
                continue;
            }
            totalParallelTablets += job.getParallelTablets();
        }
        return totalParallelTablets;
    }

    public void replayUpdateDynamicTabletJob(DynamicTabletJob dynamicTabletJob) {
        dynamicTabletJob.replay();
        jobs.put(dynamicTabletJob.getJobId(), dynamicTabletJob);
    }

    public void replayRemoveDynamicTabletJob(long dynamicTabletJobId) {
        if (jobs.remove(dynamicTabletJobId) == null) {
            // Should not happen, just add a warnning log
            LOG.warn("Failed to find dynamic tablet job {} when replaying remove dynamic tablet job",
                    dynamicTabletJobId);

        }
    }

    @Override
    protected void runAfterCatalogReady() {
        runJobs();
    }

    private void runJobs() {
        for (Iterator<Map.Entry<Long, DynamicTabletJob>> it = jobs.entrySet().iterator(); it.hasNext(); /* */) {
            DynamicTabletJob job = it.next().getValue();
            // Job is not done, run it
            if (!job.isDone()) {
                job.run();
                continue;
            }

            // Job is done, remove expired job
            if (job.isExpired()) {
                it.remove();
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
        jobs.putAll(dynamicTabletJobMgr.jobs);
    }
}
