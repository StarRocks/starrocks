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

package com.starrocks.scheduler.externalcooldown;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.io.Text;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manage lifetime of external cooldown, including create, build, refresh, destroy
 */
public class ExternalCooldownMgr {
    private static final Logger LOG = LogManager.getLogger(ExternalCooldownMgr.class);

    private final Map<Long, ExternalCooldownMaintenanceJob> jobMap = new ConcurrentHashMap<>();

    private boolean initialized = false;

    private boolean restoreOneJob(ExternalCooldownMaintenanceJob job) {
        try {
            job.restore();
        } catch (IllegalStateException e) {
            LOG.warn("reload external cooldown maintenance jobs failed", e);
            return false;
        }
        return true;
    }

    /**
     * Reload jobs from meta store
     */
    public long reload(DataInputStream input, long checksum) {
        Preconditions.checkState(jobMap.isEmpty());

        try {
            String str = Text.readString(input);
            SerializedJobs data = GsonUtils.GSON.fromJson(str, SerializedJobs.class);
            if (CollectionUtils.isNotEmpty(data.jobList)) {
                for (ExternalCooldownMaintenanceJob job : data.jobList) {
                    if (!restoreOneJob(job)) {
                        continue;
                    }
                    jobMap.put(job.getTableId(), job);
                }
                LOG.info("reload external cooldown maintenance jobs: {}", data.jobList);
                LOG.debug("reload external cooldown maintenance job details: {}", str);
            }
            checksum ^= data.jobList.size();
        } catch (IOException e) {
            LOG.warn("reload external cooldown maintenance job details: {}", e.getMessage());
        }
        return checksum;
    }

    /**
     * Replay from journal
     */
    public void replay(ExternalCooldownMaintenanceJob job) {
        if (!restoreOneJob(job)) {
            return;
        }
        jobMap.put(job.getTableId(), job);
        LOG.info("Replay external cooldown maintenance jobs: {}", job);
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

    public void prepareMaintenanceWork(long dbId, OlapTable olapTable) {
        ExternalCooldownMaintenanceJob job = jobMap.get(olapTable.getId());
        if (job != null) {
            restoreOneJob(job);
            return;
        }
        try {
            // Create the job but not execute it
            job = new ExternalCooldownMaintenanceJob(olapTable, dbId);
            Preconditions.checkState(jobMap.putIfAbsent(job.getTableId(), job) == null, "job already existed");

            GlobalStateMgr.getCurrentState().getEditLog().logExternalCooldownJobState(job);
            LOG.info("create the maintenance job for external cooldown table: {}", olapTable.getName());
        } catch (Exception e) {
            jobMap.remove(olapTable.getId());
            LOG.warn("prepare external cooldown for {} failed, ", olapTable.getName(), e);
        }

        if (job != null) {
            restoreOneJob(job);
        }
    }

    /**
     * Stop the maintenance job for external cooldown after dropped
     */
    public void stopMaintainExternalCooldown(OlapTable olapTable) {
        ExternalCooldownMaintenanceJob job = getJob(olapTable.getId());
        if (job == null) {
            LOG.warn("external cooldown job not exists {}", olapTable.getName());
            return;
        }
        job.stopJob();
        jobMap.remove(olapTable.getId());
        LOG.info("Remove maintenance job for external cooldown: {}", olapTable.getName());
    }

    private ExternalCooldownMaintenanceJob getJob(long tblId) {
        return jobMap.get(tblId);
    }

    public List<ExternalCooldownMaintenanceJob> getRunnableJobs() {
        return this.jobMap.values().stream().filter(ExternalCooldownMaintenanceJob::isRunnable).collect(Collectors.toList());
    }

    public List<ExternalCooldownMaintenanceJob> getJobs() {
        return new ArrayList<>(this.jobMap.values());
    }

    static class SerializedJobs {
        @SerializedName("jobList")
        List<ExternalCooldownMaintenanceJob> jobList;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        int numJson = 1 + jobMap.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.EXTERNAL_COOLDOWN_MGR, numJson);
        writer.writeInt(jobMap.size());
        for (ExternalCooldownMaintenanceJob job : jobMap.values()) {
            writer.writeJson(job);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockEOFException {
        int numJson = reader.readInt();
        for (int i = 0; i < numJson; ++i) {
            ExternalCooldownMaintenanceJob job = reader.readJson(ExternalCooldownMaintenanceJob.class);
            if (!restoreOneJob(job)) {
                continue;
            }
            jobMap.put(job.getTableId(), job);
        }
    }

    public void doInitializeIfNeed() {
        if (!initialized) {
            reloadJobs();
            this.initialized = true;
        }
    }

    public void reloadJobs() {
        for (ExternalCooldownMaintenanceJob job : jobMap.values()) {
            if (!restoreOneJob(job)) {
                this.jobMap.remove(job.getTableId());
            }
        }
    }
}
