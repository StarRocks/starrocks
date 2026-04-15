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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manage FE-side MV compatibility metadata such as legacy maintenance job replay/state
 * and MV timeliness tracking. Legacy jobs are replay-only metadata and do not restore
 * a live MaterializedView handle.
 */
public class MaterializedViewMgr {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewMgr.class);

    // MV's global timeliness info manager
    private final MVTimelinessMgr mvTimelinessMgr = new MVTimelinessMgr();
    // MV's maintenance job
    private final Map<MvId, MVMaintenanceJob> jobMap = new ConcurrentHashMap<>();

    /**
     * Replay from journal
     */
    public void replay(MVMaintenanceJob job) throws IOException {
        restoreAndTrackJob(job, "journal");
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

    protected MVMaintenanceJob getJob(MvId mvId) {
        return jobMap.get(mvId);
    }

    // for test
    protected void clearJobs() {
        jobMap.clear();
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
            restoreAndTrackJob(mvMaintenanceJob, "image");
        });
    }

    private void restoreAndTrackJob(MVMaintenanceJob job, String source) {
        try {
            job.restore();
            MvId mvId = new MvId(job.getDbId(), job.getViewId());
            jobMap.put(mvId, job);
            LOG.info("Replay legacy MV maintenance job from {}: {}", source, job);
        } catch (Exception e) {
            LOG.warn("Skip legacy MV maintenance job from {} because target MV could not be restored: {}", source, job);
            LOG.warn("Failed to restore legacy MV maintenance job from {}", source, e);
        }
    }

    public MVTimelinessMgr getMvTimelinessMgr() {
        return mvTimelinessMgr;
    }

    public void triggerTimelessInfoEvent(MaterializedView mv, MVTimelinessMgr.MVChangeEvent event) {
        mvTimelinessMgr.triggerEvent(mv, event);
    }
}
