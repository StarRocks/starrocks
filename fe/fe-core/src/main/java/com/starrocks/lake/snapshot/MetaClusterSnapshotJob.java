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

package com.starrocks.lake.snapshot;

import com.google.common.base.Preconditions;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.leader.CheckpointController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetaClusterSnapshotJob extends ClusterSnapshotJob {
    public static final Logger LOG = LogManager.getLogger(MetaClusterSnapshotJob.class);

    public MetaClusterSnapshotJob(long id, String snapshotName, String storageVolumeName, long createdTimeMs) {
        super(id, snapshotName, storageVolumeName, createdTimeMs);
    }

    @Override
    protected void runInitializingJob(SnapshotJobContext context) throws StarRocksException {
        Preconditions.checkState(state == ClusterSnapshotJobState.INITIALIZING, state);
        LOG.info("begin to initialize cluster snapshot job. job: {}", getId());

        Pair<Long, Long> consistentIds = context.captureConsistentCheckpointIdBetweenFEAndStarMgr();
        if (consistentIds == null) {
            throw new StarRocksException("failed to capture consistent journal id for checkpoint");
        }
        setJournalIds(consistentIds.first, consistentIds.second);
        LOG.info(
                "Successful capture consistent journal id, FE checkpoint journal Id: {}, StarMgr checkpoint journal Id: {}",
                consistentIds.first, consistentIds.second);

        setState(ClusterSnapshotJobState.SNAPSHOTING);
        logJob();
    }

    protected void runSnapshottingJob(SnapshotJobContext context) throws StarRocksException {
        Preconditions.checkState(state == ClusterSnapshotJobState.SNAPSHOTING, state);
        LOG.info("begin to snapshot cluster snapshot job. job: {}", getId());

        CheckpointController feController = context.getFeController();
        CheckpointController starMgrController = context.getStarMgrController();
        long feCheckpointJournalId = getFeJournalId();
        long starMgrCheckpointJournalId = getStarMgrJournalId();

        long feImageJournalId = feController.getImageJournalId();
        if (feImageJournalId < feCheckpointJournalId) {
            Pair<Boolean, String> createFEImageRet = feController.runCheckpointControllerWithIds(feImageJournalId,
                    feCheckpointJournalId, needClusterSnapshotInfo());
            if (!createFEImageRet.first) {
                throw new StarRocksException("checkpoint failed for FE image: " + createFEImageRet.second);
            }
        } else if (feImageJournalId > feCheckpointJournalId) {
            throw new StarRocksException("checkpoint journal id for FE is smaller than image version");
        }
        LOG.info("Finished create image for FE image, version: {}", feCheckpointJournalId);

        long starMgrImageJournalId = starMgrController.getImageJournalId();
        if (starMgrImageJournalId < starMgrCheckpointJournalId) {
            Pair<Boolean, String> createStarMgrImageRet = starMgrController
                    .runCheckpointControllerWithIds(starMgrImageJournalId, starMgrCheckpointJournalId, false);
            if (!createStarMgrImageRet.first) {
                throw new StarRocksException("checkpoint failed for starMgr image: " + createStarMgrImageRet.second);
            }
        } else if (starMgrImageJournalId > starMgrCheckpointJournalId) {
            throw new StarRocksException("checkpoint journal id for starMgr is smaller than image version");
        }
        setClusterSnapshotInfo(feController.getClusterSnapshotInfo());
        setState(ClusterSnapshotJobState.UPLOADING);
        logJob();
        LOG.info("Finished create image for starMgr image, version: {}", starMgrCheckpointJournalId);
    }

    @Override
    protected void runUploadingJob(SnapshotJobContext context) throws StarRocksException {
        Preconditions.checkState(state == ClusterSnapshotJobState.UPLOADING, state);
        LOG.info("begin to upload cluster snapshot job. job: {}", getId());

        try {
            ClusterSnapshotUtils.uploadClusterSnapshotToRemote(this);
        } catch (StarRocksException e) {
            throw new StarRocksException("upload image failed, err msg: " + e.getMessage());
        }
        setState(ClusterSnapshotJobState.FINISHED);
        logJob();
        LOG.info(
                "Finish upload image for Cluster Snapshot, FE checkpoint journal Id: {}, StarMgr checkpoint journal Id: {}",
                getFeJournalId(), getStarMgrJournalId());
    }

}
