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

package com.starrocks.journal;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.PhysicalPartitionTableDbId;
import com.starrocks.catalog.Table;
import com.starrocks.leader.CheckpointController;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GlobalStateCheckpointWorker extends CheckpointWorker {

    public GlobalStateCheckpointWorker(Journal journal) {
        super("global_state_checkpoint_worker", journal);
    }

    @Override
    void doCheckpoint(long epoch, long journalId,
                      Map<PhysicalPartitionTableDbId, Long> nativeTableCheckpointVersions) throws Exception {
        long replayedJournalId = -1;
        // generate new image file
        LOG.info("begin to generate new image: image.{}", journalId);
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        globalStateMgr.setEditLog(new EditLog(null));
        globalStateMgr.setJournal(journal);
        try {
            globalStateMgr.loadImage();
            globalStateMgr.initDefaultWarehouse();

            checkEpoch(epoch);

            globalStateMgr.replayJournal(journalId);
            globalStateMgr.clearExpiredJobs();

            checkEpoch(epoch);

            globalStateMgr.saveImage();
            replayedJournalId = globalStateMgr.getReplayedJournalId();
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_IMAGE_WRITE.increase(1L);
            }
            servingGlobalState.setImageJournalId(journalId);

            if (nativeTableCheckpointVersions != null) {
                getSnapshoVersions(globalStateMgr, nativeTableCheckpointVersions);
            }
            LOG.info("checkpoint finished save image.{}", replayedJournalId);
        } finally {
            GlobalStateMgr.destroyCheckpoint();
        }
    }

    @Override
    CheckpointController getCheckpointController() {
        return GlobalStateMgr.getServingState().getCheckpointController();
    }

    @Override
    boolean isBelongToGlobalStateMgr() {
        return true;
    }

    private void getSnapshoVersions(GlobalStateMgr globalStateMgr,
                                    Map<PhysicalPartitionTableDbId, Long> nativeTableCheckpointVersions) {
        // lock free to access metadata because global state from checkpoint worker can be accessed by single thread
        List<Long> dbIds = globalStateMgr.getLocalMetastore().getDbIds();
        for (Long dbId : dbIds) {
            Database db = globalStateMgr.getLocalMetastore().getDb(dbId);
            if (db == null) {
                continue;
            }

            List<Table> tables = new ArrayList<>();
            for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                if (table.isCloudNativeTableOrMaterializedView()) {
                    tables.add(table);
                }
            }

            for (Table table : tables) {
                OlapTable olapTable = (OlapTable) table;
                for (PhysicalPartition partition : olapTable.getPhysicalPartitions()) {
                    PhysicalPartitionTableDbId key = new PhysicalPartitionTableDbId(dbId, table.getId(), partition.getId());
                    nativeTableCheckpointVersions.computeIfAbsent(
                                                  key, k -> Lists.newArrayList()).add(partition.getVisibleVersion());
                }
            }
        }
    }

    private void checkEpoch(long epoch) throws CheckpointException {
        if (epoch != servingGlobalState.getEpoch()) {
            throw new CheckpointException("epoch outdated");
        }
    }
}
