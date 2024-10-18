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

import com.starrocks.leader.CheckpointController;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;

public class GlobalStateCheckpointWorker extends CheckpointWorker {

    public GlobalStateCheckpointWorker(Journal journal) {
        super("global_state_checkpoint_worker", journal, "");
    }

    @Override
    void doCheckpoint(long epoch, long journalId) throws Exception {
        long replayedJournalId = -1;
        // generate new image file
        LOG.info("begin to generate new image: image.{}", journalId);
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        globalStateMgr.setEditLog(new EditLog(null));
        globalStateMgr.setJournal(journal);
        try {
            globalStateMgr.loadImage(imageDir);
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

    private void checkEpoch(long epoch) throws CheckpointException {
        if (epoch != servingGlobalState.getEpoch()) {
            throw new CheckpointException("epoch outdated");
        }
    }
}
