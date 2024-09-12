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
package com.starrocks.meta.store.bdb;

import com.sleepycat.je.Transaction;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.RollupJobV2;
import com.starrocks.meta.MetadataHandler;
import com.starrocks.meta.kv.ByteCoder;
import com.starrocks.meta.store.AlterJobMgrIface;
import com.starrocks.server.GlobalStateMgr;

public class AlterJobMgr implements AlterJobMgrIface {
    @Override
    public void logAlterJob(AlterJobV2 alterJob) {
        if (alterJob.getType() == AlterJobV2.JobType.ROLLUP) {
            rollup((RollupJobV2) alterJob);
        }
    }

    private void rollup(RollupJobV2 rollupJob) {
        switch (rollupJob.getJobState()) {
            case WAITING_TXN: {
                //logEdit(OperationType.OP_ALTER_JOB_V2, alterJob);

                long jobId = rollupJob.getJobId();

                MetadataHandler metadataHandler = GlobalStateMgr.getCurrentState().getMetadataHandler();
                Transaction transaction = metadataHandler.starTransaction();
                metadataHandler.put(transaction,
                        ByteCoder.encode("alter_job", jobId), rollupJob, AlterJobV2.class);

                //modify materialized index

                break;
            }

            case FINISHED: {
                //set replica state
            }

            case CANCELLED: {
                // delete materialized index
            }
        }
    }

    @Override
    public void cancelRollupJob(RollupJobV2 rollupJob) {
        // delete materialized index
    }
}
