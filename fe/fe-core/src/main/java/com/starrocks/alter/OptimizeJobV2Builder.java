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

package com.starrocks.alter;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.sql.ast.OptimizeClause;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.thrift.TStorageType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OptimizeJobV2Builder extends AlterJobV2Builder {
    private static final Logger LOG = LogManager.getLogger(OptimizeJobV2Builder.class);

    private final OlapTable table;

    private OptimizeClause optimizeClause;

    public OptimizeJobV2Builder(OlapTable table) {
        this.table = table;
    }

    public OptimizeJobV2Builder withOptimizeClause(OptimizeClause optimizeClause) {
        this.optimizeClause = optimizeClause;
        return this;
    }

    @Override
    public AlterJobV2 build() throws StarRocksException {
        long tableId = table.getId();
        if (optimizeClause.getPartitionDesc() != null) {
            LOG.info("Merge partition job {} is created, table: {}", jobId, table.getName());
            MergePartitionJob mergePartitionJob = new MergePartitionJob(jobId, dbId, tableId, table.getName(),
                    timeoutMs, optimizeClause);
            mergePartitionJob.setComputeResource(computeResource);
            return mergePartitionJob;
        }
        if (!Config.enable_online_optimize_table || optimizeClause.getKeysDesc() != null
                || optimizeClause.getPartitionDesc() != null || optimizeClause.getSortKeys() != null
                || (optimizeClause.getDistributionDesc() != null
                    && optimizeClause.getDistributionDesc() instanceof RandomDistributionDesc)
                || table.getStorageType() == TStorageType.COLUMN_WITH_ROW
                || !table.enableReplicatedStorage()
                || table.isCloudNativeTableOrMaterializedView()) {
            LOG.info("Optimize job {} is created, table: {}", jobId, table.getName());
            OptimizeJobV2 optimizeJob = new OptimizeJobV2(jobId, dbId, tableId, table.getName(), timeoutMs, optimizeClause);
            optimizeJob.setComputeResource(computeResource);
            return optimizeJob;
        } else {
            if (Config.enable_new_publish_mechanism) {
                // A double write of an online optimize goes to a temp partition whose versions have a hole until the
                // rewrite fills it. The new publish mechanism finishes a transaction only once its replicas report its
                // version, and publishes the transactions of a table in commit order, so the rewrite would wait behind
                // double writes that wait for it.
                throw new DdlException("Online optimize does not support the new publish mechanism. Set the FE "
                        + "configuration enable_new_publish_mechanism or enable_online_optimize_table to false.");
            }
            LOG.info("Online optimize job {} is created, table: {}", jobId, table.getName());
            OnlineOptimizeJobV2 onlineOptimizeJob = new OnlineOptimizeJobV2(
                    jobId, dbId, tableId, table.getName(), timeoutMs, optimizeClause);
            onlineOptimizeJob.setComputeResource(computeResource);
            return onlineOptimizeJob;
        }
    }
}
