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

import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TableSnapshotInfo;
import com.starrocks.scheduler.TaskRunContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public final class MVPCTRefreshNonPartitioner extends MVPCTRefreshPartitioner {
    private static final Logger LOG = LogManager.getLogger(MVPCTRefreshNonPartitioner.class);

    public MVPCTRefreshNonPartitioner(MvTaskRunContext mvContext,
                                      TaskRunContext context,
                                      Database db,
                                      MaterializedView mv) {
        super(mvContext, context, db, mv);
    }

    @Override
    public boolean syncAddOrDropPartitions() {
        // do nothing
        return true;
    }

    @Override
    public Expr generatePartitionPredicate(Table table, Set<String> refBaseTablePartitionNames,
                                           Expr mvPartitionSlotRef) {
        // do nothing
        return null;
    }
    @Override
    public Set<String> getMVPartitionsToRefreshWithForce(int partitionTTLNumber) {
        return mv.getVisiblePartitionNames();
    }

    @Override
    public Set<String> getMVPartitionsToRefresh(PartitionInfo mvPartitionInfo,
                                                Map<Long, TableSnapshotInfo> snapshotBaseTables,
                                                String start, String end, boolean force,
                                                Set<String> mvPotentialPartitionNames) {
        // non-partitioned materialized view
        if (force || isNonPartitionedMVNeedToRefresh(snapshotBaseTables, mv)) {
            return mv.getVisiblePartitionNames();
        }
        return Sets.newHashSet();
    }

    @Override
    public Set<String> getMVPartitionNamesWithTTL(MaterializedView materializedView,
                                                  String start, String end,
                                                  int partitionTTLNumber,
                                                  boolean isAutoRefresh) {
        return Sets.newHashSet();
    }

    public void filterPartitionByRefreshNumber(Set<String> mvPartitionsToRefresh,
                                               Set<String> mvPotentialPartitionNames, boolean tentative) {
        // do nothing
    }
}
