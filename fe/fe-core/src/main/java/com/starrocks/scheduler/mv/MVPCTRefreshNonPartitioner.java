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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.common.PCellNone;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class MVPCTRefreshNonPartitioner extends MVPCTRefreshPartitioner {
    public MVPCTRefreshNonPartitioner(MvTaskRunContext mvContext,
                                      TaskRunContext context,
                                      Database db,
                                      MaterializedView mv,
                                      MVRefreshParams mvRefreshParams) {
        super(mvContext, context, db, mv, mvRefreshParams);
    }

    public PCellSortedSet getMVPartitionsToRefreshByParams() {
        return getNonPartitionedMVPartitionsToRefresh();
    }

    @Override
    public boolean syncAddOrDropPartitions() {
        // do nothing
        return true;
    }

    @Override
    public Expr generatePartitionPredicate(Table table, Set<String> refBaseTablePartitionNames,
                                           List<Expr> mvPartitionSlotRefs) {
        // do nothing
        return null;
    }


    @Override
    public Expr generateMVPartitionPredicate(TableName tableName,
                                             Set<String> mvPartitionNames) throws AnalysisException {
        return null;
    }

    @Override
    public PCellSortedSet getMVPartitionsToRefreshWithForce() {
        return getNonPartitionedMVPartitionsToRefresh();
    }

    private PCellSortedSet getNonPartitionedMVPartitionsToRefresh() {
        List<PCellWithName> pCellWithNames = mv.getVisiblePartitionNames()
                .stream()
                .map(partitionName -> PCellWithName.of(partitionName, new PCellNone()))
                .collect(Collectors.toList());
        return PCellSortedSet.of(pCellWithNames);
    }

    @Override
    public PCellSortedSet getMVPartitionsToRefreshWithCheck(Map<Long, BaseTableSnapshotInfo> snapshotBaseTables) {
        // non-partitioned materialized view
        if (mvRefreshParams.isForce() || isNonPartitionedMVNeedToRefresh(snapshotBaseTables, mv)) {
            return getNonPartitionedMVPartitionsToRefresh();
        }
        return PCellSortedSet.of();
    }

    @Override
    public PCellSortedSet getMVPartitionNamesWithTTL(boolean isAutoRefresh) {
        return PCellSortedSet.of();
    }

    @Override
    public void filterPartitionByRefreshNumber(PCellSortedSet mvPartitionsToRefresh) {
        // do nothing
    }

    @Override
    public void filterPartitionByAdaptiveRefreshNumber(PCellSortedSet mvPartitionsToRefresh) {
        // do nothing
    }

    @Override
    protected int getAdaptivePartitionRefreshNumber(Iterator<PCellWithName> partitionNameIter) throws MVAdaptiveRefreshException {
        return 0;
    }

    @Override
    public boolean isCalcPotentialRefreshPartition(Map<Table, PCellSortedSet> baseChangedPartitionNames,
                                                   PCellSortedSet mvPartitions) {
        return false;
    }
}
