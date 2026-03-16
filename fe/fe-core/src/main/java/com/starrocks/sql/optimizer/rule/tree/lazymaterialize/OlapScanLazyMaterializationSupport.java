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

package com.starrocks.sql.optimizer.rule.tree.lazymaterialize;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.VirtualColumnRegistry;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.type.IntegerType;

import java.util.List;
import java.util.Map;

public class OlapScanLazyMaterializationSupport implements LazyMaterializationSupport {
    private static final String SOURCE_ID = "_source_id_";
    private static final String TABLET_ID = "_tablet_id_";
    private static final String DYNAMIC_RSS_ID = "_dynamic_rssid_";
    private static final String RSS_ID = "_rss_id_";
    private static final String ROW_ID = "_row_id_";

    @Override
    public boolean supports(PhysicalScanOperator scan) {
        final OlapTable table = (OlapTable) scan.getTable();
        return !table.getKeysType().isAggregationFamily();
    }

    @Override
    public ColumnRefSet predicateUsedColumns(PhysicalScanOperator scanOperator) {
        PhysicalOlapScanOperator op = (PhysicalOlapScanOperator) scanOperator;
        ScalarOperator predicate = op.getPredicate();
        if (predicate != null) {
            return op.getPredicate().getUsedColumns();
        }
        return new ColumnRefSet();
    }

    @Override
    public List<ColumnRefOperator> addRowIdColumns(PhysicalScanOperator scanOperator,
                                                   ColumnRefFactory columnRefFactory) {
        ColumnRefOperator sourceIdColumnRef = null;
        ColumnRefOperator tabletIdColumnRef = null;
        ColumnRefOperator rssIdColumnRef = null;
        ColumnRefOperator rowIdColumnRef = null;
        String rssId;

        final OlapTable table = (OlapTable) scanOperator.getTable();
        if (table.isCloudNativeTableOrMaterializedView()) {
            rssId = RSS_ID;
        } else {
            rssId = DYNAMIC_RSS_ID;
        }

        for (Map.Entry<ColumnRefOperator, Column> entry : scanOperator.getColRefToColumnMetaMap().entrySet()) {
            ColumnRefOperator columnRefOperator = entry.getKey();
            Column column = entry.getValue();
            if (column.getName().equalsIgnoreCase(SOURCE_ID)) {
                sourceIdColumnRef = columnRefOperator;
            }
            if (column.getName().equalsIgnoreCase(TABLET_ID)) {
                tabletIdColumnRef = columnRefOperator;
            }
            if (column.getName().equalsIgnoreCase(rssId)) {
                rssIdColumnRef = columnRefOperator;
            }
            if (column.getName().equalsIgnoreCase(ROW_ID)) {
                rowIdColumnRef = columnRefOperator;
            }
        }

        if (sourceIdColumnRef == null) {
            Column sourceIdColumn = VirtualColumnRegistry.getColumn(SOURCE_ID);
            ColumnRefOperator columnRefOperator = columnRefFactory.create(SOURCE_ID, IntegerType.INT, false);
            columnRefFactory.updateColumnRefToColumns(columnRefOperator, sourceIdColumn, scanOperator.getTable());
            sourceIdColumnRef = columnRefOperator;
        }

        if (tabletIdColumnRef == null) {
            Column tabletIdColumn = VirtualColumnRegistry.getColumn(TABLET_ID);
            ColumnRefOperator columnRefOperator = columnRefFactory.create(TABLET_ID, IntegerType.BIGINT, false);
            columnRefFactory.updateColumnRefToColumns(columnRefOperator, tabletIdColumn, scanOperator.getTable());
            tabletIdColumnRef = columnRefOperator;
        }

        if (rssIdColumnRef == null) {
            Column rssIdColumn = VirtualColumnRegistry.getColumn(rssId);
            ColumnRefOperator columnRefOperator = columnRefFactory.create(rssId, IntegerType.INT, false);
            columnRefFactory.updateColumnRefToColumns(columnRefOperator, rssIdColumn, scanOperator.getTable());
            rssIdColumnRef = columnRefOperator;
        }

        if (rowIdColumnRef == null) {
            Column rowIdColumn = VirtualColumnRegistry.getColumn(ROW_ID);
            ColumnRefOperator columnRefOperator = columnRefFactory.create(ROW_ID, IntegerType.BIGINT, false);
            columnRefFactory.updateColumnRefToColumns(columnRefOperator, rowIdColumn, scanOperator.getTable());
            rowIdColumnRef = columnRefOperator;
        }

        return List.of(sourceIdColumnRef, tabletIdColumnRef, rssIdColumnRef, rowIdColumnRef);
    }

    @Override
    public Pair<Integer, ColumnDict> getGlobalDict(PhysicalScanOperator scan, ColumnRefOperator column) {
        PhysicalOlapScanOperator spec = (PhysicalOlapScanOperator) scan;
        for (Pair<Integer, ColumnDict> globalDict : spec.getGlobalDicts()) {
            if (globalDict.first.equals(column.getId())) {
                return globalDict;
            }
        }
        return null;
    }

    @Override
    public OptExpression updateOutputColumns(OptExpression scan,
                                             Map<ColumnRefOperator, Column> newOutputs) {
        PhysicalOlapScanOperator spec = (PhysicalOlapScanOperator) scan.getOp();

        // build a new optExpressions
        PhysicalOlapScanOperator.Builder builder = PhysicalOlapScanOperator.builder().withOperator(spec);
        builder.setColRefToColumnMetaMap(newOutputs);
        builder.setEnableGlobalLateMaterialization(true);

        OptExpression result = OptExpression.builder().with(scan).setOp(builder.build()).build();
        LogicalProperty newProperty = new LogicalProperty(scan.getLogicalProperty());
        newProperty.setOutputColumns(new ColumnRefSet(newOutputs.keySet()));
        result.setLogicalProperty(newProperty);

        return result;
    }
}
