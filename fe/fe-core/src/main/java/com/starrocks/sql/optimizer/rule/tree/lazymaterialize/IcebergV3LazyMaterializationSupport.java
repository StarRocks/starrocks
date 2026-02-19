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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;

import java.util.List;
import java.util.Map;

public class IcebergV3LazyMaterializationSupport implements LazyMaterializationSupport {
    private static final String ROW_SOURCE_ID = "_row_source_id";
    private static final String SCAN_RANGE_ID = "_scan_range_id";
    private static final String ROW_ID = "_row_id";

    @Override
    public boolean supports(PhysicalScanOperator scanOperator) {
        PhysicalIcebergScanOperator spec = (PhysicalIcebergScanOperator) scanOperator;
        IcebergTable scanTable = (IcebergTable) scanOperator.getTable();

        return scanTable.isParquetFormat() && scanTable.getFormatVersion() >= 3;
    }

    @Override
    public ColumnRefSet predicateUsedColumns(PhysicalScanOperator scanOperator) {
        PhysicalIcebergScanOperator spec = (PhysicalIcebergScanOperator) scanOperator;
        return spec.getScanOperatorPredicates().getUsedColumns();
    }

    @Override
    public List<ColumnRefOperator> addRowIdColumns(PhysicalScanOperator scanOperator, ColumnRefFactory columnRefFactory) {
        ColumnRefOperator rowIdColumnRef = null;
        for (Map.Entry<ColumnRefOperator, Column> entry : scanOperator.getColRefToColumnMetaMap().entrySet()) {
            ColumnRefOperator columnRefOperator = entry.getKey();
            Column column = entry.getValue();
            if (column.getName().equalsIgnoreCase(ROW_ID)) {
                rowIdColumnRef = columnRefOperator;
                break;
            }
        }

        if (rowIdColumnRef == null) {
            Column rowIdColumn = new Column(ROW_ID, IntegerType.BIGINT, true);
            ColumnRefOperator columnRefOperator = columnRefFactory.create(ROW_ID, IntegerType.BIGINT, true);
            columnRefFactory.updateColumnRefToColumns(columnRefOperator, rowIdColumn, scanOperator.getTable());
            rowIdColumnRef = columnRefOperator;
        }

        // generate row source id to distinguish scan operator
        Column rowSourceIdColumn = new Column(ROW_SOURCE_ID, IntegerType.INT, true);
        ColumnRefOperator rowSourceIdColumnRef = columnRefFactory.create(ROW_SOURCE_ID, IntegerType.INT, true);
        columnRefFactory.updateColumnRefToColumns(rowSourceIdColumnRef, rowSourceIdColumn, scanOperator.getTable());

        Column scanRangeIdColumn = new Column(SCAN_RANGE_ID, IntegerType.INT, true);
        ColumnRefOperator scanRangeIdColumnRef = columnRefFactory.create(SCAN_RANGE_ID, IntegerType.INT, true);
        columnRefFactory.updateColumnRefToColumns(scanRangeIdColumnRef, scanRangeIdColumn, scanOperator.getTable());

        return List.of(rowSourceIdColumnRef, scanRangeIdColumnRef, rowIdColumnRef);
    }

    @Override
    public OptExpression updateOutputColumns(OptExpression scan,
                                             Map<ColumnRefOperator, Column> newOutputs) {
        PhysicalIcebergScanOperator spec = (PhysicalIcebergScanOperator) scan.getOp();

        // build a new optExpressions
        PhysicalIcebergScanOperator.Builder builder = PhysicalIcebergScanOperator.builder().withOperator(spec);
        builder.setColRefToColumnMetaMap(newOutputs);

        OptExpression result = OptExpression.builder().with(scan).setOp(builder.build()).build();
        LogicalProperty newProperty = new LogicalProperty(scan.getLogicalProperty());
        newProperty.setOutputColumns(new ColumnRefSet(newOutputs.keySet()));
        result.setLogicalProperty(newProperty);

        return result;
    }


}
