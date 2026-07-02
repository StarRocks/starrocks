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
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.type.IntegerType;

import java.util.List;
import java.util.Map;

public class IcebergLazyMaterializationSupport implements LazyMaterializationSupport {
    private static final String ROW_SOURCE_ID = "_row_source_id";
    private static final String SCAN_RANGE_ID = "_scan_range_id";
    private static final String ROW_POSITION = "_pos";
    private static final String ROW_ID = "_row_id";
    private static final String LAST_UPDATED_SEQUENCE_NUMBER = "_last_updated_sequence_number";

    @Override
    public boolean supports(PhysicalScanOperator scanOperator) {
        IcebergTable scanTable = (IcebergTable) scanOperator.getTable();

        if (!scanTable.isParquetFormat() || scanTable.getFormatVersion() < 2) {
            return false;
        }

        // Row lineage columns (_row_id, _last_updated_sequence_number) cannot be read through the
        // lookup node: IcebergConnectorScanRangeSource attaches their per-file metadata
        // (first_row_id, the data-sequence-number extended-column value) to the scan tuple's slot
        // ids only, so the same columns in the lookup tuple would silently resolve to NULL.
        // Skip GLM for the whole scan instead.
        if (scanOperator.getColRefToColumnMetaMap().values().stream()
                .anyMatch(column -> isRowLineageColumn(column.getName()))) {
            return false;
        }
        return true;
    }

    @Override
    public ColumnRefSet predicateUsedColumns(PhysicalScanOperator scanOperator) {
        PhysicalIcebergScanOperator spec = (PhysicalIcebergScanOperator) scanOperator;
        return spec.getScanOperatorPredicates().getUsedColumns();
    }

    @Override
    public List<ColumnRefOperator> addRowIdColumns(PhysicalScanOperator scanOperator, ColumnRefFactory columnRefFactory) {
        // The fetch key is (scan_range_id, file-local row position). _pos is the Iceberg
        // row-position metadata column (MetadataColumns.ROW_POSITION), synthesized by the BE
        // reader from the row's position in the data file, so it works for every Iceberg
        // format version and does not depend on row lineage metadata.
        ColumnRefOperator rowPosColumnRef = null;
        for (Map.Entry<ColumnRefOperator, Column> entry : scanOperator.getColRefToColumnMetaMap().entrySet()) {
            if (entry.getValue().getName().equalsIgnoreCase(ROW_POSITION)) {
                rowPosColumnRef = entry.getKey();
                break;
            }
        }
        if (rowPosColumnRef == null) {
            Column rowPosColumn = new Column(ROW_POSITION, IntegerType.BIGINT, true);
            rowPosColumnRef = columnRefFactory.create(ROW_POSITION, IntegerType.BIGINT, true);
            columnRefFactory.updateColumnRefToColumns(rowPosColumnRef, rowPosColumn, scanOperator.getTable());
        }

        // generate row source id to distinguish scan operator
        Column rowSourceIdColumn = new Column(ROW_SOURCE_ID, IntegerType.INT, true);
        ColumnRefOperator rowSourceIdColumnRef = columnRefFactory.create(ROW_SOURCE_ID, IntegerType.INT, true);
        columnRefFactory.updateColumnRefToColumns(rowSourceIdColumnRef, rowSourceIdColumn, scanOperator.getTable());

        Column scanRangeIdColumn = new Column(SCAN_RANGE_ID, IntegerType.INT, true);
        ColumnRefOperator scanRangeIdColumnRef = columnRefFactory.create(SCAN_RANGE_ID, IntegerType.INT, true);
        columnRefFactory.updateColumnRefToColumns(scanRangeIdColumnRef, scanRangeIdColumn, scanOperator.getTable());

        return List.of(rowSourceIdColumnRef, scanRangeIdColumnRef, rowPosColumnRef);
    }

    @Override
    public Pair<Integer, ColumnDict> getGlobalDict(PhysicalScanOperator scan, ColumnRefOperator column) {
        PhysicalIcebergScanOperator spec = (PhysicalIcebergScanOperator) scan;
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
        PhysicalIcebergScanOperator spec = (PhysicalIcebergScanOperator) scan.getOp();

        // build a new optExpressions
        PhysicalIcebergScanOperator.Builder builder = PhysicalIcebergScanOperator.builder().withOperator(spec);
        builder.setColRefToColumnMetaMap(newOutputs);
        builder.setEnableGlobalLateMaterialization(true);

        OptExpression result = OptExpression.builder().with(scan).setOp(builder.build()).build();
        LogicalProperty newProperty = new LogicalProperty(scan.getLogicalProperty());
        newProperty.setOutputColumns(new ColumnRefSet(newOutputs.keySet()));
        result.setLogicalProperty(newProperty);

        return result;
    }

    private boolean isRowLineageColumn(String columnName) {
        return columnName.equalsIgnoreCase(ROW_ID)
                || columnName.equalsIgnoreCase(LAST_UPDATED_SEQUENCE_NUMBER);
    }

}
