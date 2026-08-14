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

package com.starrocks.sql.analyzer.mv;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.Config;
import com.starrocks.connector.iceberg.IcebergPartitionTransform;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;

/**
 * Handler for Iceberg tables.
 */
public class IcebergTablePartitionHandler implements MVBaseTablePartitionHandler {

    @Override
    public void checkPartitionColumn(MVPartitionCheckContext context) {
        IcebergTable table = (IcebergTable) context.getTable();
        Expr partitionByExpr = context.getPartitionByExpr();
        SlotRef slotRef = context.getSlotRef();

        // Common validation: unpartitioned check + column name match + type check
        MVBaseTablePartitionHandler.checkPartitionColumnWithBaseTable(slotRef, table);

        // Iceberg-specific: partition evolution and transform validation
        Table icebergTable = table.getNativeTable();
        if (icebergTable.specs().size() > 1) {
            boolean allowByConfig = Config.enable_mv_on_iceberg_table_with_partition_evolution
                    && table.isCurrentSnapshotAllOnCurrentSpec();
            if (!allowByConfig) {
                throw new SemanticException("Do not support create materialized view when " +
                        "base iceberg table has partition evolution");
            }
        }

        boolean withTransform = checkPartitionTransformCompatibleWithSpec(table, partitionByExpr, slotRef);
        if (withTransform) {
            context.getStatement().setRefBaseTablePartitionWithTransform(true);
        }
    }

    public static boolean checkPartitionTransformCompatibleWithSpec(IcebergTable table,
                                                                    Expr partitionByExpr,
                                                                    SlotRef slotRef) {
        Table icebergTable = table.getNativeTable();
        PartitionSpec partitionSpec = icebergTable.spec();
        for (PartitionField partitionField : partitionSpec.fields()) {
            String partitionColumnName = icebergTable.schema().findColumnName(partitionField.sourceId());
            if (partitionColumnName.equalsIgnoreCase(slotRef.getColumnName())) {
                IcebergPartitionTransform transform =
                        IcebergPartitionTransform.fromString(partitionField.transform().toString());
                switch (transform) {
                    case YEAR:
                    case MONTH:
                    case DAY:
                    case HOUR:
                        if (!isDateTruncWithUnit(partitionByExpr, transform.name())) {
                            throw new SemanticException("Materialized view partition expr %s " +
                                    "must be the same with base table partition transform %s, please use date_trunc" +
                                    "(<transform>, <partition_colum_name>) instead.",
                                    ExprToSql.toSql(partitionByExpr), transform.name());
                        }
                        return true;
                    case IDENTITY:
                        if (!(partitionByExpr instanceof SlotRef) && !MvUtils.isStr2Date(partitionByExpr) &&
                                !MvUtils.isFuncCallExpr(partitionByExpr, FunctionSet.DATE_TRUNC)) {
                            throw new SemanticException("Materialized view partition expr %s: " +
                                    "only support ref partition column for transform %s, please use " +
                                    "<partition_column_name> instead.",
                                    ExprToSql.toSql(partitionByExpr), transform.name());
                        }
                        return false;
                    default:
                        throw new SemanticException("Do not support create materialized view when " +
                                "base iceberg table partition transform is: " + transform.name());
                }
            }
        }

        throw new SemanticException("Materialized view partition expr %s is no longer compatible with " +
                "base Iceberg table current partition spec: partition column %s is not found in current " +
                "partition spec.", ExprToSql.toSql(partitionByExpr), slotRef.getColumnName());
    }

    private static boolean isDateTruncWithUnit(Expr partitionExpr, String timeUnit) {
        if (MvUtils.isFuncCallExpr(partitionExpr, FunctionSet.DATE_TRUNC)) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
            if (!(functionCallExpr.getChild(0) instanceof StringLiteral)) {
                return false;
            }
            StringLiteral stringLiteral = (StringLiteral) functionCallExpr.getChild(0);
            return stringLiteral.getStringValue().equalsIgnoreCase(timeUnit);
        }
        return false;
    }
}
