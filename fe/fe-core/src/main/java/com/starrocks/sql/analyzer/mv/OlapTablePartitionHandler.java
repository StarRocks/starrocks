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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.sql.analyzer.PartitionFunctionChecker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handler for OlapTable (including MaterializedView and CloudNative variants).
 */
public class OlapTablePartitionHandler implements MVBaseTablePartitionHandler {

    @Override
    public void checkPartitionColumn(MVPartitionCheckContext context) {
        SlotRef slotRef = context.getSlotRef();
        OlapTable table = (OlapTable) context.getTable();
        PartitionInfo partitionInfo = table.getPartitionInfo();
        if (partitionInfo.isUnPartitioned()) {
            throw new SemanticException("Materialized view partition column in partition exp " +
                    "must be base table partition column");
        } else if (partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns(table.getIdToColumn());
            if (partitionColumns.size() != 1) {
                throw new SemanticException("Materialized view related base table partition columns " +
                        "only supports single column");
            }
            // column name match + type check
            MVBaseTablePartitionHandler.checkPartitionColumnWithBaseTable(slotRef, table);
            // disable from_unix_time/cast for creating materialized view
            if (rangePartitionInfo instanceof ExpressionRangePartitionInfoV2) {
                ExpressionRangePartitionInfoV2 rangePartitionInfoV2 =
                        (ExpressionRangePartitionInfoV2) rangePartitionInfo;
                if (rangePartitionInfoV2.getPartitionColumnIdExprs().size() != 1) {
                    throw new SemanticException("Materialized view related base table partition columns " +
                            "only supports single column");
                }
                Expr partitionColumnExpr = rangePartitionInfoV2.getPartitionColumnIdExprs().get(0).getExpr();
                checkBaseTableSupportedPartitionFunc(partitionColumnExpr, table);
            }
        } else if (partitionInfo.isListPartition()) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            Set<String> partitionColumns = listPartitionInfo.getPartitionColumns(table.getIdToColumn()).stream()
                    .map(Column::getName)
                    .collect(Collectors.toSet());
            // List partition intentionally skips type check (e.g., BOOLEAN is valid)
            if (!partitionColumns.contains(slotRef.getColumnName())) {
                throw new SemanticException("Materialized view partition column in partition exp " +
                        "must be base table partition column");
            }
        } else {
            throw new SemanticException("Materialized view related base table partition type: " +
                    partitionInfo.getType().name() + " not supports");
        }
    }

    private static void checkBaseTableSupportedPartitionFunc(Expr partitionByExpr, OlapTable table) {
        if (partitionByExpr instanceof SlotRef) {
            // do nothing
        } else if (partitionByExpr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionByExpr;
            String functionName = functionCallExpr.getFunctionName();
            if (!PartitionFunctionChecker.FN_NAME_TO_PATTERN.containsKey(functionName)) {
                throw new SemanticException(String.format("Materialized view partition function derived from " +
                        functionName + " of base table %s is not supported yet", table.getName()),
                        functionCallExpr.getPos());
            }
        } else {
            throw new SemanticException(String.format("Materialized view partition function derived from " +
                    ExprToSql.toSql(partitionByExpr) + " of base table %s is not supported yet", table.getName()),
                    partitionByExpr.getPos());
        }
    }
}
