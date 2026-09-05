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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergMetadataScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergPartitionsTableRewriteRule extends TransformationRule {
    public IcebergPartitionsTableRewriteRule() {
        super(RuleType.TF_ICEBERG_PARTITIONS_TABLE_REWRITE_RULE, Pattern.create(OperatorType.LOGICAL_ICEBERG_METADATA_SCAN));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        Operator op = input.getOp();
        if (!(op instanceof LogicalIcebergMetadataScanOperator)) {
            return false;
        }
        LogicalIcebergMetadataScanOperator scanOperator = op.cast();
        if (scanOperator.isTransformed()) {
            return false;
        }
        MetadataTable metadataTable = (MetadataTable) scanOperator.getTable();
        return metadataTable.getMetadataTableType() == MetadataTableType.PARTITIONS;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalIcebergMetadataScanOperator operator = input.getOp().cast();
        Map<ColumnRefOperator, Column> originalColRefMap = operator.getColRefToColumnMetaMap();
        Map<Column, ColumnRefOperator> originalColumnMetaToColRefMap = operator.getColumnMetaToColRefMap();

        Map<String, ColumnRefOperator> refsByName = new HashMap<>();
        for (ColumnRefOperator ref : originalColRefMap.keySet()) {
            refsByName.put(ref.getName(), ref);
        }

        // Iceberg snapshot ids are not monotonic with commit time, so aggregating
        // last_updated_snapshot_id independently from last_updated_at (e.g. max() on each) can
        // pick the snapshot id and the timestamp from different scan rows. To return a coherent
        // pair we use max_by(last_updated_snapshot_id, last_updated_at), which requires the
        // last_updated_at column to be present in the scan output. If the user did not request
        // it, force-include it here and hide it again via a project on top of the aggregation.
        ColumnRefOperator syntheticLastUpdatedAt = null;
        Map<ColumnRefOperator, Column> scanColRefMap = originalColRefMap;
        Map<Column, ColumnRefOperator> scanColumnMetaToColRefMap = originalColumnMetaToColRefMap;
        if (refsByName.containsKey("last_updated_snapshot_id") && !refsByName.containsKey("last_updated_at")) {
            Column lastUpdatedAtCol = operator.getTable().getColumn("last_updated_at");
            if (lastUpdatedAtCol == null) {
                throw new StarRocksConnectorException(
                        "Missing last_updated_at column in iceberg_partitions_table metadata schema");
            }
            syntheticLastUpdatedAt = context.getColumnRefFactory().create(
                    "last_updated_at", lastUpdatedAtCol.getType(), lastUpdatedAtCol.isAllowNull());
            scanColRefMap = new HashMap<>(originalColRefMap);
            scanColRefMap.put(syntheticLastUpdatedAt, lastUpdatedAtCol);
            scanColumnMetaToColRefMap = new HashMap<>(originalColumnMetaToColRefMap);
            scanColumnMetaToColRefMap.put(lastUpdatedAtCol, syntheticLastUpdatedAt);
            refsByName.put("last_updated_at", syntheticLastUpdatedAt);
        }

        List<ColumnRefOperator> groupingKeys = new ArrayList<>();
        Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, Column> entries : originalColRefMap.entrySet()) {
            ColumnRefOperator columnRefOperator = entries.getKey();
            Column column = entries.getValue();
            CallOperator agg = null;
            Function fun;
            Type[] argTypes = new Type[] {column.getType()};
            String columnName = columnRefOperator.getName();
            switch (columnName) {
                case "partition_value":
                    groupingKeys.add(columnRefOperator);
                    break;
                case "spec_id":
                    fun = ExprUtils.getBuiltinFunction("any_value", argTypes, Function.CompareMode.IS_IDENTICAL);
                    agg = new CallOperator("any_value", IntegerType.INT, Lists.newArrayList(columnRefOperator), fun);
                    break;
                case "record_count":
                case "total_data_file_size_in_bytes":
                case "position_delete_record_count":
                case "equality_delete_record_count":
                case "file_count":
                case "position_delete_file_count":
                case "equality_delete_file_count":
                    fun = ExprUtils.getBuiltinFunction("sum", argTypes, Function.CompareMode.IS_IDENTICAL);
                    agg = new CallOperator("sum", IntegerType.BIGINT, Lists.newArrayList(columnRefOperator), fun);
                    break;
                case "last_updated_at":
                    fun = ExprUtils.getBuiltinFunction("max", argTypes, Function.CompareMode.IS_IDENTICAL);
                    agg = new CallOperator("max", DateType.DATETIME, Lists.newArrayList(columnRefOperator), fun);
                    break;
                case "last_updated_snapshot_id": {
                    ColumnRefOperator lastUpdatedAtRef = refsByName.get("last_updated_at");
                    Type[] maxByArgs = new Type[] {column.getType(), lastUpdatedAtRef.getType()};
                    fun = ExprUtils.getBuiltinFunction("max_by", maxByArgs, Function.CompareMode.IS_IDENTICAL);
                    agg = new CallOperator("max_by", IntegerType.BIGINT,
                            Lists.newArrayList(columnRefOperator, lastUpdatedAtRef), fun);
                    break;
                }
                default:
                    throw new StarRocksConnectorException("Unknown column name %s when rewriting " +
                            "iceberg partitions table", columnName);
            }

            if (agg != null) {
                aggregations.put(columnRefOperator, agg);
            }
        }

        LogicalAggregationOperator aggOp = new LogicalAggregationOperator(AggType.GLOBAL, groupingKeys, aggregations);
        LogicalIcebergMetadataScanOperator newScanOp = new LogicalIcebergMetadataScanOperator(
                operator.getTable(),
                scanColRefMap,
                scanColumnMetaToColRefMap,
                operator.getLimit(),
                operator.getPredicate());
        newScanOp.setTransformed(true);
        OptExpression aggExpr = OptExpression.create(aggOp, OptExpression.create(newScanOp));

        if (syntheticLastUpdatedAt == null) {
            return Collections.singletonList(aggExpr);
        }

        Map<ColumnRefOperator, ScalarOperator> projectMap = new HashMap<>();
        for (ColumnRefOperator ref : originalColRefMap.keySet()) {
            projectMap.put(ref, ref);
        }
        return Collections.singletonList(OptExpression.create(new LogicalProjectOperator(projectMap), aggExpr));
    }
}
