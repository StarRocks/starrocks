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
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergMetadataScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

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
        List<ColumnRefOperator> groupingKeys = new ArrayList<>();
        Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, Column> entries : operator.getColRefToColumnMetaMap().entrySet()) {
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
                    fun = Expr.getBuiltinFunction("any_value", argTypes, Function.CompareMode.IS_IDENTICAL);
                    agg = new CallOperator("any_value", Type.INT, Lists.newArrayList(columnRefOperator), fun);
                    break;
                case "record_count":
                case "total_data_file_size_in_bytes":
                case "position_delete_record_count":
                case "equality_delete_record_count":
                case "file_count":
                case "position_delete_file_count":
                case "equality_delete_file_count":
                    fun = Expr.getBuiltinFunction("sum", argTypes, Function.CompareMode.IS_IDENTICAL);
                    agg = new CallOperator("sum", Type.BIGINT, Lists.newArrayList(columnRefOperator), fun);
                    break;
                case "last_updated_at":
                    fun = Expr.getBuiltinFunction("max", argTypes, Function.CompareMode.IS_IDENTICAL);
                    agg = new CallOperator("max", Type.DATETIME, Lists.newArrayList(columnRefOperator), fun);
                    break;
                default:
                    throw new StarRocksConnectorException("Unknown column name %s when rewriting " +
                            "iceberg partitions table", columnName);
            }

            if (agg != null) {
                aggregations.put(columnRefOperator, agg);
            }
        }

        LogicalAggregationOperator agg = new LogicalAggregationOperator(AggType.GLOBAL, groupingKeys, aggregations);
        LogicalIcebergMetadataScanOperator newScanOp = new LogicalIcebergMetadataScanOperator(
                operator.getTable(),
                operator.getColRefToColumnMetaMap(),
                operator.getColumnMetaToColRefMap(),
                operator.getLimit(),
                operator.getPredicate());
        newScanOp.setTransformed(true);
        return Collections.singletonList(OptExpression.create(agg, OptExpression.create(newScanOp)));
    }
}
