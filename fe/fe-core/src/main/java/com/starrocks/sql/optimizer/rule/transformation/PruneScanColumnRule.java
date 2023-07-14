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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

public class PruneScanColumnRule extends TransformationRule {
    public static final PruneScanColumnRule OLAP_SCAN = new PruneScanColumnRule(OperatorType.LOGICAL_OLAP_SCAN);
    public static final PruneScanColumnRule SCHEMA_SCAN = new PruneScanColumnRule(OperatorType.LOGICAL_SCHEMA_SCAN);
    public static final PruneScanColumnRule MYSQL_SCAN = new PruneScanColumnRule(OperatorType.LOGICAL_MYSQL_SCAN);
    public static final PruneScanColumnRule ES_SCAN = new PruneScanColumnRule(OperatorType.LOGICAL_ES_SCAN);
    public static final PruneScanColumnRule JDBC_SCAN = new PruneScanColumnRule(OperatorType.LOGICAL_JDBC_SCAN);
    public static final PruneScanColumnRule BINLOG_SCAN = new PruneScanColumnRule(OperatorType.LOGICAL_BINLOG_SCAN);

    public PruneScanColumnRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_PRUNE_OLAP_SCAN_COLUMNS, Pattern.create(logicalOperatorType));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator scanOperator = (LogicalScanOperator) input.getOp();
        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();

        // The `outputColumns`s are some columns required but not specified by `requiredOutputColumns`.
        // including columns in predicate or some specialized columns defined by scan operator.
        Set<ColumnRefOperator> outputColumns =
                scanOperator.getColRefToColumnMetaMap().keySet().stream().filter(requiredOutputColumns::contains)
                        .collect(Collectors.toSet());
        outputColumns.addAll(Utils.extractColumnRef(scanOperator.getPredicate()));
        boolean canUseAnyColumn = false;
        if (outputColumns.size() == 0) {
            outputColumns.add(Utils.findSmallestColumnRef(
                    new ArrayList<>(scanOperator.getColRefToColumnMetaMap().keySet())));
            canUseAnyColumn = true;
        }

        if (scanOperator.getColRefToColumnMetaMap().keySet().equals(outputColumns)) {
            scanOperator.setCanUseAnyColumn(canUseAnyColumn);
            return Collections.emptyList();
        } else {
            Map<ColumnRefOperator, Column> newColumnRefMap = outputColumns.stream()
                    .collect(Collectors.toMap(identity(), scanOperator.getColRefToColumnMetaMap()::get));
            if (scanOperator instanceof LogicalOlapScanOperator) {
                LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) scanOperator;

                LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
                LogicalOlapScanOperator newScanOperator = builder.withOperator(olapScanOperator)
                        .setColRefToColumnMetaMap(newColumnRefMap).build();
                newScanOperator.setCanUseAnyColumn(canUseAnyColumn);
                return Lists.newArrayList(new OptExpression(newScanOperator));
            } else {
                LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(scanOperator);
                scanOperator.setCanUseAnyColumn(canUseAnyColumn);
                Operator newScanOperator =
                        builder.withOperator(scanOperator).setColRefToColumnMetaMap(newColumnRefMap).build();
                return Lists.newArrayList(new OptExpression(newScanOperator));
            }
        }
    }
}
