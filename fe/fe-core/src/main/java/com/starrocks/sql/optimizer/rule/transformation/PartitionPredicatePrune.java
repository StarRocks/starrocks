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
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// Prune predicate if the data of partitions is meets the predicate, can be avoid execute predicate
//
// Note:
//  Partition value range always be Left-Closed-Right-Open interval
//
// Attention:
//  1. Only support single partition column
//  2. Only support prune BinaryType predicate
//
// e.g.
// select partitions:
//  PARTITION p3 VALUES [2020-04-01, 2020-07-01)
//  PARTITION p4 VALUES [2020-07-01, 2020-12-01)
//
// predicate:
//  d = 2020-02-02 AND d > 2020-08-01, None prune
//  d >= 2020-04-01 AND d > 2020-09-01, All Prune
//  d >= 2020-04-01 AND d < 2020-09-01, "d >= 2020-04-01" prune, "d < 2020-09-01" not prune
//  d IN (2020-05-01, 2020-06-01), None prune
public class PartitionPredicatePrune extends TransformationRule {
    public PartitionPredicatePrune() {
        super(RuleType.TF_PARTITION_PREDICATE_PRUNE, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator logicalOlapScanOperator = (LogicalOlapScanOperator) input.getOp();
        OlapTable table = (OlapTable) logicalOlapScanOperator.getTable();
        PartitionInfo partitionInfo = table.getPartitionInfo();
        return partitionInfo.getType() == PartitionType.RANGE;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator logicalOlapScanOperator = (LogicalOlapScanOperator) input.getOp();
        OlapTable table = (OlapTable) logicalOlapScanOperator.getTable();
        RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        List<Long> partitionIds = logicalOlapScanOperator.getSelectedPartitionId();

        if (partitionInfo.getPartitionColumns().size() != 1 || partitionIds.isEmpty()) {
            return Collections.emptyList();
        }

        List<ScalarOperator> allPredicate = Utils.extractConjuncts(logicalOlapScanOperator.getPredicate());
        List<ScalarOperator> removePredicate = Lists.newArrayList();
        Map<String, PartitionColumnFilter> predicateRangeMap = Maps.newHashMap();

        String columnName = partitionInfo.getPartitionColumns().get(0).getName();
        Column column = logicalOlapScanOperator.getTable().getColumn(columnName);

        List<Range<PartitionKey>> partitionRanges =
                partitionIds.stream().map(partitionInfo::getRange).collect(Collectors.toList());

        PartitionKey minRange =
                Collections.min(partitionRanges.stream().map(Range::lowerEndpoint).collect(Collectors.toList()));
        PartitionKey maxRange =
                Collections.max(partitionRanges.stream().map(Range::upperEndpoint).collect(Collectors.toList()));

        for (ScalarOperator predicate : allPredicate) {
            if (!Utils.containColumnRef(predicate, columnName)) {
                continue;
            }

            predicateRangeMap.clear();
            ColumnFilterConverter.convertColumnFilter(predicate, predicateRangeMap, table);

            if (predicateRangeMap.isEmpty()) {
                continue;
            }

            PartitionColumnFilter pcf = predicateRangeMap.get(columnName);

            // In predicate don't support predicate prune
            if (null != pcf.getInPredicateLiterals()) {
                continue;
            }

            // None/Null bound predicate can't prune
            if ((null == pcf.lowerBound || pcf.lowerBound.isConstantNull()) &&
                    (null == pcf.upperBound || pcf.upperBound.isConstantNull())) {
                continue;
            }

            boolean lowerBind = true;
            boolean upperBind = true;
            if (null != pcf.lowerBound) {
                lowerBind = false;
                PartitionKey min = new PartitionKey();
                min.pushColumn(pcf.lowerBound, column.getPrimitiveType());
                int cmp = minRange.compareTo(min);
                if (cmp > 0 || (0 == cmp && pcf.lowerBoundInclusive)) {
                    lowerBind = true;
                }
            }

            if (null != pcf.upperBound) {
                upperBind = false;
                PartitionKey max = new PartitionKey();
                max.pushColumn(pcf.upperBound, column.getPrimitiveType());
                int cmp = maxRange.compareTo(max);
                if (cmp < 0 || (0 == cmp && !pcf.upperBoundInclusive)) {
                    upperBind = true;
                }
            }

            if (lowerBind && upperBind) {
                removePredicate.add(predicate);
            }
        }

        if (removePredicate.isEmpty()) {
            return Collections.emptyList();
        }

        allPredicate.removeAll(removePredicate);

        LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        return Lists.newArrayList(OptExpression.create(
                builder.withOperator(logicalOlapScanOperator).setPredicate(Utils.compoundAnd(allPredicate))
                        .setPrunedPartitionPredicates(removePredicate).build(),
                input.getInputs()));
    }
}
