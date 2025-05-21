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
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RewriteLeftJoinToInnerJoinRule extends TransformationRule {
    public RewriteLeftJoinToInnerJoinRule() {
        super(RuleType.TF_REWRITE_LEFT_JOIN_TO_INNER_JOIN,
                Pattern.create(OperatorType.LOGICAL_JOIN)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN),
                                Pattern.create(OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();
        // This rule only applies to LEFT OUTER JOIN
        if (joinOperator.getJoinType() != JoinOperator.LEFT_OUTER_JOIN) {
            return false;
        }

        // Check if the ON predicate involves a foreign key and the foreign key columns are NOT NULL.
        LogicalOlapScanOperator leftScan = (LogicalOlapScanOperator) input.inputAt(0).getOp();
        LogicalOlapScanOperator rightScan = (LogicalOlapScanOperator) input.inputAt(1).getOp();

        Table leftTable = leftScan.getTable();
        Table rightTable = rightScan.getTable();

        if (!(leftTable instanceof OlapTable) || !(rightTable instanceof OlapTable)) {
            return false;
        }

        OlapTable leftOlapTable = (OlapTable) leftTable;

        List<ForeignKeyConstraint> fkConstraints = leftOlapTable.getForeignKeyConstraints();
        if (fkConstraints == null || fkConstraints.isEmpty()) {
            return false;
        }

        List<ScalarOperator> onPredicates = Utils.extractConjuncts(joinOperator.getOnPredicate());

        for (ForeignKeyConstraint fk : fkConstraints) {
            if (fk.getParentTableInfo() == null) {
                continue;
            }
            // Check if the referenced table in FK is the right table of the join
            if (fk.getParentTableInfo().getTableId() != rightTable.getId()) {
                continue;
            }

            // Map column IDs from FK constraint to ColumnRefOperators in the ON predicate
            Map<Integer, ColumnRefOperator> leftJoinColumnRefs = leftScan.getColRefToColumnMetaMap().entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getValue().getColumnId().asInt(), Map.Entry::getKey));
            Map<Integer, ColumnRefOperator> rightJoinColumnRefs = rightScan.getColRefToColumnMetaMap().entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getValue().getColumnId().asInt(), Map.Entry::getKey));
            
            boolean allFkColumnsMatchOnPredicate = true;
            boolean allFkColumnsNotNull = true;

            for (Pair<com.starrocks.catalog.ColumnId, com.starrocks.catalog.ColumnId> fkColPair : fk.getColumnRefPairs()) {
                ColumnRefOperator leftFkColRef = leftJoinColumnRefs.get(fkColPair.first.asInt());
                ColumnRefOperator rightFkColRef = rightJoinColumnRefs.get(fkColPair.second.asInt());

                if (leftFkColRef == null || rightFkColRef == null) {
                    allFkColumnsMatchOnPredicate = false; // Should not happen if FK is valid
                    break;
                }

                // Check if this FK pair is part of the ON predicate
                boolean foundInOnPredicate = false;
                for (ScalarOperator onConjunct : onPredicates) {
                    if (onConjunct instanceof BinaryPredicateOperator) {
                        BinaryPredicateOperator binaryPred = (BinaryPredicateOperator) onConjunct;
                        if (binaryPred.getBinaryType() == BinaryPredicateOperator.BinaryType.EQ) {
                            ScalarOperator predChild0 = binaryPred.getChild(0);
                            ScalarOperator predChild1 = binaryPred.getChild(1);
                            if ((predChild0.equals(leftFkColRef) && predChild1.equals(rightFkColRef)) ||
                                (predChild0.equals(rightFkColRef) && predChild1.equals(leftFkColRef))) {
                                foundInOnPredicate = true;
                                break;
                            }
                        }
                    }
                }
                if (!foundInOnPredicate) {
                    allFkColumnsMatchOnPredicate = false;
                    break;
                }

                // Check if the left FK column is NOT NULL
                Column leftColumn = leftOlapTable.getColumn(fkColPair.first);
                if (leftColumn == null || leftColumn.isAllowNull()) {
                    allFkColumnsNotNull = false;
                    break;
                }
            }

            if (allFkColumnsMatchOnPredicate && allFkColumnsNotNull) {
                return true; // Conditions met for one of the FK constraints
            }
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator currentJoinOperator = (LogicalJoinOperator) input.getOp();

        LogicalJoinOperator.Builder builder = new LogicalJoinOperator.Builder();
        builder.withOperator(currentJoinOperator);
        builder.setJoinType(JoinOperator.INNER_JOIN);
        // The ON predicate remains the same
        builder.setOnPredicate(currentJoinOperator.getOnPredicate());
        builder.setOriginalOnPredicate(currentJoinOperator.getOriginalOnPredicate());


        OptExpression result = OptExpression.create(builder.build(), input.getInputs());
        return Lists.newArrayList(result);
    }
}
