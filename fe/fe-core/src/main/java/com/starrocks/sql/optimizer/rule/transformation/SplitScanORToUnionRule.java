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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class SplitScanORToUnionRule extends TransformationRule {
    private SplitScanORToUnionRule() {
        super(RuleType.TF_SPLIT_SCAN_OR, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    public static SplitScanORToUnionRule getInstance() {
        return INSTANCE;
    }

    private static final SplitScanORToUnionRule INSTANCE = new SplitScanORToUnionRule();

    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.getOp();
        ScalarOperator predicate = scan.getPredicate();
        if (predicate == null) {
            return false;
        }
        if (!(predicate instanceof CompoundPredicateOperator)) {
            return false;
        }
        CompoundPredicateOperator compoundOperator = (CompoundPredicateOperator) predicate;
        if (compoundOperator.isAnd() || compoundOperator.isNot()) {
            return false;
        }
        if (compoundOperator.getChildren().size() != 2) {
            return false;
        }
        ScalarOperator child1 = compoundOperator.getChild(0);
        ScalarOperator child2 = compoundOperator.getChild(1);
        return Utils.isOneLevelEqualBinaryPredicate(child1) && Utils.isOneLevelEqualBinaryPredicate(child2);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.getOp();
        ScalarOperator predicate = scan.getPredicate();
        CompoundPredicateOperator compoundOperator = (CompoundPredicateOperator) predicate;
        BinaryPredicateOperator predicate1 = (BinaryPredicateOperator) compoundOperator.getChild(0);
        BinaryPredicateOperator predicate2 = (BinaryPredicateOperator) compoundOperator.getChild(1);

        List<ColumnRefOperator> outputColumns = scan.getOutputColumns();
        List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayList();
        childOutputColumns.add(outputColumns);
        childOutputColumns.add(outputColumns);
        childOutputColumns.add(outputColumns);

        LogicalUnionOperator unionOperator = new LogicalUnionOperator(outputColumns, childOutputColumns, true);

        LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        builder.withOperator(scan);
        builder.setPredicate(predicate1);
        LogicalOlapScanOperator scan1 = builder.build();


        BinaryPredicateOperator nePredicate = predicate1.negative();
        ScalarOperator and = Utils.compoundAnd(predicate2, nePredicate);

        builder = new LogicalOlapScanOperator.Builder();
        builder.withOperator(scan);
        builder.setPredicate(and);
        LogicalOlapScanOperator scan2 = builder.build();

        IsNullPredicateOperator isNull = new IsNullPredicateOperator(predicate1.getChild(0));
        ScalarOperator an2 = Utils.compoundAnd(predicate2, isNull);

        builder = new LogicalOlapScanOperator.Builder();
        builder.withOperator(scan);
        builder.setPredicate(an2);
        LogicalOlapScanOperator scan3 = builder.build();

        OptExpression expr =
                OptExpression.create(unionOperator,
                        OptExpression.create(scan1), OptExpression.create(scan2), OptExpression.create(scan3));
        return Lists.newArrayList(expr);
    }
}
