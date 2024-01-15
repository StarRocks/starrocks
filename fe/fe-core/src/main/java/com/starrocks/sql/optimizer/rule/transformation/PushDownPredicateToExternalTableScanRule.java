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
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ExternalTablePredicateExtractor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

// Because the external table may not support the functions in StarRocks,
// to be on the safe side, we only push down partial predicates to the external table
public class PushDownPredicateToExternalTableScanRule extends TransformationRule {
    public static final PushDownPredicateToExternalTableScanRule MYSQL_SCAN =
            new PushDownPredicateToExternalTableScanRule(OperatorType.LOGICAL_MYSQL_SCAN);
    public static final PushDownPredicateToExternalTableScanRule JDBC_SCAN =
            new PushDownPredicateToExternalTableScanRule(OperatorType.LOGICAL_JDBC_SCAN);
    public static final PushDownPredicateToExternalTableScanRule ODPS_SCAN =
            new PushDownPredicateToExternalTableScanRule(OperatorType.LOGICAL_ODPS_SCAN);

    public PushDownPredicateToExternalTableScanRule(OperatorType type) {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_TO_EXTERNAL_TABLE_SCAN,
                Pattern.create(OperatorType.LOGICAL_FILTER, type));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator lfo = (LogicalFilterOperator) input.getOp();

        OptExpression optExpression = input.getInputs().get(0);
        Operator operator = optExpression.getOp();
        Operator.Builder builder = OperatorBuilderFactory.build(operator);

        ScalarOperator predicate = Utils.compoundAnd(lfo.getPredicate(), operator.getPredicate());
        ScalarOperator scanPredicate = operator.getPredicate();
        ScalarOperator filterPredicate = lfo.getPredicate();
        ExternalTablePredicateExtractor extractor = new ExternalTablePredicateExtractor(
                operator.getOpType() == OperatorType.LOGICAL_MYSQL_SCAN);
        extractor.extract(predicate);
        ScalarOperator pushedPredicate = extractor.getPushPredicate();
        ScalarOperator reservedPredicate = extractor.getReservePredicate();

        boolean newScanPredicateIsSame = Objects.equals(scanPredicate, pushedPredicate);
        boolean newFilterPredicateIsSame = Objects.equals(filterPredicate, reservedPredicate);

        if (newScanPredicateIsSame && newFilterPredicateIsSame) {
            // nothing changed after transform
            return new ArrayList<>();
        }

        Operator newOperator = builder.withOperator(operator)
                .setPredicate(pushedPredicate).build();
        LogicalScanOperator scanOperator = (LogicalScanOperator) newOperator;

        Map<ColumnRefOperator, ScalarOperator> scanOutput = scanOperator.getOutputColumns().stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
        if (reservedPredicate == null) {
            /*
             * all predicates can push down
             *
             *    Filter          Project
             *      |      --->      |
             *     Scan         Scan(Predicate)
             *
             * */

            LogicalProjectOperator projectOperator = new LogicalProjectOperator(scanOutput);

            // Add project node upon scan node for column prune later
            OptExpression project = OptExpression.create(projectOperator,
                    OptExpression.create(scanOperator, optExpression.getInputs()));
            return Lists.newArrayList(project);
        } else {
            /*
             *  some predicates can't push down
             *
             *   Filter             Project
             *     |      --->         |
             *    Scan          Filter(Reserved Predicates)
             *                         |
             *                  Scan(Pushed Predicates)
             *
             * */

            LogicalFilterOperator filterOperator = new LogicalFilterOperator(reservedPredicate);

            LogicalProjectOperator projectOperator = new LogicalProjectOperator(scanOutput);

            // Add project node upon scan node for column prune later
            OptExpression project = OptExpression.create(projectOperator,
                    OptExpression.create(filterOperator,
                            OptExpression.create(scanOperator, optExpression.getInputs())));

            return Lists.newArrayList(project);
        }
    }
}
