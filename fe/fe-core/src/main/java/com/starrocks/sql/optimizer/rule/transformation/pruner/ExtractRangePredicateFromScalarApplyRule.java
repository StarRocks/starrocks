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

package com.starrocks.sql.optimizer.rule.transformation.pruner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.util.Util;

import java.util.List;
import java.util.Map;

/* Extract range predicates from column binOp scalar-subquery.
For the following query.

select *
from  site_access
where event_day {OP} (
    select convert_tz('2020-01-05 01:01:01', server_tz, txn_tz)
    from zones
    where id = 1
);

we know that the final result of convert_tz('2020-01-05 01:01:01', server_tz, txn_tz) must between
days_add('2020-01-05 01:01:01', -1) and days_add('2020-01-05 01:01:01', 1); so we can extract such
predicates from this binary operator to prune partitions in FE or speedup rows filtering when scanning tables.
*/

public class ExtractRangePredicateFromScalarApplyRule extends TransformationRule {

    public ExtractRangePredicateFromScalarApplyRule() {
        super(RuleType.TF_EXTRACT_RANGE_PREDICATE_FROM_SCALAR_APPLY, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT).addChildren(
                        Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF,
                                OperatorType.LOGICAL_PROJECT))));
    }

    boolean canExtractRangePredicate(ScalarOperator scalarOp) {
        return Util.downcast(scalarOp, CallOperator.class)
                .map(call -> call.getFnName().equals(FunctionSet.CONVERT_TZ) && call.getChild(0).isConstantRef())
                .orElse(false);
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator projectOp = input.inputAt(0).inputAt(0).inputAt(1).getOp().cast();
        if (projectOp.getColumnRefMap().size() != 1) {
            return false;
        }
        Map.Entry<ColumnRefOperator, ScalarOperator> entry = projectOp.getColumnRefMap().entrySet().iterator().next();
        ColumnRefOperator columRef = entry.getKey();
        ScalarOperator scalarOp = entry.getValue();

        LogicalFilterOperator filterOp = input.getOp().cast();
        LogicalApplyOperator applyOp = input.inputAt(0).inputAt(0).getOp().cast();
        boolean existBinaryOp = Utils.extractConjuncts(filterOp.getPredicate()).stream().anyMatch(
                conjunct -> Util.downcast(conjunct, BinaryPredicateOperator.class)
                        .map(binOp -> binOp.getColumnRefs().contains(applyOp.getOutput())).orElse(false));
        if (!existBinaryOp) {
            return false;
        }
        return canExtractRangePredicate(scalarOp);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filterOp = input.getOp().cast();
        LogicalApplyOperator applyOp = input.inputAt(0).inputAt(0).getOp().cast();
        LogicalProjectOperator projectOp = input.inputAt(0).inputAt(0).inputAt(1).getOp().cast();
        Map.Entry<ColumnRefOperator, ScalarOperator> entry = projectOp.getColumnRefMap().entrySet().iterator().next();
        ScalarOperator scalarOp = entry.getValue();
        CallOperator callOp = (CallOperator) scalarOp;
        ConstantOperator constOp = (ConstantOperator) callOp.getChild(0);
        // timezone offsets can differ by up to 26 hours
        // (e.g., Pacific/Kiritimati +14 vs Etc/GMT+12 -12, or DST transitions)
        Function fn = Expr.getBuiltinFunction(FunctionSet.HOURS_ADD, new Type[] {Type.DATETIME, Type.INT},
                Function.CompareMode.IS_IDENTICAL);
        ScalarOperator prevDay = new CallOperator(FunctionSet.HOURS_ADD, Type.DATETIME,
                List.of(constOp, ConstantOperator.createInt(-26)), fn);
        ScalarOperator nextDay = new CallOperator(FunctionSet.HOURS_ADD, Type.DATETIME,
                List.of(constOp, ConstantOperator.createInt(26)), fn);
        Map<ColumnRefOperator, ScalarOperator> colRef2PrevDay = Maps.newHashMap();
        colRef2PrevDay.put(applyOp.getOutput(), prevDay);

        Map<ColumnRefOperator, ScalarOperator> colRef2NextDay = Maps.newHashMap();
        colRef2NextDay.put(applyOp.getOutput(), nextDay);

        ReplaceColumnRefRewriter prevDayReplacer = new ReplaceColumnRefRewriter(colRef2PrevDay);
        ReplaceColumnRefRewriter nextDayReplacer = new ReplaceColumnRefRewriter(colRef2NextDay);

        List<ScalarOperator> conjuncts = Utils.extractConjuncts(filterOp.getPredicate());
        List<ScalarOperator> newConjuncts = Lists.newArrayList();

        for (ScalarOperator conj : conjuncts) {
            if (!(conj instanceof BinaryPredicateOperator)) {
                continue;
            }
            BinaryPredicateOperator binOp = (BinaryPredicateOperator) conj;
            if (!binOp.getChild(1).getColumnRefs().contains(applyOp.getOutput())) {
                binOp = binOp.commutative();
            }
            if (binOp.getChild(1).getColumnRefs().size() != 1 ||
                    !binOp.getChild(1).getColumnRefs().contains(applyOp.getOutput())) {
                continue;
            }
            ScalarOperator actualPrevDay = prevDayReplacer.rewrite(binOp.getChild(1));
            ScalarOperator actualNextDay = nextDayReplacer.rewrite(binOp.getChild(1));
            switch (binOp.getBinaryType()) {
                case EQ: {
                    ScalarOperator newOp =
                            new BetweenPredicateOperator(false, binOp.getChild(0), actualPrevDay, actualNextDay);
                    newConjuncts.add(newOp);
                    break;
                }
                case LT:
                case LE: {
                    ScalarOperator newOp = BinaryPredicateOperator.le(binOp.getChild(0), actualNextDay);
                    newConjuncts.add(newOp);
                    break;
                }
                case GT:
                case GE: {
                    ScalarOperator newOp = BinaryPredicateOperator.ge(binOp.getChild(0), actualPrevDay);
                    newConjuncts.add(newOp);
                    break;
                }
            }
        }
        if (newConjuncts.isEmpty()) {
            return List.of();
        }
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        newConjuncts.replaceAll(op -> rewriter.rewrite(op, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES));
        newConjuncts.addAll(conjuncts);
        ScalarOperator newPredicate = Utils.compoundAnd(newConjuncts);
        Operator newOp =
                OperatorBuilderFactory.build(filterOp).withOperator(filterOp).setPredicate(newPredicate).build();
        return List.of(OptExpression.create(newOp, input.inputAt(0)));
    }
}