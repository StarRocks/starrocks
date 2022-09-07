// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The processing flow of the subquery is as follows:
 * 1. Transform `Expr` expression to `ScalarOperator` expression, while subquery in simply wrapped in `SubqueryOperator`
 * 2. Parse `SubqueryOperator` to apply operator, and save the `SubqueryOperator -> ColumnRefOperator` in ExpressionMapping
 * 3. rewrite `ScalarOperator` created by step1 with the `ExpressionMapping` created by step2
 * <p>
 * Some function, such as `if(cond, subquery1, subquery2)`, it's output type depends on the arg types,
 * but during step1, we cannot derive the real output type of subquery1 or subquery2 until they are parsed
 * So this rule, executed in step3, will recalculate the argType or returnType of such kind of functions
 */
public class ReplaceSubqueryTypeRewriteRule extends BottomUpScalarOperatorRewriteRule {

    @Override
    public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, ScalarOperatorRewriteContext context) {
        List<Type> types = Lists.newArrayList();
        for (int i = 0; i < operator.getWhenClauseSize(); i++) {
            types.add(operator.getThenClause(i).getType());
        }
        if (operator.hasElse()) {
            types.add(operator.getElseClause().getType());
        }

        Type returnType = TypeManager.getCompatibleTypeForCaseWhen(types);
        if (Objects.equals(returnType, operator.getType())) {
            return operator;
        }

        return new CaseWhenOperator(returnType, operator);
    }

    @Override
    public ScalarOperator visitCall(CallOperator call, ScalarOperatorRewriteContext context) {
        if (FunctionSet.IF.equals(call.getFnName()) || FunctionSet.IFNULL.equals(call.getFnName()) ||
                FunctionSet.NULLIF.equals(call.getFnName()) || FunctionSet.COALESCE.equals(call.getFnName())) {
            int firstExprArgIndex = FunctionSet.IF.equals(call.getFnName()) ? 1 : 0;

            Function function = call.getFunction();

            List<Type> types = call.getChildren().subList(firstExprArgIndex, call.getChildren().size())
                    .stream()
                    .map(ScalarOperator::getType)
                    .collect(Collectors.toList());
            Type returnType = TypeManager.getCompatibleTypeForIf(types);

            boolean returnTypeMatches = Objects.equals(returnType, call.getType());
            boolean argTypeMatches = true;
            for (int i = firstExprArgIndex; i < function.getArgs().length; i++) {
                if (!function.getArgs()[i].matchesType(returnType)) {
                    argTypeMatches = false;
                    break;
                }
            }

            if (returnTypeMatches && argTypeMatches) {
                return call;
            }

            List<Type> newArgTypes = Lists.newArrayList();
            if (firstExprArgIndex > 0) {
                newArgTypes.add(ScalarType.BOOLEAN);
            }
            for (int i = firstExprArgIndex; i < function.getArgs().length; i++) {
                newArgTypes.add(returnType);
            }

            Function newFunction = new Function(function.getFunctionName(), newArgTypes, function.getReturnType(),
                    function.hasVarArgs());
            newFunction.setFunctionId(function.getFunctionId());
            newFunction.setBinaryType(function.getBinaryType());
            newFunction.setChecksum(function.getChecksum());
            newFunction.setUserVisible(function.isUserVisible());
            newFunction.setCouldApplyDictOptimize(function.isCouldApplyDictOptimize());
            newFunction.setIsNullable(function.isNullable());
            newFunction.setLocation(function.getLocation());
            return new CallOperator(call.getFnName(), returnType, call.getChildren(), newFunction);
        }
        return call;
    }
}
