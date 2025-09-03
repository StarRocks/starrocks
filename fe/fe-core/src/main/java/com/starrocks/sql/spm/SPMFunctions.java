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

package com.starrocks.sql.spm;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.BetweenPredicate;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionName;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

// SPMFunction to mark the variables in plan
public class SPMFunctions {
    // spm function: NULL_TYPE _spm_xxx(placeholderID, actual parameters),
    // the actual parameters used to compute statistics
    // NULL_TYPE _spm_const_list(placeholderID)
    static final String CONST_LIST_FUNC = "_spm_const_list";
    // NULL_TYPE _spm_const_var(placeholderID)
    static final String CONST_VAR_FUNC = "_spm_const_var";

    // NULL_TYPE _spm_const_range(placeholderID, min, max)
    static final String CONST_RANGE_FUNC = "_spm_const_range";
    // NULL_TYPE _spm_const_enum(placeholderID, enum...)
    static final String CONST_ENUM_FUNC = "_spm_const_enum";

    // for ut test
    public static boolean enableSPMParamsPrint = false;

    private static final Set<String> SPM_FUNCTIONS =
            Set.of(CONST_LIST_FUNC, CONST_VAR_FUNC, CONST_RANGE_FUNC, CONST_ENUM_FUNC);

    private static final Set<String> CONTAIN_VALUES_FUNCTIONS = Set.of(CONST_LIST_FUNC, CONST_VAR_FUNC);

    public static Function getSPMFunction(FunctionCallExpr expr, List<Type> argsTypes) {
        if (!SPM_FUNCTIONS.contains(StringUtils.lowerCase(expr.getFnName().getFunction()))) {
            return null;
        }
        if (expr.getChildren().stream().anyMatch(p -> !p.isConstant())) {
            throw new SemanticException("spm function's parameters must be const");
        }
        return getSPMFunction(expr.getFnName().getFunction(), Type.NULL, argsTypes);
    }

    private static Function getSPMFunction(String fnName, Type type, List<Type> argsTypes) {
        if (!SPM_FUNCTIONS.contains(StringUtils.lowerCase(fnName))) {
            return null;
        }
        if (argsTypes.isEmpty()) {
            throw new SemanticException("spm function must have placeholder id");
        }
        if (!argsTypes.get(0).isIntegerType()) {
            throw new SemanticException("spm function placeholder id must be integer");
        }

        if (CONST_VAR_FUNC.equalsIgnoreCase(fnName)) {
            if (argsTypes.size() > 2) {
                throw new SemanticException("spm function _spm_const_var must have at most two parameters");
            }
        } else if (CONST_RANGE_FUNC.equalsIgnoreCase(fnName)) {
            if (argsTypes.size() != 3) {
                throw new SemanticException("spm function _spm_const_range must have three parameters");
            }
            if (!ScalarType.canCastTo(argsTypes.get(1), argsTypes.get(2))) {
                throw new SemanticException("spm function _spm_const_range min/max type must be same");
            }
        } else if (CONST_ENUM_FUNC.equalsIgnoreCase(fnName)) {
            if (argsTypes.size() < 2) {
                throw new SemanticException("spm function _spm_const_enum must have at least two parameters");
            }
            Type type1 = argsTypes.get(1);
            for (int i = 2; i < argsTypes.size(); i++) {
                if (!ScalarType.canCastTo(argsTypes.get(i), type1)) {
                    throw new SemanticException("spm function _spm_const_enum enum type must be same");
                }
            }
        }
        return new ScalarFunction(new FunctionName(StringUtils.lowerCase(fnName)), argsTypes, type, false);
    }

    public static FunctionCallExpr newFunc(String func, long placeholderID, List<Expr> input) {
        List<Expr> children = Lists.newArrayList(new IntLiteral(placeholderID, Type.BIGINT));
        children.addAll(input);
        FunctionCallExpr expr = new FunctionCallExpr(func, children);
        expr.setFn(getSPMFunction(func, Type.NULL, children.stream().map(Expr::getType).toList()));
        expr.setType(Type.NULL);
        return expr;
    }

    public static boolean isSPMFunctions(Expr expr) {
        if ((expr instanceof InPredicate) && expr.getChildren().size() == 2) {
            expr = expr.getChild(1);
        }
        return (expr instanceof FunctionCallExpr)
                && SPM_FUNCTIONS.contains(((FunctionCallExpr) expr).getFnName().getFunction().toLowerCase());
    }

    public static boolean isSPMFunctions(ScalarOperator operator) {
        if ((operator.getOpType() == OperatorType.IN) && operator.getChildren().size() == 2) {
            operator = operator.getChild(1);
        }
        return (operator.getOpType() == OperatorType.CALL) && SPM_FUNCTIONS.contains(
                ((CallOperator) operator).getFnName().toLowerCase());
    }

    public static ScalarOperator castSPMFunctions(ScalarOperator operator, Type type) {
        if (operator.getOpType() == OperatorType.IN) {
            return operator;
        }
        CallOperator call = (CallOperator) operator;
        call.setType(type);
        List<Type> argTypes = Lists.newArrayList();
        argTypes.add(Type.BIGINT);
        if (!call.getChild(0).getType().equals(Type.BIGINT)) {
            call.setChild(0, new CastOperator(Type.BIGINT, call.getChild(0)));
        }
        for (int i = 1; i < call.getChildren().size(); i++) {
            call.setChild(i, new CastOperator(type, call.getChild(i)));
            // skip placeholder id
            argTypes.add(type);
        }
        call.setFunction(getSPMFunction(call.getFunction().functionName(), type, argTypes));
        return call;
    }

    public static boolean canRevert2ScalarOperator(ScalarOperator operator) {
        if ((operator.getOpType() == OperatorType.IN) && operator.getChildren().size() == 2) {
            operator = operator.getChild(1);
        }
        return operator.getChildren().size() > 1 && (operator.getOpType() == OperatorType.CALL)
                && CONTAIN_VALUES_FUNCTIONS.contains(((CallOperator) operator).getFnName().toLowerCase());
    }

    // SPM operator revert to scalar operator
    public static List<ScalarOperator> revertSPMFunctions(ScalarOperator operator) {
        if (operator.getOpType() == OperatorType.CALL) {
            List<ScalarOperator> newChildren = Lists.newArrayList();
            operator.getChildren().stream().skip(1).forEach(newChildren::add);
            return newChildren;
        } else if (operator instanceof InPredicateOperator in) {
            List<ScalarOperator> newChildren = Lists.newArrayList();
            newChildren.add(operator.getChild(0));
            operator.getChild(1).getChildren().stream().skip(1).forEach(newChildren::add);
            return List.of(new InPredicateOperator(in.isNotIn(), newChildren));
        }
        return List.of(operator);
    }

    public static String toSQL(String function, List<String> children) {
        if (enableSPMParamsPrint) {
            return function + "(" + String.join(", ", children) + ")";
        }
        if (CONTAIN_VALUES_FUNCTIONS.contains(function)) {
            return function + "(" + children.get(0) + ")";
        } else {
            return function + "(" + String.join(", ", children) + ")";
        }
    }

    public static List<ColumnStatistic> getSPMFunctionStatistics(CallOperator operator,
                                                                 List<ColumnStatistic> children) {
        if (children.size() <= 1) {
            return List.of(ColumnStatistic.unknown());
        }
        if (CONST_VAR_FUNC.equals(operator.getFnName())) {
            return List.of(children.get(1));
        } else if (CONST_RANGE_FUNC.equals(operator.getFnName())) {
            ColumnStatistic min = children.get(1);
            ColumnStatistic max = children.get(2);
            ColumnStatistic result = ColumnStatistic.builder()
                    .setMaxValue(max.getMaxValue()).setMaxString(max.getMaxString())
                    .setMinValue(min.getMinValue()).setMinString(min.getMinString())
                    .setDistinctValuesCount(1)
                    .setNullsFraction(0.0)
                    .setAverageRowSize(max.getAverageRowSize())
                    .build();
            return List.of(result);
        } else if (CONST_ENUM_FUNC.equals(operator.getFnName())) {
            ColumnStatistic min = children.stream().skip(1).min(Comparator.comparing(ColumnStatistic::getMinValue))
                    .orElse(ColumnStatistic.unknown());
            ColumnStatistic max = children.stream().skip(1).max(Comparator.comparing(ColumnStatistic::getMaxValue))
                    .orElse(ColumnStatistic.unknown());
            return List.of(ColumnStatistic.builder()
                    .setMaxValue(max.getMaxValue()).setMaxString(max.getMaxString())
                    .setMinValue(min.getMinValue()).setMinString(min.getMinString())
                    .setDistinctValuesCount(1)
                    .setNullsFraction(0.0)
                    .setAverageRowSize(max.getAverageRowSize())
                    .build());
        }
        return List.of(ColumnStatistic.unknown());
    }

    private static boolean checkParameters(FunctionCallExpr spmExpr, List<Expr> checkParameters) {
        String fn = spmExpr.getFnName().getFunction();
        try {
            Expr checkExpr;
            if (CONST_RANGE_FUNC.equalsIgnoreCase(fn)) {
                Expr min = spmExpr.getChild(1);
                Expr max = spmExpr.getChild(2);
                List<Expr> values = checkParameters.stream()
                        .map(p -> (Expr) new BetweenPredicate(p, min, max, false)).toList();
                checkExpr = CompoundPredicate.compoundAnd(values);
            } else if (CONST_ENUM_FUNC.equalsIgnoreCase(fn)) {
                List<Expr> values = spmExpr.getChildren().stream().skip(1).toList();
                List<Expr> inPredicates = checkParameters.stream()
                        .map(p -> (Expr) new InPredicate(p, values, false)).toList();
                checkExpr = CompoundPredicate.compoundAnd(inPredicates);
            } else {
                return false;
            }
            ScalarOperator p = SqlToScalarOperatorTranslator.translate(checkExpr);
            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            ScalarOperator result = rewriter.rewrite(p, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
            if (result.isConstantTrue()) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    public static boolean checkParameters(FunctionCallExpr spmExpr, Expr other) {
        if (!other.isConstant()) {
            return false;
        }
        String fn = spmExpr.getFnName().getFunction();
        if (CONTAIN_VALUES_FUNCTIONS.contains(fn)) {
            return true;
        }
        return checkParameters(spmExpr, List.of(other));
    }

    public static boolean checkParameters(InPredicate spmIn, InPredicate other) {
        if (other.getChildren().stream().skip(1).anyMatch(p -> !p.isConstant())) {
            return false;
        }
        FunctionCallExpr spmFunc = (FunctionCallExpr) spmIn.getChild(1);
        String fn = spmFunc.getFnName().getFunction();
        if (CONST_LIST_FUNC.equalsIgnoreCase(fn)) {
            return true;
        }
        if (CONST_VAR_FUNC.equalsIgnoreCase(fn)) {
            return other.getChildren().size() == 2;
        }
        return checkParameters(spmFunc, other.getChildren().stream().skip(1).toList());
    }
}
