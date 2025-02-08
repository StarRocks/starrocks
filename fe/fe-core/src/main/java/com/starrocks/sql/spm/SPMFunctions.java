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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import org.apache.commons.lang3.StringUtils;

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

    private static final Set<String> SPM_FUNCTIONS = Set.of(CONST_LIST_FUNC, CONST_VAR_FUNC);

    public static Function getSPMFunction(String fnName) {
        return getSPMFunction(fnName, Type.NULL);
    }

    private static Function getSPMFunction(String fnName, Type type) {
        if (!SPM_FUNCTIONS.contains(StringUtils.lowerCase(fnName))) {
            return null;
        }

        return new ScalarFunction(new FunctionName(StringUtils.lowerCase(fnName)),
                new Type[] {Type.BIGINT}, type, false);
    }

    public static FunctionCallExpr newFunc(String func, long placeholderID, List<Expr> children) {
        FunctionCallExpr expr =
                new FunctionCallExpr(func, Lists.newArrayList(new IntLiteral(placeholderID, Type.BIGINT)));
        expr.setFn(getSPMFunction(func, Type.NULL));
        expr.setType(Type.NULL);
        expr.addChildren(children);
        return expr;
    }

    public static boolean isSPMFunctions(Expr expr) {
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }

        return SPM_FUNCTIONS.contains(((FunctionCallExpr) expr).getFnName()
                .getFunction().toLowerCase());
    }

    public static boolean isSPMFunctions(ScalarOperator operator) {
        if (!(operator instanceof CallOperator)) {
            return false;
        }

        return SPM_FUNCTIONS.contains(((CallOperator) operator).getFunction()
                .getFunctionName().getFunction().toLowerCase());
    }



    public static ScalarOperator castSPMFunctions(ScalarOperator operator, Type type) {
        CallOperator call = (CallOperator) operator;
        call.setType(type);
        call.setFunction(getSPMFunction(call.getFunction().functionName(), type));
        return call;
    }

    public static String toSQL(String function, List<String> children) {
        return function + "(" + children.get(0) + ")";
    }

    public static List<ColumnStatistic> getSPMFunctionStatistics(CallOperator operator,
                                                                 List<ColumnStatistic> children) {
        Preconditions.checkState(CONST_VAR_FUNC.equals(operator.getFnName()));
        Preconditions.checkState(children.size() > 1);
        return children;
    }
}
