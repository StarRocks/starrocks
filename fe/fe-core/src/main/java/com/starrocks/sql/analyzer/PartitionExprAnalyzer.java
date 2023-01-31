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

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;

public class PartitionExprAnalyzer {

    public static void analyzePartitionExpr(Expr expr, Type targetColType) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            Function builtinFunction = null;
            if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
                Type[] dateTruncType = {Type.VARCHAR, targetColType};
                builtinFunction = Expr.getBuiltinFunction(functionCallExpr.getFnName().getFunction(),
                        dateTruncType, Function.CompareMode.IS_IDENTICAL);
            } else if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.TIME_SLICE)) {
                Type[] timeSliceType = {targetColType, Type.INT, Type.VARCHAR, Type.VARCHAR};
                builtinFunction = Expr.getBuiltinFunction(functionCallExpr.getFnName().getFunction(),
                        timeSliceType, Function.CompareMode.IS_IDENTICAL);
            }
            if (builtinFunction  == null) {
                throw new SemanticException("Unsupported partition type %s for function %s", targetColType,
                        functionCallExpr.toSql());
            }

            functionCallExpr.setFn(builtinFunction);
        }
    }
}
