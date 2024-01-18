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

<<<<<<< HEAD
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;

import java.util.ArrayList;

public class PartitionExprAnalyzer {

    public static void analyzePartitionExpr(Expr expr, SlotRef partitionSlotRef) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            Function builtinFunction = null;
            Type targetColType = partitionSlotRef.getType();
            String functionName = functionCallExpr.getFnName().getFunction();
            if (functionName.equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
                Type[] dateTruncType = {Type.VARCHAR, targetColType};
                builtinFunction = Expr.getBuiltinFunction(functionCallExpr.getFnName().getFunction(),
                        dateTruncType, Function.CompareMode.IS_IDENTICAL);
            } else if (functionName.equalsIgnoreCase(FunctionSet.TIME_SLICE)) {
                Type[] timeSliceType = {targetColType, Type.INT, Type.VARCHAR, Type.VARCHAR};
                builtinFunction = Expr.getBuiltinFunction(functionCallExpr.getFnName().getFunction(),
                        timeSliceType, Function.CompareMode.IS_IDENTICAL);
            } else if (functionName.equalsIgnoreCase(FunctionSet.SUBSTR) ||
                    functionName.equalsIgnoreCase(FunctionSet.SUBSTRING)) {
                int paramSize = functionCallExpr.getParams().exprs().size();
                if (paramSize == 2) {
                    Type[] subStrType = {Type.VARCHAR, Type.INT};
                    builtinFunction = Expr.getBuiltinFunction(functionCallExpr.getFnName().getFunction(),
                            subStrType, Function.CompareMode.IS_IDENTICAL);
                    targetColType = Type.VARCHAR;
                } else if (paramSize == 3) {
                    Type[] subStrType = {Type.VARCHAR, Type.INT, Type.INT};
                    builtinFunction = Expr.getBuiltinFunction(functionCallExpr.getFnName().getFunction(),
                            subStrType, Function.CompareMode.IS_IDENTICAL);
                    targetColType = Type.VARCHAR;
                }
            } else if (functionName.equalsIgnoreCase(FunctionSet.STR2DATE)) {
                Type[] str2DateType = {Type.VARCHAR, Type.VARCHAR};
                builtinFunction = Expr.getBuiltinFunction(functionCallExpr.getFnName().getFunction(),
                        str2DateType, Function.CompareMode.IS_IDENTICAL);
                targetColType = Type.DATE;
            }
            if (builtinFunction == null) {
                String msg = String.format("Unsupported partition type %s for function %s", targetColType,
                        functionCallExpr.toSql());
                throw new SemanticException(msg);
            }

            functionCallExpr.setFn(builtinFunction);
            functionCallExpr.setType(targetColType);
        } else if (expr instanceof CastExpr) {
            CastExpr castExpr = (CastExpr) expr;
            castExpr.setType(castExpr.getTargetTypeDef().getType());
            try {
                castExpr.analyze();
            } catch (AnalysisException e) {
                throw new SemanticException("Failed to analyze cast expr:" + castExpr.toSql());
            }
            ArrayList<Expr> children = castExpr.getChildren();
            for (Expr child : children) {
                if (child instanceof FunctionCallExpr) {
                    SlotRef functionCallSlotRef = AnalyzerUtils.getSlotRefFromFunctionCall(child);
                    PartitionExprAnalyzer.analyzePartitionExpr(child, functionCallSlotRef);
                }
            }
        }
    }

=======
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
>>>>>>> branch-2.5-mrs
}