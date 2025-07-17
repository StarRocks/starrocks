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

package com.starrocks.connector.parser;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;

import java.util.List;
import java.util.Map;

public abstract class BaseFunctionCallTransformer {
    public static Map<String, List<FunctionCallTransformer>> TRANSFORMER_MAP = Maps.newHashMap();

    public BaseFunctionCallTransformer() {
        registerAllFunctionTransformer();
    }

    protected abstract void registerAllFunctionTransformer();

    protected abstract Expr convert(String fnName, List<Expr> children);

    public static Expr convertRegisterFn(String fnName, List<Expr> children) {
        List<FunctionCallTransformer> transformers = TRANSFORMER_MAP.get(fnName);
        if (transformers == null) {
            return null;
        }

        FunctionCallTransformer matcher = null;
        for (FunctionCallTransformer transformer : transformers) {
            if (transformer.match(children)) {
                matcher = transformer;
            }
        }
        if (matcher == null) {
            return null;
        }
        return matcher.transform(children);
    }

    protected static FunctionCallExpr buildStarRocksFunctionCall(String starRocksFnName,
                                                               List<Class<? extends Expr>> starRocksArgumentsClass) {
        List<Expr> arguments = Lists.newArrayList();
        for (int index = 0; index < starRocksArgumentsClass.size(); ++index) {
            // For a FunctionCallExpr, do not know the actual arguments here, so we use a PlaceholderExpr to replace it.
            arguments.add(new PlaceholderExpr(index + 1, starRocksArgumentsClass.get(index)));
        }
        return new FunctionCallExpr(starRocksFnName, arguments);
    }


    protected static void registerFunctionTransformer(String originFnName, int originFnArgNums, String starRocksFnName,
                                                    List<Class<? extends Expr>> starRocksArgumentsClass) {
        FunctionCallExpr starRocksFunctionCall = buildStarRocksFunctionCall(starRocksFnName, starRocksArgumentsClass);
        registerFunctionTransformer(originFnName, originFnArgNums, starRocksFunctionCall);
    }

    protected static void registerFunctionTransformer(String originFnName, String starRocksFnName) {
        FunctionCallExpr starRocksFunctionCall = buildStarRocksFunctionCall(starRocksFnName, Lists.newArrayList());
        registerFunctionTransformer(originFnName, 0, starRocksFunctionCall);
    }

    protected static void registerFunctionTransformerWithVarArgs(String originFnName, String starRocksFnName,
                                                               List<Class<? extends Expr>> starRocksArgumentsClass) {
        Preconditions.checkState(starRocksArgumentsClass.size() == 1);
        FunctionCallExpr starRocksFunctionCall = buildStarRocksFunctionCall(starRocksFnName, starRocksArgumentsClass);
        registerFunctionTransformerWithVarArgs(originFnName, starRocksFunctionCall);
    }

    protected static void registerFunctionTransformer(String originFnName, int originFnArgNums,
                                                    FunctionCallExpr starRocksFunctionCall) {
        FunctionCallTransformer transformer = new FunctionCallTransformer(starRocksFunctionCall, originFnArgNums);

        List<FunctionCallTransformer> transformerList = TRANSFORMER_MAP.computeIfAbsent(originFnName,
                k -> Lists.newArrayList());
        transformerList.add(transformer);
    }

    protected static void registerFunctionTransformerWithVarArgs(String originFnName, FunctionCallExpr starRocksFunctionCall) {
        FunctionCallTransformer transformer = new FunctionCallTransformer(starRocksFunctionCall, true);

        List<FunctionCallTransformer> transformerList = TRANSFORMER_MAP.computeIfAbsent(originFnName,
                k -> Lists.newArrayList());
        transformerList.add(transformer);
    }
}
