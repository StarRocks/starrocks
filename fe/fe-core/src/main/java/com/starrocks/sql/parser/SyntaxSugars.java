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

package com.starrocks.sql.parser;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.catalog.FunctionSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/*
 * This class is used to parse the syntax sugars in the sql.
 */
public class SyntaxSugars {
    private static final Map<String, Function<FunctionCallExpr, FunctionCallExpr>> FUNCTION_PARSER;

    static {
        FUNCTION_PARSER = ImmutableMap.<String, Function<FunctionCallExpr, FunctionCallExpr>>builder()
                .put(FunctionSet.ILIKE, SyntaxSugars::ilike)
                .put(FunctionSet.STRUCT, SyntaxSugars::struct)
                .build();
    }

    /*
     * function call syntax sugars
     * - simple functions
     * - aggregate functions
     * - window functions
     */
    public static FunctionCallExpr parse(FunctionCallExpr call) {
        return FUNCTION_PARSER.getOrDefault(call.getFnName().getFunction(), SyntaxSugars::defaultParse).apply(call);
    }

    private static FunctionCallExpr defaultParse(FunctionCallExpr call) {
        return call;
    }

    /*
     * ilike(a, b) -> like(low(a), low(b))
     */
    private static FunctionCallExpr ilike(FunctionCallExpr call) {
        List<Expr> newArguments = new ArrayList<>();
        for (Expr arg : call.getChildren()) {
            FunctionCallExpr lower = new FunctionCallExpr(FunctionSet.LOWER, Lists.newArrayList(arg));
            newArguments.add(lower);
        }
        return new FunctionCallExpr(FunctionSet.LIKE, Lists.newArrayList(newArguments));
    }

    /*
     * struct(a, b, c) -> row(a, b, c)
     */
    private static FunctionCallExpr struct(FunctionCallExpr call) {
        return new FunctionCallExpr(FunctionSet.ROW, call.getChildren());
    }
}
