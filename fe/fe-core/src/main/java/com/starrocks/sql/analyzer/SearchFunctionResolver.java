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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Config;
import com.starrocks.sql.ast.expression.FunctionCallExpr;

/** Applies the global name-binding policy for the built-in {@code search()} function. */
final class SearchFunctionResolver {
    private SearchFunctionResolver() {
    }

    static boolean isBuiltinSearchInvocation(FunctionCallExpr call) {
        return Config.enable_search_function && isUnqualifiedSearch(call) && !isResolvedUserFunction(call);
    }

    // PREPARE retains its analyzed AST. Preserve a user-function binding established while the
    // built-in search function was disabled, even if the global switch changes before EXECUTE.
    private static boolean isResolvedUserFunction(FunctionCallExpr call) {
        return call.getFn() != null && call.getFn().getFunctionName().getDb() != null;
    }

    private static boolean isUnqualifiedSearch(FunctionCallExpr call) {
        return FunctionSet.SEARCH.equalsIgnoreCase(call.getFunctionName())
                && (call.getDbName() == null || call.getDbName().isEmpty());
    }
}
