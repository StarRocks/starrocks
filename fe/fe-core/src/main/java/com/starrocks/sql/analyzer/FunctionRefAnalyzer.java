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

import com.google.common.base.Strings;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.sql.ast.FunctionArgsDef;
import com.starrocks.sql.ast.FunctionRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.type.Type;

import java.util.List;

public class FunctionRefAnalyzer {
    public static final String GLOBAL_UDF_DB = FunctionName.GLOBAL_UDF_DB;

    public static void analyzeFunctionRef(FunctionRef functionRef, String defaultDb) {
        List<String> parts = functionRef.getFnName().getParts();
        String fullFunctionName = functionRef.getFnName().toString().toLowerCase();
        if (parts.size() > 2) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Function names must be all alphanumeric or underscore. Invalid name: " + fullFunctionName);
        }
        String db = functionRef.getDbName();
        String fn = functionRef.getFunctionName();

        if (fn.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Should order by column");
        }
        for (int i = 0; i < fn.length(); ++i) {
            if (!isValidCharacter(fn.charAt(i))) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Function names must be all alphanumeric or underscore. " +
                                "Invalid name: " + fn);
            }
        }
        if (Character.isDigit(fn.charAt(0))) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Function cannot start with a digit: " + fn);
        }
        if (db == null && Strings.isNullOrEmpty(defaultDb)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
        }
    }

    public static void analyzeArgsDef(FunctionArgsDef argsDef) {
        List<TypeDef> argTypeDefs = argsDef.getArgTypeDefs();
        Type[] argTypes = new Type[argTypeDefs.size()];
        int i = 0;
        for (TypeDef typeDef : argTypeDefs) {
            TypeDefAnalyzer.analyze(typeDef);
            argTypes[i++] = typeDef.getType();
        }
        argsDef.setArgTypes(argTypes);
    }

    public static FunctionName resolveFunctionName(FunctionRef functionRef, String defaultDb) {
        FunctionName functionName = FunctionName.createFnName(functionRef.getFnName().toString());
        if (functionRef.isGlobalFunction()) {
            if (!Strings.isNullOrEmpty(functionName.getDb())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid function name: " + functionRef.getFnName().toString().toLowerCase());
            }
            functionName.setAsGlobalFunction();
            return functionName;
        } else {
            if (functionName.getDb() == null) {
                functionName.setDb(defaultDb);
            }
        }

        return functionName;
    }

    public static FunctionSearchDesc buildFunctionSearchDesc(
            FunctionRef functionRef, FunctionArgsDef argsDef, String defaultDb) {
        FunctionName functionName = resolveFunctionName(functionRef, defaultDb);
        return new FunctionSearchDesc(functionName, argsDef.getArgTypes(), argsDef.isVariadic());
    }

    private static boolean isValidCharacter(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }
}
