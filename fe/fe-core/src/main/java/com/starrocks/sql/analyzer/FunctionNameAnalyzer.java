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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.sql.ast.QualifiedName;

import java.util.List;

/**
 * Analyzer for function identifiers represented by QualifiedName.
 * Mirrors the validation behavior of FunctionName.analyze and fills default db when missing.
 */
public class FunctionNameAnalyzer {

    public static void analyzeAndNormalize(QualifiedName qualifiedName, Database defaultDb) {
        List<String> parts = qualifiedName.getParts();
        if (parts.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Should order by column");
        }

        // Only support [fn] or [db, fn]
        if (parts.size() > 2) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Invalid function name: " + qualifiedName.toString());
        }

        String functionName = parts.get(parts.size() - 1).toLowerCase();
        if (functionName.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Should order by column");
        }
        // Validate characters
        for (int i = 0; i < functionName.length(); ++i) {
            char c = functionName.charAt(i);
            if (!(Character.isLetterOrDigit(c) || c == '_')) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Function names must be all alphanumeric or underscore. Invalid name: " + functionName);
            }
        }
        if (Character.isDigit(functionName.charAt(0))) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Function cannot start with a digit: " + functionName);
        }

        // Normalize to [db, fn] with default db when needed
        String dbName = parts.size() == 2 ? parts.get(0) : null;
        if (dbName == null) {
            dbName = defaultDb == null ? null : defaultDb.getFullName();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        List<Function> functionList = defaultDb.getFunctionsByName(functionName);
        if (functionList.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                    "Invalid backup function(s), function name: " + functionName);
        }
    }
}


