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
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;

import static java.lang.String.format;

public class SemanticException extends StarRocksPlannerException {
    public SemanticException(String formatString) {
        super(formatString, ErrorType.USER_ERROR);
    }

    public SemanticException(String formatString, Object... args) {
        super(format(formatString, args), ErrorType.USER_ERROR);
    }

    public static SemanticException missingAttributeException(Expr node) throws SemanticException {
        throw new SemanticException("Column '%s' cannot be resolved", node.toSql());
    }
<<<<<<< HEAD
=======

    public String getDetailMsg() {
        return detailMsg;
    }

>>>>>>> f74e15e57 ([Enhancement] Support alter materialized view to active (#24001))
}
