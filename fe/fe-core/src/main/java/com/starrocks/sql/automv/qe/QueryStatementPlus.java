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

package com.starrocks.sql.automv.qe;

import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.automv.pieces.FQTable;

import java.util.Map;
import java.util.Objects;

public class QueryStatementPlus {
    private final QueryStatement queryStatement;
    private final Map<String, FQTable> fqTableMap;

    private QueryStatementPlus(QueryStatement queryStatement, Map<String, FQTable> fqTableMap) {
        this.queryStatement = Objects.requireNonNull(queryStatement);
        this.fqTableMap = Objects.requireNonNull(fqTableMap);
    }

    public static QueryStatementPlus of(QueryStatement queryStatement, Map<String, FQTable> fqTableMap) {
        return new QueryStatementPlus(queryStatement, fqTableMap);
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public Map<String, FQTable> getFqTableMap() {
        return fqTableMap;
    }
}
