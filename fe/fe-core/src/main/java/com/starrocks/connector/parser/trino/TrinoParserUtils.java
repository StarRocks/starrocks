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

package com.starrocks.connector.parser.trino;

import com.starrocks.sql.ast.StatementBase;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;

public class TrinoParserUtils {
    public static StatementBase toStatement(String query, long sqlMode) {
        String trimmedQuery = query.trim();
        Statement statement = TrinoParser.parse(trimmedQuery);
        if (statement instanceof Query || statement instanceof Explain || statement instanceof ExplainAnalyze) {
            return (StatementBase) statement.accept(new AstBuilder(sqlMode), new ParseTreeContext());
        } else {
            throw new UnsupportedOperationException("Unsupported statement type: " + statement.getClass().getName());
        }
    }
}
