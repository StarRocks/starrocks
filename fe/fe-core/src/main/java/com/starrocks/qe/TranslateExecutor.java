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

package com.starrocks.qe;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.translate.TranslateStmt;
import com.starrocks.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.List;

public class TranslateExecutor {
    private static final ShowResultSetMetaData COLUMN_META =
            ShowResultSetMetaData.builder().addColumn(new Column("Translated SQL", Type.STRING)).build();

    public static ShowResultSet execute(TranslateStmt stmt) {
        String dialect = stmt.getDialect();
        String translateSQL = stmt.getTranslateSQL();
        SessionVariable sessionVariable = ConnectContext.getSessionVariableOrDefault();
        sessionVariable.setSqlDialect(dialect);
        List<StatementBase> statementBases = SqlParser.parse(translateSQL, sessionVariable);

        List<List<String>> resultRows = new ArrayList<>();
        for (StatementBase statementBase : statementBases) {
            resultRows.add(List.of(AstToSQLBuilder.toSQL(statementBase)));
        }
        return new ShowResultSet(COLUMN_META, resultRows);
    }
}
