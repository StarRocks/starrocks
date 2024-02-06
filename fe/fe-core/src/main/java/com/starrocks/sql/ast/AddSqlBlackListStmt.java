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


package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

// used to add regular expression from sql.
public class AddSqlBlackListStmt extends StatementBase {
    private String sql;

    public String getSql() {
        return sql;
    }

    private Pattern sqlPattern;

    public Pattern getSqlPattern() {
        return sqlPattern;
    }

    public AddSqlBlackListStmt(String sql) {
        this(sql, NodePosition.ZERO);
    }

    public AddSqlBlackListStmt(String sql, NodePosition pos) {
        super(pos);
        this.sql = sql;
    }

    public void analyze() throws SemanticException {
        sql = sql.trim().toLowerCase().replaceAll(" +", " ")
                .replace("\r", " ")
                .replace("\n", " ")
                .replaceAll("\\s+", " ");
        if (sql.length() > 0) {
            try {
                sqlPattern = Pattern.compile(sql);
            } catch (PatternSyntaxException e) {
                throw new SemanticException("Sql syntax error: %s", e.getMessage());
            }
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddSqlBlackListStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}

