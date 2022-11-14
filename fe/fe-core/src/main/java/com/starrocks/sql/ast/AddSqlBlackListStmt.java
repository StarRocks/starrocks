// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.analyzer.SemanticException;

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
        this.sql = sql;
    }

    public void analyze() throws SemanticException {
        sql = sql.trim().toLowerCase().replaceAll(" +", " ");
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

