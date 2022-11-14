// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.proc.UserPropertyProcNode;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;

// Show Property Stmt
//  syntax:
//      SHOW PROPERTY [FOR user] [LIKE key pattern]
public class ShowUserPropertyStmt extends ShowStmt {

    private String user;
    private String pattern;

    public ShowUserPropertyStmt(String user, String pattern) {
        this.user = user;
        this.pattern = pattern;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPatter() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public List<List<String>> getRows() throws AnalysisException {
        List<List<String>> rows = GlobalStateMgr.getCurrentState().getAuth().getUserProperties(user);

        if (pattern == null) {
            return rows;
        }

        List<List<String>> result = Lists.newArrayList();
        PatternMatcher matcher = PatternMatcher.createMysqlPattern(pattern,
                CaseSensibility.USER.getCaseSensibility());
        for (List<String> row : rows) {
            String key = row.get(0).split("\\" + SetUserPropertyVar.DOT_SEPARATOR)[0];
            if (matcher.match(key)) {
                result.add(row);
            }
        }

        return result;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String col : UserPropertyProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowUserPropertyStatement(this, context);
    }
}
