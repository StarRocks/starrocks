// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AdminSetConfigStmt.ConfigType;

// admin show frontend config;
public class AdminShowConfigStmt extends ShowStmt {
    public static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>().add("Key").add("AliasNames").add(
                    "Value").add("Type").add("IsMutable").add("Comment").build();

    private final ConfigType type;

    private final String pattern;

    public AdminShowConfigStmt(ConfigType type, String pattern) {
        this.type = type;
        this.pattern = pattern;
    }

    public ConfigType getType() {
        return type;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminShowConfigStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
