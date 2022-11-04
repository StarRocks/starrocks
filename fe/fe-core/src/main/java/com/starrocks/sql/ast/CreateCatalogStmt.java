// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.common.util.PrintableMap;

import java.util.List;
import java.util.Map;

public class CreateCatalogStmt extends DdlStmt {
    public static final String TYPE = "type";
    public static final List<String> SUPPORTED_CATALOG = Lists.newArrayList("hive", "iceberg", "hudi", "jdbc",
            "deltalake");

    private final String catalogName;
    private final String comment;
    private final Map<String, String> properties;
    private String catalogType;

    public CreateCatalogStmt(String catalogName, String comment, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.comment = comment;
        this.properties = properties;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getCatalogType() {
        return catalogType;
    }

    public void setCatalogType(String catalogType) {
        this.catalogType = catalogType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateCatalogStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL CATALOG '");
        sb.append(catalogName).append("' ");
        if (comment != null) {
            sb.append("COMMENT \"").append(comment).append("\" ");
        }
        sb.append("PROPERTIES(").append(new PrintableMap<>(properties, " = ", true, false)).append(")");
        return sb.toString();
    }
}
