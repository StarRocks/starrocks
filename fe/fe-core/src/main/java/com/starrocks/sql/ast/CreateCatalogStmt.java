// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.List;
import java.util.Map;

public class CreateCatalogStmt extends DdlStmt {
    private static final String TYPE = "type";
    private static final List<String> supportedCatalog = Lists.newArrayList("hive");

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

    public void analyze() throws SemanticException {
        // TODO check permission
        if (Strings.isNullOrEmpty(catalogName)) {
            throw new SemanticException("'catalog name' can not be null or empty");
        }
        try {
            FeNameFormat.checkCatalogName(catalogName);
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }

        if (catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
            throw new SemanticException("External catalog name can't be the same as internal catalog name 'default'");
        }

        catalogType = properties.get(TYPE);
        if (Strings.isNullOrEmpty(catalogType)) {
            throw new SemanticException("'type' can not be null or empty");
        }
        if (!supportedCatalog.contains(catalogType)) {
            throw new SemanticException("[type : %s] is not supported", catalogType);
        }
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
