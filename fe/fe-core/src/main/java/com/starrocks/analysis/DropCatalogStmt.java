// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeNameFormat;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;

public class DropCatalogStmt extends DdlStmt {

    private final String catalogName;

    public DropCatalogStmt(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void analyze() throws SemanticException {
        // TODO check permission
        if (Strings.isNullOrEmpty(catalogName)) {
            throw new SemanticException("'catalog name' can not be null or empty");
        }

        if (catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
            throw new SemanticException("Can't drop the default internal catalog");
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropCatalogStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP EXTERNAL CATALOG ");
        sb.append("\'" + catalogName + "\'");
        return sb.toString();
    }
}
