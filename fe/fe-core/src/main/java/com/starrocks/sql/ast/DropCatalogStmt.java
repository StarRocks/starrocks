// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.sql.analyzer.SemanticException;

// ToDo(zhuodong): to support internal catalog in the future
public class DropCatalogStmt extends DdlStmt {

    private final String name;

    public DropCatalogStmt(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void analyze() throws SemanticException {
        // TODO check permission
        if (Strings.isNullOrEmpty(name)) {
            throw new SemanticException("'catalog name' can not be null or empty");
        }

        if (name.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
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
        sb.append("\'" + name + "\'");
        return sb.toString();
    }
}
