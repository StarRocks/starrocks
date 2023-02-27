// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;

/*
  Use catalog specified by catalog name

  syntax:
      USE 'CATALOG catalog_name'
      USE "CATALOG catalog_name"

      Note:
        A pair of single/double quotes are required

      Examples:
        USE 'CATALOG default_catalog'
        use "catalog default_catalog"
        USE 'catalog hive_metastore_catalog'
        use "CATALOG hive_metastore_catalog"
 */
public class UseCatalogStmt extends StatementBase {
    private final String catalogParts;

    private String catalogName;

    public UseCatalogStmt(String catalogParts) {
        this.catalogParts = catalogParts;
    }

    public String getCatalogParts() {
        return catalogParts;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUseCatalogStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
