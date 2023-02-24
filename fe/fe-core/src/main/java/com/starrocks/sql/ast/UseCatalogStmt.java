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
import com.starrocks.sql.parser.NodePosition;

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
        this(catalogParts, NodePosition.ZERO);
    }

    public UseCatalogStmt(String catalogParts, NodePosition pos) {
        super(pos);
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
