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

import com.google.common.base.Preconditions;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.parser.NodePosition;

/**
 * CatalogRef is used to represent Catalog used in Backup/Restore
 */
public class CatalogRef implements ParseNode {
    private final NodePosition pos;
    private String catalogName;
    private String alias;
    private Catalog catalog;
    private boolean isAnalyzed;

    public CatalogRef(String catalogName) {
        this(catalogName, "");
    }

    public CatalogRef(String catalogName, String alias) {
        this.catalogName = catalogName;
        this.alias = alias;
        this.isAnalyzed = false;
        this.pos = NodePosition.ZERO;
    }

    public String getCatalogName() {
        return this.catalogName;
    }

    public String getAlias() {
        return this.alias;
    }

    public Catalog getCatalog() {
        Preconditions.checkState(isAnalyzed);
        return this.catalog;
    }

    public void analyzeForBackup() {
        if (catalogName.equalsIgnoreCase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Do not support Backup/Restore the entire default catalog");   
        }

        if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "external catalog " + catalogName + " does not existed.");
        }

        catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(catalogName);
        isAnalyzed = true;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
