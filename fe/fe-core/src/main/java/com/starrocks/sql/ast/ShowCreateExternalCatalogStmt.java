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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

// SHOW CREATE EXTERNAL CATALOG statement.
public class ShowCreateExternalCatalogStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Catalog", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Create Catalog", ScalarType.createVarchar(30)))
                    .build();

    private final String catalogName;

    public ShowCreateExternalCatalogStmt(String catalogName) {
        this(catalogName, NodePosition.ZERO);
    }

    public ShowCreateExternalCatalogStmt(String catalogName, NodePosition pos) {
        super(pos);
        this.catalogName = catalogName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateExternalCatalogStatement(this, context);
    }
}
