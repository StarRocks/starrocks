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

import com.starrocks.sql.parser.NodePosition;

/**
 * CatalogRef is used to represent Catalog used in Backup/Restore
 */
public class CatalogRef implements ParseNode {
    private final NodePosition pos;
    private final String catalogName;
    private final String alias;

    public CatalogRef(String catalogName) {
        this(catalogName, "");
    }

    public CatalogRef(String catalogName, String alias) {
        this.catalogName = catalogName;
        this.alias = alias;
        this.pos = NodePosition.ZERO;
    }

    public String getCatalogName() {
        return this.catalogName;
    }

    public String getAlias() {
        return this.alias;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
