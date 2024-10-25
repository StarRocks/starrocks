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

import com.starrocks.common.util.PrintableMap;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class CreateCatalogStmt extends DdlStmt {
    public static final String TYPE = "type";

    private final String catalogName;
    private final String comment;
    private final Map<String, String> properties;
    private String catalogType;

    private final boolean ifNotExists;

    public CreateCatalogStmt(String catalogName, String comment, Map<String, String> properties, boolean ifNotExists) {
        this(catalogName, comment, properties, ifNotExists, NodePosition.ZERO);
    }

    public CreateCatalogStmt(String catalogName, String comment, Map<String, String> properties,
                             boolean ifNotExists, NodePosition pos) {
        super(pos);
        this.catalogName = catalogName;
        this.comment = comment;
        this.properties = properties;
        this.ifNotExists = ifNotExists;
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

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateCatalogStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL CATALOG ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append("'").append(catalogName).append("' ");
        if (comment != null) {
            sb.append("COMMENT \"").append(comment).append("\" ");
        }
        sb.append("PROPERTIES(").append(new PrintableMap<>(properties, " = ", true, false)).append(")");
        return sb.toString();
    }
}
