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

import com.google.common.collect.Maps;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class CreateDbStmt extends DdlStmt {
    private final boolean ifNotExists;
    private String catalogName;
    private final String dbName;
    private final Map<String, String> properties;

    public CreateDbStmt(boolean ifNotExists, String dbName) {
        this(ifNotExists, "", dbName, Maps.newHashMap(), NodePosition.ZERO);
    }

    public CreateDbStmt(boolean ifNotExists, String dbName, Map<String, String> properties) {
        this(ifNotExists, "", dbName, properties, NodePosition.ZERO);
    }

    public CreateDbStmt(boolean ifNotExists, String catalogName, String dbName,
                        Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.properties = properties;
    }

    public String getFullDbName() {
        return dbName;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDbStatement(this, context);
    }
}
