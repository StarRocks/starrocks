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

import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

public class PolicyName implements ParseNode {
    private String catalog;
    private String dbName;
    private final String name;

    private final NodePosition pos;

    public PolicyName(String catalog, String dbName, String name, NodePosition pos) {
        this.catalog = catalog;
        this.dbName = dbName;
        this.name = name;
        this.pos = pos;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getName() {
        return name;
    }

    public NodePosition getPos() {
        return pos;
    }

    @Override
    public String toString() {
        return name;
    }
}
