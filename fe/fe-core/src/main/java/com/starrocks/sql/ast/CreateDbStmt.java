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

public class CreateDbStmt extends DdlStmt {
    private final boolean ifNotExists;
    private final String dbName;

    public CreateDbStmt(boolean ifNotExists, String dbName) {
        this(ifNotExists, dbName, NodePosition.ZERO);
    }

    public CreateDbStmt(boolean ifNotExists, String dbName, NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.dbName = dbName;
    }

    public String getFullDbName() {
        return dbName;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDbStatement(this, context);
    }
}
