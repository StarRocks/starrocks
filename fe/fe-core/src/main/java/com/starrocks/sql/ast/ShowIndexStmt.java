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

import com.google.common.base.Strings;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.parser.NodePosition;

import static com.starrocks.common.util.Util.normalizeName;

public class ShowIndexStmt extends ShowStmt {
    private final String dbName;
    private final TableName tableName;

    public ShowIndexStmt(String dbName, TableName tableName) {
        this(dbName, tableName, NodePosition.ZERO);
    }

    public ShowIndexStmt(String dbName, TableName tableName, NodePosition pos) {
        super(pos);
        this.dbName = normalizeName(dbName);
        this.tableName = tableName;
    }

    public void init() {
        if (!Strings.isNullOrEmpty(dbName)) {
            tableName.setDb(dbName);
        }
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowIndexStatement(this, context);
    }
}
