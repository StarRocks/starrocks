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

public class ShowSmallFilesStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Id", ScalarType.createVarchar(32)))
                    .addColumn(new Column("DbName", ScalarType.createVarchar(256)))
                    .addColumn(new Column("GlobalStateMgr", ScalarType.createVarchar(32)))
                    .addColumn(new Column("FileName", ScalarType.createVarchar(16)))
                    .addColumn(new Column("FileSize", ScalarType.createVarchar(16)))
                    .addColumn(new Column("IsContent", ScalarType.createVarchar(16)))
                    .addColumn(new Column("MD5", ScalarType.createVarchar(16)))
                    .build();

    private String dbName;

    public ShowSmallFilesStmt(String dbName) {
        this(dbName, NodePosition.ZERO);
    }

    public ShowSmallFilesStmt(String dbName, NodePosition pos) {
        super(pos);
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowSmallFilesStatement(this, context);
    }
}
