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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

public class ShowIndexStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Table", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Non_unique", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Key_name", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Seq_in_index", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Column_name", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Collation", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Cardinality", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Sub_part", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Packed", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(80)))
                    .addColumn(new Column("IndexId", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Index_type", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Properties", ScalarType.createVarchar(200)))
                    .build();
    private final String dbName;
    private final TableName tableName;

    public ShowIndexStmt(String dbName, TableName tableName) {
        this(dbName, tableName, NodePosition.ZERO);
    }

    public ShowIndexStmt(String dbName, TableName tableName, NodePosition pos) {
        super(pos);
        this.dbName = dbName;
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
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowIndexStatement(this, context);
    }
}
