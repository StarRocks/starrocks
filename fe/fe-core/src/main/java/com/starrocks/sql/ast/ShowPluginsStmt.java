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

public class ShowPluginsStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Description", ScalarType.createVarchar(200)))
                    .addColumn(new Column("Version", ScalarType.createVarchar(20)))
                    .addColumn(new Column("JavaVersion", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ClassName", ScalarType.createVarchar(64)))
                    .addColumn(new Column("SoName", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Sources", ScalarType.createVarchar(200)))
                    .addColumn(new Column("Status", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Properties", ScalarType.createVarchar(250)))
                    .build();

    public ShowPluginsStmt() {
        this(NodePosition.ZERO);
    }

    public ShowPluginsStmt(NodePosition pos) {
        super(pos);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowPluginsStatement(this, context);
    }
}
