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

public class ShowTriggersStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Trigger", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Event", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Statement", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Timing", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Created", ScalarType.createVarchar(80)))
                    .addColumn(new Column("sql_mode", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Definer", ScalarType.createVarchar(80)))
                    .addColumn(new Column("character_set_client", ScalarType.createVarchar(80)))
                    .addColumn(new Column("collation_connection", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Database Collation", ScalarType.createVarchar(80)))
                    .build();

    public ShowTriggersStmt() {
        this(NodePosition.ZERO);
    }

    public ShowTriggersStmt(NodePosition pos) {
        super(pos);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
