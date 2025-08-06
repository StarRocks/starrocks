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

// Show Events statement
public class ShowEventsStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn("Db")
                    .addColumn("Name")
                    .addColumn("Definer")
                    .addColumn("Time")
                    .addColumn("Type")
                    .addColumn("Execute at")
                    .addColumn("Interval value")
                    .addColumn("Interval field")
                    .addColumn("Status")
                    .addColumn("Ends")
                    .addColumn("Status")
                    .addColumn("Originator")
                    .addColumn("character_set_client")
                    .addColumn("collation_connection")
                    .addColumn("Database Collation")
                    .build();

    public ShowEventsStmt() {
        this(NodePosition.ZERO);
    }

    public ShowEventsStmt(NodePosition pos) {
        super(pos);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowEventStatement(this, context);
    }
}
