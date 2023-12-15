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

public class ShowProfilelistStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("QueryId", ScalarType.createVarchar(48)))
                    .addColumn(new Column("StartTime", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Time", ScalarType.createVarchar(16)))
                    .addColumn(new Column("State", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Statement", ScalarType.createVarchar(128)))
                    .build();
    private final int limit;

    public ShowProfilelistStmt(int limit, NodePosition pos) {
        super(pos);
        this.limit = limit;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowProfilelistStatement(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    public int getLimit() {
        return limit;
    }
}