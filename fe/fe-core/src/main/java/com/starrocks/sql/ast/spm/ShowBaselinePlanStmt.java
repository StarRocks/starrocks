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

package com.starrocks.sql.ast.spm;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

public class ShowBaselinePlanStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Id", ScalarType.createVarchar(60)))
            .addColumn(new Column("global", ScalarType.createVarchar(10)))
            .addColumn(new Column("bindSQLDigest", ScalarType.createVarchar(65535)))
            .addColumn(new Column("bindSQLHash", ScalarType.createVarchar(60)))
            .addColumn(new Column("bindSQL", ScalarType.createVarchar(65535)))
            .addColumn(new Column("planSQL", ScalarType.createVarchar(65535)))
            .addColumn(new Column("costs", ScalarType.createVarchar(60)))
            .addColumn(new Column("updateTime", ScalarType.createVarchar(60)))
            .build();

    public ShowBaselinePlanStmt(NodePosition pos) {
        super(pos);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowBaselinePlanStatement(this, context);
    }
}
