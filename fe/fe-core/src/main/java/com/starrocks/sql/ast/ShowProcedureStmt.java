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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.Predicate;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

public class ShowProcedureStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Db", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Name", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Definer", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Modified", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Created", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Security_type", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(80)))
                    .addColumn(new Column("character_set_client", ScalarType.createVarchar(80)))
                    .addColumn(new Column("collation_connection", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Database Collation", ScalarType.createVarchar(80)))
                    .build();

    private String pattern;
    private Expr where;

    public ShowProcedureStmt(String pattern, Expr where) {
        this(pattern, where, NodePosition.ZERO);
    }

    public ShowProcedureStmt(String pattern, Expr where, NodePosition pos) {
        super(pos);
        this.pattern = pattern;
        this.predicate = (Predicate) where;
    }

    public ShowProcedureStmt() {
        super(NodePosition.ZERO);
    }


    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    public String getPattern() {
        return pattern;
    }
}
