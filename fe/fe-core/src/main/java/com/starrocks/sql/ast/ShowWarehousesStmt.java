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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

// Show warehouse statement.
public class ShowWarehousesStmt extends ShowStmt {
    private static final String WH_COL = "Warehouse";
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(WH_COL, ScalarType.createVarchar(256)))
                    .addColumn(new Column("State", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(20)))
                    .addColumn(new Column("MinCluster", ScalarType.createVarchar(20)))
                    .addColumn(new Column("MaxCluster", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ClusterCount", ScalarType.createVarchar(20)))
                    .addColumn(new Column("TotalPending", ScalarType.createVarchar(20)))
                    .addColumn(new Column("TotalRunning", ScalarType.createVarchar(20)))
                    .build();
    private final String pattern;
    private Expr where;

    public ShowWarehousesStmt(String pattern) {
        this(pattern, null, NodePosition.ZERO);
    }

    public ShowWarehousesStmt(String pattern, Expr where) {
        this(pattern, where, NodePosition.ZERO);
    }

    public ShowWarehousesStmt(String pattern, Expr where, NodePosition pos) {
        super(pos);
        this.pattern = pattern;
        this.where = where;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowWarehousesStatement(this, context);
    }
}

