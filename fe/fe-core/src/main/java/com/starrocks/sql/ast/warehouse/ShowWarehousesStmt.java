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

package com.starrocks.sql.ast.warehouse;

import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

// Show warehouse statement.
public class ShowWarehousesStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Id", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Name", ScalarType.createVarchar(256)))
                    .addColumn(new Column("State", ScalarType.createVarchar(20)))
                    .addColumn(new Column("NodeCount", ScalarType.createVarchar(20)))
                    .addColumn(new Column("CurrentClusterCount", ScalarType.createVarchar(20)))
                    .addColumn(new Column("MaxClusterCount", ScalarType.createVarchar(20)))
                    .addColumn(new Column("StartedClusters", ScalarType.createVarchar(20)))
                    .addColumn(new Column("RunningSql", ScalarType.createVarchar(20)))
                    .addColumn(new Column("QueuedSql", ScalarType.createVarchar(20)))
                    .addColumn(new Column("CreatedOn", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ResumedOn", ScalarType.createVarchar(20)))
                    .addColumn(new Column("UpdatedOn", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Property", ScalarType.createVarchar(256)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(256)))
                    .build();
    private final String pattern;

    public ShowWarehousesStmt(String pattern) {
        this(pattern, NodePosition.ZERO);
    }

    public ShowWarehousesStmt(String pattern, NodePosition pos) {
        super(pos);
        this.pattern = Strings.nullToEmpty(pattern);
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW WAREHOUSES");
        if (!pattern.isEmpty()) {
            sb.append(" LIKE '").append(pattern).append("'");
        }
        return sb.toString();
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

