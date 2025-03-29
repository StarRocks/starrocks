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
// See the License for the specific

package com.starrocks.sql.ast.warehouse;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

// Show nodes from warehouse statement
public class ShowNodesStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("WarehouseName", ScalarType.createVarchar(256)))
                    .addColumn(new Column("ClusterId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("WorkerGroupId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("NodeId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("WorkerId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("IP", ScalarType.createVarchar(256)))
                    .addColumn(new Column("HeartbeatPort", ScalarType.createVarchar(20)))
                    .addColumn(new Column("BePort", ScalarType.createVarchar(20)))
                    .addColumn(new Column("HttpPort", ScalarType.createVarchar(20)))
                    .addColumn(new Column("BrpcPort", ScalarType.createVarchar(20)))
                    .addColumn(new Column("StarletPort", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastStartTime", ScalarType.createVarchar(256)))
                    .addColumn(new Column("LastUpdateMs", ScalarType.createVarchar(256)))
                    .addColumn(new Column("Alive", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ErrMsg", ScalarType.createVarchar(256)))
                    .addColumn(new Column("Version", ScalarType.createVarchar(20)))
                    .addColumn(new Column("NumRunningQueries", ScalarType.createVarchar(20)))
                    .addColumn(new Column("CpuCores", ScalarType.createVarchar(20)))
                    .addColumn(new Column("MemUsedPct", ScalarType.createVarchar(20)))
                    .addColumn(new Column("CpuUsedPct", ScalarType.createVarchar(20)))
                    .build();

    private final String pattern;
    private final String warehouseName;

    public ShowNodesStmt(String warehouseName, String pattern, NodePosition pos) {
        super(pos);
        this.warehouseName = warehouseName;
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }

    public String getWarehouseName() {
        return warehouseName;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowNodesStatement(this, context);
    }
}