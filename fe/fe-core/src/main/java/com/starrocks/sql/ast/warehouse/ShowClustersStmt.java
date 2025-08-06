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

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowResultSetMetaData;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

// show clusters of a warehouse
public class ShowClustersStmt extends ShowStmt {
    private String warehouseName;
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn("CNGroupId")
                    .addColumn("CNGroupName")
                    .addColumn("WorkerGroupId")
                    .addColumn("ComputeNodeIds")
                    .addColumn("Pending")
                    .addColumn("Running")
                    .addColumn("Enabled")
                    .addColumn("Properties")
                    .build();

    public ShowClustersStmt(String warehouseName) {
        this(warehouseName, NodePosition.ZERO);
    }

    public ShowClustersStmt(String warehouseName, NodePosition pos) {
        super(pos);
        this.warehouseName = warehouseName;
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
        return visitor.visitShowClusterStatement(this, context);
    }
}
