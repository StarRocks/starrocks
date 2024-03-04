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

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.HashMap;
import java.util.Map;

public class CreateExternalCooldownStmt extends DdlStmt {
    private TableName tableName;
    private final boolean forceRefresh;
    private PartitionRangeDesc partitionRangeDesc;
    private Map<String, String> properties;

    public static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("QUERY_ID", ScalarType.createVarchar(60)))
                    .build();
    public CreateExternalCooldownStmt(TableName tableName, PartitionRangeDesc partitionRangeDesc,
                                      boolean forceRefresh, NodePosition pos) {
        super(pos);
        this.tableName = tableName;
        this.partitionRangeDesc = partitionRangeDesc;
        this.forceRefresh = forceRefresh;
        this.properties = new HashMap<>();
    }

    public TableName getTableName() {
        return tableName;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

    public PartitionRangeDesc getPartitionRangeDesc() {
        return partitionRangeDesc;
    }

    public void setPartitionRangeDesc(PartitionRangeDesc partitionRangeDesc) {
        this.partitionRangeDesc = partitionRangeDesc;
    }

    public boolean isForceRefresh() {
        return forceRefresh;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateExternalCooldownStatement(this, context);
    }

    @Override
    public String toSql() {
        String sql = "COOLDOWN " + tableName;
        if (partitionRangeDesc != null) {
            sql += " " + partitionRangeDesc.toSql();
        }
        if (isForceRefresh()) {
            sql += " FORCE";
        }

        return sql;
    }
}
