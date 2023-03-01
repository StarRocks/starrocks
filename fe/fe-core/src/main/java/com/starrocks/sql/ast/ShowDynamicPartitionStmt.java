// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

public class ShowDynamicPartitionStmt extends ShowStmt {
    private String db;
    private static final ShowResultSetMetaData SHOW_DYNAMIC_PARTITION_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Enable", ScalarType.createVarchar(20)))
                    .addColumn(new Column("TimeUnit", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Start", ScalarType.createVarchar(20)))
                    .addColumn(new Column("End", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Prefix", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Buckets", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ReplicationNum", ScalarType.createVarchar(20)))
                    .addColumn(new Column("StartOf", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastUpdateTime", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastSchedulerTime", ScalarType.createVarchar(20)))
                    .addColumn(new Column("State", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastCreatePartitionMsg", ScalarType.createVarchar(20)))
                    .addColumn(new Column("LastDropPartitionMsg", ScalarType.createVarchar(20)))
                    .build();

    public ShowDynamicPartitionStmt(String db) {
        this(db, NodePosition.ZERO);
    }

    public ShowDynamicPartitionStmt(String db, NodePosition pos) {
        super(pos);
        this.db = db;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return SHOW_DYNAMIC_PARTITION_META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowDynamicPartitionStatement(this, context);
    }
}