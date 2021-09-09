// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowTriggersStmt.java

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

package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

public class ShowTriggersStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Trigger", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Event", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Statement", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Timing", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Created", ScalarType.createVarchar(80)))
                    .addColumn(new Column("sql_mode", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Definer", ScalarType.createVarchar(80)))
                    .addColumn(new Column("character_set_client", ScalarType.createVarchar(80)))
                    .addColumn(new Column("collation_connection", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Database Collation", ScalarType.createVarchar(80)))
                    .build();

    @Override
    public void analyze(Analyzer analyzer) {
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
