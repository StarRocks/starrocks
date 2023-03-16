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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ShowMaterializedViewTest.java

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
import com.starrocks.catalog.SchemaTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowMaterializedViewsStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ShowMaterializedViewTest {
    private ConnectContext ctx;

    @Before
    public void setUp() {
    }

    @Test
    public void testNormal() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");

        ShowMaterializedViewsStmt stmt = new ShowMaterializedViewsStmt("");

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("testDb", stmt.getDb());
        checkShowMaterializedViewsStmt(stmt);

        stmt = new ShowMaterializedViewsStmt("abc", (String) null);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("abc", stmt.getDb());
        checkShowMaterializedViewsStmt(stmt);

        stmt = new ShowMaterializedViewsStmt("abc", "bcd");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("bcd", stmt.getPattern());
        Assert.assertEquals("abc", stmt.getDb());
        checkShowMaterializedViewsStmt(stmt);

        stmt = (ShowMaterializedViewsStmt) UtFrameUtils.parseStmtWithNewParser(
                "SHOW MATERIALIZED VIEWS FROM abc where name = 'mv1';", ctx);
        Assert.assertEquals("abc", stmt.getDb());
        Assert.assertEquals(
                "SELECT information_schema.materialized_views.id AS id, " +
                        "information_schema.materialized_views.database_name AS database_name, " +
                        "information_schema.materialized_views.name AS name, " +
                        "information_schema.materialized_views.refresh_type AS refresh_type, " +
                        "information_schema.materialized_views.is_active AS is_active, " +
                        "information_schema.materialized_views.partition_type AS partition_type, " +
                        "information_schema.materialized_views.last_refresh_start_time AS last_refresh_start_time, " +
                        "information_schema.materialized_views.last_refresh_finished_time AS last_refresh_finished_time, " +
                        "information_schema.materialized_views.last_refresh_duration AS last_refresh_duration, " +
                        "information_schema.materialized_views.last_refresh_state AS last_refresh_state, " +
                        "information_schema.materialized_views.force_refresh AS force_refresh, " +
                        "information_schema.materialized_views.start_partition AS start_partition, " +
                        "information_schema.materialized_views.end_partition AS end_partition, " +
                        "information_schema.materialized_views.base_refresh_partitions AS base_refresh_partitions, " +
                        "information_schema.materialized_views.mv_refresh_partitions AS mv_refresh_partitions, " +
                        "information_schema.materialized_views.last_refresh_error_code AS last_refresh_error_code, " +
                        "information_schema.materialized_views.last_refresh_error_reason AS last_refresh_error_reason, " +
                        "information_schema.materialized_views.text AS text, " +
                        "information_schema.materialized_views.rows AS rows " +
                        "FROM information_schema.materialized_views " +
                        "WHERE information_schema.materialized_views.name = 'mv1'",
                AstToStringBuilder.toString(stmt.toSelectStmt()));
        checkShowMaterializedViewsStmt(stmt);
    }

    private void checkShowMaterializedViewsStmt(ShowMaterializedViewsStmt stmt) {
        Table schemaMVTable = SchemaTable.getSchemaTable("materialized_views");
        Assert.assertEquals(schemaMVTable.getBaseSchema().size(), stmt.getMetaData().getColumnCount());

        List<Column> schemaCols = schemaMVTable.getFullSchema();
        for (int i = 0; i < schemaCols.size(); i++) {
            if (schemaCols.get(i).getName().equalsIgnoreCase("MATERIALIZED_VIEW_ID")) {
                Assert.assertEquals("id", stmt.getMetaData().getColumn(i).getName());
            } else if (schemaCols.get(i).getName().equalsIgnoreCase("TABLE_SCHEMA")) {
                Assert.assertEquals("database_name", stmt.getMetaData().getColumn(i).getName());
            } else if (schemaCols.get(i).getName().equalsIgnoreCase("TABLE_NAME")) {
                Assert.assertEquals("name", stmt.getMetaData().getColumn(i).getName());
            } else if (schemaCols.get(i).getName().equalsIgnoreCase("MATERIALIZED_VIEW_DEFINITION")) {
                Assert.assertEquals("text", stmt.getMetaData().getColumn(i).getName());
            } else if (schemaCols.get(i).getName().equalsIgnoreCase("TABLE_ROWS")) {
                Assert.assertEquals("rows", stmt.getMetaData().getColumn(i).getName());
            } else {
                Assert.assertEquals(schemaCols.get(i).getName().toLowerCase(), stmt.getMetaData().getColumn(i).getName());
            }
        }
    }

    @Test(expected = SemanticException.class)
    public void testNoDb() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ShowMaterializedViewsStmt stmt = new ShowMaterializedViewsStmt("");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws");
    }
}
