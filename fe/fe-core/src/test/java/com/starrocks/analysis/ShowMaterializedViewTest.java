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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowMaterializedViewsStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
                "SELECT information_schema.materialized_views.MATERIALIZED_VIEW_ID AS id, " +
                        "information_schema.materialized_views.TABLE_NAME AS name, " +
                        "information_schema.materialized_views.TABLE_SCHEMA AS database_name, " +
                        "information_schema.materialized_views.REFRESH_TYPE AS refresh_type, " +
                        "information_schema.materialized_views.IS_ACTIVE AS is_active, " +
                        "information_schema.materialized_views.LAST_REFRESH_START_TIME AS last_refresh_start_time, " +
                        "information_schema.materialized_views.LAST_REFRESH_FINISHED_TIME AS last_refresh_finished_time," +
                        " information_schema.materialized_views.LAST_REFRESH_DURATION AS last_refresh_duration, " +
                        "information_schema.materialized_views.LAST_REFRESH_STATE AS last_refresh_state, " +
                        "information_schema.materialized_views.INACTIVE_CODE AS inactive_code, " +
                        "information_schema.materialized_views.INACTIVE_REASON AS inactive_reason, " +
                        "information_schema.materialized_views.MATERIALIZED_VIEW_DEFINITION AS text, " +
                        "information_schema.materialized_views.TABLE_ROWS AS rows " +
                        "FROM information_schema.materialized_views " +
                        "WHERE information_schema.materialized_views.TABLE_NAME = 'mv1'",
                AstToStringBuilder.toString(stmt.toSelectStmt()));
        checkShowMaterializedViewsStmt(stmt);
    }

    private void checkShowMaterializedViewsStmt(ShowMaterializedViewsStmt stmt) {
        Assert.assertEquals(13, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("id", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("name", stmt.getMetaData().getColumn(1).getName());
        Assert.assertEquals("database_name", stmt.getMetaData().getColumn(2).getName());
        Assert.assertEquals("refresh_type", stmt.getMetaData().getColumn(3).getName());
        Assert.assertEquals("is_active", stmt.getMetaData().getColumn(4).getName());
        Assert.assertEquals("last_refresh_start_time", stmt.getMetaData().getColumn(5).getName());
        Assert.assertEquals("last_refresh_finished_time", stmt.getMetaData().getColumn(6).getName());
        Assert.assertEquals("last_refresh_duration", stmt.getMetaData().getColumn(7).getName());
        Assert.assertEquals("last_refresh_state", stmt.getMetaData().getColumn(8).getName());
        Assert.assertEquals("inactive_code", stmt.getMetaData().getColumn(9).getName());
        Assert.assertEquals("inactive_reason", stmt.getMetaData().getColumn(10).getName());
        Assert.assertEquals("text", stmt.getMetaData().getColumn(11).getName());
        Assert.assertEquals("rows", stmt.getMetaData().getColumn(12).getName());
    }

    @Test(expected = SemanticException.class)
    public void testNoDb() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ShowMaterializedViewsStmt stmt = new ShowMaterializedViewsStmt("");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws");
    }
}
