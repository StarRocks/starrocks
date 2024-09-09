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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/DropMaterializedViewStmtTest.java

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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DropMaterializedViewStmtNewPlannerTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();

        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase("test");
    }

    @Test
    public void testNormal() throws Exception {
        String dropMvSql = "drop materialized view mv1;";
        DropMaterializedViewStmt dropMvStmt = (DropMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(dropMvSql, connectContext);
        String dbName = dropMvStmt.getDbName();
        String mvName = dropMvStmt.getMvName();
        Assert.assertEquals("test", dbName);
        Assert.assertEquals("mv1", mvName);
        Assert.assertEquals(false, dropMvStmt.isSetIfExists());
    }

    @Test
    public void testIfExistsNormal() throws Exception {
        String dropMvSql = "drop materialized view if exists mv1;";
        DropMaterializedViewStmt dropMvStmt = (DropMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(dropMvSql, connectContext);
        Assert.assertEquals(true, dropMvStmt.isSetIfExists());
    }
}
