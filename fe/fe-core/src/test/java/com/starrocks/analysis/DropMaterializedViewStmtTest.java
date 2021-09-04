// This file is made available under Elastic License 2.0.
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

import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DropMaterializedViewStmtTest {

    Analyzer analyzer;
    @Mocked
    Auth auth;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testEmptyMVName() {
        DropMaterializedViewStmt stmt =
                new DropMaterializedViewStmt(false, new TableName("", ""), new TableName("", ""));
        try {
            stmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("mush specify database name"));
        }
    }

    @Test
    public void testRepeatedDB() {
        DropMaterializedViewStmt stmt =
                new DropMaterializedViewStmt(false, new TableName("test", "mvname"), new TableName("test", "table"));
        try {
            stmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("mush specify database name"));
        }
    }

    @Test
    public void testFromDB() {
        DropMaterializedViewStmt stmt =
                new DropMaterializedViewStmt(false, new TableName("test", "mvname"), new TableName("", "table"));
        try {
            stmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("mush specify database name explicitly"));
        }
    }

    @Test
    public void testNormal() {
        DropMaterializedViewStmt stmt = new DropMaterializedViewStmt(false, new TableName("test", "mvname"), null);
        try {
            stmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
