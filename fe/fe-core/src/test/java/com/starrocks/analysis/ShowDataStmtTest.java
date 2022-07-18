// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ShowDataStmtTest.java

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

import com.starrocks.backup.CatalogMocker;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowDataStmtTest {

    @Mocked
    private Auth auth;
    @Mocked
    private Analyzer analyzer;
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private TabletInvertedIndex invertedIndex;

    private Database db;

    @Before
    public void setUp() throws AnalysisException {
        auth = new Auth();

        new Expectations() {
            {
                GlobalStateMgr.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;
            }
        };

        db = CatalogMocker.mockDb();

        new Expectations() {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testCluster:testDb";

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                GlobalStateMgr.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                globalStateMgr.getAuth();
                minTimes = 0;
                result = auth;

                globalStateMgr.getDb(anyString);
                minTimes = 0;
                result = db;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "root";

                ctx.getRemoteIP();
                minTimes = 0;
                result = "192.168.1.1";
            }
        };

        new Expectations() {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };

        AccessTestUtil.fetchAdminAccess();
    }

    @Test
    public void testNormal() throws AnalysisException, UserException {
        ShowDataStmt stmt = new ShowDataStmt(null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW DATA FROM `testCluster:testDb`", stmt.toString());
        Assert.assertEquals(3, stmt.getMetaData().getColumnCount());
        Assert.assertEquals(false, stmt.hasTable());

        stmt = new ShowDataStmt("testDb", "test_tbl");
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW DATA FROM `default_cluster:testDb`.`test_tbl`", stmt.toString());
        Assert.assertEquals(5, stmt.getMetaData().getColumnCount());
        Assert.assertEquals(true, stmt.hasTable());
    }
}
