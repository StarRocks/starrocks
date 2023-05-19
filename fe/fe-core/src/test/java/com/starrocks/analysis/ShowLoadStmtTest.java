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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ShowLoadStmtTest.java

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

import com.google.common.collect.ImmutableSet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.ShowLoadStmt;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class ShowLoadStmtTest {
    @Before
    public void setUp() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNormal() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        ShowLoadStmt stmt = (ShowLoadStmt) analyzeSuccess("SHOW LOAD FROM test");
        ShowResultSetMetaData metaData = stmt.getMetaData();
        Assert.assertNotNull(metaData);
        Assert.assertEquals(20, metaData.getColumnCount());
        Assert.assertEquals("JobId", metaData.getColumn(0).getName());
        Assert.assertEquals("Label", metaData.getColumn(1).getName());
        Assert.assertEquals("State", metaData.getColumn(2).getName());
        Assert.assertEquals("Progress", metaData.getColumn(3).getName());
        Assert.assertEquals("Type", metaData.getColumn(4).getName());
        Assert.assertEquals("Priority", metaData.getColumn(5).getName());
        Assert.assertEquals("ScanRows", metaData.getColumn(6).getName());
        Assert.assertEquals("FilteredRows", metaData.getColumn(7).getName());
        Assert.assertEquals("UnselectedRows", metaData.getColumn(8).getName());
        Assert.assertEquals("SinkRows", metaData.getColumn(9).getName());
        Assert.assertEquals("EtlInfo", metaData.getColumn(10).getName());
        Assert.assertEquals("TaskInfo", metaData.getColumn(11).getName());
        Assert.assertEquals("ErrorMsg", metaData.getColumn(12).getName());
        Assert.assertEquals("CreateTime", metaData.getColumn(13).getName());
        Assert.assertEquals("EtlStartTime", metaData.getColumn(14).getName());
        Assert.assertEquals("EtlFinishTime", metaData.getColumn(15).getName());
        Assert.assertEquals("LoadStartTime", metaData.getColumn(16).getName());
        Assert.assertEquals("LoadFinishTime", metaData.getColumn(17).getName());
        Assert.assertEquals("TrackingSQL", metaData.getColumn(18).getName());
        Assert.assertEquals("JobDetails", metaData.getColumn(19).getName());
    }

    @Test
    public void testNoDb() throws UserException, AnalysisException {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase(null);
        analyzeFail("SHOW LOAD", "No database selected");
    }

    @Test
    public void testInvalidWhere() {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        String fallMessage = "Where clause should looks like: LABEL = \"your_load_label\", or LABEL LIKE \"matcher\","
            + "  or STATE = \"PENDING|ETL|LOADING|FINISHED|CANCELLED|QUEUEING\",  or compound predicate with operator AND";
        analyzeFail("SHOW LOAD WHERE STATE = 'RUNNING'", fallMessage);
        analyzeFail("SHOW LOAD WHERE STATE != 'LOADING'", fallMessage);
        analyzeFail("SHOW LOAD WHERE STATE LIKE 'LOADING'", fallMessage);
        analyzeFail("SHOW LOAD WHERE LABEL LIKE 'abc' AND true", fallMessage);
        analyzeFail("SHOW LOAD WHERE LABEL = 123", fallMessage);
        analyzeFail("SHOW LOAD WHERE LABEL = ''", fallMessage);
    }

    @Test
    public void testInvalidOrderBy() {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        analyzeFail("SHOW LOAD ORDER BY time", "Title name[time] does not exist");
    }

    @Test
    public void testWhere() throws UserException, AnalysisException {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        ShowLoadStmt stmt = (ShowLoadStmt) analyzeSuccess("SHOW LOAD FROM `testCluster:testDb` WHERE `label` = 'abc' LIMIT 10");
        Assert.assertEquals(10, stmt.getLimit());
        Assert.assertEquals("abc", stmt.getLabelValue());
        Assert.assertNull(stmt.getStates());
        Assert.assertEquals(-1, stmt.getOffset());

        stmt = (ShowLoadStmt) analyzeSuccess("SHOW LOAD FROM `testCluster:testDb` WHERE `label` LIKE 'abc' and `state` = 'LOADING' ORDER BY `Label` DESC");
        Assert.assertEquals("abc", stmt.getLabelValue());
        Assert.assertEquals(ImmutableSet.of(JobState.LOADING), stmt.getStates());
        Assert.assertEquals(1, stmt.getOrderByPairs().get(0).getIndex());
        Assert.assertTrue(stmt.getOrderByPairs().get(0).isDesc());
    }

    @Test
    public void testGetRedirectStatus() {
        ShowLoadStmt stmt = new ShowLoadStmt(null, null, null, null);
        Assert.assertEquals(stmt.getRedirectStatus(), RedirectStatus.FORWARD_WITH_SYNC);
    }

    @Test
    public void testShowLoadAll() {
        analyzeSuccess("show load all");
        analyzeSuccess("show load all where label = 'hehe'");
    }
}
