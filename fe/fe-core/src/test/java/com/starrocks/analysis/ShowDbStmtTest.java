// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ShowDbStmtTest.java

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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShowDbStmtTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        final Analyzer analyzer = AccessTestUtil.fetchBlockAnalyzer();
        ShowDbStmt stmt = new ShowDbStmt(null);
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getPattern());
        Assert.assertEquals("SHOW DATABASES", stmt.toString());
        Assert.assertEquals(1, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Database", stmt.getMetaData().getColumn(0).getName());

        stmt = new ShowDbStmt("abc");
        stmt.analyze(analyzer);
        Assert.assertEquals("abc", stmt.getPattern());
        Assert.assertEquals("SHOW DATABASES LIKE 'abc'", stmt.toString());
        Assert.assertEquals(1, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Database", stmt.getMetaData().getColumn(0).getName());
    }

    @Test
    public void testShowSchemas() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String showSQL = "show schemas";
        ShowDbStmt showDbStmt = (ShowDbStmt) UtFrameUtils.parseStmtWithNewParser(showSQL, ctx);
    }
}