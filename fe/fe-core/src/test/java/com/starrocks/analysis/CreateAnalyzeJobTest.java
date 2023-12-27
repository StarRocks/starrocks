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

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateAnalyzeJobTest {
    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Test
    public void createExternalAnalyzeJobTest() throws Exception {
        String sql = "create analyze table hive0.partitioned_db.t1";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assert.assertTrue(statementBase instanceof CreateAnalyzeJobStmt);

        connectContext.setCurrentCatalog("hive0");
        String sql1 = "create analyze all";
        Assert.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql1, connectContext));

        String sql2 = "create analyze sample table hive0.partitioned_db.t1";
        Assert.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql2, connectContext));

        String sql3 = "create analyze database tpch";
        Assert.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql3, connectContext));

        String sql4 = "create analyze full table hive0.partitioned_db.t1";
        statementBase = UtFrameUtils.parseStmtWithNewParser(sql4, connectContext);
        Assert.assertTrue(statementBase instanceof CreateAnalyzeJobStmt);
    }

}
