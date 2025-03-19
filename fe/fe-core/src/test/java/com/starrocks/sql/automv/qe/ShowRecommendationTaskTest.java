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

package com.starrocks.sql.automv.qe;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.automv.analysis.TunespaceAnalyzer;
import com.starrocks.sql.automv.ast.ShowRecommendationsStmt;
import com.starrocks.sql.automv.ast.SubmitRecommendationsTaskStmt;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class ShowRecommendationTaskTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() throws Exception {
        FeConstants.runningUnitTest = true;
        if (STARROCKS_ASSERT.get() == null) {
            UtFrameUtils.createMinStarRocksCluster();
            ConnectContext ctx = UtFrameUtils.createDefaultCtx();
            ctx.getSessionVariable().setEnablePipelineEngine(true);
            FeConstants.runningUnitTest = true;
            StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
            starRocksAssert.withDatabase("test_db").useDatabase("test_db");
            starRocksAssert.withTable("CREATE TABLE `_tunespace_` (\n" +
                    "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT \"\",\n" +
                    "  `ts` datetime NOT NULL COMMENT \"\",\n" +
                    "  `originalQuery` varbinary NOT NULL COMMENT \"\",\n" +
                    "  `query` varbinary NULL COMMENT \"\",\n" +
                    "  `category` varchar(255) NOT NULL COMMENT \"\",\n" +
                    "  `traits` json NOT NULL COMMENT \"\"\n" +
                    ") ENGINE=OLAP \n" +
                    "PRIMARY KEY(`id`, `ts`)\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ");");
            STARROCKS_ASSERT.set(starRocksAssert);
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
    }

    @Test
    public void testSubmitRecommendationsTask() throws Exception {
        String[] sqlList = {
                "submit recommendations task 'abc' from _tunespace_",
                "submit single recommendations task 'abc' from _tunespace_"
        };
        ConnectContext ctx = getStarRocksAssert().getCtx();
        for (String sql : sqlList) {
            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable());
            Assert.assertTrue(stmts.get(0) instanceof SubmitRecommendationsTaskStmt);
            SubmitRecommendationsTaskStmt submitStmt = (SubmitRecommendationsTaskStmt) stmts.get(0);
            Assert.assertFalse(Boolean.logicalXor(submitStmt.getStmt().isSingle(), sql.contains("single")));
            TunespaceAnalyzer.analyze(submitStmt, ctx);
            Assert.assertEquals(submitStmt.getStmt().getTableName().toSql(),
                    "`test_db`.`_tunespace_`");
        }
    }

    @Test
    public void testShowRecommendationsFromTask() throws Exception {
        String[] sqlList = {
                "show recommendations from task 'abc'",
                "show single recommendations from task 'abc'",
        };
        ConnectContext ctx = getStarRocksAssert().getCtx();
        for (String sql : sqlList) {
            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable());
            Assert.assertTrue(stmts.get(0) instanceof ShowRecommendationsStmt);
            ShowRecommendationsStmt showStmt = (ShowRecommendationsStmt) stmts.get(0);
            Assert.assertEquals("abc", showStmt.getTaskName());
            Assert.assertNull(showStmt.getTableName());
            TunespaceAnalyzer.analyze(showStmt, ctx);
        }
    }
}
