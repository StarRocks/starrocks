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

import com.starrocks.datacache.DataCacheMgr;
import com.starrocks.datacache.DataCacheRule;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateDataCacheRuleStmt;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class DataCacheStmtAnalyzerTest {

    private final DataCacheMgr DATACACHE_MGR = DataCacheMgr.getInstance();

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        ConnectorPlanTestBase.mockHiveCatalog(AnalyzeTestUtil.getConnectContext());
    }

    @After
    public void clearDataCacheMgr() {
        DATACACHE_MGR.clearRules();
    }

    @Test
    public void testAddSimpleRule() {
        CreateDataCacheRuleStmt stmt = (CreateDataCacheRuleStmt)
                analyzeSuccess("create datacache rule hive0.partitioned_db.orders priority = -1");
        Assert.assertEquals(-1, stmt.getPriority());
        Assert.assertNull(stmt.getPredicates());
        Assert.assertNull(stmt.getProperties());
        Assert.assertEquals(QualifiedName.of(List.of("hive0", "partitioned_db", "orders")), stmt.getTarget());
    }

    @Test
    public void testAddRuleForInternalCatalog() {
        analyzeFail("create datacache rule default_catalog.test.c0 priority = -1", "DataCache only support external catalog now");
    }

    @Test
    public void testForNoneBlackListRule() {
        analyzeFail("create datacache rule hive0.partitioned_db.orders priority = 0",
                "DataCache only support priority = -1 (aka BlackList) now");
        analyzeFail("create datacache rule hive0.partitioned_db.orders priority = 1",
                "DataCache only support priority = -1 (aka BlackList) now");
    }

    @Test
    public void testAddProperties() {
        analyzeFail("create datacache rule hive0.partitioned_db.orders priority = -1 " +
                "properties(\"a\"=\"b\")", "DataCache don't support specify properties now");
    }

    @Test
    public void testCreateRuleValidity() {
        analyzeFail("create datacache rule * priority = -1");
        analyzeFail("create datacache rule *.a.b priority = -1", "Catalog is *, database and table must use * either");
        analyzeFail("create datacache rule a.*.b priority = -1", "Database is *, table must use * either");
        analyzeFail("create datacache rule catalog.a.b priority = -1", "DataCache target catalog: catalog does not exist.");
        analyzeFail("create datacache rule hive0.partitioned_db.b priority = -1",
                "DataCache target table: b does not exist in [catalog: hive0, database: partitioned_db]");
        analyzeFail("create datacache rule hive0.partitioned_db.* WHERE dt>'2042' priority = -1",
                "You must have a specific table when using where clause");
    }

    @Test
    public void testCreateConflictRule() {
        DATACACHE_MGR.createCacheRule(QualifiedName.of(List.of("hive0", "partitioned_db", "orders")), null, -1, null);
        analyzeSuccess("create datacache rule hive0.partitioned_db.lineitem_par priority = -1");
        analyzeFail("create datacache rule hive0.partitioned_db.orders priority = -1");
        analyzeFail("create datacache rule hive0.partitioned_db.* priority = -1");
        analyzeFail("create datacache rule hive0.*.* priority = -1");
        analyzeFail("create datacache rule *.*.* priority = -1");

        DATACACHE_MGR.clearRules();
        analyzeSuccess("create datacache rule *.*.* priority = -1");

        DATACACHE_MGR.clearRules();
        DATACACHE_MGR.createCacheRule(QualifiedName.of(List.of("*", "*", "*")), null, -1, null);
        analyzeFail("create datacache rule hive0.partitioned_db.lineitem_par priority = -1");
    }

    @Test
    public void testCreateRuleWithPredicates() {
        CreateDataCacheRuleStmt stmt = (CreateDataCacheRuleStmt)
                analyzeSuccess(
                        "create datacache rule hive0.datacache_db.multi_partition_table where l_shipdate > '2012-1-1' priority " +
                                "= -1");
        DATACACHE_MGR.createCacheRule(stmt.getTarget(), stmt.getPredicates(), stmt.getPriority(), stmt.getProperties());
        Optional<DataCacheRule> dataCacheRule = DataCacheMgr.getInstance().getCacheRule(stmt.getTarget());
        Assert.assertTrue(dataCacheRule.isPresent());
        Assert.assertEquals(
                "[id = 0, target = hive0.datacache_db.multi_partition_table, predicates = `hive0`.`datacache_db`" +
                        ".`multi_partition_table`.`l_shipdate` > '2012-1-1', priority = -1, properties = NULL]",
                dataCacheRule.get().toString());
    }

    @Test
    public void testCacheSelect() throws Exception {
        {
            DataCacheSelectStatement stmt = (DataCacheSelectStatement) analyzeSuccess(
                    "cache select * from hive0.datacache_db.multi_partition_table");
            Assert.assertEquals("black_hole_catalog.black_hole_db.black_hole_table",
                    stmt.getInsertStmt().getTableName().toString());
        }
        {
            DataCacheSelectStatement stmt = (DataCacheSelectStatement) analyzeSuccess(
                    "cache  select /*+ set_var(query_timeout=30) */  * from hive0.datacache_db.multi_partition_table where " +
                            "l_shipdate > '2012-1-1'");
            Assert.assertEquals(1, stmt.getAllQueryScopeHints().size());
            Assert.assertEquals("30", stmt.getAllQueryScopeHints().get(0).getValue().get("query_timeout"));
        }
        analyzeSuccess("cache select * from hive0.datacache_db.multi_partition_table where l_shipdate > '2012-1-1'");
        analyzeFail("cache select * from hive0.datacache_db.not_existed");
        analyzeFail("cache select * from default_catalog.test.t0",
                "Currently cache select is not supported in local olap table");
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test").withView("create view aaa as select 1;");
        analyzeFail("cache select * from test.aaa",
                "Cache select only support olap table, external table or materialized view.");
    }

    @Test
    public void testCacheSelectProperties() {
        DataCacheSelectStatement stmt = (DataCacheSelectStatement) analyzeSuccess(
                "cache select * from hive0.datacache_db.multi_partition_table properties(\"verBose\"=\"true\")");
        Assert.assertTrue(stmt.isVerbose());
        Assert.assertEquals(0, stmt.getPriority());
        Assert.assertEquals(0, stmt.getTTLSeconds());

        analyzeFail("cache select * from hive0.datacache_db.multi_partition_table properties(\"priority\"=\"1\")",
                "TTL must be specified when priority > 0");

        analyzeFail(
                "cache select * from hive0.datacache_db.multi_partition_table properties(\"priority\"=\"1\", \"TTL\"=\"P1Y\")");

        stmt = (DataCacheSelectStatement) analyzeSuccess(
                "cache select * from hive0.datacache_db.multi_partition_table properties(\"priority\"=\"1\", \"TTL\"=\"P1d\")");
        Assert.assertEquals(1, stmt.getPriority());
        Assert.assertEquals(24 * 3600, stmt.getTTLSeconds());

        stmt = (DataCacheSelectStatement) analyzeSuccess(
                "cache select * from hive0.datacache_db.multi_partition_table properties(\"priority\"=\"1\", " +
                        "\"TTL\"=\"P1DT1S\")");
        Assert.assertEquals(1, stmt.getPriority());
        Assert.assertEquals(24 * 3600 + 1, stmt.getTTLSeconds());
    }
}
