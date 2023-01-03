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


package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;

public class AnalyzeStructTest {

    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = getStarRocksAssert();
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("test").useDatabase("test");

        FeConstants.runningUnitTest = true;
        String createStructTableSql = "CREATE TABLE struct_a(\n" +
                "a INT, \n" +
                "b STRUCT<a INT, c INT> COMMENT 'smith',\n" +
                "c STRUCT<a INT, b DOUBLE>,\n" +
                "d STRUCT<a INT, b ARRAY<STRUCT<a INT, b DOUBLE>>, c STRUCT<a INT>>,\n" +
                "struct_a STRUCT<struct_a STRUCT<struct_a INT>, other INT> COMMENT 'alias test'\n" +
                ") DISTRIBUTED BY HASH(`a`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(createStructTableSql);

        String deeperStructTableSql = "CREATE TABLE deeper_table(\n" +
                "a INT, \n" +
                "b STRUCT<b STRUCT<c STRUCT<d STRUCT<e INT>>>>,\n" +
                "struct_a STRUCT<struct_a STRUCT<struct_a INT>, other INT>\n" +
                ") DISTRIBUTED BY HASH(`a`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(deeperStructTableSql);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testStructSubFieldAccess() {
        analyzeSuccess("select * from struct_a");
        analyzeSuccess("select b from struct_a");
        analyzeSuccess("select b.a from struct_a");

        // Qualified name test
        analyzeSuccess("select struct_a from struct_a");
        analyzeSuccess("select struct_a.struct_a from struct_a");
        analyzeSuccess("select test.struct_a.struct_a from struct_a");
        analyzeSuccess("select struct_a from struct_a");
        analyzeSuccess("select struct_a.other from struct_a");
        analyzeFail("select struct_a.p from struct_a");
        analyzeFail("select p.a from struct_a");
    }

    @Test
    public void testSubfieldCaseSensitive() {
        analyzeSuccess("select b.a from struct_a");
        analyzeSuccess("select b.A from struct_a");
        analyzeSuccess("select d.B[10].A from struct_a");
    }

    @Test
    public void testInvalidSql() {
        analyzeFail("select b + 1 from struct_a;");
        analyzeFail("select * from struct_a order by b;");
        analyzeFail("select sum(b) from struct_a;");
        analyzeFail("select * from struct_a a join struct_a b on a.b=b.b;");
        analyzeFail("select sum(a) from struct_a group by b;");
    }

    @Test
    public void testDeeperTable() {
        analyzeFail("SELECT b.b.c.d.f FROM deeper_table");
        analyzeSuccess("SELECT b.b.c.d.e FROM deeper_table");
    }

    @Test
    public void testShowCreateTable() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());

        ShowCreateTableStmt stmt = (ShowCreateTableStmt) analyzeSuccess("SHOW CREATE TABLE deeper_table");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        String res = resultSet.getResultRows().get(0).get(1);
        Assert.assertTrue(res.contains("`b` STRUCT<b STRUCT<c STRUCT<d STRUCT<e int(11)>>>> NULL COMMENT \"\""));
        Assert.assertTrue(
                res.contains("`struct_a` STRUCT<struct_a STRUCT<struct_a int(11)>, other int(11)> NULL COMMENT \"\""));
    }
}
