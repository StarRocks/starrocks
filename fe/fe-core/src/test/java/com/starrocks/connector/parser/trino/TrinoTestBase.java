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

package com.starrocks.connector.parser.trino;

import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.LogicalPlanPrinter;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ErrorCollector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TrinoTestBase {
    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t1` (\n" +
                "  `v4` bigint NULL COMMENT \"\",\n" +
                "  `v5` bigint NULL COMMENT \"\",\n" +
                "  `v6` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v4`, `v5`, v6)\n" +
                "DISTRIBUTED BY HASH(`v4`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t2` (\n" +
                "  `v7` bigint NULL COMMENT \"\",\n" +
                "  `v8` bigint NULL COMMENT \"\",\n" +
                "  `v9` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v7`, `v8`, v9)\n" +
                "DISTRIBUTED BY HASH(`v7`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tall` (\n" +
                "  `ta` varchar(20) NULL COMMENT \"\",\n" +
                "  `tb` smallint(6) NULL COMMENT \"\",\n" +
                "  `tc` int(11) NULL COMMENT \"\",\n" +
                "  `td` bigint(20) NULL COMMENT \"\",\n" +
                "  `te` float NULL COMMENT \"\",\n" +
                "  `tf` double NULL COMMENT \"\",\n" +
                "  `tg` bigint(20) NULL COMMENT \"\",\n" +
                "  `th` datetime NULL COMMENT \"\",\n" +
                "  `ti` date NULL COMMENT \"\",\n" +
                "  `tj` decimal(9, 3) NULL COMMENT \"\",\n" +
                "  `tk` varbinary NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ta`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ta`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE region ( R_REGIONKEY  INTEGER NOT NULL,\n" +
                "                            R_NAME       CHAR(25) NOT NULL,\n" +
                "                            R_COMMENT    VARCHAR(152),\n" +
                "                            PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`r_regionkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE supplier ( S_SUPPKEY     INTEGER NOT NULL,\n" +
                "                             S_NAME        CHAR(25) NOT NULL,\n" +
                "                             S_ADDRESS     VARCHAR(40) NOT NULL, \n" +
                "                             S_NATIONKEY   INTEGER NOT NULL,\n" +
                "                             S_PHONE       CHAR(15) NOT NULL,\n" +
                "                             S_ACCTBAL     double NOT NULL,\n" +
                "                             S_COMMENT     VARCHAR(101) NOT NULL,\n" +
                "                             PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`s_suppkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE partsupp ( PS_PARTKEY     INTEGER NOT NULL,\n" +
                "                             PS_SUPPKEY     INTEGER NOT NULL,\n" +
                "                             PS_AVAILQTY    INTEGER NOT NULL,\n" +
                "                             PS_SUPPLYCOST  double  NOT NULL,\n" +
                "                             PS_COMMENT     VARCHAR(199) NOT NULL,\n" +
                "                             PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ps_partkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE orders  ( O_ORDERKEY       INTEGER NOT NULL,\n" +
                "                           O_CUSTKEY        INTEGER NOT NULL,\n" +
                "                           O_ORDERSTATUS    CHAR(1) NOT NULL,\n" +
                "                           O_TOTALPRICE     double NOT NULL,\n" +
                "                           O_ORDERDATE      DATE NOT NULL,\n" +
                "                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  \n" +
                "                           O_CLERK          CHAR(15) NOT NULL, \n" +
                "                           O_SHIPPRIORITY   INTEGER NOT NULL,\n" +
                "                           O_COMMENT        VARCHAR(79) NOT NULL,\n" +
                "                           PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`o_orderkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE customer ( C_CUSTKEY     INTEGER NOT NULL,\n" +
                "                             C_NAME        VARCHAR(25) NOT NULL,\n" +
                "                             C_ADDRESS     VARCHAR(40) NOT NULL,\n" +
                "                             C_NATIONKEY   INTEGER NOT NULL,\n" +
                "                             C_PHONE       CHAR(15) NOT NULL,\n" +
                "                             C_ACCTBAL     double   NOT NULL,\n" +
                "                             C_MKTSEGMENT  CHAR(10) NOT NULL,\n" +
                "                             C_COMMENT     VARCHAR(117) NOT NULL,\n" +
                "                             PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c_custkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `nation` (\n" +
                "  `N_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `N_NAME` char(25) NOT NULL COMMENT \"\",\n" +
                "  `N_REGIONKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `N_COMMENT` varchar(152) NULL COMMENT \"\",\n" +
                "  `PAD` char(1) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`N_NATIONKEY`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE part  ( P_PARTKEY     INTEGER NOT NULL,\n" +
                "                          P_NAME        VARCHAR(55) NOT NULL,\n" +
                "                          P_MFGR        CHAR(25) NOT NULL,\n" +
                "                          P_BRAND       CHAR(10) NOT NULL,\n" +
                "                          P_TYPE        VARCHAR(25) NOT NULL,\n" +
                "                          P_SIZE        INTEGER NOT NULL,\n" +
                "                          P_CONTAINER   CHAR(10) NOT NULL,\n" +
                "                          P_RETAILPRICE double NOT NULL,\n" +
                "                          P_COMMENT     VARCHAR(23) NOT NULL,\n" +
                "                          PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`p_partkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE lineitem ( L_ORDERKEY    INTEGER NOT NULL,\n" +
                "                             L_PARTKEY     INTEGER NOT NULL,\n" +
                "                             L_SUPPKEY     INTEGER NOT NULL,\n" +
                "                             L_LINENUMBER  INTEGER NOT NULL,\n" +
                "                             L_QUANTITY    double NOT NULL,\n" +
                "                             L_EXTENDEDPRICE  double NOT NULL,\n" +
                "                             L_DISCOUNT    double NOT NULL,\n" +
                "                             L_TAX         double NOT NULL,\n" +
                "                             L_RETURNFLAG  CHAR(1) NOT NULL,\n" +
                "                             L_LINESTATUS  CHAR(1) NOT NULL,\n" +
                "                             L_SHIPDATE    DATE NOT NULL,\n" +
                "                             L_COMMITDATE  DATE NOT NULL,\n" +
                "                             L_RECEIPTDATE DATE NOT NULL,\n" +
                "                             L_SHIPINSTRUCT CHAR(25) NOT NULL,\n" +
                "                             L_SHIPMODE     CHAR(10) NOT NULL,\n" +
                "                             L_COMMENT      VARCHAR(44) NOT NULL,\n" +
                "                             PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`l_orderkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 20\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("create table test_array(c0 INT, " +
                "c1 array<varchar(65533)>, " +
                "c2 array<int>) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");

        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("create table test_struct(c0 INT, " +
                "c1 struct<a array<struct<b int>>>," +
                "c2 struct<a int,b double>) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");

        starRocksAssert.withTable("create table test_map(c0 int, " +
                "c1 map<int,int>, " +
                "c2 array<map<int,int>>, " +
                "c3 map<varchar(65533), date>) " +
                "engine=olap distributed by hash(c0) buckets 10 " +
                "properties('replication_num'='1');");

        FeConstants.runningUnitTest = false;

        connectContext.getSessionVariable().setSqlDialect("trino");
    }

    public static StatementBase analyzeSuccess(String originStmt) {
        try {
            StatementBase statementBase = com.starrocks.sql.parser.SqlParser.parse(originStmt,
                    connectContext.getSessionVariable()).get(0);
            Analyzer.analyze(statementBase, connectContext);
            return statementBase;
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail();
            throw ex;
        }
    }

    public static void analyzeFail(String originStmt) {
        analyzeFail(originStmt, "");
    }

    public static void analyzeFail(String originStmt, String exceptMessage) {
        try {
            StatementBase statementBase = com.starrocks.sql.parser.SqlParser.parse(originStmt,
                    connectContext.getSessionVariable()).get(0);
            Analyzer.analyze(statementBase, connectContext);
            Assert.fail("Miss semantic error exception");
        } catch (ParsingException | SemanticException | UnsupportedException e) {
            if (!exceptMessage.equals("")) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(exceptMessage));
            }
        } catch (Exception e) {
            Assert.fail("analyze exception");
        }
    }

    public String getFragmentPlan(String sql) throws Exception {
        return getPlanAndFragment(connectContext, sql).second.getExplainString(TExplainLevel.NORMAL);
    }

    public String getExplain(String sql) throws Exception {
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));

        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable());
        connectContext.getDumpInfo().setOriginStmt(sql);
        StatementBase statementBase = statements.get(0);

        ExecPlan execPlan = StatementPlanner.plan(statementBase, connectContext);

        return execPlan.getExplainString(statementBase.getExplainLevel());
    }

    public static Pair<String, ExecPlan> getPlanAndFragment(ConnectContext connectContext, String originStmt)
            throws Exception {
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));

        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable());
        connectContext.getDumpInfo().setOriginStmt(originStmt);
        StatementBase statementBase = statements.get(0);

        ExecPlan execPlan = StatementPlanner.plan(statementBase, connectContext);
        return new Pair<>(LogicalPlanPrinter.print(execPlan.getPhysicalPlan()), execPlan);
    }

    protected void assertPlanContains(String sql, String... explain) throws Exception {
        String explainString = getFragmentPlan(sql);

        for (String expected : explain) {
            Assert.assertTrue("expected is: " + expected + " but plan is \n" + explainString,
                    StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected));
        }
    }

    public void runFileUnitTest(String filename) {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/" + filename + ".sql");

        String mode = "";
        String tempStr;
        StringBuilder sql = new StringBuilder();
        StringBuilder result = new StringBuilder();
        StringBuilder comment = new StringBuilder();

        boolean isComment = false;
        boolean hasResult = false;

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            int nth = 0;
            while ((tempStr = reader.readLine()) != null) {
                if (tempStr.startsWith("/*")) {
                    isComment = true;
                    comment.append(tempStr).append("\n");
                }
                if (tempStr.endsWith("*/")) {
                    isComment = false;
                    comment.append(tempStr).append("\n");
                    continue;
                }

                if (isComment || tempStr.startsWith("//")) {
                    comment.append(tempStr);
                    continue;
                }

                switch (tempStr) {
                    case "[sql]":
                        sql = new StringBuilder();
                        mode = "sql";
                        continue;
                    case "[result]":
                        result = new StringBuilder();
                        mode = "result";
                        hasResult = true;
                        continue;
                    case "[end]":
                        Pair<String, ExecPlan> pair = getPlanAndFragment(connectContext, sql.toString());

                        try {
                            if (hasResult) {
                                checkWithIgnoreTabletList(result.toString().trim(), pair.first.trim());
                            }
                        } catch (Error error) {
                            collector.addError(new Throwable(nth + " plan " + "\n" + sql, error));
                        }

                        hasResult = false;
                        comment = new StringBuilder();
                        continue;
                }

                switch (mode) {
                    case "sql":
                        sql.append(tempStr).append("\n");
                        break;
                    case "result":
                        result.append(tempStr).append("\n");
                        break;
                }
            }
        } catch (Exception e) {
            System.out.println(sql);
            e.printStackTrace();
            Assert.fail();
        }
    }

    private void checkWithIgnoreTabletList(String expect, String actual) {
        expect = Stream.of(expect.split("\n")).
                filter(s -> !s.contains("tabletList")).collect(Collectors.joining("\n"));

        actual = Stream.of(actual.split("\n")).
                filter(s -> !s.contains("tabletList")).collect(Collectors.joining("\n"));
        Assert.assertEquals(expect, actual);
    }
}
