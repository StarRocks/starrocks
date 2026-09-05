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

package com.starrocks.sql.optimizer;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransformerTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static String DB_NAME = "test";

    // Using List to collect errors instead of JUnit 4 ErrorCollector
    private final List<Throwable> errors = new ArrayList<>();

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

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
                "  `ti` date NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ta`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ta`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void testSingle() {
        runUnitTest("scan");
    }

    @Test
    public void testAggregate() {
        runUnitTest("aggregate");
    }

    @Test
    public void testSort() {
        runUnitTest("sort");
    }

    @Test
    public void testJoin() {
        runUnitTest("join");
    }

    @Test
    public void testSubquery() {
        runUnitTest("subquery");
    }

    @Test
    public void testPivot() {
        runUnitTest("pivot");
    }

    public void analyzeAndBuildOperator(String originStmt, String operatorString, String except) {
        try {
            StatementBase statementBase = com.starrocks.sql.parser.SqlParser.parse(originStmt,
                    connectContext.getSessionVariable().getSqlMode()).get(0);

            Analyzer.analyze(statementBase, connectContext);
            LogicalPlan logicalPlan = new RelationTransformer(new ColumnRefFactory(), connectContext)
                    .transform(((QueryStatement) statementBase).getQueryRelation());

            try {
                Assertions.assertEquals(operatorString.substring(0, operatorString.length() - 1),
                        LogicalPlanPrinter.print(logicalPlan.getRoot()));
            } catch (AssertionFailedError error) {
                errors.add(new Throwable("\n" + originStmt, error));
            }
        } catch (Exception ex) {
            if (!except.isEmpty()) {
                Assertions.assertEquals(ex.getMessage(), except);
                return;
            }
            Assertions.fail("build operator fail, message: " + ex.getMessage() + ", sql: " + originStmt);
        }
    }

    private void runUnitTest(String filename) {
        String path = ClassLoader.getSystemClassLoader().getResource("sql").getPath();
        File file = new File(path + "/logical-plan/" + filename + ".sql");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String sql = "";
            String result = "";
            String except = "";
            String mode = "";
            String tempStr;

            boolean comment = false;
            while ((tempStr = reader.readLine()) != null) {
                if (tempStr.startsWith("/*")) {
                    comment = true;
                }
                if (tempStr.endsWith("*/")) {
                    comment = false;
                    continue;
                }

                if (comment) {
                    continue;
                }

                if (tempStr.equals("[sql]")) {
                    sql = "";
                    mode = "sql";
                    continue;
                } else if (tempStr.equals("[result]")) {
                    result = "";
                    mode = "result";
                    continue;
                } else if (tempStr.equals("[except]")) {
                    except = "";
                    mode = "except";
                    continue;
                } else if (tempStr.equals("[end]")) {
                    analyzeAndBuildOperator(sql, result, except);
                    continue;
                }

                if (mode.equals("sql")) {
                    sql += tempStr;
                } else if (mode.equals("result")) {
                    result += tempStr + "\n";
                } else if (mode.equals("except")) {
                    except += tempStr;
                }
            }
            reader.close();

            // Report any collected errors after test completion (JUnit 5 style)
            if (!errors.isEmpty()) {
                AssertionFailedError error = new AssertionFailedError(
                        "There were " + errors.size() + " errors in test " + filename);
                errors.forEach(error::addSuppressed);
                throw error;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }
}
