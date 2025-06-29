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

package com.starrocks.planner;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class MVViewRewriteWithManyJoinTest extends MaterializedViewTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MaterializedViewTestBase.beforeClass();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);
        starRocksAssert.withTable(
                "CREATE TABLE tbl_0 (\n" +
                        " dt date NULL COMMENT \"etl\",\n" +
                        " p1_col1 varchar(60) NULL COMMENT \"\",\n" +
                        " p1_col2 varchar(240) NULL COMMENT \"\",\n" +
                        " p1_col3 varchar(30) NULL COMMENT \"\",\n" +
                        " p1_col4 decimal128(22, 2) NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(dt, p1_col1)\n" +
                        "PARTITION BY RANGE(dt)\n" +
                        "(PARTITION p20221230 VALUES [(\"2022-12-30\"), (\"2022-12-31\")),\n" +
                        "PARTITION p20230331 VALUES [(\"2023-03-31\"), (\"2023-04-01\")))\n" +
                        "DISTRIBUTED BY HASH(dt, p1_col2) BUCKETS 1 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\"=\"1\"" +
                        ");");

        for (int i = 1; i < 50; i++) {
            starRocksAssert.withTable(
                    MessageFormat.format(
                            "CREATE TABLE tbl_{0} (\n" +
                                    " dt date NULL COMMENT \"\",\n" +
                                    " p{0}_col1 varchar(240) NULL COMMENT \"\",\n" +
                                    " p{0}_col2 varchar(240) NULL COMMENT \"\"\n" +
                                    ") ENGINE=OLAP \n" +
                                    "DUPLICATE KEY(dt, p{0}_col1)\n" +
                                    "PARTITION BY RANGE(dt)\n" +
                                    "   (PARTITION p202212 VALUES [(\"2022-12-01\"), (\"2023-01-01\")),\n" +
                                    "       PARTITION p202301 VALUES [(\"2023-01-01\"), (\"2023-02-01\")),\n" +
                                    "       PARTITION p202302 VALUES [(\"2023-02-01\"), (\"2023-03-01\")))\n" +
                                    "DISTRIBUTED BY HASH(dt, p{0}_col2) BUCKETS 1 \n" +
                                    "PROPERTIES (\n" +
                                    "\"replication_num\"=\"1\",\n" +
                                    "\"in_memory\"=\"false\",\n" +
                                    "\"storage_format\"=\"DEFAULT\",\n" +
                                    "\"enable_persistent_index\"=\"true\",\n" +
                                    "\"compression\"=\"LZ4\"\n" +
                                    ")", i));
        }
    }

    @ParameterizedTest(name = "{index}-{0}")
    @MethodSource("generateManyJoinArguments")
    @Timeout(30)
    public void testManyJoins(String name, String query) throws Exception {
        LOG.info("test {}, query: {}", name, query);
        String mvName = "mv_manyjoin";
        String viewName = "view_0";
        // create view
        String createViewDDL = String.format("CREATE VIEW %s as %s", viewName, query);
        starRocksAssert.withView(createViewDDL);

        String viewQuery = String.format("select * from %s", viewName);
        // create mv
        String createMv = "CREATE MATERIALIZED VIEW " + mvName + "\n" +
                "REFRESH  DEFERRED MANUAL \n" +
                "PROPERTIES (\n" +
                "\"replication_num\"=\"1\"\n" +
                ")\n" +
                "AS " + viewQuery;
        starRocksAssert.withMaterializedView(createMv);
        Stopwatch watch = Stopwatch.createStarted();
        // Make sure it's not empty
        starRocksAssert.query(viewQuery).explainContains(mvName);
        LOG.info("query takes {}ms: {}", watch.elapsed(TimeUnit.MILLISECONDS), query);
        starRocksAssert.dropView(viewName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    private static Stream<Arguments> generateManyJoinArguments() {
        final String smallQuery = generateJoinQuery(3);
        final String bigQuery = generateJoinQuery(20);
        return Stream.of(
                // join query
                Arguments.of("small query", smallQuery),
                Arguments.of("big query", bigQuery),

                // union query
                Arguments.of("union query 5", generateUnionQuery(5)),
                Arguments.of("union query 20", generateUnionQuery(20)),
                Arguments.of("union query 50", generateUnionQuery(50)),

                // union&join query
                Arguments.of("union & join query 5", generateUnionAndJoinQuery(5)),
                Arguments.of("union & join query 20", generateUnionAndJoinQuery(20)),
                Arguments.of("union & join query 50", generateUnionAndJoinQuery(50)),

                // query with predicate
                Arguments.of("query with predicates 5", generateJoinWithManyPredicates(5)),
                Arguments.of("query with predicates 10", generateJoinWithManyPredicates(10)),
                Arguments.of("query with predicates 50", generateJoinWithManyPredicates(50))
        );
    }

    @NotNull
    private static String generateJoinQuery(int numTables) {
        StringBuilder query = new StringBuilder("SELECT p0.dt FROM tbl_0 AS p0 ");
        for (int i = 1; i < numTables; i++) {
            String alias = "p" + i;
            String sourceTableName = String.format("tbl_%d", i);
            query.append(String.format("LEFT OUTER JOIN %s AS %s ON %s.dt=p1.dt\n", sourceTableName, alias, alias));
        }
        return query.toString();
    }

    /**
     * Generate a query whose fact-table is a union
     */
    private static String generateUnionAndJoinQuery(int numTables) {
        StringBuilder sb = new StringBuilder("SELECT u0.dt FROM (" +
                " SELECT p0.dt FROM tbl_0 p0 WHERE p1_col1 = 'a' " +
                " UNION ALL" +
                " SELECT p0.dt FROM tbl_0 p0 WHERE p1_col2 = 'b') u0\n");
        for (int i = 1; i < numTables; i++) {
            String alias = "p" + i;
            String sourceTableName = String.format("tbl_%d", i);
            sb.append(String.format("LEFT OUTER JOIN %s AS %s ON %s.dt=p1.dt\n", sourceTableName, alias, alias));
        }
        return sb.toString();
    }

    private static String generateUnionQuery(int numTables) {
        StringBuilder sb = new StringBuilder("SELECT u0.dt FROM (" +
                " SELECT p0.dt FROM tbl_0 p0 WHERE p1_col1 = 'a' " +
                " UNION ALL" +
                " SELECT p0.dt FROM tbl_0 p0 WHERE p1_col2 = 'b') u0\n");
        for (int i = 1; i < numTables; i++) {
            String alias = "p" + i;
            String sourceTableName = String.format("tbl_%d", i);
            sb.append(MessageFormat.format("UNION ALL SELECT {0}.dt FROM {1} AS {0} WHERE {0}.{0}_col1 = ''{2}''\n",
                    alias, sourceTableName, RandomStringUtils.randomAlphabetic(3)));
        }
        return sb.toString();
    }

    private static String generateJoinWithManyPredicates(int numPredicates) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT p0.dt FROM tbl_0 p0 " +
                "LEFT OUTER JOIN tbl_1 AS p1 ON p0.dt=p1.dt \n" +
                "WHERE p0.p1_col1 = 'a' ");
        for (int i = 0; i < numPredicates; i++) {
            sb.append(String.format(" OR (p0.p1_col1 = '%s' AND p0.p1_col2 = '%s' ) ",
                    RandomStringUtils.randomAlphabetic(2),
                    RandomStringUtils.randomAlphabetic(5)));
        }
        return sb.toString();
    }
}
