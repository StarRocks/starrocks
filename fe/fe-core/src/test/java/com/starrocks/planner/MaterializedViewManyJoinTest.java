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
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Test MV with many tables join
 */
public class MaterializedViewManyJoinTest extends MaterializedViewTestBase {

    @BeforeAll
    public static void setUp() throws Exception {
        MaterializedViewTestBase.setUp();
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

        for (int i = 1; i < 20; i++) {
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
                                    "\"enable_persistent_index\"=\"false\",\n" +
                                    "\"compression\"=\"LZ4\"\n" +
                                    ")", i));
        }
    }

    @ParameterizedTest(name = "{index}-{0}")
    @MethodSource("generateManyJoinArguments")
    @Timeout(5)
    public void testManyJoins(String name, String mvQuery, String query, boolean expectHitMv) throws Exception {
        LOG.info("create mv {}", mvQuery);
        String mvName = "mv_manyjoin";
        String createMv = "CREATE MATERIALIZED VIEW " + mvName + "\n" +
                "REFRESH MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\"=\"1\"\n" +
                ")\n" +
                "AS " + mvQuery;
        starRocksAssert.withMaterializedView(createMv);

        Stopwatch watch = Stopwatch.createStarted();
        // Make sure it's not empty
        starRocksAssert.query(query).explainContains("OlapScanNode");
        LOG.info("query takes {}ms: {}", watch.elapsed(TimeUnit.MILLISECONDS), query);

        starRocksAssert.dropMaterializedView(mvName);
    }

    private static Stream<Arguments> generateManyJoinArguments() {
        final String smallQuery = generateJoinQuery(8);
        final String bigQuery = generateJoinQuery(20);
        return Stream.of(
                Arguments.of("small_query-small_mv", smallQuery, smallQuery, true),
                Arguments.of("small_query-big_mv", smallQuery, bigQuery, false),
                Arguments.of("big_query-small_mv", bigQuery, smallQuery, false),
                Arguments.of("big_query-big_mv", bigQuery, bigQuery, true)
        );
    }

    @NotNull
    private static String generateJoinQuery(int numTables) {
        StringBuilder query = new StringBuilder("SELECT p1.dt FROM tbl_0 AS p0 ");
        for (int i = 1; i < numTables; i++) {
            String alias = "p" + i;
            String sourceTableName = String.format("tbl_%d", i);
            query.append(String.format("LEFT OUTER JOIN %s AS %s ON %s.dt=p1.dt\n", sourceTableName, alias, alias));
        }
        return query.toString();
    }

}
