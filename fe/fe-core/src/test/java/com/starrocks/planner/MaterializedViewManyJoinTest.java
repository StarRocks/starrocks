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

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.sql.plan.PlanTestBase;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Test MV with many tables join and complex structures.
 * The main purpose of this case is to guarantee that the rewrite-procedure should not take too long even if
 * the query/mv is very complex
 */
public class MaterializedViewManyJoinTest extends MaterializedViewTestBase {

    private static final AtomicLong MV_ID = new AtomicLong(0);
    private static final AtomicLong NESTED_MV_ID = new AtomicLong(0);
    private static final AtomicLong NESTED_NESTED_MV_ID = new AtomicLong(0);
    private static final AtomicLong NESTED_NESTED_NESTED_MV_ID = new AtomicLong(0);
    private static final Map<String, String> MV_TO_DEFINED_QUERY = Maps.newHashMap();
    private static final List<Arguments> ARGUMENTS = Lists.newArrayList();

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
                                    "\"enable_persistent_index\"=\"true\",\n" +
                                    "\"compression\"=\"LZ4\"\n" +
                                    ")", i));
        }
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(false);
        connectContext.getSessionVariable().setCboMaterializedViewRewriteCandidateLimit(3);
        connectContext.getSessionVariable().setCboMaterializedViewRewriteRuleOutputLimit(3);
        List<Arguments> arguments = generateManyJoinArguments();
        for (Arguments argument : arguments) {
            String name = (String) argument.get()[0];
            String mvQuery = (String) argument.get()[1];
            String testQuery = (String) argument.get()[2];
            boolean expectHitMv = (boolean) argument.get()[3];
            long mvId = MV_ID.getAndAdd(1);
            String mvName = String.format("mv_manyjoin_%s", mvId);
            String createMv = "CREATE MATERIALIZED VIEW " + mvName + "\n" +
                    "REFRESH  DEFERRED MANUAL \n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\"=\"1\"\n" +
                    ")\n" +
                    "AS " + mvQuery;
            starRocksAssert.withMaterializedView(createMv);
            MV_TO_DEFINED_QUERY.put(mvName, mvQuery);

            ARGUMENTS.add(Arguments.of(name, testQuery, false));
            if (mvId > 1) {
                long nestedMVId = NESTED_MV_ID.getAndAdd(1);
                String sql = generateNestedMV("mv_manyjoin", "nested_mv", nestedMVId, mvId);
                ARGUMENTS.add(Arguments.of(name, sql, expectHitMv));
                if (nestedMVId > 1) {
                    long nestedNestedMVId = NESTED_NESTED_MV_ID.getAndAdd(1);
                    sql = generateNestedMV("nested_mv", "nested_nested_mv", nestedNestedMVId, nestedMVId);
                    ARGUMENTS.add(Arguments.of(name, sql, expectHitMv));
                    if (nestedNestedMVId > 1) {
                        long nestedNestedNestedMVId = NESTED_NESTED_NESTED_MV_ID.getAndAdd(1);
                        sql = generateNestedMV("nested_nested_mv", "nested_nested_nested_mv", nestedNestedNestedMVId,
                                nestedNestedMVId);
                        ARGUMENTS.add(Arguments.of(name, sql, expectHitMv));
                    }
                }
            }
        }
    }

    @ParameterizedTest(name = "{index}-{0}")
    @MethodSource("generateArguments")
    @Timeout(30)
    public void testManyJoins(String name, String query, boolean expectHitMv) throws Exception {
        Stopwatch watch = Stopwatch.createStarted();
        // Make sure it's not empty
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "OlapScanNode");
        if (expectHitMv) {
            PlanTestBase.assertContains(plan, "MaterializedView: true");
        }
        LOG.info("query takes {}ms: {}", watch.elapsed(TimeUnit.MILLISECONDS), query);
    }

    private static Stream<Arguments> generateArguments() {
        return ARGUMENTS.stream();
    }

    private static List<Arguments> generateManyJoinArguments() {
        final String smallQuery = generateJoinQuery(3);
        final String bigQuery = generateJoinQuery(20);
        return ImmutableList.of(
                // join query
                Arguments.of("small_query-small_mv", smallQuery, smallQuery, true),
                Arguments.of("small_query-big_mv", smallQuery, bigQuery, true),
                Arguments.of("big_query-small_mv", bigQuery, smallQuery, true),
                Arguments.of("big_query-big_mv", bigQuery, bigQuery, true),
                Arguments.of("multi join mv(10-10)", generateJoinQuery(10),
                        generateJoinQuery(10), true),
                Arguments.of("multi join mv(5-5)", generateJoinQuery(5),
                        generateJoinQuery(5), true),

                // union&join query
                Arguments.of("union small-query and small mv",
                        generateUnionAndJoinQuery(5), generateJoinQuery(5), false),
                Arguments.of("union bigquery and big join mv",
                        generateUnionAndJoinQuery(20), generateJoinQuery(20), false),
                Arguments.of("union bigquery and big union mv",
                        generateUnionAndJoinQuery(20), generateUnionAndJoinQuery(20), false),

                // union query
                Arguments.of("union small-query and small mv",
                        generateUnionAndJoinQuery(5), generateUnionQuery(5), false),
                Arguments.of("union bigquery and big join mv",
                        generateUnionAndJoinQuery(20), generateUnionQuery(20), false),

                // query with predicate
                Arguments.of("query with predicates(10-5) ",
                        generateJoinWithManyPredicates(10),
                        generateJoinWithManyPredicates(5), false),
                Arguments.of("query with predicates(5-10) ",
                        generateJoinWithManyPredicates(5),
                        generateJoinWithManyPredicates(10), false),
                Arguments.of("query with predicates(10-10) ",
                        generateJoinWithManyPredicates(10),
                        generateJoinWithManyPredicates(10), true),
                Arguments.of("query with predicates(1-50) ",
                        generateJoinWithManyPredicates(1),
                        generateJoinWithManyPredicates(50), true),
                Arguments.of("query with predicates(5-50) ",
                        generateJoinWithManyPredicates(5),
                        generateJoinWithManyPredicates(50), true)
        );
    }

    @NotNull
    private static String generateNestedMV(String tablePrefix, String nestMVPrefix,
                                           long nestedMVId, long numTables) throws Exception {
        String nestedMVName = String.format("%s_%s", nestMVPrefix, nestedMVId);
        String nestedMVQuery = generateNestedMVJoinQuery(tablePrefix, numTables);
        String nestedMv = "CREATE MATERIALIZED VIEW " + nestedMVName + "\n" +
                "REFRESH  DEFERRED MANUAL \n" +
                "PROPERTIES (\n" +
                "\"replication_num\"=\"1\"\n" +
                ")\n" +
                "AS " + nestedMVQuery;
        MV_TO_DEFINED_QUERY.put(nestedMVName, nestedMVQuery);
        starRocksAssert.withMaterializedView(nestedMv);
        // generate unwrapped mv defined query
        return generateUnNestedMVJoinQuery(tablePrefix, numTables);
    }

    private static String generateUnNestedMVJoinQuery(String tablePrefix, long numTables) {
        String sourceTableName = String.format("%s_%d", tablePrefix, 0);
        Preconditions.checkArgument(MV_TO_DEFINED_QUERY.containsKey(sourceTableName));
        String mvDefinedQuery = MV_TO_DEFINED_QUERY.get(sourceTableName);
        StringBuilder query = new StringBuilder(String.format("SELECT p0.dt FROM (%s) AS p0 ", mvDefinedQuery));
        for (int i = 1; i < numTables; i++) {
            String alias = "p" + i;
            sourceTableName = String.format("%s_%d", tablePrefix, i);
            Preconditions.checkArgument(MV_TO_DEFINED_QUERY.containsKey(sourceTableName));
            mvDefinedQuery = MV_TO_DEFINED_QUERY.get(sourceTableName);
            query.append(String.format("LEFT OUTER JOIN (%s) AS %s ON %s.dt=p1.dt\n", mvDefinedQuery, alias, alias));
        }
        return query.toString();
    }

    @NotNull
    private static String generateNestedMVJoinQuery(String tablePrefix, long numTables) {
        StringBuilder query = new StringBuilder(String.format("SELECT p0.dt FROM %s AS p0 ", tablePrefix + "_0"));
        for (int i = 1; i < numTables; i++) {
            String alias = "p" + i;
            String sourceTableName = String.format("%s_%d", tablePrefix, i);
            query.append(String.format("LEFT OUTER JOIN %s AS %s ON %s.dt=p1.dt\n", sourceTableName, alias, alias));
        }
        return query.toString();
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
