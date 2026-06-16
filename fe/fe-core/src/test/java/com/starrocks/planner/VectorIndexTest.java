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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/planner/QueryPlanTest.java

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

package com.starrocks.planner;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class VectorIndexTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        Config.enable_experimental_vector = true;
        FeConstants.enablePruneEmptyOutputScan = false;
        starRocksAssert.withTable("CREATE TABLE test.test_cosine ("
                + " c0 INT,"
                + " c1 array<float> NOT NULL,"
                + " c2 array<float>,"
                + " vector_distance float,"
                + " INDEX index_vector1 (c1) USING VECTOR ('metric_type' = 'cosine_similarity', "
                + "'is_vector_normed' = 'false', 'M' = '512', 'index_type' = 'hnsw', 'dim'='5') "
                + ") "
                + "DUPLICATE KEY(c0) "
                + "DISTRIBUTED BY HASH(c0) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");

        starRocksAssert.withTable("CREATE TABLE test.test_l2 ("
                + " c0 INT,"
                + " c1 array<float> NOT NULL,"
                + " c2 array<float>,"
                + " INDEX index_vector1 (c1) USING VECTOR ('metric_type' = 'l2_distance', "
                + "'is_vector_normed' = 'false', 'M' = '512', 'index_type' = 'hnsw', 'dim'='5') "
                + ") "
                + "DUPLICATE KEY(c0) "
                + "DISTRIBUTED BY HASH(c0) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");

        starRocksAssert.withTable("CREATE TABLE test.test_ivfpq ("
                + " c0 INT,"
                + " c1 array<float> NOT NULL,"
                + " c2 array<float>,"
                + " INDEX index_vector1 (c1) USING VECTOR ('metric_type' = 'l2_distance', "
                + "'is_vector_normed' = 'false', 'nbits' = '8', 'index_type' = 'ivfpq', 'dim'='4', 'm_ivfpq'='2') "
                + ") "
                + "DUPLICATE KEY(c0) "
                + "DISTRIBUTED BY HASH(c0) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");

        starRocksAssert.withTable("CREATE TABLE test.test_no_vector_index ("
                + " c0 INT,"
                + " c1 array<float>,"
                + " c2 array<float>"
                + ") "
                + "DUPLICATE KEY(c0) "
                + "DISTRIBUTED BY HASH(c0) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");
    }

    @Test
    public void testMeetOrderByRequirement() throws Exception {
        String sql;
        String plan;

        // Basic cases.
        sql = "select c1 from test_cosine " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  2:TOP-N\n" +
                "  |  order by: [5, FLOAT, false] DESC\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (<slot 5> 5: approx_cosine_similarity), remote = false\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  2 <-> [2: c1, ARRAY<FLOAT>, false]\n" +
                "  |  5 <-> [6: __vector_approx_cosine_similarity, FLOAT, false]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: test_cosine, rollup: test_cosine\n" +
                "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: -1.0\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=3.0\n" +
                "     Pruned type: 2 <-> [ARRAY<FLOAT>]\n" +
                "     cardinality: 1\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (6: __vector_approx_cosine_similarity)");

        sql = "select c1 from test_l2 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: -1.0");

        // Constant vector with cast.
        sql = "select c1 from test_cosine " +
                "order by approx_cosine_similarity([cast(1.1 as double),cast(2.1 as double)," +
                "cast(3.1 as double),cast(4.1 as double),cast(5.1 as double)], c1) desc " +
                "limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.1, 3.1, 4.1, 5.1], Predicate Range: -1.0");

        sql = "select c1 from test_cosine " +
                "order by approx_cosine_similarity([cast(1.1 as float),cast(2.1 as float),cast(3.1 as float)" +
                ",cast(4.1 as float),cast(5.1 as float)], c1) desc " +
                "limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.1, 3.1, 4.1, 5.1], Predicate Range: -1.0");

        sql = "select c1 from test_cosine " +
                "order by approx_cosine_similarity([cast(1.1 as int),cast(2.1 as int),cast(3.1 as int)" +
                ",cast(4.1 as int),cast(5.1 as int)], c1) desc " +
                "limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.1, 3.1, 4.1, 5.1], Predicate Range: -1.0");
    }

    @Test
    public void testNotMeetOrderByRequirement() throws Exception {
        String sql;
        String plan;

        // Wrong function name.
        sql = "select c1 from test_l2 " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        sql = "select c1 from test_cosine " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        // Wrong column ref.
        sql = "select c1 from test_l2 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c2) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        // Wrong constant vector
        sql = "select c1 from test_l2 " +
                "order by approx_l2_distance(['a', 'b', 'c'], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        sql = "select c1 from test_l2 " +
                "order by approx_l2_distance(c2, c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        // Wrong ASC/DESC
        sql = "select c1 from test_l2 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) DESC limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        sql = "select c1 from test_cosine " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        // No limit.
        sql = "select c1 from test_cosine " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) DESC";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");
    }

    @Test
    public void testMeetPredicateRequirement() throws Exception {
        String sql;
        String plan;

        // Basic cases.
        sql = "select c1 from test_cosine " +
                "where approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) >= 100 " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 100 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        sql = "select c1 from test_cosine " +
                "where approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) >= 100 " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 100 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        // Cast
        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= cast(100 as double) " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= cast(100 as int) " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= cast(100 as float) " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        // AND
        sql = "select c1 from test_cosine " +
                "where approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) >= 1000 " +
                "and approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) >= 100 " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 1000.0");

        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 100 and approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 1000 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");
    }

    @Test
    public void testNotMeetPredicateRequirement() throws Exception {
        String sql;
        String plan;

        // Predicate direction wrong.
        sql = "select c1 from test_cosine " +
                "where approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) <= 100 " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) >= 100 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        // Must >=, <=, not >, <.
        sql = "select c1 from test_cosine " +
                "where approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) > 100 " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) < 100 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        // Column ref is not vector column.
        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c2) <= 100 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        // constant vector is not the same.
        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([10,2.2,3.3], c2) <= 100 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        // Cannot deal with approx_l2_distance with other functions.
        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) * 2 <= 100 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        // Cannot deal with approx_l2_distance with other predicates.
        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 100 and c0 < 10 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");

        // OR
        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 100 or approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 1000 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: OFF");
    }

    @Test
    public void testRewrite() throws Exception {
        String sql;
        String plan;

        sql = "select c1, " +
                "approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1)+1, " +
                "approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1)+2, " +
                "cast(approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) as string), " +
                "approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c2)+2 " +
                "from test_cosine " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  2:TOP-N\n" +
                "  |  order by: [9, FLOAT, false] DESC\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (<slot 9> 9: approx_cosine_similarity), remote = false\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  2 <-> [2: c1, ARRAY<FLOAT>, false]\n" +
                "  |  5 <-> [11: cast, DOUBLE, true] + 1.0\n" +
                "  |  6 <-> [11: cast, DOUBLE, true] + 2.0\n" +
                "  |  7 <-> cast([10: __vector_approx_cosine_similarity, FLOAT, false] as VARCHAR(65533))\n" +
                "  |  8 <-> cast(approx_cosine_similarity[([1.1,2.2,3.3,4.4,5.5], [3: c2, ARRAY<FLOAT>, true]);" +
                " args: INVALID_TYPE,INVALID_TYPE; result: FLOAT; args nullable: true; result nullable: true] " +
                "as DOUBLE) + 2.0\n" +
                "  |  9 <-> [10: __vector_approx_cosine_similarity, FLOAT, false]\n" +
                "  |  common expressions:\n" +
                "  |  11 <-> cast([10: __vector_approx_cosine_similarity, FLOAT, false] as DOUBLE)\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: test_cosine, rollup: test_cosine\n" +
                "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <10:__vector_approx_cosine_similarity>, " +
                "LimitK: 10, Order: DESC, Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: -1.0\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=8.0\n" +
                "     Pruned type: 2 <-> [ARRAY<FLOAT>]\n" +
                "     Pruned type: 3 <-> [ARRAY<FLOAT>]\n" +
                "     cardinality: 1\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (10: __vector_approx_cosine_similarity)\n");
    }

    @Test
    public void testArgumentOrder() throws Exception {
        String sql;
        String plan;

        // Vector function argument order doesn't matter.
        sql = "select c1 from test_cosine " +
                "order by approx_cosine_similarity(c1, [1.1,2.2,3.3,4.4,5.5]) desc limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: -1.0");

        sql = "select c1 from test_l2 " +
                "order by approx_l2_distance(c1, [1.1,2.2,3.3,4.4,5.5]) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: -1.0");

        // Predicate argument order doesn't matter.
        sql = "select c1 from test_cosine " +
                "where 100 <= approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        sql = "select c1 from test_l2 " +
                "where 100 >= approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");
    }

    @Test
    public void testMultipleTables() throws Exception {
        String sql;
        String plan;

        sql = "(select c1 from test_cosine " +
                "where approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) >= 100 " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10) " +
                "UNION ALL " +
                "(select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 100 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10) " +
                "UNION ALL " +
                "(select c1 from test_cosine " +
                "where approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) >= 100 " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) limit 10) " +
                "UNION ALL " +
                "(select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 100 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) DESC limit 10) " +
                "UNION ALL " +
                "(select c1 from test_no_vector_index)";
        plan = getVerboseExplain(sql);
        System.out.println(plan);
        assertContains(plan, "  1:OlapScanNode\n" +
                "     table: test_cosine, rollup: test_cosine\n" +
                "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <24:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");
        assertContains(plan, "  7:OlapScanNode\n" +
                "     table: test_l2, rollup: test_l2\n" +
                "     VECTORINDEX: ON\n" +
                "          IVFPQ: OFF, Distance Column: <23:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");
        assertContains(plan, "  13:OlapScanNode\n" +
                "     table: test_cosine, rollup: test_cosine\n" +
                "     VECTORINDEX: OFF");
        assertContains(plan, "  25:OlapScanNode\n" +
                "     table: test_no_vector_index, rollup: test_no_vector_index\n" +
                "     VECTORINDEX: OFF");
    }

    @Test
    public void testQueryVectorDimNotMatch() throws Exception {
        String sql = "select c1 from test.test_cosine " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4], c1) desc limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining(
                        "The vector query size ([1.1, 2.2, 3.3, 4.4]) is not equal to the vector index dimension (5)");
    }

    @Test
    public void testIvfpq() throws Exception {
        String sql = "select c1, approx_l2_distance([1.1,2.2,3.3,4.4], c1) as score"
                + " from test_ivfpq order by score limit 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  2:TOP-N\n" +
                "  |  order by: [4, FLOAT, true] ASC\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (<slot 4> 4: approx_l2_distance), remote = false\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  2 <-> [2: c1, ARRAY<FLOAT>, false]\n" +
                "  |  4 <-> approx_l2_distance[([1.1,2.2,3.3,4.4], [2: c1, ARRAY<FLOAT>, false]); " +
                "args: INVALID_TYPE,INVALID_TYPE; result: FLOAT; args nullable: true; result nullable: true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: test_ivfpq, rollup: test_ivfpq\n" +
                "     VECTORINDEX: ON\n" +
                "          IVFPQ: ON, Distance Column: <0:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, Query Vector: [1.1, 2.2, 3.3, 4.4], Predicate Range: -1.0");
    }

    @Test
    public void testVectorIndexSyntax() throws Exception {
        String sql1 = "select c1 from test.test_cosine " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        assertPlanContains(sql1, "VECTORINDEX: ON");

        String sql2 = "select c1 from test.test_l2 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        assertPlanContains(sql2, "VECTORINDEX: ON");

        // Sorting in desc order doesn't make sense in l2_distance,
        // which won't trigger the vector retrieval logic.
        String sql3 = "select c1 from test.test_l2 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        assertPlanContains(sql3, "VECTORINDEX: OFF");

        String sql4 = "select c1 from test.test_cosine " +
                "order by cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        assertPlanContains(sql4, "VECTORINDEX: OFF");

        String sql5 = "select c1, approx_l2_distance([1.1,2.2,3.3,4.4], c1) as score"
                + " from test.test_ivfpq order by score limit 10";
        assertPlanContains(sql5, "VECTORINDEX: ON");

        String sql6 = "select c1, approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) as score"
                + " from test.test_cosine order by score desc limit 10";
        assertPlanContains(sql6, "VECTORINDEX: ON");

        String sql7 = "select c1, approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) as score"
                + " from test.test_cosine where c0 = 1 order by score desc limit 10";
        assertPlanContains(sql7, "VECTORINDEX: OFF");

        String sql8 = "select c1, approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) as score"
                + " from test.test_cosine having score >= cast(0.8 as float) order by score desc limit 10";
        assertPlanContains(sql8, "VECTORINDEX: ON");
    }

    // Regression guard for the vector distance-column schema pollution bug.
    //
    // RewriteToVectorPlanRule used to call scanOp.getTable().addColumn(distanceColumn) on the scan's
    // table. On the whole-phase-lock planning path that table is the shared catalog OlapTable, and
    // Table.addColumn appends to fullSchema (a List that does not dedup). So every vector ANN query
    // that planned on the live table appended another "__vector_*" column; once two accumulated, the
    // Column-keyed ImmutableMap in RelationTransformer.visitTable threw "Multiple entries with same
    // key" for any later statement touching the table (and the failure was intermittent because
    // analyze-phase column pruning sometimes dropped the synthetic columns).
    //
    // The fix keeps the synthetic column only in the scan operator's colRef maps and never mutates
    // the table schema. This test forces the lock path (cbo_use_lock_db) and plans the same vector
    // query repeatedly; the shared schema must stay clean and planning must keep succeeding.
    @Test
    public void testRewriteDoesNotPolluteSharedCatalogSchema() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.test_vi_no_pollution ("
                + " c0 INT,"
                + " c1 array<float> NOT NULL,"
                + " INDEX index_vector1 (c1) USING VECTOR ('metric_type' = 'cosine_similarity', "
                + "'is_vector_normed' = 'false', 'M' = '512', 'index_type' = 'hnsw', 'dim'='5') "
                + ") DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");

        OlapTable table = (OlapTable) starRocksAssert.getTable("test", "test_vi_no_pollution");
        String distanceColumn = "__vector_approx_cosine_similarity";
        // No WHERE clause so the rewrite reaches the distance-column code path (a scalar predicate
        // would disable the vector index and never get there).
        String vectorSql = "select c1 from test.test_vi_no_pollution "
                + "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";

        boolean originalLock = connectContext.getSessionVariable().isCboUseDBLock();
        // Force the whole-phase-lock path so the rewrite plans on the live shared table, not a copy.
        connectContext.getSessionVariable().setCboUseDBLock(true);
        try {
            assertEquals(0, countColumns(table, distanceColumn));
            for (int i = 0; i < 5; i++) {
                String plan = getVerboseExplain(vectorSql);
                assertContains(plan, "VECTORINDEX: ON");
                assertEquals(0, countColumns(table, distanceColumn),
                        "the rewrite must not add the distance column to the shared catalog schema");
            }
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(originalLock);
        }
    }

    private static long countColumns(OlapTable table, String columnName) {
        return table.getFullSchema().stream()
                .filter(c -> c.getName().equalsIgnoreCase(columnName))
                .count();
    }

}
