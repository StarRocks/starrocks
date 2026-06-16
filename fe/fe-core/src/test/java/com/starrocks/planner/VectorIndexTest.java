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

        // HNSW with a non-flat quantizer (sq8) -- a quantized index, so its index distance is lossy
        // and the refine path applies just like IVFPQ.
        starRocksAssert.withTable("CREATE TABLE test.test_hnsw_sq8 ("
                + " c0 INT,"
                + " c1 array<float> NOT NULL,"
                + " c2 array<float>,"
                + " INDEX index_vector1 (c1) USING VECTOR ('metric_type' = 'l2_distance', "
                + "'is_vector_normed' = 'false', 'M' = '16', 'efconstruction' = '40', "
                + "'index_type' = 'hnsw', 'quantizer' = 'sq8', 'dim'='5') "
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
        // This test asserts the rewrite-stage plan structure (RewriteToVectorPlanRule).
        // Global lazy-materialize defers/prunes the embedding column for HNSW queries
        // (see testLazyMaterializationFor* below) which changes the shape of the project
        // above the scan. Disable it here so the rewrite assertions stay focused.
        boolean originalLazyMat = connectContext.getSessionVariable().isEnableGlobalLateMaterialization();
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(false);
        try {
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
                "          Refine: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: -1.0\n" +
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
                "          Refine: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: -1.0");

        // Constant vector with cast.
        sql = "select c1 from test_cosine " +
                "order by approx_cosine_similarity([cast(1.1 as double),cast(2.1 as double)," +
                "cast(3.1 as double),cast(4.1 as double),cast(5.1 as double)], c1) desc " +
                "limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.1, 3.1, 4.1, 5.1], Predicate Range: -1.0");

        sql = "select c1 from test_cosine " +
                "order by approx_cosine_similarity([cast(1.1 as float),cast(2.1 as float),cast(3.1 as float)" +
                ",cast(4.1 as float),cast(5.1 as float)], c1) desc " +
                "limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.1, 3.1, 4.1, 5.1], Predicate Range: -1.0");

        sql = "select c1 from test_cosine " +
                "order by approx_cosine_similarity([cast(1.1 as int),cast(2.1 as int),cast(3.1 as int)" +
                ",cast(4.1 as int),cast(5.1 as int)], c1) desc " +
                "limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.1, 3.1, 4.1, 5.1], Predicate Range: -1.0");
        } finally {
            connectContext.getSessionVariable().setEnableGlobalLateMaterialization(originalLazyMat);
        }
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
                "          Refine: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 100 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        sql = "select c1 from test_cosine " +
                "where approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) >= 100 " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 100 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        // Cast
        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= cast(100 as double) " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= cast(100 as int) " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= cast(100 as float) " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        // AND
        sql = "select c1 from test_cosine " +
                "where approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) >= 1000 " +
                "and approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) >= 100 " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 1000.0");

        sql = "select c1 from test_l2 " +
                "where approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 100 and approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) <= 1000 " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
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
        // This test asserts the rewrite-stage plan structure. Disable global lazy-mat
        // so the assertions stay focused (lazy-mat behavior is covered by
        // testLazyMaterializationFor* below).
        boolean originalLazyMat = connectContext.getSessionVariable().isEnableGlobalLateMaterialization();
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(false);
        try {
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
        assertContains(plan, "  4:Project\n" +
                "  |  output columns:\n" +
                "  |  2 <-> [2: c1, ARRAY<FLOAT>, false]\n" +
                "  |  5 <-> [12: cast, DOUBLE, true] + 1.0\n" +
                "  |  6 <-> [12: cast, DOUBLE, true] + 2.0\n" +
                "  |  7 <-> cast([11: approx_cosine_similarity, FLOAT, true] as VARCHAR(65533))\n" +
                "  |  8 <-> cast(approx_cosine_similarity[([1.1,2.2,3.3,4.4,5.5], [3: c2, ARRAY<FLOAT>, true]); " +
                "args: INVALID_TYPE,INVALID_TYPE; result: FLOAT; args nullable: true; result nullable: true] as DOUBLE) + 2.0\n" +
                "  |  common expressions:\n" +
                "  |  11 <-> approx_cosine_similarity[([1.1,2.2,3.3,4.4,5.5], [2: c1, ARRAY<FLOAT>, false]); " +
                "args: INVALID_TYPE,INVALID_TYPE; result: FLOAT; args nullable: true; result nullable: true]\n" +
                "  |  12 <-> cast([11: approx_cosine_similarity, FLOAT, true] as DOUBLE)\n" +
                "  |  limit: 10\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  3:MERGING-EXCHANGE");
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
                "  |  3 <-> [3: c2, ARRAY<FLOAT>, true]\n" +
                "  |  9 <-> [10: __vector_approx_cosine_similarity, FLOAT, false]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: test_cosine, rollup: test_cosine\n" +
                "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <10:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: -1.0\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=4.0\n" +
                "     Pruned type: 2 <-> [ARRAY<FLOAT>]\n" +
                "     Pruned type: 3 <-> [ARRAY<FLOAT>]\n" +
                "     cardinality: 1\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (10: __vector_approx_cosine_similarity)");
        } finally {
            connectContext.getSessionVariable().setEnableGlobalLateMaterialization(originalLazyMat);
        }
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
                "          Refine: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: -1.0");

        sql = "select c1 from test_l2 " +
                "order by approx_l2_distance(c1, [1.1,2.2,3.3,4.4,5.5]) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: -1.0");

        // Predicate argument order doesn't matter.
        sql = "select c1 from test_cosine " +
                "where 100 <= approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) " +
                "order by approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) desc limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");

        sql = "select c1 from test_l2 " +
                "where 100 >= approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) " +
                "order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
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
                "          Refine: OFF, Distance Column: <24:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, " +
                "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5], Predicate Range: 100.0");
        assertContains(plan, "  7:OlapScanNode\n" +
                "     table: test_l2, rollup: test_l2\n" +
                "     VECTORINDEX: ON\n" +
                "          Refine: OFF, Distance Column: <23:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, " +
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
        // Default (enable_vector_index_refine = false): a quantized index trusts the lossy index
        // distance, so the rule rewrites the order-by to the BE-produced distance slot (trust plan),
        // exactly like a non-quantized HNSW.
        String sql = "select c1, approx_l2_distance([1.1,2.2,3.3,4.4], c1) as score"
                + " from test_ivfpq order by score limit 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: ON");
        assertContains(plan, "Refine: OFF");
    }

    @Test
    public void testIvfpqRefineOn() throws Exception {
        // enable_vector_index_refine = true: a quantized index refines -- the rule keeps the
        // approx_*_distance function so the TopN recomputes the exact distance and re-ranks; the
        // distance is not lifted into a BE-produced slot (Distance Column slot stays 0).
        connectContext.getSessionVariable().setEnableVectorIndexRefine(true);
        try {
            String sql = "select c1, approx_l2_distance([1.1,2.2,3.3,4.4], c1) as score"
                    + " from test_ivfpq order by score limit 10";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     table: test_ivfpq, rollup: test_ivfpq\n" +
                    "     VECTORINDEX: ON\n" +
                    "          Refine: ON, Distance Column: <0:__vector_approx_l2_distance>, LimitK: 10, " +
                    "Order: ASC, Query Vector: [1.1, 2.2, 3.3, 4.4], Predicate Range: -1.0");
        } finally {
            connectContext.getSessionVariable().setEnableVectorIndexRefine(false);
        }
    }

    @Test
    public void testIvfpqRefineOnKeepsRangePredicateForRecheck() throws Exception {
        // enable_vector_index_refine = true + a distance range bound: the bound is folded into the ANN
        // (Predicate Range) for the lossy range_search prefilter AND kept as a scan predicate, so it is
        // re-applied on the recomputed exact distance above the scan -- a precision recheck that drops
        // false positives the lossy prefilter (and segments whose .vi is still missing) would leak.
        // The trust path (refine off) instead folds the bound entirely into Predicate Range and drops it.
        connectContext.getSessionVariable().setEnableVectorIndexRefine(true);
        try {
            String sql = "select c1, approx_l2_distance([1.1,2.2,3.3,4.4], c1) as score"
                    + " from test_ivfpq where approx_l2_distance([1.1,2.2,3.3,4.4], c1) <= 100"
                    + " order by score limit 10";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "VECTORINDEX: ON");
            assertContains(plan, "Refine: ON");
            assertContains(plan, "Predicate Range: 100.0");
            // The distance bound survives as a recheck predicate on the recomputed exact distance.
            assertContains(plan, "approx_l2_distance");
            assertContains(plan, "<= 100");
        } finally {
            connectContext.getSessionVariable().setEnableVectorIndexRefine(false);
        }
    }

    @Test
    public void testHnswQuantizerRefineOffTrustsLossyDistance() throws Exception {
        // HNSW + sq8 is a quantized index, but with refine off (default) it trusts the lossy index
        // distance like any trust-path query: the order-by is lifted to the BE-produced distance slot.
        String sql = "select c1, approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) as score"
                + " from test_hnsw_sq8 order by score limit 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: ON");
        assertContains(plan, "Refine: OFF");
    }

    @Test
    public void testHnswQuantizerRefineOn() throws Exception {
        // HNSW + sq8 with refine on takes the same refine path as IVFPQ: keep the function, recompute
        // the exact distance above the scan (the refine path is keyed on quantization, not index family).
        connectContext.getSessionVariable().setEnableVectorIndexRefine(true);
        try {
            String sql = "select c1, approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) as score"
                    + " from test_hnsw_sq8 order by score limit 10";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "VECTORINDEX: ON");
            assertContains(plan, "Refine: ON");
            assertContains(plan, "Distance Column: <0:__vector_approx_l2_distance>");
        } finally {
            connectContext.getSessionVariable().setEnableVectorIndexRefine(false);
        }
    }

    @Test
    public void testHnswFlatRefineSessionOnStaysOff() throws Exception {
        // A non-quantized HNSW (flat) returns exact distances, so enable_vector_index_refine has no
        // effect: it stays on the trust plan even with the session variable on.
        connectContext.getSessionVariable().setEnableVectorIndexRefine(true);
        try {
            String sql = "select c1 from test.test_l2"
                    + " order by approx_l2_distance([1.1,2.2,3.3,4.4,5.5], c1) limit 10";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "VECTORINDEX: ON");
            assertContains(plan, "Refine: OFF");
        } finally {
            connectContext.getSessionVariable().setEnableVectorIndexRefine(false);
        }
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

    // Prepared statements that send the query vector as a string parameter end
    // up with `CAST(StringLiteral AS ARRAY<FLOAT>)` in the AST after analysis.
    // The rewrite rule must recognize this form, parse the literal, and dispatch
    // through the same VECTORINDEX path as a native array literal.
    @Test
    public void testPreparedStatementCastStringArrayHnsw() throws Exception {
        String sql = "select c1 from test.test_cosine "
                + "order by approx_cosine_similarity(CAST('[1.1,2.2,3.3,4.4,5.5]' AS ARRAY<FLOAT>), c1) desc "
                + "limit 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: ON");
        assertContains(plan,
                "Distance Column: <6:__vector_approx_cosine_similarity>, LimitK: 10, Order: DESC, "
                        + "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5]");
    }

    @Test
    public void testPreparedStatementCastStringArrayWithWhitespace() throws Exception {
        // Tokens with surrounding whitespace are trimmed; the resulting query vector
        // should be byte-identical to the no-whitespace form.
        String sql = "select c1 from test.test_l2 "
                + "order by approx_l2_distance(CAST('[ 1.1 , 2.2 , 3.3 , 4.4 , 5.5 ]' AS ARRAY<FLOAT>), c1) "
                + "limit 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "VECTORINDEX: ON");
        assertContains(plan,
                "Distance Column: <5:__vector_approx_l2_distance>, LimitK: 10, Order: ASC, "
                        + "Query Vector: [1.1, 2.2, 3.3, 4.4, 5.5]");
    }

    @Test
    public void testPreparedStatementCastStringArrayIvfpq() throws Exception {
        // With refine on, a quantized index (IVFPQ) keeps the function plan: the rewrite rule
        // recognizes the cast, builds the query vector, but does not swap the order-by expression
        // (the exact distance is recomputed in the TopN). VECTORINDEX must be ON with Refine: ON.
        connectContext.getSessionVariable().setEnableVectorIndexRefine(true);
        try {
            String sql = "select c1, approx_l2_distance(CAST('[1.1,2.2,3.3,4.4]' AS ARRAY<FLOAT>), c1) as score "
                    + "from test.test_ivfpq order by score limit 10";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "VECTORINDEX: ON");
            assertContains(plan, "Refine: ON");
        } finally {
            connectContext.getSessionVariable().setEnableVectorIndexRefine(false);
        }
    }

    @Test
    public void testPreparedStatementCastStringArrayDimMismatch() throws Exception {
        // String literal has 4 floats but the index is dim=5 — the existing dim check
        // must fire just as it does for native array literals.
        String sql = "select c1 from test.test_cosine "
                + "order by approx_cosine_similarity(CAST('[1.1,2.2,3.3,4.4]' AS ARRAY<FLOAT>), c1) desc "
                + "limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("not equal to the vector index dimension");
    }

    @Test
    public void testPreparedStatementCastStringArrayMissingBrackets() throws Exception {
        // Malformed string literal — no enclosing `[..]` brackets.
        String sql = "select c1 from test.test_cosine "
                + "order by approx_cosine_similarity(CAST('1.1,2.2,3.3,4.4,5.5' AS ARRAY<FLOAT>), c1) desc "
                + "limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("must be enclosed in [..]");
    }

    @Test
    public void testPreparedStatementCastStringArrayInvalidFloat() throws Exception {
        // Non-numeric token inside the array.
        String sql = "select c1 from test.test_cosine "
                + "order by approx_cosine_similarity(CAST('[1.1,abc,3.3,4.4,5.5]' AS ARRAY<FLOAT>), c1) desc "
                + "limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("Invalid float in vector array literal");
    }

    @Test
    public void testPreparedStatementCastStringArrayEmptyElement() throws Exception {
        // Two adjacent commas yield an empty interior token.
        String sql = "select c1 from test.test_cosine "
                + "order by approx_cosine_similarity(CAST('[1.1,2.2,,4.4,5.5]' AS ARRAY<FLOAT>), c1) desc "
                + "limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("Empty element in vector array literal");
    }

    @Test
    public void testPreparedStatementCastStringArrayTrailingComma() throws Exception {
        // A comma at the very end of the array must be rejected; otherwise the literal
        // would be silently truncated to N-1 elements (and could accidentally match dim).
        String sql = "select c1 from test.test_cosine "
                + "order by approx_cosine_similarity(CAST('[1.1,2.2,3.3,4.4,5.5,]' AS ARRAY<FLOAT>), c1) desc "
                + "limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("Trailing comma in vector array literal");
    }

    @Test
    public void testPreparedStatementCastStringArrayNaNRejected() throws Exception {
        // BE cast_expr rejects NaN when casting string -> float; the rewrite path must
        // do the same so rule-fires vs rule-misses produce identical semantics.
        String sql = "select c1 from test.test_cosine "
                + "order by approx_cosine_similarity(CAST('[1.1,NaN,3.3,4.4,5.5]' AS ARRAY<FLOAT>), c1) desc "
                + "limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("Non-finite float in vector array literal");
    }

    @Test
    public void testPreparedStatementCastStringArrayInfinityRejected() throws Exception {
        String sql = "select c1 from test.test_cosine "
                + "order by approx_cosine_similarity(CAST('[1.1,Infinity,3.3,4.4,5.5]' AS ARRAY<FLOAT>), c1) desc "
                + "limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("Non-finite float in vector array literal");
    }

    @Test
    public void testPreparedStatementCastStringArrayOverflowRejected() throws Exception {
        // Double.parseDouble("1e5000") returns Double.POSITIVE_INFINITY, which is the same
        // overflow path BE rejects. Cover it explicitly so future parseDouble swaps don't
        // silently re-open the hole.
        String sql = "select c1 from test.test_cosine "
                + "order by approx_cosine_similarity(CAST('[1.1,1e5000,3.3,4.4,5.5]' AS ARRAY<FLOAT>), c1) desc "
                + "limit 10";
        assertThatThrownBy(() -> getVerboseExplain(sql))
                .isInstanceOf(SemanticException.class)
                .hasMessageContaining("Non-finite float in vector array literal");
    }

    // Late-materialization behavior for vector queries:
    //   * Trust path (refine off): BE produces the distance via id2distance_map, the rewrite swaps
    //     the order-by to reference that virtual distance column, so the embedding column can be
    //     deferred (FetchNode after final TopN) or pruned entirely.
    //   * Refine path (refine on): the rule keeps the order-by function -- TopN evaluates
    //     approx_*_distance(v, [...]) row by row, so v must remain eager at the scan output.
    @Test
    public void testLazyMaterializationForHnswSelectDistanceOnly() throws Exception {
        // Quadrant 1: HNSW + SELECT does not reference embedding c1.
        // Expected: c1 is pruned entirely from the BE scan output — neither the scan-side
        // projection nor the FETCH operator references it. The BE only fills the virtual
        // distance slot via id2distance_map and ships row_id columns up; the FETCH at the
        // coordinator fetches only the small c0 column for the K survivors.
        boolean originalLazyMat = connectContext.getSessionVariable().isEnableGlobalLateMaterialization();
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(true);
        try {
            String sql = "select c0, approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) as score "
                    + "from test.test_cosine order by score desc limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "VECTORINDEX: ON");
            assertContains(plan, "Refine: OFF");
            // The FETCH operator's lookup descriptor for table test_cosine should reference
            // c0 but not c1.
            assertContains(plan, "<slot 1> => c0");
            assertNotContains(plan, "=> c1");
        } finally {
            connectContext.getSessionVariable().setEnableGlobalLateMaterialization(originalLazyMat);
        }
    }

    @Test
    public void testLazyMaterializationForHnswSelectEmbedding() throws Exception {
        // Quadrant 2: HNSW + SELECT v explicitly. The embedding c1 is in the projection so
        // global lazy-mat defers it to the FETCH operator above the final TopN, which reads
        // only the K survivors' rows of c1 via row-id lookup.
        boolean originalLazyMat = connectContext.getSessionVariable().isEnableGlobalLateMaterialization();
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(true);
        try {
            String sql = "select c1, approx_cosine_similarity([1.1,2.2,3.3,4.4,5.5], c1) as score "
                    + "from test.test_cosine order by score desc limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "VECTORINDEX: ON");
            assertContains(plan, "Refine: OFF");
            // c1 must appear as a FETCH lookup target — not in the scan-side projection.
            assertContains(plan, "FETCH");
            assertContains(plan, "=> c1");
        } finally {
            connectContext.getSessionVariable().setEnableGlobalLateMaterialization(originalLazyMat);
        }
    }

    @Test
    public void testLazyMaterializationForRefineKeepsEmbeddingEager() throws Exception {
        // Refine path (enable_vector_index_refine = true on a quantized index): the rule keeps the
        // order-by function, the TopN evaluates approx_*_distance(v, [...]) row by row. The embedding
        // c1 must remain eager at scan output. No FETCH should appear in the plan (everything eager).
        boolean originalLazyMat = connectContext.getSessionVariable().isEnableGlobalLateMaterialization();
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(true);
        connectContext.getSessionVariable().setEnableVectorIndexRefine(true);
        try {
            String sql = "select c0, approx_l2_distance([1.1,2.2,3.3,4.4], c1) as score "
                    + "from test.test_ivfpq order by score limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "VECTORINDEX: ON");
            assertContains(plan, "Refine: ON");
            assertNotContains(plan, "FETCH");
        } finally {
            connectContext.getSessionVariable().setEnableGlobalLateMaterialization(originalLazyMat);
            connectContext.getSessionVariable().setEnableVectorIndexRefine(false);
        }
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
