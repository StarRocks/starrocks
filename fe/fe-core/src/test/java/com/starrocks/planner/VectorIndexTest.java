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

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.PlanTestBase;

import org.junit.BeforeClass;
import org.junit.Test;

public class VectorIndexTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        Config.enable_experimental_vector = true;
        FeConstants.enablePruneEmptyOutputScan = false;
        starRocksAssert.withTable("CREATE TABLE test.test_cosine ("
                + " c0 INT,"
                + " c1 array<float>,"
                + " INDEX index_vector1 (c1) USING VECTOR ('metric_type' = 'cosine_similarity', "
                + "'is_vector_normed' = 'false', 'M' = '512', 'index_type' = 'hnsw', 'dim'='5') "
                + ") "
                + "DUPLICATE KEY(c0) "
                + "DISTRIBUTED BY HASH(c0) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");

        starRocksAssert.withTable("CREATE TABLE test.test_l2 ("
                + " c0 INT,"
                + " c1 array<float>,"
                + " INDEX index_vector1 (c1) USING VECTOR ('metric_type' = 'l2_distance', "
                + "'is_vector_normed' = 'false', 'M' = '512', 'index_type' = 'hnsw', 'dim'='5') "
                + ") "
                + "DUPLICATE KEY(c0) "
                + "DISTRIBUTED BY HASH(c0) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");

        starRocksAssert.withTable("CREATE TABLE test.test_ivfpq ("
                + " c0 INT,"
                + " c1 array<float>,"
                + " INDEX index_vector1 (c1) USING VECTOR ('metric_type' = 'l2_distance', "
                + "'is_vector_normed' = 'false', 'nbits' = '1', 'index_type' = 'ivfpq', 'dim'='5') "
                + ") "
                + "DUPLICATE KEY(c0) "
                + "DISTRIBUTED BY HASH(c0) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1');");
    }

    @Test
    public void testVectorIndexSyntax() throws Exception {
        String sql1 = "select c1 from test.test_cosine " +
                "order by approx_cosine_similarity([1.1,2.2,3.3], c1) desc limit 10";
        assertPlanContains(sql1, "VECTORINDEX: ON");

        String sql2 = "select c1 from test.test_l2 " +
                "order by approx_l2_distance([1.1,2.2,3.3], c1) limit 10";
        assertPlanContains(sql2, "VECTORINDEX: ON");

        // Sorting in desc order doesn't make sense in l2_distance,
        // which won't trigger the vector retrieval logic.
        String sql3 = "select c1 from test.test_l2 " +
                "order by approx_l2_distance([1.1,2.2,3.3], c1) desc limit 10";
        assertPlanContains(sql3, "VECTORINDEX: OFF");

        String sql4 = "select c1 from test.test_cosine " +
                "order by cosine_similarity([1.1,2.2,3.3], c1) desc limit 10";
        assertPlanContains(sql4, "VECTORINDEX: OFF");

        String sql5 = "select c1, approx_l2_distance([1.1,2.2,3.3], c1) as score"
                + " from test.test_ivfpq order by score limit 10";
        assertPlanContains(sql5, "VECTORINDEX: ON");

        String sql6 = "select c1, approx_cosine_similarity([1.1,2.2,3.3], c1) as score"
                + " from test.test_cosine order by score desc limit 10";
        assertPlanContains(sql6, "VECTORINDEX: ON");

        String sql7 = "select c1, approx_cosine_similarity([1.1,2.2,3.3], c1) as score"
                + " from test.test_cosine where c0 = 1 order by score desc limit 10";
        assertPlanContains(sql7, "VECTORINDEX: OFF");

        String sql8 = "select c1, approx_cosine_similarity([1.1,2.2,3.3], c1) as score"
                + " from test.test_cosine having score > 0.8 order by score desc limit 10";
        assertPlanContains(sql8, "VECTORINDEX: ON");

        String sql9 = "select c1, approx_cosine_similarity([1.1,2.2,3.3], c1) as score"
                + " from test.test_cosine having score < 0.8 order by score desc limit 10";
        assertPlanContains(sql9, "VECTORINDEX: OFF");
    }
}
