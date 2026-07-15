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

package com.starrocks.sql.plan;

import com.starrocks.common.Config;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Covers the BM25 score() top-k pushdown in {@code RewriteToBm25ScorePlanRule}:
 * the LIMIT reaches the scored GIN scan only when MATCH is the whole filter, so a
 * post-scan predicate can't shrink the result below the limit.
 */
public class Bm25ScoreLimitPushdownTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.enable_experimental_gin = true;
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE test.bm25_docs ("
                + " id INT,"
                + " request VARCHAR(1024),"
                + " status INT,"
                + " INDEX idx_request (request) USING GIN ('imp_lib' = 'tantivy', 'parser' = 'english')"
                + ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');");
    }

    @Test
    public void pushesLimitWhenMatchIsSoleFilter() throws Exception {
        String plan = getFragmentPlan("SELECT score() s FROM bm25_docs "
                + "WHERE request MATCH 'fox' ORDER BY s DESC LIMIT 10");
        assertContains(plan, "BM25SCORE: ON, topk=10");
    }

    @Test
    public void skipsLimitWithPostScanPredicate() throws Exception {
        String plan = getFragmentPlan("SELECT score() s FROM bm25_docs "
                + "WHERE request MATCH 'fox' AND status = 200 ORDER BY s DESC LIMIT 10");
        assertContains(plan, "BM25SCORE: ON, topk=0");
    }

    @Test
    public void skipsLimitForAscendingOrder() throws Exception {
        String plan = getFragmentPlan("SELECT score() s FROM bm25_docs "
                + "WHERE request MATCH 'fox' ORDER BY s ASC LIMIT 10");
        assertContains(plan, "BM25SCORE: ON, topk=0");
    }
}
