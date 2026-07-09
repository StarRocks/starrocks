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

package com.starrocks.context.retrieval;

import com.starrocks.context.ContextMgr;
import com.starrocks.persist.ContextOpLog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Reproduces the cross-encoder rerank pool-widening bug.
 *
 * <p>When {@code rerank=true}, the intent (per commit message and docs) is that "the first-stage
 * fusion pool is widened to rerank_top_n so the reranker has candidates to reorder". But the
 * first-stage text/vector retrieval depth is computed from the ORIGINAL {@code maxResults}
 * ({@code maxResults * 3}, see {@code ContextSearchExecutor} lines ~328 / ~366) and those requests
 * are issued BEFORE {@code request.maxResults} is widened to {@code rerankTopN} (line ~532).
 *
 * <p>Consequence: the candidate pool handed to the reranker can never exceed {@code maxResults * 3},
 * and any {@code rerank_top_n} larger than that is silently ignored. The real
 * {@link TextSearchExecutor} translates {@code request.maxResults} into a SQL {@code LIMIT}, so the
 * depth the executor asks the text path for is exactly the pool ceiling. This test captures that
 * depth.
 */
public class ContextSearchExecutorRerankPoolTest {

    private static ContextMgr newMgrWithBase(String baseName) {
        ContextMgr mgr = new ContextMgr();
        mgr.replayCreateContextBase(ContextOpLog.forContextBase(1L, baseName, null));
        return mgr;
    }

    private static ContextSearchExecutor.Request baseRequest() {
        ContextSearchExecutor.Request req = new ContextSearchExecutor.Request();
        req.contextBase = "cb1";
        req.queryText = "q";
        req.textWeight = 1.0;
        req.vectorWeight = 0.0;            // isolate the text path
        req.graphMode = ContextSearchExecutor.GraphMode.OFF;
        req.maxResults = 5;
        return req;
    }

    @Test
    public void testRerankPoolIsCappedAtMaxResultsTimesThree() {
        CapturingTextSearch textSearch = new CapturingTextSearch();
        ContextSearchExecutor exec = new ContextSearchExecutor(
                newMgrWithBase("cb1"), textSearch, new StubReferenceExpander(), new StubVectorSearch());

        // ---- 1. Sanity: with rerank OFF, the text path over-fetches maxResults * 3 = 15. ----
        exec.search(baseRequest());
        Assertions.assertEquals(15, textSearch.lastMaxResults,
                "baseline first-stage text retrieval should fetch maxResults * 3");

        // ---- 2. rerank ON with rerank_top_n = 30 (>> maxResults * 3 = 15). ----
        // The reranker is meant to reorder a pool of up to rerank_top_n candidates, so the
        // first-stage retrieval must fetch at least rerank_top_n. The widening at line ~532
        // happens AFTER this request is issued, so the depth never grows.
        ContextSearchExecutor.Request rr = baseRequest();
        rr.rerank = true;
        rr.rerankTopN = 30;
        exec.search(rr);

        // EXPECTED (correct behavior): retrieval depth widened so the pool can reach rerank_top_n.
        // ACTUAL (bug): still 15, i.e. maxResults * 3 — rerank_top_n=30 is silently capped.
        Assertions.assertTrue(textSearch.lastMaxResults >= 30,
                "rerank pool is silently capped at maxResults*3 (got retrieval depth "
                        + textSearch.lastMaxResults + "); it never widens to rerank_top_n=30, "
                        + "so the reranker can only ever see " + textSearch.lastMaxResults + " candidates");
    }

    /** Captures the {@code maxResults} (i.e. the SQL LIMIT) the executor asks the text path for. */
    private static final class CapturingTextSearch extends TextSearchExecutor {
        int lastMaxResults = -1;

        @Override
        public List<EntityHit> search(Request request) {
            lastMaxResults = request.maxResults;
            return Collections.emptyList();
        }
    }

    private static final class StubVectorSearch extends VectorSearchExecutor {
        @Override
        public List<EntityHit> search(Request request) {
            return Collections.emptyList();
        }
    }

    private static final class StubReferenceExpander extends ReferenceExpander {
        @Override
        public Result expand(Request request) {
            return new Result(new ArrayList<>(Arrays.asList()), false, 0);
        }
    }
}
