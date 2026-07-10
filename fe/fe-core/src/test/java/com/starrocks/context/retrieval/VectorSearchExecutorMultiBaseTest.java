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

import com.google.gson.JsonArray;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Asserts that {@link VectorSearchExecutor#search} pushes a multi-contextbase scope onto the inner
 * ANN scan as a {@code f.contextbase_id IN (...)} residual predicate (filtered-ANN). The
 * single-base {@code = id} path is covered by {@link VectorSearchExecutorFoldTest}.
 */
public class VectorSearchExecutorMultiBaseTest {

    @Test
    public void multiBaseScopeUsesContextbaseInClauseOnAnnScan() {
        StubExecutor exec = new StubExecutor();
        VectorSearchExecutor.Request req = new VectorSearchExecutor.Request();
        req.queryEmbedding = new float[] {0.1f, 0.2f, 0.3f};
        req.contextBaseId = null;
        req.contextBaseIds = Arrays.asList(7L, 9L);
        req.maxFragmentScan = 100;
        req.maxResults = 10;
        exec.search(req);
        String sql = exec.calls.get(0);
        Assertions.assertTrue(sql.contains("f.contextbase_id IN (7,9)"), sql);
        Assertions.assertTrue(sql.contains("ORDER BY score DESC LIMIT"), sql);
        Assertions.assertFalse(sql.contains("f.contextbase_id = "), sql);
    }

    private static final class StubExecutor extends VectorSearchExecutor {
        final List<String> calls = new ArrayList<>();

        @Override
        protected JsonArray runQuery(String sql) {
            calls.add(sql);
            return new JsonArray();
        }
    }
}
