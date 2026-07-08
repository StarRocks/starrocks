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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the vector-search SQL shape, in particular that an as-of read fences the fragments on the
 * inner ANN scan (before ORDER BY ... LIMIT) rather than only in the outer versions subquery.
 * Without the inner fence, future-version fragments can fill the TopN scan window and then be
 * dropped by the outer as-of JOIN, leaving the visible older versions missing or the page empty.
 */
public class VectorSearchExecutorTest {

    private static VectorSearchExecutor.Request request(long snapshotFence) {
        VectorSearchExecutor.Request r = new VectorSearchExecutor.Request();
        r.snapshotFence = snapshotFence;
        r.contextBaseId = 42L;
        return r;
    }

    @Test
    public void testAsOfFencesFragmentsBeforeTopN() {
        String sql = VectorSearchExecutor.buildSearchSql("[0.1,0.2]", request(7777L));
        // The fence must sit on the inner scan, ahead of the TopN, and reference the fragment column.
        int fencePos = sql.indexOf("f.snapshot_version <= 7777");
        int orderPos = sql.indexOf("ORDER BY score DESC LIMIT");
        assertTrue(fencePos >= 0, "inner scan must fence f.snapshot_version: " + sql);
        assertTrue(orderPos >= 0 && fencePos < orderPos,
                "fence must precede the inner ORDER BY ... LIMIT: " + sql);
        // The outer as-of version resolution stays as well.
        assertTrue(sql.contains("WHERE snapshot_version <= 7777"), "outer as-of version fence: " + sql);
    }

    @Test
    public void testCurrentReadHasNoFragmentFence() {
        String sql = VectorSearchExecutor.buildSearchSql("[0.1,0.2]", request(-1L));
        assertFalse(sql.contains("snapshot_version <="), "current read must not fence any snapshot: " + sql);
        assertTrue(sql.contains("h.current_deleted = false"), "current read joins heads: " + sql);
    }
}
