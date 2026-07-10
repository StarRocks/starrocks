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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Pins the multi-contextbase scope shape that {@link ReferenceExpander#buildHopSql} emits when a
 * search spans more than one contextbase. The single-base {@code = id} path is covered by
 * {@link ReferenceExpanderRefsViewTest}; here every contextbase predicate must turn into an
 * {@code IN (...)} list and the key fallback for unresolved forward edges must still fire.
 */
public class ReferenceExpanderMultiBaseTest {

    @Test
    public void multiBaseScopeUsesContextbaseInClause() {
        ReferenceExpander expander = new ReferenceExpander();
        List<Long> frontier = Arrays.asList(1L, 2L);
        String sql = expander.buildHopSql(frontier, ReferenceExpander.Direction.FORWARD,
                Collections.emptyList(), /*contextBaseId=*/null, /*contextBaseIds=*/Arrays.asList(7L, 9L),
                /*collectionId=*/null, /*collectionIds=*/null, /*snapshotFence=*/-1L);
        // Both src and dst scope predicates must be IN lists over the requested bases.
        Assertions.assertTrue(sql.contains("h.contextbase_id IN (7,9)"), sql);
        Assertions.assertTrue(sql.contains("hd.contextbase_id IN (7,9)"), sql);
        // The unresolved-edge key fallback fires for a multi-base scope too, scoped by IN.
        Assertions.assertTrue(sql.contains("CASE WHEN r0.dst_entity_id > 0"), sql);
        Assertions.assertTrue(sql.contains("kh.contextbase_id IN (7,9)"), sql);
        // No bare single-base equality leaked in.
        Assertions.assertFalse(sql.contains("contextbase_id = "), sql);
    }

    @Test
    public void singleContextBaseIdStillWinsOverList() {
        // contextBaseId (single) takes precedence; the list is ignored when the single id is set.
        ReferenceExpander expander = new ReferenceExpander();
        String sql = expander.buildHopSql(Arrays.asList(1L), ReferenceExpander.Direction.FORWARD,
                Collections.emptyList(), /*contextBaseId=*/7L, /*contextBaseIds=*/Arrays.asList(11L, 12L),
                /*collectionId=*/null, /*collectionIds=*/null, /*snapshotFence=*/-1L);
        Assertions.assertTrue(sql.contains("h.contextbase_id = 7"), sql);
        Assertions.assertFalse(sql.contains("IN (11,12)"), sql);
    }
}
