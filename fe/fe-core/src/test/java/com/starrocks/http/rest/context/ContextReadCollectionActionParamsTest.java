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

package com.starrocks.http.rest.context;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Cursor-emission rules for the {@code /api/context/read-collection} response. The cursor protocol
 * is the contract clients pin loops against, so the "end-of-scan" sentinel is encoded explicitly
 * (null when the page is short) rather than inferred client-side from row counts.
 */
public class ContextReadCollectionActionParamsTest {

    @Test
    public void testFullPageReturnsLastEntityId() {
        List<Map<String, Object>> rows = rowsWithIds(101L, 102L, 103L);
        assertEquals(Long.valueOf(103L),
                ContextReadCollectionAction.computeNextAfterEntityId(rows, 3));
    }

    @Test
    public void testShortPageReturnsNullSignalingEndOfScan() {
        // Server returned fewer rows than requested — no point asking for the next page.
        List<Map<String, Object>> rows = rowsWithIds(101L, 102L);
        assertNull(ContextReadCollectionAction.computeNextAfterEntityId(rows, 3));
    }

    @Test
    public void testEmptyPageReturnsNull() {
        assertNull(ContextReadCollectionAction.computeNextAfterEntityId(
                Collections.emptyList(), 100));
    }

    @Test
    public void testMissingIdYieldsNullRatherThanThrowing() {
        // Defensive — if a row lacks the id column for any reason, we surface "no cursor"
        // instead of crashing the response encoder mid-write.
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("entity_key", "k");
        assertNull(ContextReadCollectionAction.computeNextAfterEntityId(
                Collections.singletonList(row), 1));
    }

    private static List<Map<String, Object>> rowsWithIds(long... ids) {
        List<Map<String, Object>> out = new ArrayList<>(ids.length);
        for (long id : ids) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", id);
            out.add(row);
        }
        return out;
    }
}
