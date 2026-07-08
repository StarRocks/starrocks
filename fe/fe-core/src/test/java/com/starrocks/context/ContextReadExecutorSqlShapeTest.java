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

package com.starrocks.context;

import com.starrocks.common.Config;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks in the SQL that {@link ContextReadExecutor#readCollection} and
 * {@link ContextReadExecutor#readContextBase} produce across the four pagination matrices:
 *
 * <pre>
 *               | offset=0, no cursor | offset>0, no cursor | cursor set         |
 *   current head | LIMIT only         | LIMIT + OFFSET     | LIMIT + cursor pred |
 *   as-of fence  | LIMIT only         | LIMIT + OFFSET     | LIMIT + cursor pred |
 * </pre>
 *
 * Invariants this test exists to enforce (the original bug had two failures here):
 * <ol>
 *   <li>Every variant orders by {@code entity_id ASC}. The previous SQL ordered by
 *       {@code current_snapshot_version DESC} (non-unique → MPP tiebreak is non-deterministic →
 *       OFFSET silently duplicates / skips rows on retries) or had no ORDER BY at all (as-of
 *       path). Replacing it with a PK-unique total order is what makes both OFFSET and cursor
 *       pagination correct in a distributed plan.</li>
 *   <li>Cursor supersedes offset — emitting both would compound the skip semantics. The test
 *       confirms {@code OFFSET} is absent whenever {@code afterEntityId >= 0}.</li>
 * </ol>
 */
public class ContextReadExecutorSqlShapeTest {

    // ---------- readCollection: current-head path (snapshotFence < 0) ----------

    @Test
    public void testReadCollectionCurrentHeadNoOffset() {
        String sql = ContextReadExecutor.buildReadCollectionSql(123L, -1L, 500, 0, -1L);
        assertContainsOrdered(sql,
                "WHERE h.collection_id = 123 ORDER BY h.entity_id ASC LIMIT 500");
        assertFalse(sql.contains("OFFSET"), "no OFFSET when offset=0: " + sql);
        assertFalse(sql.contains("h.entity_id >"), "no cursor predicate when afterEntityId<0: " + sql);
        assertFalse(sql.contains("current_snapshot_version DESC"),
                "must not use the old non-deterministic ORDER BY: " + sql);
    }

    @Test
    public void testReadCollectionCurrentHeadWithOffset() {
        String sql = ContextReadExecutor.buildReadCollectionSql(123L, -1L, 500, 1000, -1L);
        assertContainsOrdered(sql,
                "WHERE h.collection_id = 123 ORDER BY h.entity_id ASC LIMIT 500 OFFSET 1000");
    }

    @Test
    public void testReadCollectionCurrentHeadWithCursor() {
        String sql = ContextReadExecutor.buildReadCollectionSql(123L, -1L, 500, 0, 9999L);
        assertContainsOrdered(sql,
                "WHERE h.collection_id = 123 AND h.entity_id > 9999 ORDER BY h.entity_id ASC LIMIT 500");
        assertFalse(sql.contains("OFFSET"), "cursor path must not emit OFFSET: " + sql);
    }

    @Test
    public void testReadCollectionCursorBeatsOffset() {
        // Caller passed both — cursor wins, OFFSET dropped (otherwise we'd skip rows twice).
        String sql = ContextReadExecutor.buildReadCollectionSql(123L, -1L, 500, 1000, 9999L);
        assertTrue(sql.contains("h.entity_id > 9999"), "cursor predicate present: " + sql);
        assertFalse(sql.contains("OFFSET"), "OFFSET must be dropped when cursor is set: " + sql);
    }

    // ---------- readCollection: as-of fence path (snapshotFence >= 0) ----------

    @Test
    public void testReadCollectionAsOfNoOffset() {
        String sql = ContextReadExecutor.buildReadCollectionSql(123L, 7777L, 500, 0, -1L);
        assertTrue(sql.contains("snapshot_version <= 7777"), "fence appears in inner select: " + sql);
        assertContainsOrdered(sql,
                "WHERE v.collection_id = 123 ORDER BY v.entity_id ASC LIMIT 500");
        assertFalse(sql.contains("OFFSET"), "no OFFSET when offset=0: " + sql);
    }

    @Test
    public void testReadCollectionAsOfWithOffset() {
        String sql = ContextReadExecutor.buildReadCollectionSql(123L, 7777L, 500, 1000, -1L);
        assertContainsOrdered(sql,
                "WHERE v.collection_id = 123 ORDER BY v.entity_id ASC LIMIT 500 OFFSET 1000");
    }

    @Test
    public void testReadCollectionAsOfWithCursor() {
        String sql = ContextReadExecutor.buildReadCollectionSql(123L, 7777L, 500, 0, 9999L);
        assertContainsOrdered(sql,
                "WHERE v.collection_id = 123 AND v.entity_id > 9999 ORDER BY v.entity_id ASC LIMIT 500");
    }

    // ---------- readContextBase: current-head path ----------

    @Test
    public void testReadContextBaseCurrentHeadNoOffset() {
        String sql = ContextReadExecutor.buildReadContextBaseSql(42L, -1L, 1000, 0, -1L);
        assertContainsOrdered(sql,
                "WHERE h.contextbase_id = 42 ORDER BY h.entity_id ASC LIMIT 1000");
        assertFalse(sql.contains("current_snapshot_version DESC"),
                "must not regress to the old non-deterministic ORDER BY: " + sql);
    }

    @Test
    public void testReadContextBaseCurrentHeadWithCursor() {
        String sql = ContextReadExecutor.buildReadContextBaseSql(42L, -1L, 1000, 5000, 8888L);
        // Both supplied → cursor wins.
        assertTrue(sql.contains("h.entity_id > 8888"));
        assertFalse(sql.contains("OFFSET"));
    }

    // ---------- readContextBase: as-of fence path ----------

    @Test
    public void testReadContextBaseAsOfWithOffset() {
        String sql = ContextReadExecutor.buildReadContextBaseSql(42L, 7777L, 1000, 2000, -1L);
        assertTrue(sql.contains("snapshot_version <= 7777"));
        assertContainsOrdered(sql,
                "WHERE v.contextbase_id = 42 ORDER BY v.entity_id ASC LIMIT 1000 OFFSET 2000");
    }

    @Test
    public void testReadContextBaseAsOfCursorBeatsOffset() {
        String sql = ContextReadExecutor.buildReadContextBaseSql(42L, 7777L, 1000, 2000, 8888L);
        assertTrue(sql.contains("v.entity_id > 8888"));
        assertFalse(sql.contains("OFFSET"));
    }

    // ---------- getHistory: the row cap is a hard ceiling, not just a default ----------

    @Test
    public void testHistoryLimitDefaultsToMaxWhenNonPositive() {
        int max = Config.context_entity_history_max_rows;
        assertEquals(max, ContextReadExecutor.effectiveHistoryLimit(-1));
        assertEquals(max, ContextReadExecutor.effectiveHistoryLimit(0));
    }

    @Test
    public void testHistoryLimitHonoursSmallerCallerLimit() {
        assertEquals(10, ContextReadExecutor.effectiveHistoryLimit(10));
    }

    @Test
    public void testHistoryLimitClampsLargerCallerLimit() {
        int max = Config.context_entity_history_max_rows;
        // A caller asking for far more than the configured ceiling is clamped down to it.
        assertEquals(max, ContextReadExecutor.effectiveHistoryLimit(1_000_000));
        assertEquals(max, ContextReadExecutor.effectiveHistoryLimit(max + 1));
    }

    // ---------- neighbour expansion: references must be fenced per ordinal ----------

    @Test
    public void testNeighbourPreviewsResolvesActiveRefPerOrdinal() {
        String sql = ContextReadExecutor.buildNeighbourPreviewsSql(111L, 5L, -1L, 0);
        // Active-ref resolution: MAX(snapshot_version) per ordinal, joined back on (ord, snapshot_version).
        assertTrue(sql.contains("MAX(snapshot_version) AS active_sv"), "active-ref aggregate: " + sql);
        assertTrue(sql.contains("GROUP BY ord"), "group per ordinal: " + sql);
        assertTrue(sql.contains("r.ord = am.ord AND r.snapshot_version = am.active_sv"),
                "join to the active ref row: " + sql);
        assertFalse(sql.contains("SELECT DISTINCT r.dst_entity_id"),
                "must not collect every ref row unfenced: " + sql);
    }

    @Test
    public void testNeighbourPreviewsCurrentReadHasNoFence() {
        String sql = ContextReadExecutor.buildNeighbourPreviewsSql(111L, 5L, -1L, 0);
        // A current read (fence < 0) picks the latest ref per ordinal — no snapshot bound anywhere.
        assertFalse(sql.contains("snapshot_version <= "), "current read must not fence: " + sql);
    }

    @Test
    public void testNeighbourPreviewsAsOfFencesRefsAndVersions() {
        String sql = ContextReadExecutor.buildNeighbourPreviewsSql(111L, 5L, 7777L, 0);
        // The fence bounds both the ref resolution and the destination version selection.
        assertTrue(sql.contains("WHERE src_entity_id = 111 AND src_version = 5 AND snapshot_version <= 7777"),
                "ref resolution fenced: " + sql);
        assertTrue(sql.contains("vv.snapshot_version <= 7777"), "destination version fenced: " + sql);
    }

    @Test
    public void testNeighbourBodiesResolvesActiveRefAndExcludesDeleted() {
        String sql = ContextReadExecutor.buildNeighbourBodiesSql(111L, 5L, 7777L, 0);
        assertTrue(sql.contains("MAX(snapshot_version) AS active_sv"), "active-ref aggregate: " + sql);
        assertTrue(sql.contains("WHERE src_entity_id = 111 AND src_version = 5 AND snapshot_version <= 7777"),
                "ref resolution fenced: " + sql);
        assertTrue(sql.contains("v.deleted = false"), "bodies drop tombstones: " + sql);
    }

    private static void assertContainsOrdered(String sql, String snippet) {
        assertTrue(sql.contains(snippet), "expected SQL to contain:\n  " + snippet + "\ngot:\n  " + sql);
    }
}
