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

package com.starrocks.planner;

import com.starrocks.catalog.IntVariant;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.planner.QueryGroupManifest.Group;
import com.starrocks.planner.QueryGroupManifest.KeyDefinition;
import com.starrocks.planner.QueryGroupManifest.Source;
import com.starrocks.planner.QueryGroupManifest.Task;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryGroupManifestTest {
    private static final KeyDefinition KEYS = new KeyDefinition(11, 12, List.of(3, 7, 9));

    private static Tuple tuple(long value) {
        return new Tuple(new ArrayList<>(List.of(new IntVariant(IntegerType.BIGINT, value),
                new IntVariant(IntegerType.BIGINT, 0), new IntVariant(IntegerType.BIGINT, 0))));
    }

    private static Range<Tuple> range(long lower, long upper) {
        return Range.gelt(tuple(lower), tuple(upper));
    }

    private static Source source(int id, long partition, Range<Tuple> range, long bytes, long rows) {
        TInternalScanRange internal = new TInternalScanRange();
        internal.setTablet_id(10000L + id);
        internal.setPartition_id(partition);
        internal.setVersion("23");
        internal.setSchema_hash("41");
        internal.setVersion_hash("0");
        internal.setDb_name("captured_db");
        internal.setHosts(new ArrayList<>());
        internal.setRow_count(777);
        internal.setFill_data_cache(false);
        internal.setGtid(99);
        TScanRangeLocations original = new TScanRangeLocations();
        original.setScan_range(new TScanRange().setInternal_scan_range(internal));
        original.setLocations(new ArrayList<>());
        return new Source(id, partition, internal.getTablet_id(), 23, 41, range, original, bytes, rows);
    }

    private static QueryGroupManifest build(Source... sources) throws StarRocksException {
        return QueryGroupManifestBuilder.build(5, KEYS, List.of(sources));
    }

    private static void assertTiles(QueryGroupManifest manifest) {
        for (Source source : manifest.getSources()) {
            List<Task> tasks = manifest.getTasks().stream()
                    .filter(task -> task.sourceIndex() == source.sourceIndex()).collect(Collectors.toList());
            assertFalse(tasks.isEmpty());
            assertEquals(source.tabletRange().getLowerBound(), tasks.get(0).clip().getLowerBound());
            assertEquals(source.tabletRange().getUpperBound(), tasks.get(tasks.size() - 1).clip().getUpperBound());
            for (int i = 0; i < tasks.size(); i++) {
                Task task = tasks.get(i);
                assertTrue(source.tabletRange().contains(task.clip()));
                assertEquals(manifest.getGroups().get(task.groupId()).interval(), task.clip());
                if (i > 0) {
                    assertEquals(tasks.get(i - 1).clip().getUpperBound(), task.clip().getLowerBound());
                    assertFalse(tasks.get(i - 1).clip().isOverlapping(task.clip()));
                }
            }
        }
    }

    @Test
    public void testDifferentDatesSplitOneTabletIntoDisjointClips() throws Exception {
        QueryGroupManifest manifest = build(source(0, 1, range(0, 100), 101, 100),
                source(1, 2, range(0, 40), 40, 40), source(2, 2, range(40, 100), 60, 60));
        assertEquals(2, manifest.getGroupCount());
        assertEquals(4, manifest.getTaskCount());
        assertEquals(3, manifest.getSourceCount());
        assertEquals(range(0, 40), manifest.getTasks().get(0).clip());
        assertEquals(range(40, 100), manifest.getTasks().get(1).clip());
        assertEquals(91, manifest.getGroups().get(0).estimatedWeight());
        assertEquals(110, manifest.getGroups().get(1).estimatedWeight());
        assertTiles(manifest);
    }

    @Test
    public void testThirdCutDeterministicIdsAndExactCoverage() throws Exception {
        List<Source> sources = new ArrayList<>(List.of(source(8, 1, range(0, 100), 100, 100),
                source(3, 2, range(0, 40), 40, 40), source(9, 2, range(40, 100), 60, 60),
                source(2, 3, range(0, 70), 70, 70), source(6, 3, range(70, 100), 30, 30)));
        QueryGroupManifest manifest = QueryGroupManifestBuilder.build(5, KEYS, sources);
        assertEquals(List.of(range(0, 40), range(40, 70), range(70, 100)), manifest.getGroups().stream()
                .map(Group::interval).collect(Collectors.toList()));
        assertEquals(9, manifest.getTaskCount());
        assertTiles(manifest);
        Collections.reverse(sources);
        QueryGroupManifest reordered = QueryGroupManifestBuilder.build(5, KEYS, sources);
        assertEquals(manifest.getGroups(), reordered.getGroups());
        assertEquals(manifest.getTasks(), reordered.getTasks());
    }

    @Test
    public void testInfinitiesAndBigintExtremes() throws Exception {
        QueryGroupManifest manifest = build(source(0, 1, Range.all(), 10, 10),
                source(1, 2, Range.lt(tuple(Long.MIN_VALUE)), 1, 1),
                source(2, 2, range(Long.MIN_VALUE, Long.MAX_VALUE), 1, 1),
                source(3, 2, Range.ge(tuple(Long.MAX_VALUE)), 1, 1));
        assertEquals(3, manifest.getGroupCount());
        assertEquals(6, manifest.getTaskCount());
        assertTiles(manifest);
    }

    @Test
    public void testLexicographicFullTupleCuts() throws Exception {
        Tuple middle = new Tuple(List.of(new IntVariant(IntegerType.BIGINT, 0),
                new IntVariant(IntegerType.BIGINT, 1), new IntVariant(IntegerType.BIGINT, Long.MIN_VALUE)));
        QueryGroupManifest manifest = build(source(0, 1, range(0, 1), 1, 1),
                source(1, 2, Range.gelt(tuple(0), middle), 1, 1),
                source(2, 2, Range.gelt(middle, tuple(1)), 1, 1));
        assertEquals(middle, manifest.getGroups().get(0).interval().getUpperBound());
        assertTiles(manifest);
    }

    @Test
    public void testGapsAreOmittedAndZeroUnknownStatsRetainSources() throws Exception {
        QueryGroupManifest manifest = build(source(0, 1, range(0, 10), 0, 0),
                source(1, 2, range(20, 30), -1, -1));
        assertEquals(List.of(range(0, 10), range(20, 30)), manifest.getGroups().stream()
                .map(Group::interval).collect(Collectors.toList()));
        assertEquals(2, manifest.getSourceCount());
        assertEquals(2, manifest.getTaskCount());
        assertEquals(1, manifest.getGroups().get(0).estimatedWeight());
        assertEquals(1, manifest.getGroups().get(1).estimatedWeight());
        assertTiles(manifest);
    }

    @Test
    public void testDeclinesOneGroupAndOneDate() {
        assertThrows(StarRocksException.class, () -> build(source(0, 1, Range.all(), 0, 0),
                source(1, 2, Range.all(), 0, 0)));
        assertThrows(StarRocksException.class, () -> build(source(0, 1, range(0, 40), 0, 0),
                source(1, 1, range(40, 100), 0, 0)));
    }

    @Test
    public void testRejectsDuplicateSourcesAndConflictingVersions() {
        Source a = source(0, 1, range(0, 100), 1, 1);
        assertThrows(StarRocksException.class, () -> build(a, a));
        Source sameIndex = source(0, 2, range(0, 40), 1, 1);
        assertThrows(StarRocksException.class, () -> build(a, sameIndex));
        TScanRangeLocations original = a.original();
        original.getScan_range().getInternal_scan_range().setVersion("24");
        Source conflict = new Source(1, 1, a.tabletId(), 24, 41, a.tabletRange(), original, 1, 1);
        assertThrows(StarRocksException.class, () -> build(a, conflict));
    }

    @Test
    public void testRejectsOverlappingPhysicalSources() {
        assertThrows(StarRocksException.class, () -> build(source(0, 1, range(0, 60), 1, 1),
                source(1, 1, range(40, 100), 1, 1), source(2, 2, range(0, 100), 1, 1)));
    }

    @Test
    public void testRejectsMalformedRangesAndTupleTypes() {
        List<Range<Tuple>> invalid = List.of(Range.gelt(tuple(2), tuple(1)), range(1, 1),
                Range.gtlt(tuple(0), tuple(1)), Range.gele(tuple(0), tuple(1)),
                Range.gelt(new Tuple(List.of(new IntVariant(IntegerType.BIGINT, 0))), tuple(1)),
                Range.gelt(new Tuple(List.of(Variant.nullVariant(IntegerType.BIGINT),
                        new IntVariant(IntegerType.BIGINT, 0), new IntVariant(IntegerType.BIGINT, 0))), tuple(1)),
                Range.gelt(new Tuple(List.of(Variant.minVariant(IntegerType.BIGINT),
                        new IntVariant(IntegerType.BIGINT, 0), new IntVariant(IntegerType.BIGINT, 0))), tuple(1)),
                Range.gelt(new Tuple(List.of(new IntVariant(IntegerType.INT, 0),
                        new IntVariant(IntegerType.BIGINT, 0), new IntVariant(IntegerType.BIGINT, 0))), tuple(1)));
        for (Range<Tuple> range : invalid) {
            assertThrows(StarRocksException.class, () -> build(source(0, 1, range, 1, 1),
                    source(1, 2, range(0, 100), 1, 1)));
        }
    }

    @Test
    public void testRejectsMissingOrInconsistentCapturedIdentity() {
        Source valid = source(0, 1, range(0, 100), 1, 1);
        for (int field = 0; field < 5; field++) {
            TScanRangeLocations original = valid.original();
            TInternalScanRange internal = original.getScan_range().getInternal_scan_range();
            switch (field) {
                case 0: internal.unsetVersion(); break;
                case 1: internal.setSchema_hash("42"); break;
                case 2: internal.setTablet_id(99); break;
                case 3: internal.unsetPartition_id(); break;
                default: original.unsetScan_range(); break;
            }
            Source invalid = new Source(0, 1, valid.tabletId(), 23, 41, valid.tabletRange(), original, 1, 1);
            assertThrows(StarRocksException.class, () -> build(invalid, source(1, 2, range(0, 40), 1, 1)));
        }
    }

    @Test
    public void testRejectsInvalidKeyDefinitionsAndMissingInputs() {
        List<KeyDefinition> invalid = List.of(new KeyDefinition(0, 12, List.of(3, 7, 9)),
                new KeyDefinition(11, 12, List.of(3, 3, 9)), new KeyDefinition(11, 12, List.of(3, 7)),
                new KeyDefinition(11, 12, List.of(-1, 7, 9)));
        for (KeyDefinition key : invalid) {
            assertThrows(StarRocksException.class, () -> QueryGroupManifestBuilder.build(5, key, List.of()));
        }
        assertThrows(StarRocksException.class, () -> QueryGroupManifestBuilder.build(5, KEYS, null));
        assertThrows(StarRocksException.class, () -> QueryGroupManifestBuilder.build(5, KEYS, List.of()));
    }

    @Test
    public void testDefensiveCopiesOfTuplesThriftAndCollections() throws Exception {
        List<Integer> ids = new ArrayList<>(List.of(3, 7, 9));
        KeyDefinition keys = new KeyDefinition(11, 12, ids);
        Tuple lower = tuple(0);
        Source captured = source(0, 1, Range.gelt(lower, tuple(100)), 10, 10);
        TScanRangeLocations original = captured.original();
        Source source = new Source(0, 1, captured.tabletId(), 23, 41, captured.tabletRange(), original, 10, 10);
        List<Source> sources = new ArrayList<>(List.of(source, source(1, 2, range(0, 40), 1, 1),
                source(2, 2, range(40, 100), 1, 1)));
        QueryGroupManifest manifest = QueryGroupManifestBuilder.build(5, keys, sources);
        ids.clear();
        lower.getValues().clear();
        sources.clear();
        original.getScan_range().getInternal_scan_range().setVersion("999");
        source.original().getScan_range().getInternal_scan_range().setVersion("888");
        Range<Tuple> returned = manifest.getTasks().get(0).clip();
        returned.getLowerBound().getValues().clear();
        manifest.getSources().get(0).tabletRange().getLowerBound().getValues().clear();
        manifest.getGroups().get(0).interval().getUpperBound().getValues().clear();
        assertEquals(List.of(3, 7, 9), manifest.getKeys().orderedCUniqueIds());
        assertEquals(range(0, 40), manifest.getTasks().get(0).clip());
        assertEquals("23", manifest.getSources().get(0).original().getScan_range().getInternal_scan_range().getVersion());
        assertThrows(UnsupportedOperationException.class, () -> manifest.getTasks().clear());
        assertThrows(UnsupportedOperationException.class, () -> manifest.getGroups().get(0).tasks().clear());
        assertTiles(manifest);
    }

    @Test
    public void testFreshWireTasksPreserveOriginalMetadataAndRejectForgery() throws Exception {
        QueryGroupManifest manifest = build(source(0, 1, range(0, 100), 10, 10),
                source(1, 2, range(0, 40), 1, 1), source(2, 2, range(40, 100), 1, 1));
        Task task = manifest.getTasks().get(1);
        TScanRange wire = manifest.toScanRange(task);
        TInternalScanRange internal = wire.getInternal_scan_range();
        assertEquals(10000, internal.getTablet_id());
        assertEquals(1, internal.getPartition_id());
        assertEquals("23", internal.getVersion());
        assertEquals("41", internal.getSchema_hash());
        assertEquals(777, internal.getRow_count());
        assertEquals(99, internal.getGtid());
        assertFalse(internal.isFill_data_cache());
        assertEquals(1, internal.getBucket_sequence());
        assertEquals(1, wire.getGrouped_range_task().getGroup_id());
        assertEquals(List.of(3, 7, 9), wire.getGrouped_range_task().getSort_key_unique_ids());
        assertEquals(tuple(40).toThrift(), wire.getGrouped_range_task().getKey_range().getLower_bound());
        wire.getGrouped_range_task().getSort_key_unique_ids().clear();
        wire.getGrouped_range_task().getKey_range().getLower_bound().getValues().clear();
        internal.setVersion("999");
        TScanRange fresh = manifest.toScanRange(task);
        assertNotSame(wire, fresh);
        assertEquals("23", fresh.getInternal_scan_range().getVersion());
        assertEquals(List.of(3, 7, 9), fresh.getGrouped_range_task().getSort_key_unique_ids());
        assertEquals(tuple(40).toThrift(), fresh.getGrouped_range_task().getKey_range().getLower_bound());
        assertFalse(manifest.getSources().get(0).original().getScan_range().isSetGrouped_range_task());
        assertThrows(IllegalArgumentException.class, () -> manifest.toScanRange(new Task(0, 1, range(0, 100))));
        assertThrows(IllegalArgumentException.class, () -> manifest.toScanRange(new Task(99, 1, range(40, 100))));
        assertThrows(IllegalArgumentException.class,
                () -> new Group(0, range(0, 100), List.of(task, task), 1));
    }

    @Test
    public void testTaskLookupRejectsWrongIdentityGeometryAndInclusiveness() throws Exception {
        QueryGroupManifest manifest = build(source(8, 1, range(0, 100), 10, 10),
                source(3, 2, range(0, 40), 1, 1), source(9, 2, range(40, 100), 1, 1));
        Task canonical = new Task(8, 1, range(40, 100));
        assertEquals(10008, manifest.toScanRange(canonical).getInternal_scan_range().getTablet_id());
        for (Task forged : List.of(new Task(0, 1, range(40, 100)), new Task(3, 1, range(40, 100)),
                new Task(8, 0, range(40, 100)), new Task(8, 1, range(41, 100)),
                new Task(8, 1, Range.gtlt(tuple(40), tuple(100))),
                new Task(8, 1, Range.gele(tuple(40), tuple(100))), new Task(8, 1, null),
                new Task(8, -1, range(40, 100)), new Task(8, 2, range(40, 100)))) {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> manifest.toScanRange(forged));
            assertEquals("Task is not part of this grouped scan manifest", error.getMessage());
        }
        assertThrows(IllegalArgumentException.class, () -> manifest.toScanRange(null));
    }

    @Test
    public void testEqualValuedIntBoundsUseCanonicalBigintWireClip() throws Exception {
        QueryGroupManifest manifest = build(source(0, 1, range(0, 100), 10, 10),
                source(1, 2, range(0, 40), 1, 1), source(2, 2, range(40, 100), 1, 1));
        Task canonical = manifest.getTasks().get(1);
        Tuple intLower = new Tuple(canonical.clip().getLowerBound().getValues().stream()
                .map(value -> (Variant) new IntVariant(IntegerType.INT, value.getLongValue()))
                .collect(Collectors.toList()));
        Tuple intUpper = new Tuple(canonical.clip().getUpperBound().getValues().stream()
                .map(value -> (Variant) new IntVariant(IntegerType.INT, value.getLongValue()))
                .collect(Collectors.toList()));
        Task supplied = new Task(canonical.sourceIndex(), canonical.groupId(), Range.gelt(intLower, intUpper));
        // IntVariant equality ignores its type, but the Thrift endpoint codec preserves it.
        assertEquals(canonical, supplied);
        TScanRange wire = manifest.toScanRange(supplied);
        assertEquals(canonical.clip().getLowerBound().toThrift(),
                wire.getGrouped_range_task().getKey_range().getLower_bound());
        assertEquals(canonical.clip().getUpperBound().toThrift(),
                wire.getGrouped_range_task().getKey_range().getUpper_bound());
        assertEquals(manifest.toScanRange(canonical), wire);
    }

    @Test
    public void testExactly1024GroupsAndOneMoreRefused() throws Exception {
        List<Source> sources = new ArrayList<>();
        for (int i = 0; i < 1024; i++) {
            sources.add(source(i, 1, range(i, i + 1), 1, 1));
        }
        sources.add(source(1024, 2, range(0, 1024), 1, 1));
        assertEquals(1024, QueryGroupManifestBuilder.build(5, KEYS, sources).getGroupCount());
        sources.add(source(1025, 3, range(1024, 1025), 1, 1));
        assertThrows(StarRocksException.class, () -> QueryGroupManifestBuilder.build(5, KEYS, sources));
    }

    @Test
    public void testExactly8192SourcesAndEarlySourceBudgetRefusal() throws Exception {
        List<Source> sources = new ArrayList<>();
        for (int i = 0; i < 8192; i++) {
            sources.add(source(17 + 3 * i, i / 2 + 1, range(i % 2, i % 2 + 1), 0, 0));
        }
        QueryGroupManifest manifest = QueryGroupManifestBuilder.build(5, KEYS, sources);
        assertEquals(8192, manifest.getTaskCount());
        assertEquals(8192, manifest.getSourceCount());
        // Materialize every source across 4096 partitions, with large queues and noncontiguous source IDs.
        for (int i = manifest.getTaskCount() - 1; i >= 0; i--) {
            Task canonical = manifest.getTasks().get(i);
            Task supplied = new Task(canonical.sourceIndex(), canonical.groupId(), canonical.clip());
            TScanRange wire = manifest.toScanRange(supplied);
            assertEquals(10000L + 17 + 3 * i, wire.getInternal_scan_range().getTablet_id());
            assertEquals(i / 2 + 1, wire.getInternal_scan_range().getPartition_id());
            assertEquals("23", wire.getInternal_scan_range().getVersion());
            assertEquals("41", wire.getInternal_scan_range().getSchema_hash());
            assertEquals(i % 2, wire.getInternal_scan_range().getBucket_sequence());
            assertEquals(i % 2, wire.getGrouped_range_task().getGroup_id());
            assertEquals(canonical.clip().getLowerBound().toThrift(),
                    wire.getGrouped_range_task().getKey_range().getLower_bound());
            assertEquals(canonical.clip().getUpperBound().toThrift(),
                    wire.getGrouped_range_task().getKey_range().getUpper_bound());
        }
        sources.add(null);
        StarRocksException error = assertThrows(StarRocksException.class,
                () -> QueryGroupManifestBuilder.build(5, KEYS, sources));
        assertTrue(error.getMessage().contains("8192"));
    }

    @Test
    public void testExactly8192TasksAndOneMoreRefused() throws Exception {
        List<Source> sources = new ArrayList<>(List.of(source(0, 1, range(0, 1), 1, 1),
                source(1, 1, range(1, 2), 1, 1)));
        for (int i = 2; i < 4097; i++) {
            sources.add(source(i, i, range(0, 2), 1, 1));
        }
        assertEquals(8192, QueryGroupManifestBuilder.build(5, KEYS, sources).getTaskCount());
        sources.add(source(4097, 4097, range(0, 1), 1, 1));
        assertThrows(StarRocksException.class, () -> QueryGroupManifestBuilder.build(5, KEYS, sources));
    }

    @Test
    public void testWeightsUseBytesThenRowsWithFloorsAndSaturation() throws Exception {
        QueryGroupManifest manifest = build(source(0, 1, range(0, 100), 0, 5),
                source(1, 2, range(0, 40), 0, 0), source(2, 2, range(40, 100), -1, -1));
        assertEquals(4, manifest.getGroups().get(0).estimatedWeight());
        assertEquals(3, manifest.getGroups().get(1).estimatedWeight());
        QueryGroupManifest saturated = build(source(0, 1, range(0, 100), Long.MAX_VALUE, 0),
                source(1, 2, range(0, 40), Long.MAX_VALUE, 0),
                source(2, 2, range(40, 100), Long.MAX_VALUE, 0));
        assertEquals(Long.MAX_VALUE, saturated.getGroups().get(0).estimatedWeight());
        assertEquals(Long.MAX_VALUE, saturated.getGroups().get(1).estimatedWeight());
    }
}
