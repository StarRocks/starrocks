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

package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StarRocksIcebergTableScanTest {
    @Test
    public void testGetCompleteCachedFilesReturnsMatchingCacheEntry() {
        ManifestFile manifest = mockManifestFile("matching-manifest", 1, 1);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> cache = Mockito.mock(Cache.class);
        Set<DataFile> files = Set.of(Mockito.mock(DataFile.class), Mockito.mock(DataFile.class));
        Mockito.when(cache.getIfPresent(manifest.path())).thenReturn(files);

        Set<DataFile> cachedFiles = StarRocksIcebergTableScan.getCompleteCachedFiles(cache, manifest);

        Assertions.assertSame(files, cachedFiles);
        Mockito.verify(cache, Mockito.never()).invalidate(manifest.path());
    }

    @Test
    public void testGetCompleteCachedFilesInvalidatesPartialCacheEntry() {
        ManifestFile manifest = mockManifestFile("partial-manifest", 2, 1);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> cache = Mockito.mock(Cache.class);
        Set<DataFile> files = Set.of(Mockito.mock(DataFile.class), Mockito.mock(DataFile.class));
        Mockito.when(cache.getIfPresent(manifest.path())).thenReturn(files);

        Set<DataFile> cachedFiles = StarRocksIcebergTableScan.getCompleteCachedFiles(cache, manifest);

        Assertions.assertNull(cachedFiles);
        Mockito.verify(cache, Mockito.times(1)).invalidate(manifest.path());
    }

    @Test
    public void testGetCompleteCachedFilesRejectsEmptyPlaceholderWhenCountsUnknown() {
        ManifestFile manifest = mockManifestFile("placeholder-manifest", null, null);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> cache = Mockito.mock(Cache.class);
        Set<DataFile> files = Set.of();
        Mockito.when(cache.getIfPresent(manifest.path())).thenReturn(files);

        Set<DataFile> cachedFiles = StarRocksIcebergTableScan.getCompleteCachedFiles(cache, manifest);

        Assertions.assertNull(cachedFiles);
        Mockito.verify(cache, Mockito.times(1)).invalidate(manifest.path());
    }

    @Test
    public void testGetCompleteCachedFilesKeepsNonEmptyCacheWhenCountsUnknown() {
        ManifestFile manifest = mockManifestFile("unknown-count-manifest", null, null);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DeleteFile>> cache = Mockito.mock(Cache.class);
        Set<DeleteFile> files = Set.of(Mockito.mock(DeleteFile.class));
        Mockito.when(cache.getIfPresent(manifest.path())).thenReturn(files);

        Set<DeleteFile> cachedFiles = StarRocksIcebergTableScan.getCompleteCachedFiles(cache, manifest);

        Assertions.assertSame(files, cachedFiles);
        Mockito.verify(cache, Mockito.never()).invalidate(manifest.path());
    }

    @Test
    public void testStatsKeepColumnIdsCoversPartitionAndSortColumns() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "data", Types.StringType.get()),
                Types.NestedField.required(3, "extra", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("data").build();
        // Two sort orders to cover sort-order evolution: "id" exists only in the older order.
        SortOrder oldOrder = SortOrder.builderFor(schema).asc("id").build();
        SortOrder currentOrder = SortOrder.builderFor(schema).asc("data").build();
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.specs()).thenReturn(ImmutableMap.of(spec.specId(), spec));
        Mockito.when(table.sortOrders()).thenReturn(ImmutableMap.of(1, oldOrder, 2, currentOrder));

        Set<Integer> keep = StarRocksIcebergTableScan.statsKeepColumnIds(table, schema);

        Assertions.assertTrue(keep.contains(2), "partition source column kept");
        Assertions.assertTrue(keep.contains(1), "sort source column from an evolved order kept");
        Assertions.assertFalse(keep.contains(3), "non-key column dropped");
    }

    @Test
    public void testStatsCandidateRanksPreferTopLevelColumnsAndSkipListsAndMaps() {
        Schema oldSchema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "s", Types.StructType.of(
                        Types.NestedField.optional(3, "a", Types.LongType.get()),
                        Types.NestedField.optional(4, "inner", Types.StructType.of(
                                Types.NestedField.optional(5, "b", Types.StringType.get()))))),
                Types.NestedField.optional(6, "l", Types.ListType.ofOptional(7, Types.IntegerType.get())),
                Types.NestedField.optional(8, "m", Types.MapType.ofOptional(9, 10,
                        Types.StringType.get(), Types.StructType.of(
                                Types.NestedField.optional(11, "c", Types.IntegerType.get())))));
        // a column added later exists only in the current schema
        List<Types.NestedField> currentColumns = new ArrayList<>(oldSchema.columns());
        currentColumns.add(Types.NestedField.optional(12, "added", Types.DateType.get()));
        Schema currentSchema = new Schema(currentColumns);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(0, oldSchema, 1, currentSchema));

        Map<Integer, Long> ranks = StarRocksIcebergTableScan.statsCandidateRanks(table);

        Assertions.assertEquals(Set.of(1, 3, 5, 12), ranks.keySet(), "list and map fields are not candidates");
        Assertions.assertTrue(ranks.get(1) < ranks.get(12));
        Assertions.assertTrue(ranks.get(12) < ranks.get(3), "top-level columns rank before struct fields");
        Assertions.assertTrue(ranks.get(3) < ranks.get(5));
    }

    // 150 top-level int columns c1..c150 (field ids 1..150)
    private static Table wideTable(Map<String, String> properties) {
        List<Types.NestedField> columns = new ArrayList<>();
        for (int id = 1; id <= 150; id++) {
            columns.add(Types.NestedField.optional(id, "c" + id, Types.IntegerType.get()));
        }
        Schema schema = new Schema(columns);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(0, schema));
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.sortOrder()).thenReturn(SortOrder.unsorted());
        Mockito.when(table.properties()).thenReturn(properties);
        return table;
    }

    private static List<Integer> rankOrder(Map<Integer, Long> ranks) {
        List<Integer> ids = new ArrayList<>(ranks.keySet());
        ids.sort((a, b) -> Long.compare(ranks.get(a), ranks.get(b)));
        return ids;
    }

    @Test
    public void testStatsCandidateRanksPreferBoundedColumnsOverCountsOnly() {
        // default counts: only c140 and c150 write bounds; c150 is the explicit truncate column, c140 full
        Table table = wideTable(ImmutableMap.of(
                TableProperties.DEFAULT_WRITE_METRICS_MODE, "counts",
                TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "c150", "truncate(16)",
                TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "c140", "full",
                TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "c130", "counts"));

        List<Integer> order = rankOrder(StarRocksIcebergTableScan.statsCandidateRanks(table));

        Assertions.assertEquals(List.of(140, 150, 1, 2), order.subList(0, 4),
                "explicit bounded columns first, then the counts-only columns by field id");
    }

    @Test
    public void testStatsCandidateRanksKeepExplicitColumnBeyondInferredDefaults() {
        // no explicit default and more than 100 columns: only c1..c100 get the inferred truncate(16), the rest none;
        // c150 is set explicitly and must rank inside the first 100
        Table table = wideTable(ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "c150", "truncate(16)"));

        List<Integer> order = rankOrder(StarRocksIcebergTableScan.statsCandidateRanks(table));

        Assertions.assertEquals(150, order.get(0));
        Assertions.assertEquals(1, order.get(1));
        Assertions.assertEquals(100, order.get(100));
        Assertions.assertEquals(101, order.get(101));
    }

    @Test
    public void testTopRankedStatsColumnsSkipTheKeepSet() {
        // c150 is ranked first by its explicit mode; c1 is in the keep-set, which does not count against the limit
        Table table = wideTable(ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "c150", "truncate(16)"));
        Map<Integer, Long> ranks = StarRocksIcebergTableScan.statsCandidateRanks(table);

        Assertions.assertEquals(Set.of(150, 2, 3), StarRocksIcebergTableScan.topRankedStatsColumnIds(ranks, Set.of(1), 3));
        Assertions.assertEquals(Set.of(), StarRocksIcebergTableScan.topRankedStatsColumnIds(ranks, Set.of(1), 0));
        Assertions.assertEquals(150, StarRocksIcebergTableScan.topRankedStatsColumnIds(ranks, Set.of(), 1000).size());
    }

    @Test
    public void testStatsCandidateRanksWithoutMetricsConfigKeepFieldIdOrder() {
        Table table = wideTable(ImmutableMap.of());
        Mockito.when(table.schema()).thenThrow(new IllegalStateException("no current schema"));

        List<Integer> order = rankOrder(StarRocksIcebergTableScan.statsCandidateRanks(table));

        Assertions.assertEquals(1, order.get(0));
        Assertions.assertEquals(150, order.get(149));
    }

    @Test
    public void testStatsTruncatedTypesCoverStringAndBinaryFields() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "s", Types.StringType.get()),
                Types.NestedField.optional(2, "b", Types.BinaryType.get()),
                Types.NestedField.optional(3, "st", Types.StructType.of(
                        Types.NestedField.optional(4, "inner", Types.StringType.get()))),
                Types.NestedField.optional(5, "d", Types.DecimalType.of(38, 2)),
                Types.NestedField.optional(6, "f", Types.FixedType.ofLength(32)));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(0, schema));

        Assertions.assertEquals(Map.of(1, Type.TypeID.STRING, 2, Type.TypeID.BINARY, 4, Type.TypeID.STRING),
                StarRocksIcebergTableScan.statsTruncatedTypes(table));
    }

    // The on-read completeness check compares only cardinality, so a cache entry that lost one live file
    // but is padded back to the expected size is served as "complete" and silently drops rows.
    @Test
    public void testCompletenessCheckIsContentBlindToSizeMatchingWrongFileSet() {
        // Manifest advertises three live files; the true set is {f1, f2, f3}.
        ManifestFile manifest = mockManifestFile("three-live-files-manifest", 1, 2);
        DataFile f1 = Mockito.mock(DataFile.class);
        DataFile f2 = Mockito.mock(DataFile.class);
        DataFile f3 = Mockito.mock(DataFile.class);

        // Corrupted entry: f3 is missing but a stale file pads the size back to three.
        DataFile stale = Mockito.mock(DataFile.class);
        Set<DataFile> corrupted = Set.of(f1, f2, stale);
        Assertions.assertFalse(corrupted.contains(f3), "precondition: a genuine live file is absent");

        Assertions.assertTrue(StarRocksIcebergTableScan.isCompleteCachedFiles(manifest, corrupted),
                "size-only check accepts a wrong-content set of the expected size -- the check is content-blind");

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> cache = Mockito.mock(Cache.class);
        Mockito.when(cache.getIfPresent(manifest.path())).thenReturn(corrupted);

        Set<DataFile> served = StarRocksIcebergTableScan.getCompleteCachedFiles(cache, manifest);
        Assertions.assertSame(corrupted, served, "the corrupted entry is served to the query verbatim");
        Mockito.verify(cache, Mockito.never()).invalidate(manifest.path());
    }

    private ManifestFile mockManifestFile(String path, Integer existingFilesCount, Integer addedFilesCount) {
        ManifestFile manifest = Mockito.mock(ManifestFile.class);
        Mockito.when(manifest.path()).thenReturn(path);
        Mockito.when(manifest.existingFilesCount()).thenReturn(existingFilesCount);
        Mockito.when(manifest.addedFilesCount()).thenReturn(addedFilesCount);
        return manifest;
    }
}
