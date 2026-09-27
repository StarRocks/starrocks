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
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
