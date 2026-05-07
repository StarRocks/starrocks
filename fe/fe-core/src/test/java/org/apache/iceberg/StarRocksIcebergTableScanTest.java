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

    private ManifestFile mockManifestFile(String path, Integer existingFilesCount, Integer addedFilesCount) {
        ManifestFile manifest = Mockito.mock(ManifestFile.class);
        Mockito.when(manifest.path()).thenReturn(path);
        Mockito.when(manifest.existingFilesCount()).thenReturn(existingFilesCount);
        Mockito.when(manifest.addedFilesCount()).thenReturn(addedFilesCount);
        return manifest;
    }
}
