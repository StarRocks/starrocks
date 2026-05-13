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
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iceberg.types.Types.NestedField.required;

public class ManifestReaderTest {
    private static final Schema TEST_SCHEMA =
            new Schema(required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));
    private static final PartitionSpec TEST_SPEC = PartitionSpec.unpartitioned();
    private static final FileIO FILE_IO = new LocalFileIO();
    private static final List<String> SCAN_COLUMNS_WITHOUT_STATS = List.of(
            DataFile.CONTENT.name(),
            DataFile.FILE_PATH.name(),
            DataFile.FILE_FORMAT.name(),
            DataFile.PARTITION_NAME,
            DataFile.RECORD_COUNT.name(),
            DataFile.FILE_SIZE.name(),
            DataFile.SPLIT_OFFSETS.name(),
            DataFile.SORT_ORDER_ID.name(),
            DataFile.SPEC_ID.name());

    @TempDir
    Path tempDir;

    @Test
    public void testFillCacheIfNeededWritesCompleteFilesWhenCacheEntryDisappearsMidIteration() throws IOException {
        DataFile file1 = newDataFile("data-file-1.parquet", 10L);
        DataFile file2 = newDataFile("data-file-2.parquet", 20L);
        ManifestFile manifest = writeManifest("complete-cache.avro", file1, file2);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> dataFileCache = Mockito.mock(Cache.class);
        Set<DataFile> placeholder = ConcurrentHashMap.newKeySet();
        AtomicInteger getCount = new AtomicInteger(0);
        Mockito.when(dataFileCache.getIfPresent(manifest.path()))
                .thenAnswer(invocation -> getCount.getAndIncrement() == 0 ? placeholder : null);

        AtomicReference<Set<DataFile>> cachedFilesRef = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Set<DataFile> cachedFiles = invocation.getArgument(1);
            cachedFilesRef.set(cachedFiles);
            return null;
        }).when(dataFileCache).put(Mockito.eq(manifest.path()), Mockito.anySet());

        ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, Map.of(TEST_SPEC.specId(), TEST_SPEC))
                .select(ManifestReader.ALL_COLUMNS)
                .dataFileCache(dataFileCache)
                .cacheWithMetrics(false);

        try (CloseableIterable<ManifestEntry<DataFile>> entries = reader.liveEntries();
                CloseableIterator<ManifestEntry<DataFile>> iterator = entries.iterator()) {
            while (iterator.hasNext()) {
                iterator.next();
            }
        }

        Mockito.verify(dataFileCache, Mockito.times(1)).getIfPresent(manifest.path());
        Mockito.verify(dataFileCache, Mockito.times(1)).put(Mockito.eq(manifest.path()), Mockito.anySet());
        Assertions.assertTrue(placeholder.isEmpty(), "placeholder should stay empty until the full manifest is ready");
        Assertions.assertNotNull(cachedFilesRef.get(), "a fully materialized cache entry should be published on close");
        Assertions.assertEquals(Set.of(file1.location(), file2.location()),
                cachedFilesRef.get().stream().map(DataFile::location).collect(Collectors.toSet()));
    }

    @Test
    public void testFillCacheWithMetricsProjectsStatsWhenScanDoesNotRequestStats() throws IOException {
        DataFile file = newDataFileWithStats("data-file-with-stats.parquet", 10L);
        ManifestFile manifest = writeManifest("cache-with-stats.avro", file);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> dataFileCache = Mockito.mock(Cache.class);
        Set<DataFile> placeholder = ConcurrentHashMap.newKeySet();
        Mockito.when(dataFileCache.getIfPresent(manifest.path())).thenReturn(placeholder);

        AtomicReference<Set<DataFile>> cachedFilesRef = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Set<DataFile> cachedFiles = invocation.getArgument(1);
            cachedFilesRef.set(cachedFiles);
            return null;
        }).when(dataFileCache).put(Mockito.eq(manifest.path()), Mockito.anySet());

        ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, Map.of(TEST_SPEC.specId(), TEST_SPEC))
                .select(SCAN_COLUMNS_WITHOUT_STATS)
                .dataFileCache(dataFileCache)
                .cacheWithMetrics(true);

        try (CloseableIterable<ManifestEntry<DataFile>> entries = reader.liveEntries();
                CloseableIterator<ManifestEntry<DataFile>> iterator = entries.iterator()) {
            while (iterator.hasNext()) {
                iterator.next();
            }
        }

        Assertions.assertNotNull(cachedFilesRef.get(), "cache entry should be published after full consumption");
        DataFile cachedFile = cachedFilesRef.get().iterator().next();
        Assertions.assertNotNull(cachedFile.lowerBounds(), "cached data file should keep lower bounds");
        Assertions.assertEquals(file.lowerBounds(), cachedFile.lowerBounds());
        Assertions.assertEquals(file.upperBounds(), cachedFile.upperBounds());
        Assertions.assertEquals(file.valueCounts(), cachedFile.valueCounts());
        Assertions.assertEquals(file.nullValueCounts(), cachedFile.nullValueCounts());
    }

    @Test
    public void testFillCacheIfNeededSkipsPublishingPartialDataOnEarlyClose() throws IOException {
        DataFile file1 = newDataFile("partial-file-1.parquet", 10L);
        DataFile file2 = newDataFile("partial-file-2.parquet", 20L);
        ManifestFile manifest = writeManifest("partial-close.avro", file1, file2);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> dataFileCache = Mockito.mock(Cache.class);
        Set<DataFile> placeholder = ConcurrentHashMap.newKeySet();
        Mockito.when(dataFileCache.getIfPresent(manifest.path())).thenReturn(placeholder);

        ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, Map.of(TEST_SPEC.specId(), TEST_SPEC))
                .select(ManifestReader.ALL_COLUMNS)
                .dataFileCache(dataFileCache)
                .cacheWithMetrics(false);

        try (CloseableIterable<ManifestEntry<DataFile>> entries = reader.liveEntries();
                CloseableIterator<ManifestEntry<DataFile>> iterator = entries.iterator()) {
            Assertions.assertTrue(iterator.hasNext());
            iterator.next();
        }

        Mockito.verify(dataFileCache, Mockito.times(1)).getIfPresent(manifest.path());
        Mockito.verify(dataFileCache, Mockito.never()).put(Mockito.eq(manifest.path()), Mockito.anySet());
        Assertions.assertTrue(placeholder.isEmpty(), "partial iteration must not leak partially cached files");
    }

    private ManifestFile writeManifest(String manifestFileName, DataFile... dataFiles) throws IOException {
        File manifestFile = tempDir.resolve(manifestFileName).toFile();
        OutputFile outputFile = FILE_IO.newOutputFile(manifestFile.getCanonicalPath());
        ManifestWriter<DataFile> writer = ManifestFiles.write(1, TEST_SPEC, outputFile, 1L);
        try {
            for (DataFile dataFile : dataFiles) {
                writer.add(dataFile);
            }
        } finally {
            writer.close();
        }
        return writer.toManifestFile();
    }

    private DataFile newDataFile(String fileName, long recordCount) {
        return DataFiles.builder(TEST_SPEC)
                .withPath(tempDir.resolve(fileName).toString())
                .withFileSizeInBytes(64L)
                .withRecordCount(recordCount)
                .build();
    }

    private DataFile newDataFileWithStats(String fileName, long recordCount) {
        Metrics metrics = new Metrics(
                recordCount,
                Map.of(1, 4L),
                Map.of(1, recordCount),
                Map.of(1, 0L),
                null,
                Map.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1)),
                Map.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 10)));
        return DataFiles.builder(TEST_SPEC)
                .withPath(tempDir.resolve(fileName).toString())
                .withFileSizeInBytes(64L)
                .withRecordCount(recordCount)
                .withMetrics(metrics)
                .build();
    }

    private static final class LocalFileIO implements FileIO {
        @Override
        public InputFile newInputFile(String path) {
            return Files.localInput(path);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            return Files.localOutput(path);
        }

        @Override
        public void deleteFile(String path) {
            if (!new File(path).delete()) {
                throw new RuntimeIOException("Failed to delete file: " + path);
            }
        }

        @Override
        public Map<String, String> properties() {
            return Maps.newHashMap();
        }
    }
}
