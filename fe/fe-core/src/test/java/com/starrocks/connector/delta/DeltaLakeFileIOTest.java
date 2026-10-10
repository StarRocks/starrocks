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

package com.starrocks.connector.delta;

import com.google.common.cache.CacheBuilder;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeltaLakeFileIOTest {
    // No Hadoop filesystem is registered for this scheme: any bypass of the supplied FileIO fails.
    private static final String PREFIX = "injected://table/_delta_log/";

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReadersUseInjectedFileIO(boolean cached) throws Exception {
        FileIO fileIO = mock(FileIO.class);
        HadoopFileIO localIO = new HadoopFileIO(new Configuration());
        String jsonName = "00000000000000000031.json";
        String parquetName = "00000000000000000030.checkpoint.parquet";
        for (String name : new String[] {jsonName, parquetName}) {
            String localPath = Path.of(getClass().getClassLoader().getResource("connector/deltalake/" + name)
                    .toURI()).toString();
            when(fileIO.newInputFile(eq(PREFIX + name), anyLong()))
                    .thenAnswer(invocation -> localIO.newInputFile(localPath, localIO.getFileStatus(localPath).getSize()));
        }
        when(fileIO.getConf("delta.kernel.default.json.reader.batch-size")).thenReturn(Optional.of("1"));
        DeltaLakeEngine engine = engine(fileIO, cached);
        StructType schema = LogReplay.getAddRemoveReadSchema(true);
        try (CloseableIterator<ColumnarBatch> batches = engine.getJsonHandler().readJsonFiles(
                Utils.singletonCloseableIterator(FileStatus.of(PREFIX + jsonName, 0, 0)), schema, Optional.empty())) {
            assertTrue(batches.hasNext());
            assertEquals(1, batches.next().getSize());
        }
        try (CloseableIterator<FileReadResult> batches = engine.getParquetHandler().readParquetFiles(
                Utils.singletonCloseableIterator(FileStatus.of(PREFIX + parquetName, 0, 0)), schema, Optional.empty())) {
            assertTrue(batches.hasNext());
            batches.next();
        }
        verify(fileIO).newInputFile(eq(PREFIX + jsonName), anyLong());
        verify(fileIO).newInputFile(eq(PREFIX + parquetName), anyLong());
    }

    @Test
    public void testFileSystemClientUsesInjectedFileIO() throws IOException {
        FileIO fileIO = mock(FileIO.class);
        FileStatus status = FileStatus.of(PREFIX + "commit.json", 10, 20);
        when(fileIO.listFrom(PREFIX)).thenReturn(Utils.singletonCloseableIterator(status));
        when(fileIO.resolvePath(PREFIX)).thenReturn(PREFIX);
        when(fileIO.getFileStatus(status.getPath())).thenReturn(status);
        DeltaLakeEngine engine = engine(fileIO, true);
        try (CloseableIterator<FileStatus> files = engine.getFileSystemClient().listFrom(PREFIX)) {
            assertSame(status, files.next());
        }
        assertEquals(PREFIX, engine.getFileSystemClient().resolvePath(PREFIX));
        assertSame(status, engine.getFileSystemClient().getFileStatus(status.getPath()));
    }

    @Test
    public void testCachedJsonClosesStreamOnReadFailure() throws IOException {
        FileIO fileIO = mock(FileIO.class);
        InputFile input = mock(InputFile.class);
        SeekableInputStream stream = mock(SeekableInputStream.class);
        when(fileIO.newInputFile(PREFIX, -1)).thenReturn(input);
        when(input.newStream()).thenReturn(stream);
        when(stream.read(org.mockito.ArgumentMatchers.any(byte[].class),
                org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt()))
                .thenThrow(new IOException("read failed"));
        assertThrows(IOException.class, () -> DeltaLakeJsonHandler.readJsonFile(PREFIX, fileIO));
        verify(stream).close();
    }

    private static DeltaLakeEngine engine(FileIO fileIO, boolean cached) {
        DeltaLakeCatalogProperties properties = new DeltaLakeCatalogProperties(Map.of(
                DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_JSON_META_CACHE, String.valueOf(cached),
                DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_CHECKPOINT_META_CACHE, String.valueOf(cached)));
        return DeltaLakeEngine.create(fileIO, properties, CacheBuilder.newBuilder().build(), CacheBuilder.newBuilder().build());
    }
}
