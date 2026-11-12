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

package com.starrocks.connector.share.iceberg;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AzurePathNormalizingFileIOTest {

    @Test
    public void testNormalizeAzurePath() {
        assertNull(AzurePathNormalizingFileIO.normalizeAzurePath(null));
        assertEquals("wasbs://c@acct.blob.core.windows.net/a/b.avro",
                AzurePathNormalizingFileIO.normalizeAzurePath("abfss://c@acct.blob.core.windows.net/a/b.avro"));
        assertEquals("wasb://c@acct.blob.core.windows.net/a",
                AzurePathNormalizingFileIO.normalizeAzurePath("abfs://c@acct.blob.core.windows.net/a"));
        assertEquals("wasbs://c@acct.blob.core.windows.net",
                AzurePathNormalizingFileIO.normalizeAzurePath("abfss://c@acct.blob.core.windows.net"));
        // dfs endpoints and other schemes are left alone
        String dfs = "abfss://c@acct.dfs.core.windows.net/a";
        assertEquals(dfs, AzurePathNormalizingFileIO.normalizeAzurePath(dfs));
        String wasbs = "wasbs://c@acct.blob.core.windows.net/a";
        assertEquals(wasbs, AzurePathNormalizingFileIO.normalizeAzurePath(wasbs));
        String s3 = "s3://bucket/a";
        assertEquals(s3, AzurePathNormalizingFileIO.normalizeAzurePath(s3));
    }

    @Test
    public void testDelegatesWithNormalizedPath() {
        RecordingFileIO recording = new RecordingFileIO();
        FileIO io = new AzurePathNormalizingFileIO(recording);

        InputFile a = io.newInputFile("abfss://c@acct.blob.core.windows.net/m/a.avro");
        InputFile b = io.newInputFile("abfss://c@acct.blob.core.windows.net/m/b.avro", 10L);
        io.newOutputFile("abfss://c@acct.blob.core.windows.net/m/c.avro");
        io.deleteFile("abfss://c@acct.blob.core.windows.net/m/d.avro");
        io.newInputFile("abfss://c@acct.dfs.core.windows.net/m/e.avro");
        io.newInputFile("s3://bucket/m/f.avro");

        assertEquals(List.of(
                "in:wasbs://c@acct.blob.core.windows.net/m/a.avro",
                "in:wasbs://c@acct.blob.core.windows.net/m/b.avro:10",
                "out:wasbs://c@acct.blob.core.windows.net/m/c.avro",
                "delete:wasbs://c@acct.blob.core.windows.net/m/d.avro",
                "in:abfss://c@acct.dfs.core.windows.net/m/e.avro",
                "in:s3://bucket/m/f.avro"), recording.calls);
        recording.calls.clear();

        // location() keeps the caller's path; the rest goes to the file opened on the normalized path
        assertEquals("abfss://c@acct.blob.core.windows.net/m/a.avro", a.location());
        assertEquals("abfss://c@acct.blob.core.windows.net/m/b.avro", b.location());
        assertEquals(10L, b.getLength());
        // a path that is not rewritten gets the delegate's file as is
        InputFile s3 = io.newInputFile("s3://bucket/m/g.avro");
        assertSame(recording.files.get("s3://bucket/m/g.avro"), s3);

        assertEquals(Map.of("k", "v"), io.properties());
        io.close();
        assertTrue(recording.closed);
    }

    @Test
    public void testMetadataFileLocationKeepsCatalogPath() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", new HashMap<>());
        catalog.createNamespace(Namespace.of("db"));
        Table table = catalog.createTable(TableIdentifier.of("db", "azure_tbl"),
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
        String stored = ((HasTableOperations) table).operations().current().metadataFileLocation();

        // the catalog hands out the abfss+blob path; the file is only reachable under its wasbs form
        InMemoryFileIO inMemory = new InMemoryFileIO();
        String abfss = "abfss://c@acct.blob.core.windows.net/db/azure_tbl/metadata/v1.metadata.json";
        String wasbs = "wasbs://c@acct.blob.core.windows.net/db/azure_tbl/metadata/v1.metadata.json";
        try (InputStream in = inMemory.newInputFile(stored).newStream()) {
            inMemory.addFile(wasbs, in.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        StaticTableOperations ops = new StaticTableOperations(abfss, new AzurePathNormalizingFileIO(inMemory));
        // $metadata_log_entries shows this location, so it must stay the catalog's path
        assertEquals(abfss, ops.current().metadataFileLocation());
        assertEquals(table.schema().asStruct(), ops.current().schema().asStruct());
        inMemory.deleteFile(wasbs);
    }

    static class RecordingFileIO implements FileIO {
        final List<String> calls = new ArrayList<>();
        boolean closed = false;

        final Map<String, InputFile> files = new HashMap<>();

        @Override
        public InputFile newInputFile(String path) {
            calls.add("in:" + path);
            return file(path, -1L);
        }

        @Override
        public InputFile newInputFile(String path, long length) {
            calls.add("in:" + path + ":" + length);
            return file(path, length);
        }

        private InputFile file(String path, long length) {
            InputFile file = new InputFile() {
                @Override
                public long getLength() {
                    return length;
                }

                @Override
                public SeekableInputStream newStream() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public String location() {
                    return path;
                }

                @Override
                public boolean exists() {
                    return true;
                }
            };
            files.put(path, file);
            return file;
        }

        @Override
        public OutputFile newOutputFile(String path) {
            calls.add("out:" + path);
            return null;
        }

        @Override
        public void deleteFile(String path) {
            calls.add("delete:" + path);
        }

        @Override
        public Map<String, String> properties() {
            return Map.of("k", "v");
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
