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

package com.starrocks.lance.reader;

import com.starrocks.utils.Platform;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LanceSplitScannerTest {
    @TempDir
    public Path tempDir;

    private String previousTestMode;

    @BeforeEach
    public void enableTestAllocator() {
        previousTestMode = System.getProperty(Platform.UT_KEY);
        System.setProperty(Platform.UT_KEY, "true");
    }

    @AfterEach
    public void restoreTestAllocator() {
        if (previousTestMode == null) {
            System.clearProperty(Platform.UT_KEY);
        } else {
            System.setProperty(Platform.UT_KEY, previousTestMode);
        }
    }

    @Test
    public void testReadRealDatasetAcrossChunks() throws Exception {
        String uri = tempDir.resolve("rows.lance").toString();
        // Exercise the native Lance/Arrow boundary; vector-only tests cannot detect ABI mismatches.
        try (BufferAllocator allocator = new RootAllocator();
                BigIntVector ids = new BigIntVector("id", allocator);
                ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            ids.allocateNew(7);
            for (int i = 0; i < 7; i++) {
                ids.setSafe(i, i);
            }
            ids.setValueCount(7);
            try (VectorSchemaRoot root = VectorSchemaRoot.of(ids);
                    ArrowStreamWriter writer = new ArrowStreamWriter(root, null, bytes)) {
                root.setRowCount(7);
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            try (ArrowStreamReader reader = new ArrowStreamReader(
                    new ByteArrayInputStream(bytes.toByteArray()), allocator);
                    Dataset dataset = Dataset.write().allocator(allocator).reader(reader).uri(uri)
                            .maxRowsPerFile(3).execute()) {
                assertEquals(7, dataset.countRows());
                assertEquals(3, dataset.getFragments().size());
            }
        }

        LanceSplitScanner scanner = new LanceSplitScanner(3,
                Map.of("required_fields", "id", "lance_dataset_uri", uri));
        try {
            scanner.open();
            int nextId = 0;
            for (int expectedRows : new int[] {3, 3, 1, 0}) {
                scanner.getNextOffHeapChunk();
                try {
                    assertEquals(expectedRows, scanner.getOffHeapTable().getNumRows());
                    StringBuilder expected = new StringBuilder();
                    for (int row = 0; row < expectedRows; row++) {
                        expected.append("row").append(row).append(": [id:").append(nextId++).append("]\n");
                    }
                    assertEquals(expected.toString(), scanner.getOffHeapTable().dump(expectedRows));
                } finally {
                    scanner.getOffHeapTable().close();
                }
            }
        } finally {
            scanner.close();
        }
    }
}
