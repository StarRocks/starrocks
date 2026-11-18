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

import com.starrocks.common.Pair;
import com.starrocks.persist.gson.GsonUtils;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.StructType;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ScanFileUtilsTest {
    private static final StructType ADD_FILE_SCHEMA =
            (StructType) InternalScanFileUtils.SCAN_FILE_SCHEMA_WITH_STATS.get("add").getDataType();
    private static final int TABLE_ROOT_ORDINAL = InternalScanFileUtils.SCAN_FILE_SCHEMA.indexOf("tableRoot");

    @Mocked
    private Metadata metadata;

    @BeforeEach
    public void setUp() {
        new Expectations() {
            {
                metadata.getPartitionColNames();
                result = Collections.emptySet();
                minTimes = 0;

                metadata.getSchema();
                result = new StructType();
                minTimes = 0;
            }
        };
    }

    private static Row buildScanFileRow(String addPath, String tableRoot) {
        Map<Integer, Object> addValues = new HashMap<>();
        if (addPath != null) {
            addValues.put(ADD_FILE_SCHEMA.indexOf("path"), addPath);
        }
        addValues.put(ADD_FILE_SCHEMA.indexOf("size"), 100L);
        addValues.put(ADD_FILE_SCHEMA.indexOf("modificationTime"), 1L);
        addValues.put(ADD_FILE_SCHEMA.indexOf("partitionValues"),
                VectorUtils.stringStringMapValue(new HashMap<>()));
        Row addRow = new GenericRow(ADD_FILE_SCHEMA, addValues);

        Map<Integer, Object> scanValues = new HashMap<>();
        scanValues.put(InternalScanFileUtils.ADD_FILE_ORDINAL, addRow);
        scanValues.put(TABLE_ROOT_ORDINAL, tableRoot);
        return new GenericRow(InternalScanFileUtils.SCAN_FILE_SCHEMA, scanValues);
    }

    @Test
    public void testConvertPreservesEncodedPath() {
        String encodedPath = "col_timestamp=2023-01-01%2001%3A01%3A01/part-00000.snappy.parquet";
        Row scanFileRow = buildScanFileRow(encodedPath, "oss://bucket/db/t");

        Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> result =
                ScanFileUtils.convertFromRowToFileScanTask(false, scanFileRow, metadata, 1, null);

        String path = result.first.getFileStatus().getPath();
        Assertions.assertEquals(
                "oss://bucket/db/t/col_timestamp=2023-01-01%2001%3A01%3A01/part-00000.snappy.parquet", path);
        Assertions.assertNotEquals(InternalScanFileUtils.getAddFileStatus(scanFileRow).getPath(), path);
    }

    @Test
    public void testConvertThrowsWhenPathMissing() {
        Row scanFileRow = buildScanFileRow(null, "oss://bucket/db/t");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ScanFileUtils.convertFromRowToFileScanTask(false, scanFileRow, metadata, 1, null));
    }

    @Test
    public void testConvertReadsSizeAndModificationTime() {
        Row scanFileRow = buildScanFileRow("part-00000.snappy.parquet", "oss://bucket/db/t");
        FileScanTask task = ScanFileUtils.convertFromRowToFileScanTask(false, scanFileRow, metadata, 1, null).first;
        Assertions.assertEquals(100L, task.getFileStatus().getSize());
        Assertions.assertEquals(1L, task.getFileStatus().getModificationTime());
    }

    private static void assertResolvesLikePath(String root, String child) {
        String expected;
        try {
            expected = new Path(root, child).toString();
        } catch (IllegalArgumentException e) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> ScanFileUtils.resolveEncodedPath(root, child, ScanFileUtils.rootPrefix(root)), root + " + " + child);
            return;
        }
        Assertions.assertEquals(expected,
                ScanFileUtils.resolveEncodedPath(root, child, ScanFileUtils.rootPrefix(root)), root + " + " + child);
    }

    @Test
    public void testResolveEncodedPathMatchesHadoopPath() {
        List<String> roots = List.of("oss://bucket/db/t", "oss://bucket/db/t/", "s3a://b/a%20b/t", "s3://bucket",
                "s3://bucket/", "hdfs://nn:8020/warehouse/db.db/t", "file:/tmp/delta/t", "/tmp/delta/t",
                "abfss://c@a.dfs.core.windows.net/p/t",
                "gs://b/x//y/t", "oss://bucket/db/t%3Ax", "oss://bucket/db/t#frag");
        List<String> children = List.of("part-0.parquet", "dt=2024-01-01/part-0.parquet",
                "col_timestamp=2023-01-01%2001%3A01%3A01/part-00000.snappy.parquet", "a%25b/c+d~e.parquet",
                "./part-0.parquet", "../part-0.parquet", "a/./b.parquet", "a/../b.parquet", "a//b.parquet", "/abs/part.parquet",
                "s3://other/part.parquet", "a:b/part.parquet", "a b.parquet", "a#b.parquet", "a?b.parquet", "dir/",
                ".hidden/part.parquet", "..x/part.parquet", "x../part.parquet", "café/part.parquet");
        for (String root : roots) {
            for (String child : children) {
                assertResolvesLikePath(root, child);
            }
        }
        // a root without a path: Path fails on it, so the fast path must not take it
        Assertions.assertNull(ScanFileUtils.rootPrefix("s3://bucket"));
        Assertions.assertEquals("s3://bucket/", ScanFileUtils.rootPrefix("s3://bucket/"));
        Assertions.assertEquals("oss://bucket/db/t/", ScanFileUtils.rootPrefix("oss://bucket/db/t/"));

        // random plain-looking children, including the characters the fast path accepts
        String alphabet = "abcXYZ019._-=%+~/";
        Random random = new Random(7);
        for (int i = 0; i < 20000; i++) {
            StringBuilder sb = new StringBuilder();
            int len = 1 + random.nextInt(24);
            for (int j = 0; j < len; j++) {
                sb.append(alphabet.charAt(random.nextInt(alphabet.length())));
            }
            String child = sb.toString();
            String root = roots.get(random.nextInt(roots.size()));
            assertResolvesLikePath(root, child);
        }
    }

    @Test
    public void testReadNumRecordsMatchesGson() {
        List<String> stats = List.of("{\"numRecords\":12,\"minValues\":{\"a\":1}}",
                "{\"minValues\":{\"a\":{\"x\":[1,2]}},\"numRecords\":34}",
                "{\"numRecords\":\"56\"}", "{\"numRecords\":9223372036854775807}");
        for (String s : stats) {
            Assertions.assertEquals(GsonUtils.GSON.fromJson(s, ScanFileUtils.Records.class).numRecords,
                    ScanFileUtils.readNumRecords(s), s);
        }
        // whatever the shortcut cannot answer exactly is left to the full parse
        for (String s : List.of("{}", "{\"minValues\":{}}", "{\"numRecords\":null}", "null", "[]", "{\"numRecords\":",
                "{\"numRecords\":1.5}", "")) {
            Assertions.assertNull(ScanFileUtils.readNumRecords(s), s);
        }
    }
}
