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
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.StructType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
}
