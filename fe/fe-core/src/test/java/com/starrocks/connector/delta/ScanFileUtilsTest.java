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
import com.starrocks.connector.exception.StarRocksConnectorException;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
        return buildScanFileRow(addPath, tableRoot, Map.of(), null);
    }

    private static Row buildScanFileRow(String addPath, String tableRoot, Map<String, String> partitions, String stats) {
        Map<Integer, Object> addValues = new HashMap<>();
        if (addPath != null) {
            addValues.put(ADD_FILE_SCHEMA.indexOf("path"), addPath);
        }
        addValues.put(ADD_FILE_SCHEMA.indexOf("size"), 100L);
        addValues.put(ADD_FILE_SCHEMA.indexOf("modificationTime"), 1L);
        addValues.put(ADD_FILE_SCHEMA.indexOf("partitionValues"),
                VectorUtils.stringStringMapValue(partitions));
        addValues.put(ADD_FILE_SCHEMA.indexOf("stats"), stats);
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
    public void testConverterRetainsSnapshotMappingAndIndependentFileValues() {
        StructType schema = new StructType().add(new StructField("Region", StringType.STRING, true,
                FieldMetadata.builder().putString("delta.columnMapping.physicalName", "col-123").build()));
        new Expectations() {
            {
                metadata.getPartitionColNames();
                result = Set.of("region");
                metadata.getSchema();
                result = schema;
            }
        };
        ScanFileUtils.FileScanTaskConverter original = new ScanFileUtils.FileScanTaskConverter(metadata, 10);
        Map<String, String> nullPartition = new HashMap<>();
        nullPartition.put("col-123", null);
        Row first = buildScanFileRow("part-1.parquet", "s3://bucket/t", Map.of("col-123", "west"), "{\"numRecords\":7}");
        Row second = buildScanFileRow("part-2.parquet", "s3://bucket/t", nullPartition, null);
        DeletionVectorDescriptor dv = new DeletionVectorDescriptor("p", "s3://bucket/dv", Optional.of(0), 8, 3);
        Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> task = original.convert(true, first, dv);
        Assertions.assertEquals(Map.of("region", "west"), task.first.getPartitionValues());
        Assertions.assertEquals(7, task.first.getRecords());
        Assertions.assertEquals(7, task.second.numRecords);
        Assertions.assertSame(dv, task.first.getDv());
        Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> emptyStats = original.convert(false, second, null);
        Assertions.assertTrue(emptyStats.first.getPartitionValues().containsKey("region"));
        Assertions.assertNull(emptyStats.first.getPartitionValues().get("region"));
        Assertions.assertEquals(10, emptyStats.first.getRecords());
        Assertions.assertNull(emptyStats.second);
        Assertions.assertEquals(Map.of("region", "west"), task.first.getPartitionValues());
        new Verifications() {
            {
                metadata.getSchema();
                times = 1;
            }
        };

        // A rename in a later snapshot must not mutate a converter already serving a query.
        StructType renamed = new StructType().add(new StructField("Area", StringType.STRING, true,
                FieldMetadata.builder().putString("delta.columnMapping.physicalName", "col-123").build()));
        new Expectations() {
            {
                metadata.getPartitionColNames();
                result = Set.of("Area");
                metadata.getSchema();
                result = renamed;
            }
        };
        ScanFileUtils.FileScanTaskConverter next = new ScanFileUtils.FileScanTaskConverter(metadata, 10);
        Assertions.assertEquals(Map.of("Area", "west"), next.convert(false, first, null).first.getPartitionValues());
        Assertions.assertEquals(Map.of("region", "west"), original.convert(false, first, null).first.getPartitionValues());
    }

    @Test
    public void testConverterRejectsMissingPartitionColumn() {
        new Expectations() {
            {
                metadata.getPartitionColNames();
                result = Set.of("missing");
            }
        };
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new ScanFileUtils.FileScanTaskConverter(metadata, 1));
    }

}
