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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.common.Pair;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.FileStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class DeltaLakeCacheSizeEstimatorTest {

    @Test
    public void testEstimateCheckpointByStructure() {
        // Create sample key
        FileStatus fileStatus = FileStatus.of("s3://bucket/test.parquet", 1024, 123);
        DeltaLakeFileStatus deltaStatus = DeltaLakeFileStatus.of(fileStatus);
        StructType schema = new StructType()
                .add("col1", IntegerType.INTEGER)
                .add("col2", StringType.STRING)
                .add("col3", LongType.LONG);
        Pair<DeltaLakeFileStatus, StructType> key = Pair.create(deltaStatus, schema);

        // Create empty value
        List<ColumnarBatch> value = new ArrayList<>();

        // Estimate size
        long size = DeltaLakeCacheSizeEstimator.estimateCheckpointByStructure(key, value);

        // Verify size is positive
        assertTrue(size > 0);
    }

    @Test
    public void testEstimateCheckpointWithBatches() {
        // Create sample key
        FileStatus fileStatus = FileStatus.of("s3://bucket/test.parquet", 1024, 123);
        DeltaLakeFileStatus deltaStatus = DeltaLakeFileStatus.of(fileStatus);
        StructType schema = new StructType()
                .add("col1", IntegerType.INTEGER)
                .add("col2", StringType.STRING);
        Pair<DeltaLakeFileStatus, StructType> key = Pair.create(deltaStatus, schema);

        // Create sample ColumnarBatch value
        List<ColumnarBatch> value = new ArrayList<>();

        // Add some batches (this is a simplified test)
        // In practice, these would be real ColumnarBatch objects created by DeltaKernel

        // Estimate size
        long size = DeltaLakeCacheSizeEstimator.estimateCheckpointByStructure(key, value);
        assertTrue(size > 0);
    }

    @Test
    public void testEstimateCheckpointByStructureNullKey() {
        // Test with null key
        List<ColumnarBatch> value = new ArrayList<>();
        long size = DeltaLakeCacheSizeEstimator.estimateCheckpointByStructure(null, value);
        assertTrue(size >= 0);
    }

    @Test
    public void testEstimateCheckpointByStructureNullValue() {
        // Create sample key
        FileStatus fileStatus = FileStatus.of("s3://bucket/test.parquet", 1024, 123);
        DeltaLakeFileStatus deltaStatus = DeltaLakeFileStatus.of(fileStatus);
        StructType schema = new StructType().add("col1", IntegerType.INTEGER);
        Pair<DeltaLakeFileStatus, StructType> key = Pair.create(deltaStatus, schema);

        // Test with null value
        long size = DeltaLakeCacheSizeEstimator.estimateCheckpointByStructure(key, null);
        assertTrue(size > 0);
    }

    @Test
    public void testEstimateJsonByStructure() {
        // Create sample key
        FileStatus fileStatus = FileStatus.of("s3://bucket/test.json", 512, 456);
        DeltaLakeFileStatus deltaStatus = DeltaLakeFileStatus.of(fileStatus);

        // Create sample JSON values
        List<JsonNode> jsonNodes = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();

        try {
            // Create sample JSON objects
            JsonNode node1 = mapper.readTree("{\"id\": 1, \"name\": \"test\", \"value\": 123}");
            JsonNode node2 = mapper.readTree("{\"id\": 2, \"name\": \"test2\", \"value\": 456}");
            jsonNodes.add(node1);
            jsonNodes.add(node2);
        } catch (Exception e) {
            fail("Failed to create JSON nodes: " + e.getMessage());
        }

        // Estimate size
        long size = DeltaLakeCacheSizeEstimator.estimateJsonByStructure(deltaStatus, jsonNodes);

        assertTrue(size > 0);
    }

    @Test
    public void testEstimateJsonByStructureNullKey() {
        // Create sample JSON values
        List<JsonNode> jsonNodes = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();

        try {
            jsonNodes.add(mapper.readTree("{\"id\": 1}"));
        } catch (Exception e) {
            fail("Failed to create JSON nodes: " + e.getMessage());
        }

        // Test with null key
        long size = DeltaLakeCacheSizeEstimator.estimateJsonByStructure(null, jsonNodes);
        assertTrue(size > 0);
    }

    @Test
    public void testEstimateJsonWithEmptyArray() {
        // Create sample key
        FileStatus fileStatus = FileStatus.of("s3://bucket/test.json", 512, 456);
        DeltaLakeFileStatus deltaStatus = DeltaLakeFileStatus.of(fileStatus);

        // Test with empty JSON list
        List<JsonNode> emptyNodes = new ArrayList<>();
        long size = DeltaLakeCacheSizeEstimator.estimateJsonByStructure(deltaStatus, emptyNodes);
        assertTrue(size > 0);
    }

    @Test
    public void testEstimateComplexSchema() {
        // Create complex schema
        StructType complexSchema = new StructType()
                .add("id", IntegerType.INTEGER)
                .add("name", StringType.STRING)
                .add("price", DecimalType.USER_DEFAULT)
                .add("created_at", TimestampType.TIMESTAMP)
                .add("metadata", new StructType()
                        .add("author", StringType.STRING)
                        .add("version", IntegerType.INTEGER))
                .add("tags", new ArrayType(StringType.STRING, true));

        FileStatus fileStatus = FileStatus.of("s3://bucket/test.parquet", 1024, 123);
        DeltaLakeFileStatus deltaStatus = DeltaLakeFileStatus.of(fileStatus);
        Pair<DeltaLakeFileStatus, StructType> key = Pair.create(deltaStatus, complexSchema);

        List<ColumnarBatch> value = new ArrayList<>();

        long size = DeltaLakeCacheSizeEstimator.estimateCheckpointByStructure(key, value);
        assertTrue(size > 0);
    }

    @Test
    public void testEstimatePrimitiveTypes() {
        // Test all primitive types
        StructType primitiveSchema = new StructType()
                .add("bool_col", BooleanType.BOOLEAN)
                .add("byte_col", ByteType.BYTE)
                .add("short_col", ShortType.SHORT)
                .add("int_col", IntegerType.INTEGER)
                .add("long_col", LongType.LONG)
                .add("float_col", FloatType.FLOAT)
                .add("double_col", DoubleType.DOUBLE)
                .add("string_col", StringType.STRING)
                .add("binary_col", BinaryType.BINARY)
                .add("date_col", DateType.DATE)
                .add("timestamp_col", TimestampType.TIMESTAMP);

        FileStatus fileStatus = FileStatus.of("s3://bucket/test.parquet", 1024, 123);
        DeltaLakeFileStatus deltaStatus = DeltaLakeFileStatus.of(fileStatus);
        Pair<DeltaLakeFileStatus, StructType> key = Pair.create(deltaStatus, primitiveSchema);

        List<ColumnarBatch> value = new ArrayList<>();

        long size = DeltaLakeCacheSizeEstimator.estimateCheckpointByStructure(key, value);
        assertTrue(size > 0);
    }

    @Test
    public void testEstimateNestedStructures() {
        // Test deeply nested structures
        StructType deeplyNested = new StructType()
                .add("level1", new StructType()
                        .add("level2", new StructType()
                                .add("level3", new StructType()
                                        .add("value", StringType.STRING))));

        FileStatus fileStatus = FileStatus.of("s3://bucket/test.parquet", 1024, 123);
        DeltaLakeFileStatus deltaStatus = DeltaLakeFileStatus.of(fileStatus);
        Pair<DeltaLakeFileStatus, StructType> key = Pair.create(deltaStatus, deeplyNested);

        List<ColumnarBatch> value = new ArrayList<>();

        long size = DeltaLakeCacheSizeEstimator.estimateCheckpointByStructure(key, value);
        assertTrue(size > 0);
    }

    @Test
    public void testJsonComplexStructure() {
        // Create complex JSON structure
        ObjectMapper mapper = new ObjectMapper();
        FileStatus fileStatus = FileStatus.of("s3://bucket/test.json", 512, 456);
        DeltaLakeFileStatus deltaStatus = DeltaLakeFileStatus.of(fileStatus);

        try {
            String complexJson = "{\"id\": 1, \"user\": {\"name\": \"John\", \"age\": 30}, \"items\": [1, 2, 3], \"active\": true}";
            JsonNode node = mapper.readTree(complexJson);
            List<JsonNode> jsonNodes = new ArrayList<>();
            jsonNodes.add(node);

            long size = DeltaLakeCacheSizeEstimator.estimateJsonByStructure(deltaStatus, jsonNodes);
            assertTrue(size > 0);
        } catch (Exception e) {
            fail("Failed to parse complex JSON: " + e.getMessage());
        }
    }
}
