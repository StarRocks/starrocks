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
import com.starrocks.common.Pair;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;

import java.util.List;

public class DeltaLakeCacheSizeEstimator {
    // Checkpoint cache size estimation
    public static long estimateCheckpointByStructure(
            Pair<DeltaLakeFileStatus, StructType> key,
            List<ColumnarBatch> value) {
        long size = 0;

        // Estimate key size
        if (key != null) {
            // DeltaLakeFileStatus size
            size += 48; // Object overhead
            if (key.first != null && key.first.getPath() != null) {
                size += key.first.getPath().length() * 2L; // String characters (UTF-16)
                size += 40; // String object overhead
            }

            // StructType size
            size += 64; // Base object overhead
            if (key.second != null) {
                size += 48; // StructType fields
            }
        }

        // Estimate value size (List<ColumnarBatch>)
        if (value != null) {
            size += 40; // ArrayList overhead
            int batchCount = value.size();
            size += batchCount * 8L; // ArrayList references

            for (ColumnarBatch batch : value) {
                size += estimateColumnarBatchStructure(batch);
            }
        }

        return size;
    }

    private static long estimateColumnarBatchStructure(ColumnarBatch batch) {
        if (batch == null) {
            return 0;
        }

        long size = 64; // Base ColumnarBatch overhead

        // Basic info we can get from any ColumnarBatch
        int batchSize = batch.getSize();
        StructType schema = batch.getSchema();

        // Estimate schema size
        if (schema != null) {
            size += estimateSchemaSize(schema);
        }

        // Estimate based on batch type and characteristics
        if (batch instanceof io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch) {
            // Row-based batches typically store data as List<Row>
            size += estimateRowBasedBatchSize(batchSize, schema);
        } else if (batch instanceof io.delta.kernel.defaults.internal.data.DefaultColumnarBatch) {
            // True columnar batch - estimate column vectors
            size += estimateTrueColumnarBatchSize(batch, batchSize);
        } else {
            // Unknown implementation - use conservative estimate
            size += batchSize * 384L; // 384 bytes per row (conservative)
        }

        return size;
    }

    private static long estimateSchemaSize(StructType schema) {
        long size = 64; // Base StructType overhead

        // Estimate field sizes
        List<StructField> fields = schema.fields();
        size += fields.size() * 96L; // Each field overhead

        // Add string sizes for field names
        for (StructField field : fields) {
            if (field.getName() != null) {
                size += field.getName().length() * 2L + 40; // UTF-16 string
            }
            // Estimate data type size
            size += estimateDataTypeSize(field.getDataType());
        }

        return size;
    }

    private static long estimateDataTypeSize(DataType dataType) {
        if (dataType == null) {
            return 0;
        }

        long size = 32; // Base DataType overhead

        // Add size based on type complexity
        if (dataType instanceof StructType) {
            // Nested struct
            size += estimateSchemaSize((StructType) dataType);
        } else if (dataType instanceof ArrayType) {
            // Array type
            size += 24; // ArrayType overhead
            size += estimateDataTypeSize(((ArrayType) dataType).getElementType());
        } else if (dataType instanceof MapType) {
            // Map type
            size += 32; // MapType overhead
            size += estimateDataTypeSize(((MapType) dataType).getKeyType());
            size += estimateDataTypeSize(((MapType) dataType).getValueType());
        }

        return size;
    }

    private static long estimateRowBasedBatchSize(int batchSize, StructType schema) {
        long size = 0;

        // ArrayList overhead for storing rows
        size += 48; // ArrayList object
        size += batchSize * 8L; // References array

        // Estimate per-row overhead
        int fieldCount = schema.length();
        long perRowOverhead = 48; // Row object + field array
        perRowOverhead += fieldCount * 24L; // Field references and metadata

        // Add data size based on field types
        for (StructField field : schema.fields()) {
            perRowOverhead += estimateFieldDataSize(field.getDataType());
        }

        size += perRowOverhead * batchSize;

        return size;
    }

    private static long estimateFieldDataSize(DataType dataType) {
        if (dataType == null) {
            return 8; // Reference size
        }

        // Conservative estimates based on data type
        // Conservative estimates based on data type
        if (dataType instanceof BooleanType) {
            return 2;
        } else if (dataType instanceof ByteType) {
            return 2;
        } else if (dataType instanceof ShortType) {
            return 4;
        } else if (dataType instanceof IntegerType) {
            return 8;
        } else if (dataType instanceof LongType) {
            return 16;
        } else if (dataType instanceof FloatType) {
            return 16;
        } else if (dataType instanceof DoubleType) {
            return 24;
        } else if (dataType instanceof StringType) {
            return 64;
        } else if (dataType instanceof DecimalType) {
            return 32;
        } else if (dataType instanceof DateType) {
            return 16;
        } else if (dataType instanceof TimestampType) {
            return 24;
        } else if (dataType instanceof BinaryType) {
            return 128;
        } else if (dataType instanceof ArrayType) {
            return 128;
        } else if (dataType instanceof MapType) {
            return 256;
        } else if (dataType instanceof StructType) {
            return 256;
        } else {
            return 64;
        }
    }

    private static long estimateTrueColumnarBatchSize(ColumnarBatch batch, int batchSize) {
        long size = 0;

        // Estimate each column vector
        StructType schema = batch.getSchema();
        int columnCount = schema.length();

        for (int i = 0; i < columnCount; i++) {
            try {
                ColumnVector vector = batch.getColumnVector(i);
                if (vector != null) {
                    size += estimateColumnVectorSize(vector, batchSize);
                }
            } catch (Exception e) {
                // If we can't access the vector, estimate conservatively
                size += batchSize * 1024L * 3; // 3K bytes per cell
            }
        }

        return size;
    }

    private static long estimateColumnVectorSize(ColumnVector vector, int size) {
        if (vector == null) {
            return 0;
        }

        long vectorSize = 64; // Base ColumnVector overhead

        // Estimate based on common vector types
        if (vector instanceof io.delta.kernel.defaults.internal.data.vector.DefaultStructVector) {
            // Conservative estimate for structs
            vectorSize += size * 1024L * 5; // Assume 5K bytes average per struct
        } else {
            // Generic estimation
            vectorSize += size * 512L;
        }

        return vectorSize;
    }

    // JSON cache size estimation
    public static long estimateJsonByStructure(
            DeltaLakeFileStatus key,
            List<JsonNode> value) {
        long size = 0;

        // Estimate key size
        if (key != null) {
            size += 32; // Object overhead
            if (key.getPath() != null) {
                size += key.getPath().length() * 2L; // String
                size += 40; // String overhead
            }
        }

        // Estimate value size (List<JsonNode>)
        if (value != null) {
            size += 40; // ArrayList overhead
            int nodeCount = value.size();
            size += nodeCount * 8L; // References

            // Sample to estimate average JsonNode size
            if (!value.isEmpty()) {
                long sampleSize = 0;
                int sampleCount = Math.min(10, nodeCount);
                for (int i = 0; i < sampleCount; i++) {
                    sampleSize += estimateJsonNodeStructure(value.get(i));
                }
                size += (sampleSize / sampleCount) * nodeCount;
            }
        }

        return size;
    }

    private static long estimateJsonNodeStructure(JsonNode node) {
        if (node == null) {
            return 0;
        }

        long size = 40; // Base JsonNode overhead

        if (node.isObject() || node.isArray()) {
            size += 160; // Container overhead
            int childCount = node.size();
            size += childCount * 1024L; // Average child overhead
        } else if (node.isTextual()) {
            String text = node.asText();
            size += text.length() * 2L; // UTF-16 characters
            size += 40; // String overhead
        } else if (node.isBinary()) {
            try {
                size += node.binaryValue().length; // Binary data
            } catch (Exception e) {
                size += 64; // Conservative estimate
            }
        } else {
            size += 32; // Primitive value
        }

        return size;
    }
}
