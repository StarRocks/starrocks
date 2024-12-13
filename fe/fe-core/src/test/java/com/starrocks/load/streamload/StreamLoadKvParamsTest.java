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

package com.starrocks.load.streamload;

import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TPartialUpdateMode;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.starrocks.http.rest.RestBaseAction.WAREHOUSE_KEY;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_BATCH_WRITE_ASYNC;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_BATCH_WRITE_INTERVAL_MS;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_BATCH_WRITE_PARALLEL;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_COLUMNS;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_COLUMN_SEPARATOR;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_COMPRESSION;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_ENABLE_REPLICATED_STORAGE;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_ENCLOSE;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_ESCAPE;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_FORMAT;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_JSONPATHS;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_JSONROOT;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_LOAD_DOP;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_LOAD_MEM_LIMIT;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_LOG_REJECTED_RECORD_NUM;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_MAX_FILTER_RATIO;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_MERGE_CONDITION;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_NEGATIVE;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_PARTIAL_UPDATE;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_PARTIAL_UPDATE_MODE;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_PARTITIONS;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_ROW_DELIMITER;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_SKIP_HEADER;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_STRICT_MODE;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_STRIP_OUTER_ARRAY;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_TEMP_PARTITIONS;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_TIMEOUT;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_TIMEZONE;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_TRANSMISSION_COMPRESSION_TYPE;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_TRIM_SPACE;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_WHERE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link StreamLoadKvParams}. */
public class StreamLoadKvParamsTest extends StreamLoadParamsTestBase {

    @Override
    protected StreamLoadParams buildFileFormatType(TFileFormatType expected) {
        return buildParams(HTTP_FORMAT, expected == null ? null : getFormatKey(expected));
    }

    private String getFormatKey(TFileFormatType formatType) {
        switch (formatType) {
            case FORMAT_CSV_PLAIN:
                return "csv";
            case FORMAT_JSON:
                return "json";
            case FORMAT_CSV_GZ:
                return "gzip";
            case FORMAT_CSV_BZ2:
                return "bzip2";
            case FORMAT_CSV_LZ4_FRAME:
                return "lz4";
            case FORMAT_CSV_DEFLATE:
                return "deflate";
            case FORMAT_CSV_ZSTD:
                return "zstd";
            default:
                return null;
        }
    }

    @Override
    protected StreamLoadParams buildFileType(TFileType expected) {
        return buildParams(HTTP_FORMAT, expected == null ? null : "csv");
    }

    @Override
    protected StreamLoadParams buildFilePath(String expected) {
        return new StreamLoadKvParams(Collections.emptyMap());
    }

    @Override
    protected StreamLoadParams buildColumns(String expected) {
        return buildParams(HTTP_COLUMNS, expected);
    }

    @Override
    protected StreamLoadParams buildWhere(String expected) {
        return buildParams(HTTP_WHERE, expected);
    }

    @Test
    @Override
    public void testPartitions() {
        StreamLoadParams param1 = new StreamLoadKvParams(Collections.emptyMap());
        assertFalse(param1.getPartitions().isPresent());

        StreamLoadParams param2 = buildParams(HTTP_PARTITIONS, "p0");
        assertEquals("p0", param2.getPartitions().orElse(null));

        StreamLoadParams param3 = buildParams(HTTP_TEMP_PARTITIONS, "p1");
        assertEquals("p1", param3.getPartitions().orElse(null));

        Map<String, String> map = new HashMap<>();
        map.put(HTTP_PARTITIONS, "p0");
        map.put(HTTP_TEMP_PARTITIONS, "p1");
        StreamLoadParams param4 = new StreamLoadKvParams(map);
        try {
            param4.getPartitions();
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Can not specify both partitions and temporary partitions"));
        }
    }

    @Override
    protected StreamLoadParams buildPartitions(String expected) {
        throw new UnsupportedOperationException();
    }

    @Test
    @Override
    public void testIsTempPartition() {
        StreamLoadParams param1 = new StreamLoadKvParams(Collections.emptyMap());
        assertFalse(param1.getIsTempPartition().isPresent());

        StreamLoadParams param2 = buildParams(HTTP_PARTITIONS, "p0");
        assertFalse(param2.getIsTempPartition().get());

        StreamLoadParams param3 = buildParams(HTTP_TEMP_PARTITIONS, "p1");
        assertTrue(param3.getIsTempPartition().get());

        Map<String, String> map = new HashMap<>();
        map.put(HTTP_PARTITIONS, "p0");
        map.put(HTTP_TEMP_PARTITIONS, "p1");
        StreamLoadParams param4 = new StreamLoadKvParams(map);
        try {
            param4.getIsTempPartition();
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Can not specify both partitions and temporary partitions"));
        }
    }

    @Override
    protected StreamLoadParams buildIsTempPartition(Boolean expected) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected StreamLoadParams buildNegative(Boolean expected) {
        return buildParams(HTTP_NEGATIVE, expected == null ? null : String.valueOf(expected));
    }

    @Override
    protected StreamLoadParams buildMaxFilterRatio(Double expected) {
        return buildParams(HTTP_MAX_FILTER_RATIO, expected == null ? null : String.valueOf(expected));
    }

    @Override
    protected StreamLoadParams buildTimeout(Integer expected) {
        return buildParams(HTTP_TIMEOUT, expected == null ? null : String.valueOf(expected));
    }

    @Override
    protected StreamLoadParams buildStrictMode(Boolean expected) {
        return buildParams(HTTP_STRICT_MODE, expected == null ? null : String.valueOf(expected));
    }

    @Override
    protected StreamLoadParams buildTimezone(String expected) {
        return buildParams(HTTP_TIMEZONE, expected);
    }

    @Override
    protected StreamLoadParams buildLoadMemLimit(Long expected) {
        return buildParams(HTTP_LOAD_MEM_LIMIT, expected == null ? null : String.valueOf(expected));
    }

    @Override
    protected StreamLoadParams buildTransmissionCompressionType(String expected) {
        return buildParams(HTTP_TRANSMISSION_COMPRESSION_TYPE, expected);
    }

    @Override
    protected StreamLoadParams buildLoadDop(Integer expected) {
        return buildParams(HTTP_LOAD_DOP, expected == null ? null : String.valueOf(expected));
    }

    @Override
    protected StreamLoadParams buildEnableReplicatedStorage(Boolean expected) {
        return buildParams(HTTP_ENABLE_REPLICATED_STORAGE, expected == null ? null : String.valueOf(expected));
    }

    @Override
    protected StreamLoadParams buildMergeCondition(String expected) {
        return buildParams(HTTP_MERGE_CONDITION, expected);
    }

    @Override
    protected StreamLoadParams buildLogRejectedRecordNum(Long expected) {
        return buildParams(HTTP_LOG_REJECTED_RECORD_NUM, expected == null ? null : String.valueOf(expected));
    }

    @Override
    protected StreamLoadParams buildPartialUpdate(Boolean expected) {
        return buildParams(HTTP_PARTIAL_UPDATE, expected == null ? null : String.valueOf(expected));
    }

    @Override
    protected StreamLoadParams buildPartialUpdateMode(TPartialUpdateMode expected) {
        String mode = null;
        if (expected == TPartialUpdateMode.COLUMN_UPSERT_MODE) {
            mode = "column";
        } else if (expected == TPartialUpdateMode.AUTO_MODE) {
            mode = "auto";
        } else if (expected == TPartialUpdateMode.ROW_MODE) {
            mode = "row";
        }
        return buildParams(HTTP_PARTIAL_UPDATE_MODE, mode);
    }

    @Override
    protected StreamLoadParams buildPayloadCompressionType(String expected) {
        return buildParams(HTTP_COMPRESSION, expected);
    }

    @Override
    protected StreamLoadParams buildWarehouse(String expected) {
        return buildParams(WAREHOUSE_KEY, expected);
    }

    @Override
    protected StreamLoadParams buildColumnSeparator(String expected) {
        return buildParams(HTTP_COLUMN_SEPARATOR, expected);
    }

    @Override
    protected StreamLoadParams buildRowDelimiter(String expected) {
        return buildParams(HTTP_ROW_DELIMITER, expected);
    }

    @Override
    protected StreamLoadParams buildSkipHeader(Long expected) {
        return buildParams(HTTP_SKIP_HEADER, expected == null ? null : String.valueOf(expected));
    }

    @Override
    protected StreamLoadParams buildEnclose(Byte expected) {
        return buildParams(HTTP_ENCLOSE, expected == null ? null : new String(new byte[] {expected}));
    }

    @Override
    protected StreamLoadParams buildEscape(Byte expected) {
        return buildParams(HTTP_ESCAPE, expected == null ? null : new String(new byte[] {expected}));
    }

    @Override
    protected StreamLoadParams buildTrimSpace(Boolean expected) {
        return buildParams(HTTP_TRIM_SPACE, expected == null ? null : String.valueOf(expected));
    }

    @Override
    protected StreamLoadParams buildJsonPaths(String expected) {
        return buildParams(HTTP_JSONPATHS, expected);
    }

    @Override
    protected StreamLoadParams buildJsonRoot(String expected) {
        return buildParams(HTTP_JSONROOT, expected);
    }

    @Override
    protected StreamLoadParams buildStripOuterArray(Boolean expected) {
        return buildParams(HTTP_STRIP_OUTER_ARRAY, expected == null ? null : String.valueOf(expected));
    }

    private StreamLoadParams buildParams(String key, String value) {
        if (key == null || value == null) {
            return new StreamLoadKvParams(Collections.emptyMap());
        }
        return new StreamLoadKvParams(Collections.singletonMap(key, value));
    }

    @Test
    public void testBatchWrite() {
        {
            StreamLoadKvParams params = new StreamLoadKvParams(new HashMap<>());
            assertFalse(params.getEnableBatchWrite().isPresent());
            assertFalse(params.getBatchWriteAsync().isPresent());
            assertFalse(params.getBatchWriteIntervalMs().isPresent());
            assertFalse(params.getBatchWriteParallel().isPresent());
        }

        {
            Map<String, String> map = new HashMap<>();
            map.put(HTTP_ENABLE_BATCH_WRITE, "true");
            map.put(HTTP_BATCH_WRITE_ASYNC, "true");
            map.put(HTTP_BATCH_WRITE_INTERVAL_MS, "1000");
            map.put(HTTP_BATCH_WRITE_PARALLEL, "4");
            StreamLoadKvParams params = new StreamLoadKvParams(map);
            assertEquals(true, params.getEnableBatchWrite().orElse(null));
            assertEquals(true, params.getBatchWriteAsync().orElse(null));
            assertEquals(Integer.valueOf(1000), params.getBatchWriteIntervalMs().orElse(null));
            assertEquals(Integer.valueOf(4), params.getBatchWriteParallel().orElse(null));
        }
    }

    @Test
    public void testHashCodeAndEquals() {
        Map<String, String> map1 = new HashMap<>();
        map1.put(HTTP_FORMAT, "csv");
        map1.put(HTTP_COLUMN_SEPARATOR, "|");
        map1.put(HTTP_COLUMNS, "c0");
        StreamLoadKvParams params1 = new StreamLoadKvParams(map1);

        Map<String, String> map2 = new HashMap<>();
        map2.put(HTTP_COLUMNS, "c0");
        map2.put(HTTP_COLUMN_SEPARATOR, "|");
        map2.put(HTTP_FORMAT, "csv");
        StreamLoadKvParams params2 = new StreamLoadKvParams(map2);

        Map<String, String> map3 = new HashMap<>();
        map3.put(HTTP_FORMAT, "csv");
        map3.put(HTTP_COLUMN_SEPARATOR, ",");
        map3.put(HTTP_COLUMNS, "c0");
        StreamLoadKvParams params3 = new StreamLoadKvParams(map3);

        assertEquals(params1.hashCode(), params2.hashCode());
        assertEquals(params1, params2);

        assertNotEquals(params1.hashCode(), params3.hashCode());
        assertNotEquals(params1, params3);
    }
}
