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
import io.netty.handler.codec.http.HttpHeaders;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_HEADER_LIST;
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

/**
 * An implementation of {@link StreamLoadParams} which is backed by a map
 * of key-value pairs. It's used when want to retrieve stream load parameters
 * from http request headers.
 */
public class StreamLoadKvParams implements StreamLoadParams {

    // A map of key-value pairs to describe parameters.
    // The key is defined by {@link StreamLoadHttpHeader}
    private final HashMap<String, String> params;

    public StreamLoadKvParams(Map<String, String> params) {
        this.params = new HashMap<>(params);
    }

    @Override
    public Optional<TFileFormatType> getFileFormatType() {
        if (!params.containsKey(HTTP_FORMAT)) {
            return Optional.empty();
        }
        String format = params.get(HTTP_FORMAT);
        TFileFormatType formatType = parseStreamLoadFormat(format);
        if (formatType == TFileFormatType.FORMAT_UNKNOWN) {
            throw new RuntimeException("Unknown data format " + format);
        }
        return Optional.ofNullable(formatType);
    }

    // Keep consistent with stream_load.cpp parse_format
    private TFileFormatType parseStreamLoadFormat(String formatKey) {
        if (formatKey.equalsIgnoreCase("csv")) {
            return TFileFormatType.FORMAT_CSV_PLAIN;
        } else if (formatKey.equalsIgnoreCase("json")) {
            return TFileFormatType.FORMAT_JSON;
        } else if (formatKey.equalsIgnoreCase("gzip")) {
            return TFileFormatType.FORMAT_CSV_GZ;
        } else if (formatKey.equalsIgnoreCase("bzip2")) {
            return TFileFormatType.FORMAT_CSV_BZ2;
        } else if (formatKey.equalsIgnoreCase("lz4")) {
            return TFileFormatType.FORMAT_CSV_LZ4_FRAME;
        } else if (formatKey.equalsIgnoreCase("deflate")) {
            return TFileFormatType.FORMAT_CSV_DEFLATE;
        } else if (formatKey.equalsIgnoreCase("zstd")) {
            return TFileFormatType.FORMAT_CSV_ZSTD;
        }
        return TFileFormatType.FORMAT_UNKNOWN;
    }

    @Override
    public Optional<TFileType> getFileType() {
        Optional<TFileFormatType> formatType = getFileFormatType();
        return formatType.isPresent() ? Optional.of(TFileType.FILE_STREAM) : Optional.empty();
    }

    @Override
    public Optional<String> getFilePath() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getColumns() {
        return Optional.ofNullable(params.get(HTTP_COLUMNS));
    }

    @Override
    public Optional<String> getWhere() {
        return Optional.ofNullable(params.get(HTTP_WHERE));
    }

    @Override
    public Optional<String> getPartitions() {
        String partitions = params.get(HTTP_PARTITIONS);
        String tempPartitions = params.get(HTTP_TEMP_PARTITIONS);
        if (partitions != null && tempPartitions != null) {
            throw new RuntimeException("Can not specify both partitions and temporary partitions");
        }
        return Optional.ofNullable(partitions != null ? partitions : tempPartitions);
    }

    @Override
    public Optional<Boolean> getIsTempPartition() {
        Optional<String> partitions = getPartitions();
        if (partitions.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(params.containsKey(HTTP_TEMP_PARTITIONS));
    }

    @Override
    public Optional<Boolean> getNegative() {
        return getBoolParam(HTTP_NEGATIVE);
    }

    @Override
    public Optional<Double> getMaxFilterRatio() {
        String maxFilterRatio = params.get(HTTP_MAX_FILTER_RATIO);
        if (maxFilterRatio == null) {
            return Optional.empty();
        }
        return Optional.of(Double.parseDouble(maxFilterRatio));
    }

    @Override
    public Optional<Integer> getTimeout() {
        return getIntParam(HTTP_TIMEOUT);
    }

    @Override
    public Optional<Boolean> getStrictMode() {
        return getBoolParam(HTTP_STRICT_MODE);
    }

    @Override
    public Optional<String> getTimezone() {
        return Optional.ofNullable(params.get(HTTP_TIMEZONE));
    }

    @Override
    public Optional<Long> getLoadMemLimit() {
        return getLongParam(HTTP_LOAD_MEM_LIMIT);
    }

    @Override
    public Optional<String> getTransmissionCompressionType() {
        return Optional.ofNullable(params.get(HTTP_TRANSMISSION_COMPRESSION_TYPE));
    }

    @Override
    public Optional<Integer> getLoadDop() {
        return getIntParam(HTTP_LOAD_DOP);
    }

    @Override
    public Optional<Boolean> getEnableReplicatedStorage() {
        return getBoolParam(HTTP_ENABLE_REPLICATED_STORAGE);
    }

    @Override
    public Optional<String> getMergeCondition() {
        return Optional.ofNullable(params.get(HTTP_MERGE_CONDITION));
    }

    @Override
    public Optional<Long> getLogRejectedRecordNum() {
        return getLongParam(HTTP_LOG_REJECTED_RECORD_NUM);
    }

    @Override
    public Optional<Boolean> getPartialUpdate() {
        return getBoolParam(HTTP_PARTIAL_UPDATE);
    }

    @Override
    public Optional<TPartialUpdateMode> getPartialUpdateMode() {
        String partialUpdateMode = params.get(HTTP_PARTIAL_UPDATE_MODE);
        if (partialUpdateMode == null) {
            return Optional.empty();
        }
        TPartialUpdateMode mode = null;
        switch (partialUpdateMode) {
            case "column":
                mode = TPartialUpdateMode.COLUMN_UPSERT_MODE;
                break;
            case "auto":
                mode = TPartialUpdateMode.AUTO_MODE;
                break;
            case "row":
                mode = TPartialUpdateMode.ROW_MODE;
                break;
        }
        return Optional.ofNullable(mode);
    }

    @Override
    public Optional<String> getPayloadCompressionType() {
        return Optional.ofNullable(params.get(HTTP_COMPRESSION));
    }

    @Override
    public Optional<String> getWarehouse() {
        return Optional.ofNullable(params.get(WAREHOUSE_KEY));
    }

    @Override
    public Optional<String> getColumnSeparator() {
        return Optional.ofNullable(params.get(HTTP_COLUMN_SEPARATOR));
    }

    @Override
    public Optional<String> getRowDelimiter() {
        return Optional.ofNullable(params.get(HTTP_ROW_DELIMITER));
    }

    @Override
    public Optional<Long> getSkipHeader() {
        return getLongParam(HTTP_SKIP_HEADER);
    }

    @Override
    public Optional<Byte> getEnclose() {
        String enclose = params.get(HTTP_ENCLOSE);
        if (enclose == null || enclose.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of((byte) enclose.charAt(0));
    }

    @Override
    public Optional<Byte> getEscape() {
        String escape = params.get(HTTP_ESCAPE);
        if (escape == null || escape.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of((byte) escape.charAt(0));
    }

    @Override
    public Optional<Boolean> getTrimSpace() {
        return getBoolParam(HTTP_TRIM_SPACE);
    }

    @Override
    public Optional<String> getJsonPaths() {
        return Optional.ofNullable(params.get(HTTP_JSONPATHS));
    }

    @Override
    public Optional<String> getJsonRoot() {
        return Optional.ofNullable(params.get(HTTP_JSONROOT));
    }

    @Override
    public Optional<Boolean> getStripOuterArray() {
        return getBoolParam(HTTP_STRIP_OUTER_ARRAY);
    }

    public Optional<Boolean> getEnableBatchWrite() {
        return getBoolParam(HTTP_ENABLE_BATCH_WRITE);
    }

    public Optional<Boolean> getBatchWriteAsync() {
        return getBoolParam(HTTP_BATCH_WRITE_ASYNC);
    }

    public Optional<Integer> getBatchWriteIntervalMs() {
        return getIntParam(HTTP_BATCH_WRITE_INTERVAL_MS);
    }

    public Optional<Integer> getBatchWriteParallel() {
        return getIntParam(HTTP_BATCH_WRITE_PARALLEL);
    }

    private Optional<Boolean> getBoolParam(String paramName) {
        String value = params.get(paramName);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(Boolean.parseBoolean(value));
    }

    private Optional<Integer> getIntParam(String paramName) {
        String value = params.get(paramName);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(Integer.parseInt(value));
    }

    private Optional<Long> getLongParam(String paramName) {
        String value = params.get(paramName);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(Long.parseLong(value));
    }

    public Map<String, String> toMap() {
        return Collections.unmodifiableMap(params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StreamLoadKvParams that = (StreamLoadKvParams) o;
        return params.equals(that.params);
    }

    @Override
    public String toString() {
        return "StreamLoadKvParams{" +
                "params=" + params +
                '}';
    }

    public static StreamLoadKvParams fromHttpHeaders(HttpHeaders httpHeaders) {
        Map<String, String> params = new HashMap<>();
        for (String header : HTTP_HEADER_LIST) {
            if (httpHeaders.contains(header)) {
                params.put(header, httpHeaders.get(header));
            }
        }
        return new StreamLoadKvParams(params);
    }
}
