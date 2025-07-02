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

package com.starrocks.load.streamload;

import java.util.Arrays;
import java.util.List;

import static com.starrocks.http.rest.RestBaseAction.WAREHOUSE_KEY;

/**
 * Http header names for stream load. They should be consistent
 * with those in http_common.h
 */
public class StreamLoadHttpHeader {

    public static final String HTTP_FORMAT = "format";
    public static final String HTTP_COLUMNS = "columns";
    public static final String HTTP_WHERE = "where";
    public static final String HTTP_MAX_FILTER_RATIO = "max_filter_ratio";
    public static final String HTTP_TIMEOUT = "timeout";
    public static final String HTTP_PARTITIONS = "partitions";
    public static final String HTTP_TEMP_PARTITIONS = "temporary_partitions";
    public static final String HTTP_NEGATIVE = "negative";
    public static final String HTTP_STRICT_MODE = "strict_mode";
    public static final String HTTP_TIMEZONE = "timezone";
    public static final String HTTP_LOAD_MEM_LIMIT = "load_mem_limit";
    public static final String HTTP_PARTIAL_UPDATE = "partial_update";
    public static final String HTTP_PARTIAL_UPDATE_MODE = "partial_update_mode";
    public static final String HTTP_TRANSMISSION_COMPRESSION_TYPE = "transmission_compression_type";
    public static final String HTTP_LOAD_DOP = "load_dop";
    public static final String HTTP_ENABLE_REPLICATED_STORAGE = "enable_replicated_storage";
    public static final String HTTP_MERGE_CONDITION = "merge_condition";
    public static final String HTTP_LOG_REJECTED_RECORD_NUM = "log_rejected_record_num";
    public static final String HTTP_COMPRESSION = "compression";
    public static final String HTTP_WAREHOUSE = WAREHOUSE_KEY;

    // Headers for csv format ============================
    public static final String HTTP_COLUMN_SEPARATOR = "column_separator";
    public static final String HTTP_ROW_DELIMITER = "row_delimiter";
    public static final String HTTP_SKIP_HEADER = "skip_header";
    public static final String HTTP_TRIM_SPACE = "trim_space";
    public static final String HTTP_ENCLOSE = "enclose";
    public static final String HTTP_ESCAPE = "escape";

    // Headers for json format ===========================
    public static final String HTTP_JSONPATHS = "jsonpaths";
    public static final String HTTP_JSONROOT = "json_root";
    public static final String HTTP_STRIP_OUTER_ARRAY = "strip_outer_array";

    // Headers for batch write ==========================
    public static final String HTTP_ENABLE_BATCH_WRITE = "enable_merge_commit";
    public static final String HTTP_BATCH_WRITE_ASYNC = "merge_commit_async";
    public static final String HTTP_BATCH_WRITE_INTERVAL_MS = "merge_commit_interval_ms";
    public static final String HTTP_BATCH_WRITE_PARALLEL = "merge_commit_parallel";

    // A list of all headers. If add a new header, should also add it to the list.
    public static final List<String> HTTP_HEADER_LIST = Arrays.asList(
            HTTP_FORMAT, HTTP_COLUMNS, HTTP_WHERE, HTTP_COLUMN_SEPARATOR, HTTP_ROW_DELIMITER, HTTP_SKIP_HEADER,
            HTTP_TRIM_SPACE, HTTP_ENCLOSE, HTTP_ESCAPE, HTTP_MAX_FILTER_RATIO, HTTP_TIMEOUT, HTTP_PARTITIONS,
            HTTP_TEMP_PARTITIONS, HTTP_NEGATIVE, HTTP_STRICT_MODE, HTTP_TIMEZONE, HTTP_LOAD_MEM_LIMIT,
            HTTP_JSONPATHS, HTTP_JSONROOT, HTTP_STRIP_OUTER_ARRAY, HTTP_PARTIAL_UPDATE, HTTP_PARTIAL_UPDATE_MODE,
            HTTP_TRANSMISSION_COMPRESSION_TYPE, HTTP_LOAD_DOP, HTTP_ENABLE_REPLICATED_STORAGE, HTTP_MERGE_CONDITION,
            HTTP_LOG_REJECTED_RECORD_NUM, HTTP_COMPRESSION, HTTP_WAREHOUSE, HTTP_ENABLE_BATCH_WRITE,
            HTTP_BATCH_WRITE_ASYNC, HTTP_BATCH_WRITE_INTERVAL_MS, HTTP_BATCH_WRITE_PARALLEL
    );
}
