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

import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.UserException;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StreamLoadParam {
    private static final Logger LOG = LogManager.getLogger(StreamLoadParam.class);
    private static final String FORMAT_KEY = "format";
    private static final String COLUMNS = "columns";
    private static final String WHERE = "where";
    private static final String COLUMN_SEPARATOR = "column_separator";
    private static final String ROW_DELIMITER = "row_delimiter";
    private static final String PARTITIONS = "partitions";
    private static final String TEMP_PARTITIONS = "temporary_partitions";
    private static final String NEGATIVE = "negative";
    private static final String STRICT_MODE = "strict_mode";
    private static final String TIMEZONE = "timezone";
    private static final String LOAD_MEM_LIMIT = "load_mem_limit";
    private static final String EXEC_MEM_LIMIT = "exec_mem_limit";
    private static final String JSONPATHS = "jsonpaths";
    private static final String JSONROOT = "json_root";
    private static final String IGNORE_JSON_SIZE = "ignore_json_size";
    private static final String STRIP_OUTER_ARRAY = "strip_outer_array";
    private static final String PARTIAL_UPDATE = "partial_update";
    private static final String TRANSMISSION_COMPRESSION_TYPE = "transmission_compression_type";
    private static final String LOAD_DOP = "load_dop";
    private static final String MAX_FILTER_RATIO = "max_filter_ratio";
    private static final String IDLE_TRANSACTION_TIMEOUT = "idle_transaction_timeout";
    private static final String PARTIAL_UPDATE_MODE = "partial_update_mode";

    public TFileFormatType formatType = TFileFormatType.FORMAT_CSV_PLAIN;
    public TFileType fileType = TFileType.FILE_STREAM;
    public int jsonMaxBodyBytes = Config.max_stream_load_batch_size_mb * 1024 * 1024;
    public boolean ignoreJsonMaxSize = false;
    public String columns = null;
    public String whereExpr = null;
    public String columnSeparator = null;
    public String rowDelimiter = null;
    public String partitions = null;
    public String path = null;
    public boolean useTempPartition = false;
    public boolean negative = false;
    public boolean strictMode = false;
    public String timezone = null;
    public long loadMemLimit = 0;
    public long execMemLimit = 0;
    public String jsonPaths = null;
    public String jsonRoot = null;
    public boolean stripOuterArray = false;
    public boolean partialUpdate = false;
    public String transmissionCompressionType = null;
    // load dop always be 1 here
    public int loadDop = 1;
    public double maxFilterRatio = 0.0;
    public int idleTransactionTimeout = 1000;
    public String partialUpdateMode = "row";

    public TFileFormatType parseStreamLoadFormat(String formatKey) {
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

    public StreamLoadParam() {
    }

    public static StreamLoadParam parseHttpHeader(HttpHeaders headers) throws UserException {
        StreamLoadParam context = new StreamLoadParam();

        if (headers.contains(FORMAT_KEY)) {
            String formatKey = headers.get(FORMAT_KEY);
            context.formatType = context.parseStreamLoadFormat(formatKey);
            if (context.formatType == TFileFormatType.FORMAT_UNKNOWN) {
                throw new UserException("unknown data format " + formatKey);
            }
        }

        if (headers.contains(IGNORE_JSON_SIZE)) {
            context.ignoreJsonMaxSize = Boolean.parseBoolean(headers.get(IGNORE_JSON_SIZE));
        }
        if (headers.contains(COLUMNS)) {
            context.columns = headers.get(COLUMNS);
        }
        if (headers.contains(COLUMNS)) {
            context.whereExpr = headers.get(WHERE);
        }
        if (headers.contains(COLUMN_SEPARATOR)) {
            context.columnSeparator = headers.get(COLUMN_SEPARATOR);
        }
        if (headers.contains(ROW_DELIMITER)) {
            context.rowDelimiter = headers.get(ROW_DELIMITER);
        }
        if (headers.contains(PARTITIONS)) {
            context.partitions = headers.get(PARTITIONS);
            context.useTempPartition = false;
            if (headers.contains(TEMP_PARTITIONS)) {
                throw new UserException("Can not specify both partitions and temporary partitions");
            }
        }
        if (headers.contains(TEMP_PARTITIONS)) {
            context.partitions = headers.get(TEMP_PARTITIONS);
            context.useTempPartition = true;
            if (headers.contains(PARTITIONS)) {
                throw new UserException("Can not specify both partitions and temporary partitions");
            }
        }
        if (headers.contains(NEGATIVE)) {
            context.negative = Boolean.parseBoolean(headers.get(NEGATIVE));
        }
        if (headers.contains(STRICT_MODE)) {
            context.strictMode = Boolean.parseBoolean(headers.get(STRICT_MODE));
        }
        if (headers.contains(TIMEZONE)) {
            context.timezone = headers.get(TIMEZONE);
        }
        if (headers.contains(LOAD_MEM_LIMIT)) {
            context.loadMemLimit = Long.parseLong(headers.get(LOAD_MEM_LIMIT));
            if (context.loadMemLimit < 0) {
                throw new UserException("load_mem_limit must be greater than or equal to 0");
            }
        }
        if (headers.contains(EXEC_MEM_LIMIT)) {
            context.loadMemLimit = Long.parseLong(headers.get(EXEC_MEM_LIMIT));
            if (context.loadMemLimit < 0) {
                throw new UserException("exec_mem_limit must be greater than or equal to 0");
            }
        }
        if (headers.contains(JSONPATHS)) {
            context.jsonPaths = headers.get(JSONPATHS);
        }
        if (headers.contains(JSONROOT)) {
            context.jsonRoot = headers.get(JSONROOT);
        }
        if (headers.contains(STRIP_OUTER_ARRAY)) {
            context.stripOuterArray = Boolean.parseBoolean(headers.get(STRIP_OUTER_ARRAY));
        }
        if (headers.contains(PARTIAL_UPDATE)) {
            context.partialUpdate = Boolean.parseBoolean(headers.get(PARTIAL_UPDATE));
        }
        if (headers.contains(TRANSMISSION_COMPRESSION_TYPE)) {
            context.transmissionCompressionType = headers.get(TRANSMISSION_COMPRESSION_TYPE);
        }
        if (headers.contains(MAX_FILTER_RATIO)) {
            context.maxFilterRatio = Double.parseDouble(headers.get(MAX_FILTER_RATIO));
        }
        if (headers.contains(IDLE_TRANSACTION_TIMEOUT)) {
            context.idleTransactionTimeout = Integer.parseInt(headers.get(IDLE_TRANSACTION_TIMEOUT));
            if (context.idleTransactionTimeout <= 0) {
                throw new UserException("idle transaction timeout must be greater than 0");
            }
        }
        if (headers.contains(PARTIAL_UPDATE_MODE)) {
            context.partialUpdateMode = headers.get(PARTIAL_UPDATE_MODE);
        }
        return context;
    }

    public long getExecMemLimit() {
        return 0;
    }
}
