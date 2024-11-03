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

#include "runtime/batch_write/batch_write_util.h"

#include "http/http_request.h"
#include "runtime/stream_load/stream_load_context.h"

namespace starrocks {

#define POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, param_name) \
    do {                                                                     \
        if (!http_req->header(param_name).empty()) {                         \
            load_params.emplace(param_name, http_req->header(param_name));   \
        }                                                                    \
    } while (0);

StatusOr<LoadParams> get_batch_write_load_parameters(HttpRequest* http_req, StreamLoadContext* ctx) {
    std::map<std::string, std::string> load_params;
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_FORMAT_KEY);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_COLUMNS);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_WHERE);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_MAX_FILTER_RATIO);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_TIMEOUT);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_PARTITIONS);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_TEMP_PARTITIONS);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_NEGATIVE);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_STRICT_MODE);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_TIMEZONE);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_LOAD_MEM_LIMIT);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_EXEC_MEM_LIMIT);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_PARTIAL_UPDATE);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_PARTIAL_UPDATE_MODE);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_TRANSMISSION_COMPRESSION_TYPE);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_LOAD_DOP);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_MERGE_CONDITION);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_LOG_REJECTED_RECORD_NUM);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_COMPRESSION);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_WAREHOUSE);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_ENABLE_BATCH_WRITE);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_BATCH_WRITE_ASYNC);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_BATCH_WRITE_INTERVAL_MS);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_BATCH_WRITE_PARALLEL);

    // csv format parameters
    if (ctx->format != TFileFormatType::FORMAT_JSON) {
        if (!http_req->header(HTTP_SKIP_HEADER).empty()) {
            return Status::NotSupported("Csv format not support skip header when enable batch write");
        }
    }
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_COLUMN_SEPARATOR);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_ROW_DELIMITER);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_TRIM_SPACE);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_ENCLOSE);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_ESCAPE);

    // json format parameters
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_JSONPATHS);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_JSONROOT);
    POPULATE_LOAD_PARAMETER_FROM_HTTP(http_req, load_params, HTTP_STRIP_OUTER_ARRAY);

    return load_params;
}

#define POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, param_name) \
    do {                                                                        \
        auto it = input_params.find(param_name);                                \
        if (it != input_params.end()) {                                         \
            load_params.emplace(param_name, it->second);                        \
        }                                                                       \
    } while (0);

StatusOr<LoadParams> get_batch_write_load_parameters(const std::map<std::string, std::string>& input_params) {
    std::map<std::string, std::string> load_params;
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_FORMAT_KEY);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_COLUMNS);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_WHERE);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_MAX_FILTER_RATIO);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_TIMEOUT);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_PARTITIONS);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_TEMP_PARTITIONS);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_NEGATIVE);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_STRICT_MODE);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_TIMEZONE);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_LOAD_MEM_LIMIT);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_EXEC_MEM_LIMIT);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_PARTIAL_UPDATE);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_PARTIAL_UPDATE_MODE);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_TRANSMISSION_COMPRESSION_TYPE);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_LOAD_DOP);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_MERGE_CONDITION);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_LOG_REJECTED_RECORD_NUM);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_COMPRESSION);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_WAREHOUSE);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_ENABLE_BATCH_WRITE);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_BATCH_WRITE_ASYNC);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_BATCH_WRITE_INTERVAL_MS);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_BATCH_WRITE_PARALLEL);

    // csv format parameters
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_COLUMN_SEPARATOR);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_ROW_DELIMITER);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_TRIM_SPACE);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_ENCLOSE);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_ESCAPE);

    // json format parameters
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_JSONPATHS);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_JSONROOT);
    POPULATE_LOAD_PARAMETER_FROM_MAP(input_params, load_params, HTTP_STRIP_OUTER_ARRAY);

    return load_params;
}

} // namespace starrocks
