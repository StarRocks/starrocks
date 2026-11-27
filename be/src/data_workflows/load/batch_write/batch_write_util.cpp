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

#include "data_workflows/load/batch_write/batch_write_util.h"

#include <fmt/format.h>

#include <boost/algorithm/string/predicate.hpp>
#include <limits>
#include <vector>

#include "base/string/string_parser.hpp"
#include "compute_env/load/http_load_params.h"
#include "platform/http/http_request.h"

namespace starrocks {

const std::vector<std::string> LOAD_PARAMETER_NAMES = {HTTP_FORMAT_KEY,
                                                       HTTP_COLUMNS,
                                                       HTTP_WHERE,
                                                       HTTP_MAX_FILTER_RATIO,
                                                       HTTP_TIMEOUT,
                                                       HTTP_PARTITIONS,
                                                       HTTP_TEMP_PARTITIONS,
                                                       HTTP_NEGATIVE,
                                                       HTTP_STRICT_MODE,
                                                       HTTP_TIMEZONE,
                                                       HTTP_LOAD_MEM_LIMIT,
                                                       HTTP_EXEC_MEM_LIMIT,
                                                       HTTP_PARTIAL_UPDATE,
                                                       HTTP_PARTIAL_UPDATE_MODE,
                                                       HTTP_TRANSMISSION_COMPRESSION_TYPE,
                                                       HTTP_LOAD_DOP,
                                                       HTTP_MERGE_CONDITION,
                                                       HTTP_LOG_REJECTED_RECORD_NUM,
                                                       HTTP_COMPRESSION,
                                                       HTTP_WAREHOUSE,
                                                       HTTP_ENABLE_MERGE_COMMIT,
                                                       HTTP_MERGE_COMMIT_ASYNC,
                                                       HTTP_MERGE_COMMIT_INTERVAL_MS,
                                                       HTTP_MERGE_COMMIT_PARALLEL,
                                                       HTTP_COLUMN_SEPARATOR,
                                                       HTTP_ROW_DELIMITER,
                                                       HTTP_TRIM_SPACE,
                                                       HTTP_ENCLOSE,
                                                       HTTP_ESCAPE,
                                                       HTTP_JSONPATHS,
                                                       HTTP_JSONROOT,
                                                       HTTP_STRIP_OUTER_ARRAY,
                                                       HTTP_ENVELOPE};

std::ostream& operator<<(std::ostream& out, const BatchWriteId& id) {
    out << "db: " << id.db << ", table: " << id.table << ", load_params: {";
    bool first = true;
    for (const auto& [key, value] : id.load_params) {
        if (!first) {
            out << ",";
        }
        first = false;
        out << key << ":" << value;
    }
    out << "}";
    return out;
}

BatchWriteLoadParams get_load_parameters(
        const std::function<std::optional<std::string>(const std::string&)>& getter_func) {
    std::map<std::string, std::string> load_params;
    for (const auto& name : LOAD_PARAMETER_NAMES) {
        auto value_opt = getter_func(name);
        if (value_opt) {
            load_params.emplace(name, *value_opt);
        }
    }
    return load_params;
}

BatchWriteLoadParams get_load_parameters_from_brpc(const std::map<std::string, std::string>& input_params) {
    return get_load_parameters([&input_params](const std::string& param_name) -> std::optional<std::string> {
        auto it = input_params.find(param_name);
        if (it != input_params.end()) {
            return it->second;
        } else {
            return std::nullopt;
        }
    });
}

BatchWriteLoadParams get_load_parameters_from_http(HttpRequest* http_req) {
    return get_load_parameters([http_req](const std::string& param_name) -> std::optional<std::string> {
        std::string value = http_req->header(param_name);
        if (!value.empty()) {
            return value;
        } else {
            return std::nullopt;
        }
    });
}

// The format names of Stream Load that are CSV, as parse_format of the Stream Load action maps them. A request that
// sets no format is CSV.
static bool is_csv_format(const std::string& format) {
    if (format.empty()) {
        return true;
    }
    for (const char* name : {"csv", "gzip", "bzip2", "lz4", "deflate", "zstd"}) {
        if (boost::iequals(format, name)) {
            return true;
        }
    }
    return false;
}

Status check_merge_commit_skip_header(const std::string& format, const std::string& skip_header) {
    if (skip_header.empty() || !is_csv_format(format)) {
        return Status::OK();
    }
    // Parsed and reported as parse_int64_param does for the classic Stream Load.
    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
    int64_t value = StringParser::string_to_int<int64_t>(skip_header.data(), skip_header.length(), &parse_result);
    if (parse_result == StringParser::PARSE_FAILURE) {
        return Status::InvalidArgument(
                fmt::format("Invalid parameter {}. The value must be an integer", HTTP_SKIP_HEADER));
    }
    if (parse_result != StringParser::PARSE_SUCCESS) {
        return Status::InvalidArgument(fmt::format("Invalid parameter {}. The value must be between {} and {}",
                                                   HTTP_SKIP_HEADER, std::numeric_limits<int64_t>::min(),
                                                   std::numeric_limits<int64_t>::max()));
    }
    if (value < 0) {
        return Status::InvalidArgument("skip_header must be equal or greater than 0");
    }
    if (value > 0) {
        return Status::InvalidArgument(
                "skip_header is not supported by merge commit, which loads the requests of a merge window as one "
                "file. Send the data without its header rows and without skip_header, or load it with an HTTP "
                "Stream Load that does not enable merge commit");
    }
    return Status::OK();
}

Status check_merge_commit_format(const std::string& format) {
    if (boost::iequals(format, "arrow")) {
        return Status::InvalidArgument(
                "The arrow format is not supported by merge commit, which loads the requests of a merge window as one "
                "file. Load Arrow data with an HTTP Stream Load that does not enable merge commit, or send it in "
                "another format such as CSV or JSON");
    }
    return Status::OK();
}

} // namespace starrocks
