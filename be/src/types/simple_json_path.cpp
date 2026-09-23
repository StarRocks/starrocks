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

#include "types/simple_json_path.h"

#include <re2/re2.h>

#include <boost/tokenizer.hpp>
#include <cstdlib>

#include "fmt/format.h"
#include "glog/logging.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

// json path cannot contains: ", [, ]
static const re2::RE2 SIMPLE_JSONPATH_PATTERN(R"(^([^\"\[\]]*)(?:\[([0-9]+|\*)\])?)", re2::RE2::Quiet);

static Status get_parsed_paths(const std::vector<std::string>& path_exprs, std::vector<SimpleJsonPath>* parsed_paths) {
    // Allow two kind of syntax:
    // strict jsonpath: $.a.b[x]
    // simple syntax: a.b
    for (int i = 0; i < path_exprs.size(); i++) {
        std::string col;
        std::string index;
        auto& current = path_exprs[i];

        if (i == 0) {
            if (current.empty() || current[0] != '$') {
                parsed_paths->emplace_back("", -1, true);
                continue;
            }
        }

        if (!RE2::FullMatch(path_exprs[i], SIMPLE_JSONPATH_PATTERN, &col, &index)) {
            parsed_paths->emplace_back("", -1, false);
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", path_exprs[i]));
        } else {
            int idx = -1;
            if (!index.empty()) {
                if (index == "*") {
                    idx = -2;
                } else {
                    idx = atoi(index.c_str());
                }
            }
            parsed_paths->emplace_back(col, idx, true);
        }
    }
    return Status::OK();
}

Status parse_simple_json_paths(const std::string& path_string, std::vector<SimpleJsonPath>* parsed_paths) {
    // split path by ".", and escape quota by "\"
    // eg:
    //    '$.text#abc.xyz'  ->  [$, text#abc, xyz]
    //    '$."text.abc".xyz'  ->  [$, text.abc, xyz]
    //    '$."text.abc"[1].xyz'  ->  [$, text.abc[1], xyz]
    boost::tokenizer<boost::escaped_list_separator<char>> tok(path_string,
                                                              boost::escaped_list_separator<char>("\\", ".", "\""));
    std::vector<std::string> paths(tok.begin(), tok.end());
    return get_parsed_paths(paths, parsed_paths);
}

Status extract_from_object(simdjson::ondemand::object& obj, const std::vector<SimpleJsonPath>& jsonpath,
                           simdjson::ondemand::value* value) noexcept {
#define HANDLE_SIMDJSON_ERROR(err, msg)                                                          \
    do {                                                                                         \
        const simdjson::error_code& _err = err;                                                  \
        const std::string& _msg = msg;                                                           \
        if (_err) {                                                                              \
            auto err_msg = fmt::format("err: {}, msg: {}", simdjson::error_message(_err), _msg); \
            VLOG(2) << err_msg;                                                                  \
            if (_err == simdjson::NO_SUCH_FIELD || _err == simdjson::INDEX_OUT_OF_BOUNDS) {      \
                return Status::NotFound(err_msg);                                                \
            }                                                                                    \
            return Status::DataQualityError(err_msg);                                            \
        }                                                                                        \
    } while (false)

    if (jsonpath.size() <= 1) {
        // The first elem of json path should be '$'.
        // A valid json path's size is >= 2.
        return Status::InvalidArgument("empty json path");
    }

    simdjson::ondemand::value tvalue;

    // Skip the first $.
    for (int i = 1; i < jsonpath.size(); i++) {
        if (!jsonpath[i].is_valid) {
            auto msg = fmt::format("invalid json path: {}", jsonpaths_to_string(jsonpath));
            VLOG(2) << msg;
            return Status::InvalidArgument(msg);
        }

        const std::string& col = jsonpath[i].key;
        int index = jsonpath[i].idx;

        // Since the simdjson::ondemand::object cannot be converted to simdjson::ondemand::value,
        // we have to do some special treatment for the second elem of json path.
        // If the key is not found in json object, simdjson::NO_SUCH_FIELD would be returned.
        if (i == 1) {
            if (obj.is_empty()) {
                auto msg = fmt::format("unable to find key: {}", jsonpaths_to_string(jsonpath, i));
                VLOG(2) << msg;
                return Status::NotFound(msg);
            }

            if (col == "*") {
                // There should be no jsonpath for this pattern, $.*
                return Status::InvalidArgument(
                        fmt::format("extracting * from root-object is not supported, the json path: {}",
                                    jsonpaths_to_string(jsonpath)));
            } else {
                HANDLE_SIMDJSON_ERROR(obj.find_field_unordered(col).get(tvalue),
                                      fmt::format("unable to find key: {}", jsonpaths_to_string(jsonpath, i)));
            }
        } else {
            // There are always two patterns.
            // 1. {"field_name": null}
            // 2. {"field_name": {"field_type": data}}
            // For pattern1, we just return null value.
            // For pattern2, we get the first field of object as next value.

            if (tvalue.is_null()) {
                auto msg = fmt::format("unable to find key: {}", jsonpaths_to_string(jsonpath, i));
                VLOG(2) << msg;
                return Status::NotFound(msg);
            }

            if (tvalue.type() != simdjson::ondemand::json_type::object) {
                auto msg = fmt::format("unable to find key: {}", jsonpaths_to_string(jsonpath, i));
                VLOG(2) << msg;
                return Status::NotFound(msg);
            }

            if (col == "*") {
                for (auto field : tvalue.get_object()) {
                    tvalue = field.value();
                    break;
                }
            } else {
                HANDLE_SIMDJSON_ERROR(tvalue.find_field_unordered(col).get(tvalue),
                                      fmt::format("unable to find key: {}", jsonpaths_to_string(jsonpath, i)));
            }
        }

        if (index != -1) {
            if (tvalue.is_null()) {
                auto msg = fmt::format("unable to find key: {}", jsonpaths_to_string(jsonpath, i));
                VLOG(2) << msg;
                return Status::NotFound(msg);
            }

            // try to access tvalue as array.
            // If the index is beyond the length of array, simdjson::INDEX_OUT_OF_BOUNDS would be returned.
            simdjson::ondemand::array arr;
            HANDLE_SIMDJSON_ERROR(tvalue.get_array().get(arr),
                                  fmt::format("failed to access field as array, field: {}, jsonpath: {}", col,
                                              jsonpaths_to_string(jsonpath)));

            HANDLE_SIMDJSON_ERROR(arr.at(index).get(tvalue),
                                  fmt::format("failed to access array field: {}, index: {}, jsonpath: {}", col, index,
                                              jsonpaths_to_string(jsonpath)));
        }
    }

    if (tvalue.is_null()) {
        auto msg = fmt::format("unable to find key: {}", jsonpaths_to_string(jsonpath));
        VLOG(2) << msg;
        return Status::NotFound(msg);
    }

    std::swap(*value, tvalue);

    return Status::OK();

#undef HANDLE_SIMDJSON_ERROR
}

std::string jsonpaths_to_string(const std::vector<SimpleJsonPath>& paths, size_t sub_index) {
    std::string output{"$"};
    size_t sz = (sub_index == -1 || sub_index >= paths.size()) ? paths.size() - 1 : sub_index;
    for (size_t i = 1; i <= sz; ++i) {
        output.append(".").append(paths[i].to_string());
    }
    return output;
}

} // namespace starrocks
