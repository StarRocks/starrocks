// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/json_functions.h"

#include <re2/re2.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"

namespace starrocks::vectorized {

// static const re2::RE2 JSON_PATTERN("^([a-zA-Z0-9_\\-\\:\\s#\\|\\.]*)(?:\\[([0-9]+)\\])?");
// json path cannot contains: ", [, ]
static const re2::RE2 JSON_PATTERN(R"(^([^\"\[\]]*)(?:\[([0-9]+|\*)\])?)");

Status JsonFunctions::_get_parsed_paths(const std::vector<std::string>& path_exprs,
                                        std::vector<JsonPath>* parsed_paths) {
    if (path_exprs[0] != "$") {
        parsed_paths->emplace_back("", -1, false);
        return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", path_exprs[0]));
    } else {
        parsed_paths->emplace_back("$", -1, true);
    }

    for (int i = 1; i < path_exprs.size(); i++) {
        std::string col;
        std::string index;
        if (UNLIKELY(!RE2::FullMatch(path_exprs[i], JSON_PATTERN, &col, &index))) {
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

Status JsonFunctions::json_path_prepare(starrocks_udf::FunctionContext* context,
                                        starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!context->is_constant_column(1)) {
        return Status::OK();
    }

    ColumnPtr path = context->get_constant_column(1);
    if (path->only_null()) {
        return Status::OK();
    }

    auto path_value = ColumnHelper::get_const_value<TYPE_VARCHAR>(path);
    std::string path_str(path_value.data, path_value.size);
    // Must remove or replace the escape sequence.
    path_str.erase(std::remove(path_str.begin(), path_str.end(), '\\'), path_str.end());
    if (path_str.empty()) {
        return Status::OK();
    }

    boost::tokenizer<boost::escaped_list_separator<char> > tok(path_str,
                                                               boost::escaped_list_separator<char>("\\", ".", "\""));
    std::vector<std::string> path_exprs(tok.begin(), tok.end());
    std::vector<JsonPath>* parsed_paths = new std::vector<JsonPath>();
    _get_parsed_paths(path_exprs, parsed_paths);

    context->set_function_state(scope, parsed_paths);
    VLOG(10) << "prepare json path. size: " << parsed_paths->size();
    return Status::OK();
}

Status JsonFunctions::json_path_close(starrocks_udf::FunctionContext* context,
                                      starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        std::vector<JsonPath>* parsed_paths =
                reinterpret_cast<std::vector<JsonPath>*>(context->get_function_state(scope));
        if (parsed_paths != nullptr) {
            delete parsed_paths;
            VLOG(10) << "close json path";
        }
    }

    return Status::OK();
}

bool JsonFunctions::extract_from_object(simdjson::ondemand::object& obj, const std::vector<JsonPath>& jsonpath,
                                        simdjson::ondemand::value& value) {
    if (obj.is_empty()) {
        return false;
    }

    simdjson::ondemand::value tvalue;

    // Skip the first $.
    for (int i = 1; i < jsonpath.size(); i++) {
        if (UNLIKELY(!jsonpath[i].is_valid)) {
            return false;
        }

        const std::string& col = jsonpath[i].key;
        int index = jsonpath[i].idx;

        // TODO: ignore json path index, eg: $.data[0]
        if (index != -1) {
            return false;
        }

        if (i == 1) {
            auto err = obj.find_field(col).get(tvalue);
            if (err) {
                return false;
            }
        } else {
            auto err = tvalue.find_field(col).get(tvalue);
            if (err) {
                return false;
            }
        }
    }

    std::swap(value, tvalue);

    return true;
}

void JsonFunctions::parse_json_paths(const std::string& path_string, std::vector<JsonPath>* parsed_paths) {
    // split path by ".", and escape quota by "\"
    // eg:
    //    '$.text#abc.xyz'  ->  [$, text#abc, xyz]
    //    '$."text.abc".xyz'  ->  [$, text.abc, xyz]
    //    '$."text.abc"[1].xyz'  ->  [$, text.abc[1], xyz]
    boost::tokenizer<boost::escaped_list_separator<char> > tok(path_string,
                                                               boost::escaped_list_separator<char>("\\", ".", "\""));
    std::vector<std::string> paths(tok.begin(), tok.end());
    _get_parsed_paths(paths, parsed_paths);
}

JsonFunctionType JsonTypeTraits<TYPE_INT>::JsonType = JSON_FUN_INT;
JsonFunctionType JsonTypeTraits<TYPE_DOUBLE>::JsonType = JSON_FUN_DOUBLE;
JsonFunctionType JsonTypeTraits<TYPE_VARCHAR>::JsonType = JSON_FUN_STRING;

template <PrimitiveType primitive_type>
ColumnPtr JsonFunctions::_iterate_rows(FunctionContext* context, const Columns& columns) {
    auto json_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto path_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    simdjson::ondemand::parser parser;

    ColumnBuilder<primitive_type> result;
    auto size = columns[0]->size();
    for (int row = 0; row < size; ++row) {
        if (json_viewer.is_null(row) || path_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto json_value = json_viewer.value(row);
        if (json_value.empty()) {
            result.append_null();
            continue;
        }
        std::string json_string(json_value.data, json_value.size);

        auto path_value = path_viewer.value(row);
        std::string path_string(path_value.data, path_value.size);
        // Must remove or replace the escape sequence.
        path_string.erase(std::remove(path_string.begin(), path_string.end(), '\\'), path_string.end());
        if (path_string.empty()) {
            result.append_null();
            continue;
        }

        // Reserve for simdjson padding.
        json_string.reserve(json_string.size() + simdjson::SIMDJSON_PADDING);

        auto doc = parser.iterate(json_string);
        if (doc.error()) {
            result.append_null();
            continue;
        }

        // Object only.
        if (doc.type() != simdjson::ondemand::json_type::object) {
            result.append_null();
            continue;
        }

        auto res = doc.get_object();
        if (res.error()) {
            result.append_null();
            continue;
        }

        auto obj = res.value();

        std::vector<JsonPath> jsonpath;
        parse_json_paths(path_string, &jsonpath);

        simdjson::ondemand::value value;
        if (!extract_from_object(obj, jsonpath, value)) {
            result.append_null();
            continue;
        }

        if constexpr (primitive_type == TYPE_INT) {
            if (value.type() == simdjson::ondemand::json_type::number) {
                switch (value.get_number_type()) {
                case simdjson::ondemand::number_type::signed_integer: {
                    result.append(value.get_int64());
                    break;
                }

                case simdjson::ondemand::number_type::unsigned_integer: {
                    result.append(value.get_uint64());
                    break;
                }
                default: {
                    result.append_null();
                    break;
                }
                }
            } else {
                result.append_null();
            }

        } else if constexpr (primitive_type == TYPE_DOUBLE) {
            if (value.type() == simdjson::ondemand::json_type::number &&
                value.get_number_type() == simdjson::ondemand::number_type::floating_point_number) {
                result.append(value.get_double());
            } else {
                result.append_null();
            }
        } else if constexpr (primitive_type == TYPE_VARCHAR) {
            if (value.type() == simdjson::ondemand::json_type::string) {
                auto v = value.get_string();
                if(v.error()) {
                    result.append_null();
                }
                auto s = v.value();
                result.append(Slice{s.data(), s.size()});
            } else {
                result.append_null();
            }
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr JsonFunctions::get_json_int(FunctionContext* context, const Columns& columns) {
    return JsonFunctions::template _iterate_rows<TYPE_INT>(context, columns);
}

ColumnPtr JsonFunctions::get_json_double(FunctionContext* context, const Columns& columns) {
    return JsonFunctions::template _iterate_rows<TYPE_DOUBLE>(context, columns);
}

ColumnPtr JsonFunctions::get_json_string(FunctionContext* context, const Columns& columns) {
    return JsonFunctions::template _iterate_rows<TYPE_VARCHAR>(context, columns);
}

} // namespace starrocks::vectorized
