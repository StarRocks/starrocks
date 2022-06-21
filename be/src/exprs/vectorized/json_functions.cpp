// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
static const re2::RE2 JSON_PATTERN(R"(^([^\"\[\]]*)(?:\[([0-9]+|\*)\])?)", re2::RE2::Quiet);

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

    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    ColumnPtr path = context->get_constant_column(1);
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

Status JsonFunctions::extract_from_object(simdjson::ondemand::object& obj, const std::vector<JsonPath>& jsonpath,
                                          simdjson::ondemand::value* value) noexcept {
    if (jsonpath.size() <= 1) {
        // The first elem of json path should be '$'.
        // A valid json path's size is >= 2.
        return Status::InvalidArgument("empty json path");
    }

    simdjson::ondemand::value tvalue;

    // Skip the first $.
    for (int i = 1; i < jsonpath.size(); i++) {
        if (UNLIKELY(!jsonpath[i].is_valid)) {
            return Status::InvalidArgument(fmt::format("invalid json path: {}", jsonpath[i].key));
        }

        const std::string& col = jsonpath[i].key;
        int index = jsonpath[i].idx;

        // Since the simdjson::ondemand::object cannot be converted to simdjson::ondemand::value,
        // we have to do some special treatment for the second elem of json path.
        if (i == 1) {
            auto err = obj.find_field_unordered(col).get(tvalue);
            if (err) {
                return Status::NotFound(
                        fmt::format("failed to access field: {}, err: {}", col, simdjson::error_message(err)));
            }
        } else {
            auto err = tvalue.find_field_unordered(col).get(tvalue);
            if (err) {
                return Status::NotFound(
                        fmt::format("failed to access field: {}, err: {}", col, simdjson::error_message(err)));
            }
        }

        if (index != -1) {
            // try to access tvalue as array.
            simdjson::ondemand::array arr;
            auto err = tvalue.get_array().get(arr);
            if (err) {
                return Status::InvalidArgument(fmt::format("failed to access field as array, field: {}, err: {}", col,
                                                           simdjson::error_message(err)));
            }

            size_t sz;
            err = arr.count_elements().get(sz);
            if (err) {
                return Status::InvalidArgument(
                        fmt::format("failed to get array size, field: {}, err: {}", col, simdjson::error_message(err)));
            }

            if (index >= sz) {
                return Status::NotFound(
                        fmt::format("index beyond array size, field: {}, index: {}, array size: {}", col, index, sz));
            }

            err = arr.at(index).get(tvalue);
            if (err) {
                return Status::InvalidArgument(
                        fmt::format("failed to access array, field: {}, index: {}, array size: {}, err: {}", col, index,
                                    sz, simdjson::error_message(err)));
            }
        }
    }

    std::swap(*value, tvalue);

    return Status::OK();
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

    auto size = columns[0]->size();
    ColumnBuilder<primitive_type> result(size);
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

        std::vector<JsonPath> jsonpath;
        parse_json_paths(path_string, &jsonpath);

        simdjson::ondemand::json_type tp;

        auto err = doc.type().get(tp);
        if (err) {
            result.append_null();
            continue;
        }

        switch (tp) {
        case simdjson::ondemand::json_type::object: {
            simdjson::ondemand::object obj;

            err = doc.get_object().get(obj);
            if (err) {
                result.append_null();
                continue;
            }

            simdjson::ondemand::value value;
            auto st = extract_from_object(obj, jsonpath, &value);
            if (!st.ok()) {
                result.append_null();
                continue;
            }

            _build_column(result, value);
            break;
        }

        case simdjson::ondemand::json_type::array: {
            simdjson::ondemand::array arr;
            err = doc.get_array().get(arr);
            if (err) {
                result.append_null();
                continue;
            }

            for (auto a : arr) {
                simdjson::ondemand::json_type tp;
                err = a.type().get(tp);
                if (err) {
                    result.append_null();
                    continue;
                }

                if (tp != simdjson::ondemand::json_type::object) {
                    result.append_null();
                    continue;
                }

                simdjson::ondemand::object obj;

                err = a.get_object().get(obj);
                if (err) {
                    result.append_null();
                    continue;
                }

                simdjson::ondemand::value value;
                auto st = extract_from_object(obj, jsonpath, &value);
                if (!st.ok()) {
                    result.append_null();
                    continue;
                }

                _build_column(result, value);
            }
            break;
        }

        default: {
            result.append_null();
            break;
        }
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
} // namespace starrocks::vectorized

template <>
void JsonFunctions::_build_column(ColumnBuilder<TYPE_INT>& result, simdjson::ondemand::value& value) {
    int64_t i64;
    auto err = value.get_int64().get(i64);
    APPEND_NULL_AND_RETURN_IF_ERROR(result, err);

    result.append(i64);
    return;
}

template <>
void JsonFunctions::_build_column(ColumnBuilder<TYPE_DOUBLE>& result, simdjson::ondemand::value& value) {
    double d;
    auto err = value.get_double().get(d);
    APPEND_NULL_AND_RETURN_IF_ERROR(result, err);

    result.append(d);
    return;
}

template <>
void JsonFunctions::_build_column(ColumnBuilder<TYPE_VARCHAR>& result, simdjson::ondemand::value& value) {
    simdjson::ondemand::json_type tp;
    auto err = value.type().get(tp);
    APPEND_NULL_AND_RETURN_IF_ERROR(result, err);

    if (tp == simdjson::ondemand::json_type::string) {
        std::string_view sv;
        auto err = value.get_string().get(sv);
        APPEND_NULL_AND_RETURN_IF_ERROR(result, err);

        result.append(Slice{sv.data(), sv.size()});
    } else {
        // For compatible consideration, format json in non-string type as string.
        std::string_view sv = simdjson::to_json_string(value);
        std::unique_ptr<char[]> buf{new char[sv.size()]};
        size_t new_length{};

        auto err = simdjson::minify(sv.data(), sv.size(), buf.get(), new_length);
        APPEND_NULL_AND_RETURN_IF_ERROR(result, err);

        result.append(Slice{buf.get(), new_length});
    }

    return;
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
