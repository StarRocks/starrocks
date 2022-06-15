// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/json_functions.h"

#include <re2/re2.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/status.h"
#include "exprs/vectorized/function_helper.h"
#include "exprs/vectorized/jsonpath.h"
#include "glog/logging.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "udf/udf.h"
#include "util/json.h"
#include "velocypack/vpack.h"

namespace starrocks::vectorized {

// static const re2::RE2 JSON_PATTERN("^([a-zA-Z0-9_\\-\\:\\s#\\|\\.]*)(?:\\[([0-9]+)\\])?");
// json path cannot contains: ", [, ]
const re2::RE2 SIMPLE_JSONPATH_PATTERN(R"(^([^\"\[\]]*)(?:\[([0-9]+|\*)\])?)");

Status JsonFunctions::_get_parsed_paths(const std::vector<std::string>& path_exprs,
                                        std::vector<SimpleJsonPath>* parsed_paths) {
    // Allow two kind of syntax:
    // strict jsonpath: $.a.b[x]
    // simple syntax: a.b

    for (int i = 0; i < path_exprs.size(); i++) {
        std::string col;
        std::string index;
        auto& current = path_exprs[i];

        if (i == 0) {
            if (current != "$") {
                parsed_paths->emplace_back("", -1, true);
            } else {
                parsed_paths->emplace_back("$", -1, true);
                continue;
            }
        }

        if (UNLIKELY(!RE2::FullMatch(path_exprs[i], SIMPLE_JSONPATH_PATTERN, &col, &index))) {
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

    std::vector<std::string> path_exprs;
    try {
        boost::tokenizer<boost::escaped_list_separator<char>> tok(path_str,
                                                                  boost::escaped_list_separator<char>("\\", ".", "\""));
        path_exprs.assign(tok.begin(), tok.end());
    } catch (const boost::escaped_list_error& e) {
        return Status::InvalidArgument(strings::Substitute("Illegal json path: $0", e.what()));
    }
    std::vector<SimpleJsonPath>* parsed_paths = new std::vector<SimpleJsonPath>();
    _get_parsed_paths(path_exprs, parsed_paths);

    context->set_function_state(scope, parsed_paths);
    VLOG(10) << "prepare json path. size: " << parsed_paths->size();
    return Status::OK();
}

Status JsonFunctions::json_path_close(starrocks_udf::FunctionContext* context,
                                      starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        std::vector<SimpleJsonPath>* parsed_paths =
                reinterpret_cast<std::vector<SimpleJsonPath>*>(context->get_function_state(scope));
        if (parsed_paths != nullptr) {
            delete parsed_paths;
            VLOG(10) << "close json path";
        }
    }

    return Status::OK();
}

Status JsonFunctions::extract_from_object(simdjson::ondemand::object& obj, const std::vector<SimpleJsonPath>& jsonpath,
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

void JsonFunctions::parse_json_paths(const std::string& path_string, std::vector<SimpleJsonPath>* parsed_paths) {
    // split path by ".", and escape quota by "\"
    // eg:
    //    '$.text#abc.xyz'  ->  [$, text#abc, xyz]
    //    '$."text.abc".xyz'  ->  [$, text.abc, xyz]
    //    '$."text.abc"[1].xyz'  ->  [$, text.abc[1], xyz]
    boost::tokenizer<boost::escaped_list_separator<char>> tok(path_string,
                                                              boost::escaped_list_separator<char>("\\", ".", "\""));
    std::vector<std::string> paths(tok.begin(), tok.end());
    _get_parsed_paths(paths, parsed_paths);
}

JsonFunctionType JsonTypeTraits<TYPE_INT>::JsonType = JSON_FUN_INT;
JsonFunctionType JsonTypeTraits<TYPE_DOUBLE>::JsonType = JSON_FUN_DOUBLE;
JsonFunctionType JsonTypeTraits<TYPE_VARCHAR>::JsonType = JSON_FUN_STRING;

ColumnPtr JsonFunctions::get_json_int(FunctionContext* context, const Columns& columns) {
    auto jsons = _string_json(context, columns);
    auto paths = columns[1];

    jsons = json_query(context, Columns{jsons, paths});
    return _json_int(context, Columns{jsons});
}

ColumnPtr JsonFunctions::get_json_double(FunctionContext* context, const Columns& columns) {
    auto jsons = _string_json(context, columns);
    auto paths = columns[1];

    jsons = json_query(context, Columns{jsons, paths});
    return _json_double(context, Columns{jsons});
}

ColumnPtr JsonFunctions::get_json_string(FunctionContext* context, const Columns& columns) {
    auto jsons = _string_json(context, columns);
    auto paths = columns[1];

    jsons = json_query(context, Columns{jsons, paths});
    return json_string(context, Columns{jsons});
}

ColumnPtr JsonFunctions::parse_json(FunctionContext* context, const Columns& columns) {
    int num_rows = columns[0]->size();
    ColumnViewer<TYPE_VARCHAR> viewer(columns[0]);
    ColumnBuilder<TYPE_JSON> result(num_rows);

    for (int row = 0; row < columns[0]->size(); row++) {
        if (viewer.is_null(row)) {
            result.append_null();
            continue;
        }
        auto slice = viewer.value(row);

        auto json = JsonValue::parse(slice);
        if (!json.ok()) {
            result.append_null();
        } else {
            result.append(std::move(json.value()));
        }
    }

    DCHECK(num_rows == result.data_column()->size());
    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr JsonFunctions::json_string(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_JSON> viewer(columns[0]);
    ColumnBuilder<TYPE_VARCHAR> result(columns[0]->size());

    for (int row = 0; row < columns[0]->size(); row++) {
        if (viewer.is_null(row)) {
            result.append_null();
        } else {
            JsonValue* json = viewer.value(row);
            auto json_str = json->to_string();
            if (!json_str.ok()) {
                result.append_null();
            } else {
                result.append(std::move(json_str.value()));
            }
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr JsonFunctions::_json_int(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_JSON> viewer(columns[0]);
    ColumnBuilder<TYPE_INT> result(columns[0]->size());

    for (int row = 0; row < columns[0]->size(); row++) {
        if (viewer.is_null(row)) {
            result.append_null();
        } else {
            JsonValue* json = viewer.value(row);
            auto json_int = json->get_int();
            if (!json_int.ok()) {
                result.append_null();
            } else {
                result.append(std::move(json_int.value()));
            }
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr JsonFunctions::_json_double(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_JSON> viewer(columns[0]);
    ColumnBuilder<TYPE_DOUBLE> result(columns[0]->size());

    for (int row = 0; row < columns[0]->size(); row++) {
        if (viewer.is_null(row)) {
            result.append_null();
        } else {
            JsonValue* json = viewer.value(row);
            auto json_d = json->get_double();
            if (!json_d.ok()) {
                result.append_null();
            } else {
                result.append(std::move(json_d.value()));
            }
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr JsonFunctions::_string_json(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_VARCHAR> viewer(columns[0]);
    ColumnBuilder<TYPE_JSON> result(columns[0]->size());

    for (int row = 0; row < columns[0]->size(); row++) {
        if (viewer.is_null(row)) {
            result.append_null();
        } else {
            auto raw = viewer.value(row);
            JsonValue json_value;
            auto st = JsonValue::parse(raw, &json_value);
            if (!st.ok()) {
                result.append_null();
            } else {
                result.append(std::move(json_value));
            }
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

//////////////////////////// User visiable functions /////////////////////////////////

static StatusOr<JsonPath*> get_prepared_or_parse(FunctionContext* context, Slice slice, JsonPath* out) {
    JsonPath* prepared = reinterpret_cast<JsonPath*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (prepared != nullptr) {
        return prepared;
    }
    auto res = JsonPath::parse(slice);
    RETURN_IF(!res.ok(), res.status());
    out->reset(std::move(res.value()));
    return out;
}

Status JsonFunctions::native_json_path_prepare(starrocks_udf::FunctionContext* context,
                                               starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || !context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    auto path_column = context->get_constant_column(1);
    Slice path_value = ColumnHelper::get_const_value<TYPE_VARCHAR>(path_column);
    auto json_path = JsonPath::parse(path_value);
    RETURN_IF(!json_path.ok(), json_path.status());
    JsonPath* state = new JsonPath(std::move(json_path.value()));
    context->set_function_state(scope, state);

    VLOG(10) << "prepare json path: " << path_value;
    return Status::OK();
}

Status JsonFunctions::native_json_path_close(starrocks_udf::FunctionContext* context,
                                             starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        JsonPath* state = reinterpret_cast<JsonPath*>(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

ColumnPtr JsonFunctions::json_query(FunctionContext* context, const Columns& columns) {
    auto num_rows = columns[0]->size();
    auto json_viewer = ColumnViewer<TYPE_JSON>(columns[0]);
    auto path_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    ColumnBuilder<TYPE_JSON> result(num_rows);

    JsonPath stored_path;
    for (int row = 0; row < num_rows; ++row) {
        if (json_viewer.is_null(row) || path_viewer.is_null(row)) {
            result.append_null();
            continue;
        }
        JsonValue* json_value = json_viewer.value(row);
        auto path_value = path_viewer.value(row);

        auto jsonpath = get_prepared_or_parse(context, path_value, &stored_path);
        if (!jsonpath.ok()) {
            VLOG(2) << "parse json path failed: " << path_value;
            result.append_null();
            continue;
        }

        vpack::Builder builder;
        vpack::Slice slice = JsonPath::extract(json_value, *jsonpath.value(), &builder);
        if (slice.isNone()) {
            result.append_null();
        } else {
            JsonValue value(slice);
            result.append(std::move(value));
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr JsonFunctions::json_exists(FunctionContext* context, const Columns& columns) {
    auto num_rows = columns[0]->size();
    auto json_viewer = ColumnViewer<TYPE_JSON>(columns[0]);
    auto path_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    ColumnBuilder<TYPE_BOOLEAN> result(num_rows);

    JsonPath stored_path;
    for (int row = 0; row < num_rows; row++) {
        if (json_viewer.is_null(row) || json_viewer.value(row) == nullptr || path_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        JsonValue* json_value = json_viewer.value(row);
        Slice path_str = path_viewer.value(row);
        auto jsonpath = get_prepared_or_parse(context, path_str, &stored_path);

        if (!jsonpath.ok()) {
            result.append_null();
            VLOG(2) << "parse json path failed: " << path_str;
            continue;
        }
        VLOG(2) << "json_exists for  " << path_str << " of " << json_value->to_string().value();
        vpack::Builder builder;
        vpack::Slice slice = JsonPath::extract(json_value, *jsonpath.value(), &builder);
        result.append(!slice.isNone());
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr JsonFunctions::json_array_empty(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(0, columns.size());
    ColumnBuilder<TYPE_JSON> result(1);
    JsonValue json(vpack::Slice::emptyArraySlice());
    result.append(std::move(json));
    return result.build(true);
}

ColumnPtr JsonFunctions::json_array(FunctionContext* context, const Columns& columns) {
    namespace vpack = arangodb::velocypack;

    DCHECK_GT(columns.size(), 0);

    size_t rows = columns[0]->size();
    ColumnBuilder<TYPE_JSON> result(rows);
    std::vector<ColumnViewer<TYPE_JSON>> viewers;
    for (auto& col : columns) {
        viewers.emplace_back(ColumnViewer<TYPE_JSON>(col));
    }

    for (int row = 0; row < rows; row++) {
        vpack::Builder builder;
        {
            vpack::ArrayBuilder ab(&builder);
            for (auto& view : viewers) {
                if (view.is_null(row)) {
                    builder.add(vpack::Value(vpack::ValueType::Null));
                } else {
                    JsonValue* json = view.value(row);
                    builder.add(json->to_vslice());
                }
            }
        }
        vpack::Slice json_slice = builder.slice();
        JsonValue json(json_slice);
        result.append(std::move(json));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr JsonFunctions::json_object_empty(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(0, columns.size());
    ColumnBuilder<TYPE_JSON> result(1);
    JsonValue json(vpack::Slice::emptyObjectSlice());
    result.append(std::move(json));
    return result.build(true);
}

ColumnPtr JsonFunctions::json_object(FunctionContext* context, const Columns& columns) {
    namespace vpack = arangodb::velocypack;

    DCHECK_GT(columns.size(), 0);

    size_t rows = columns[0]->size();
    ColumnBuilder<TYPE_JSON> result(rows);
    std::vector<ColumnViewer<TYPE_JSON>> viewers;
    for (auto& col : columns) {
        viewers.emplace_back(ColumnViewer<TYPE_JSON>(col));
    }

    for (int row = 0; row < rows; row++) {
        vpack::Builder builder;
        bool ok = false;
        {
            vpack::ObjectBuilder ob(&builder);
            for (int i = 0; i < viewers.size(); i += 2) {
                if (viewers[i].is_null(row)) {
                    ok = false;
                    break;
                }

                JsonValue* field_name = viewers[i].value(row);
                vpack::Slice field_name_slice = field_name->to_vslice();
                DCHECK(field_name != nullptr);

                if (!field_name_slice.isString()) {
                    VLOG(2) << "nonstring json field name" << field_name->to_string().value();
                    ok = false;
                    break;
                }
                if (field_name_slice.stringRef().length() == 0) {
                    VLOG(2) << "json field name could not be empty string" << field_name->to_string().value();
                    ok = false;
                    break;
                }
                if (i + 1 < viewers.size() && !viewers[i + 1].is_null(row)) {
                    JsonValue* field_value = viewers[i + 1].value(row);
                    DCHECK(field_value != nullptr);
                    builder.add(field_name->to_vslice().stringRef(), field_value->to_vslice());
                } else {
                    VLOG(2) << "field value not exists, patch a null value" << field_name->to_string().value();
                    builder.add(field_name->to_vslice().stringRef(), vpack::Value(vpack::ValueType::Null));
                }
                ok = true;
            }
        }
        if (ok) {
            vpack::Slice json_slice = builder.slice();
            JsonValue json(json_slice);
            result.append(std::move(json));
        } else {
            result.append_null();
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks::vectorized
