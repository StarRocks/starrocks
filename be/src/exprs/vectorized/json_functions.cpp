// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/json_functions.h"

#include <re2/re2.h>

#include <algorithm>
#include <boost/tokenizer.hpp>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/status.h"
#include "exprs/vectorized/cast_expr.h"
#include "exprs/vectorized/jsonpath.h"
#include "glog/logging.h"
#include "gutil/strings/escaping.h"
#include "gutil/strings/substitute.h"
#include "udf/udf.h"
#include "util/json.h"
#include "velocypack/Builder.h"
#include "velocypack/Iterator.h"

namespace starrocks::vectorized {

// static const re2::RE2 JSON_PATTERN("^([a-zA-Z0-9_\\-\\:\\s#\\|\\.]*)(?:\\[([0-9]+)\\])?");
// json path cannot contains: ", [, ]
const re2::RE2 SIMPLE_JSONPATH_PATTERN(R"(^([^\"\[\]]*)(?:\[([0-9]+|\*)\])?)", re2::RE2::Quiet);

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
    auto* parsed_paths = new std::vector<SimpleJsonPath>();
    _get_parsed_paths(path_exprs, parsed_paths);

    context->set_function_state(scope, parsed_paths);
    VLOG(10) << "prepare json path. size: " << parsed_paths->size();
    return Status::OK();
}

Status JsonFunctions::json_path_close(starrocks_udf::FunctionContext* context,
                                      starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* parsed_paths = reinterpret_cast<std::vector<SimpleJsonPath>*>(context->get_function_state(scope));
        if (parsed_paths != nullptr) {
            delete parsed_paths;
            VLOG(10) << "close json path";
        }
    }

    return Status::OK();
}

Status JsonFunctions::extract_from_object(simdjson::ondemand::object& obj, const std::vector<SimpleJsonPath>& jsonpath,
                                          simdjson::ondemand::value* value) noexcept {
#define HANDLE_SIMDJSON_ERROR(err, msg)                                                                            \
    do {                                                                                                           \
        const simdjson::error_code& _err = err;                                                                    \
        const std::string& _msg = msg;                                                                             \
        if (UNLIKELY(_err)) {                                                                                      \
            if (_err == simdjson::NO_SUCH_FIELD || _err == simdjson::INDEX_OUT_OF_BOUNDS) {                        \
                return Status::NotFound(fmt::format("err: {}, msg: {}", simdjson::error_message(_err), _msg));     \
            }                                                                                                      \
            return Status::DataQualityError(fmt::format("err: {}, msg: {}", simdjson::error_message(_err), _msg)); \
        }                                                                                                          \
    } while (false);

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
        // If the key is not found in json object, simdjson::NO_SUCH_FIELD would be returned.
        if (i == 1) {
            HANDLE_SIMDJSON_ERROR(obj.find_field_unordered(col).get(tvalue),
                                  fmt::format("unable to find field: {}", col));
        } else {
            HANDLE_SIMDJSON_ERROR(tvalue.find_field_unordered(col).get(tvalue),
                                  fmt::format("unable to find field: {}", col));
        }

        if (index != -1) {
            // try to access tvalue as array.
            // If the index is beyond the length of array, simdjson::INDEX_OUT_OF_BOUNDS would be returned.
            simdjson::ondemand::array arr;
            HANDLE_SIMDJSON_ERROR(tvalue.get_array().get(arr),
                                  fmt::format("failed to access field as array, field: {}", col));

            HANDLE_SIMDJSON_ERROR(arr.at(index).get(tvalue),
                                  fmt::format("failed to access array field: {}, index: {}", col, index));
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

StatusOr<ColumnPtr> JsonFunctions::get_json_int(FunctionContext* context, const Columns& columns) {
    ASSIGN_OR_RETURN(auto jsons, _string_json(context, columns));
    const auto& paths = columns[1];

    ASSIGN_OR_RETURN(jsons, json_query(context, Columns{jsons, paths}));
    return _json_int(context, Columns{jsons});
}

StatusOr<ColumnPtr> JsonFunctions::get_json_double(FunctionContext* context, const Columns& columns) {
    ASSIGN_OR_RETURN(auto jsons, _string_json(context, columns))
    const auto& paths = columns[1];

    ASSIGN_OR_RETURN(jsons, json_query(context, Columns{jsons, paths}))
    return _json_double(context, Columns{jsons});
}

StatusOr<ColumnPtr> JsonFunctions::get_json_string(FunctionContext* context, const Columns& columns) {
    ASSIGN_OR_RETURN(auto jsons, _string_json(context, columns));
    const auto& paths = columns[1];

    ASSIGN_OR_RETURN(jsons, json_query(context, Columns{jsons, paths}))
    return _json_string_unescaped(context, Columns{jsons});
}

StatusOr<ColumnPtr> JsonFunctions::get_native_json_int(FunctionContext* context, const Columns& columns) {
    return _json_query_impl<TYPE_INT>(context, columns);
}

StatusOr<ColumnPtr> JsonFunctions::get_native_json_double(FunctionContext* context, const Columns& columns) {
    return _json_query_impl<TYPE_DOUBLE>(context, columns);
}

StatusOr<ColumnPtr> JsonFunctions::get_native_json_string(FunctionContext* context, const Columns& columns) {
    return _json_query_impl<TYPE_VARCHAR>(context, columns);
}

StatusOr<ColumnPtr> JsonFunctions::parse_json(FunctionContext* context, const Columns& columns) {
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

StatusOr<ColumnPtr> JsonFunctions::json_string(FunctionContext* context, const Columns& columns) {
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
                result.append(json_str.value());
            }
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> JsonFunctions::_json_int(FunctionContext* context, const Columns& columns) {
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
                result.append(json_int.value());
            }
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> JsonFunctions::_json_double(FunctionContext* context, const Columns& columns) {
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
                result.append(json_d.value());
            }
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> JsonFunctions::_string_json(FunctionContext* context, const Columns& columns) {
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

StatusOr<ColumnPtr> JsonFunctions::_json_string_unescaped(FunctionContext* context, const Columns& columns) {
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
                continue;
            }
            auto str = json_str.value();

            // Since the string extract from json may be escaped, unescaping is needed.
            // The src and dest of strings::CUnescape could be the same.
            if (!strings::CUnescape(StringPiece{str}, &str, nullptr)) {
                result.append_null();
                continue;
            }

            if (str.length() < 2) {
                result.append(str);
                continue;
            }

            // Try to trim the first/last quote.
            if (str[0] == '"') str = str.substr(1, str.size() - 1);
            if (str[str.size() - 1] == '"') str = str.substr(0, str.size() - 1);

            result.append(str);
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

//////////////////////////// User visiable functions /////////////////////////////////

static StatusOr<JsonPath*> get_prepared_or_parse(FunctionContext* context, Slice slice, JsonPath* out) {
    auto* prepared = reinterpret_cast<JsonPath*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
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
    auto* state = new JsonPath(std::move(json_path.value()));
    context->set_function_state(scope, state);

    VLOG(10) << "prepare json path: " << path_value;
    return Status::OK();
}

Status JsonFunctions::native_json_path_close(starrocks_udf::FunctionContext* context,
                                             starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<JsonPath*>(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

StatusOr<ColumnPtr> JsonFunctions::json_query(FunctionContext* context, const Columns& columns) {
    return _json_query_impl<TYPE_JSON>(context, columns);
}

// Convert the JSON Slice to a PrimitiveType through ColumnBuilder
template <PrimitiveType ResultType>
static Status _convert_json_slice(const vpack::Slice& slice, vectorized::ColumnBuilder<ResultType>& result) {
    try {
        if (slice.isNone()) {
            result.append_null();
        } else if constexpr (ResultType == TYPE_JSON) {
            JsonValue value(slice);
            result.append(std::move(value));
        } else if (slice.isNull()) {
            result.append_null();
        } else if constexpr (ResultType == TYPE_VARCHAR || ResultType == TYPE_CHAR) {
            if (LIKELY(slice.isType(vpack::ValueType::String))) {
                vpack::ValueLength len;
                const char* str = slice.getStringUnchecked(len);
                result.append(Slice(str, len));
            } else {
                vpack::Options options = vpack::Options::Defaults;
                options.singleLinePrettyPrint = true;
                std::string str = slice.toJson(&options);

                result.append(Slice(str));
            }
        } else if constexpr (ResultType == TYPE_INT) {
            result.append(slice.getNumber<int64_t>());
        } else if constexpr (ResultType == TYPE_DOUBLE) {
            result.append(slice.getNumber<double>());
        } else {
            CHECK(false) << "unsupported";
        }
    } catch (const vpack::Exception& e) {
        return Status::InvalidArgument("failed to convert json to primitive");
    }
    return Status::OK();
}

template <PrimitiveType ResultType>
StatusOr<ColumnPtr> JsonFunctions::_json_query_impl(FunctionContext* context, const Columns& columns) {
    auto num_rows = columns[0]->size();
    auto json_viewer = ColumnViewer<TYPE_JSON>(columns[0]);
    auto path_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    ColumnBuilder<ResultType> result(num_rows);

    JsonPath stored_path;
    vpack::Builder builder;
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

        builder.clear();
        vpack::Slice slice = JsonPath::extract(json_value, *jsonpath.value(), &builder);
        Status st = _convert_json_slice<ResultType>(slice, result);
        if (!st.ok()) {
            result.append_null();
            continue;
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> JsonFunctions::json_exists(FunctionContext* context, const Columns& columns) {
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

StatusOr<ColumnPtr> JsonFunctions::json_array_empty(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(0, columns.size());
    ColumnBuilder<TYPE_JSON> result(1);
    JsonValue json(vpack::Slice::emptyArraySlice());
    result.append(std::move(json));
    return result.build(true);
}

StatusOr<ColumnPtr> JsonFunctions::json_array(FunctionContext* context, const Columns& columns) {
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

StatusOr<ColumnPtr> JsonFunctions::json_object_empty(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(0, columns.size());
    ColumnBuilder<TYPE_JSON> result(1);
    JsonValue json(vpack::Slice::emptyObjectSlice());
    result.append(std::move(json));
    return result.build(true);
}

StatusOr<ColumnPtr> JsonFunctions::json_object(FunctionContext* context, const Columns& columns) {
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

StatusOr<ColumnPtr> JsonFunctions::json_length(FunctionContext* context, const Columns& columns) {
    DCHECK_GT(columns.size(), 0);
    size_t rows = columns[0]->size();
    ColumnBuilder<TYPE_INT> result(rows);
    ColumnViewer<TYPE_JSON> json_column(columns[0]);

    std::unique_ptr<ColumnViewer<TYPE_VARCHAR>> path_viewer;
    if (columns.size() >= 2) {
        path_viewer = std::make_unique<ColumnViewer<TYPE_VARCHAR>>(columns[1]);
    }

    JsonPath stored_path;
    for (size_t row = 0; row < rows; row++) {
        if (json_column.is_null(row)) {
            result.append_null();
            continue;
        }

        JsonValue* json = json_column.value(row);
        vpack::Slice target_slice;
        vpack::Builder builder;
        if (!path_viewer || path_viewer->value(row).empty()) {
            target_slice = json->to_vslice();
        } else {
            Slice path_str = path_viewer->value(row);
            auto jsonpath = get_prepared_or_parse(context, path_str, &stored_path);
            if (UNLIKELY(!jsonpath.ok())) {
                result.append_null();
                continue;
            }

            target_slice = JsonPath::extract(json, *jsonpath.value(), &builder);
        }

        if (target_slice.isObject() || target_slice.isArray()) {
            result.append(target_slice.length());
        } else if (target_slice.isNone()) {
            result.append(0);
        } else {
            result.append(1);
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> JsonFunctions::json_keys(FunctionContext* context, const Columns& columns) {
    auto rows = columns[0]->size();
    auto json_viewer = ColumnViewer<TYPE_JSON>(columns[0]);
    ColumnBuilder<TYPE_JSON> result(rows);

    std::unique_ptr<ColumnViewer<TYPE_VARCHAR>> path_viewer;
    JsonPath stored_path;
    if (columns.size() >= 2) {
        path_viewer = std::make_unique<ColumnViewer<TYPE_VARCHAR>>(columns[1]);
    }

    for (size_t row = 0; row < rows; row++) {
        if (json_viewer.is_null(row) || json_viewer.value(row) == nullptr) {
            result.append_null();
            continue;
        }

        JsonValue* json_value = json_viewer.value(row);
        vpack::Slice vslice;
        vpack::Builder extract_builder;
        if (!path_viewer || path_viewer->value(row).empty()) {
            vslice = json_value->to_vslice();
        } else {
            Slice path_str = path_viewer->value(row);
            auto jsonpath = get_prepared_or_parse(context, path_str, &stored_path);
            if (UNLIKELY(!jsonpath.ok())) {
                result.append_null();
                continue;
            }

            vslice = JsonPath::extract(json_value, *jsonpath.value(), &extract_builder);
        }

        if (!vslice.isObject()) {
            result.append_null();
        } else {
            vpack::Builder builder;
            {
                vpack::ArrayBuilder ab(&builder);
                for (const auto& iter : vpack::ObjectIterator(vslice)) {
                    std::string key = iter.key.copyString();
                    ab->add(vpack::Value(key));
                }
            }
            vpack::Slice json_array = builder.slice();
            result.append(JsonValue(json_array));
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

<<<<<<< HEAD
StatusOr<ColumnPtr> JsonFunctions::to_json(FunctionContext* context, const Columns& columns) {
=======
ColumnPtr JsonFunctions::to_json(FunctionContext* context, const Columns& columns) {
>>>>>>> branch-2.5-mrs
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    return cast_nested_to_json(columns[0]).value();
}

} // namespace starrocks::vectorized
