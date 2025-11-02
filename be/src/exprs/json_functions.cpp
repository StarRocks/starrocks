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

#include "exprs/json_functions.h"

#include <re2/re2.h>

#include <algorithm>
#include <boost/tokenizer.hpp>
#include <memory>
#include <mutex>
#include <vector>

#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "exprs/jsonpath.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "runtime/types.h"
#include "storage/chunk_helper.h"
#include "types/logical_type.h"
#include "util/json.h"
#include "util/json_converter.h"
#include "util/json_flattener.h"
#include "velocypack/Builder.h"
#include "velocypack/Iterator.h"

namespace starrocks {

// Forward declaration for helper function
static bool json_slice_contains(const arangodb::velocypack::Slice& target,
                                const arangodb::velocypack::Slice& candidate);

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
            if (current.size() == 0 || current[0] != '$') {
                parsed_paths->emplace_back("", -1, true);
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

Status JsonFunctions::json_path_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
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
    RETURN_IF_ERROR(_get_parsed_paths(path_exprs, parsed_paths));

    context->set_function_state(scope, parsed_paths);
    VLOG(10) << "prepare json path. size: " << parsed_paths->size();
    return Status::OK();
}

Status JsonFunctions::json_path_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
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
#define HANDLE_SIMDJSON_ERROR(err, msg)                                                          \
    do {                                                                                         \
        const simdjson::error_code& _err = err;                                                  \
        const std::string& _msg = msg;                                                           \
        if (UNLIKELY(_err)) {                                                                    \
            auto err_msg = fmt::format("err: {}, msg: {}", simdjson::error_message(_err), _msg); \
            VLOG(2) << err_msg;                                                                  \
            if (_err == simdjson::NO_SUCH_FIELD || _err == simdjson::INDEX_OUT_OF_BOUNDS)        \
                return Status::NotFound(err_msg);                                                \
            return Status::DataQualityError(err_msg);                                            \
        }                                                                                        \
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
}

Status JsonFunctions::parse_json_paths(const std::string& path_string, std::vector<SimpleJsonPath>* parsed_paths) {
    // split path by ".", and escape quota by "\"
    // eg:
    //    '$.text#abc.xyz'  ->  [$, text#abc, xyz]
    //    '$."text.abc".xyz'  ->  [$, text.abc, xyz]
    //    '$."text.abc"[1].xyz'  ->  [$, text.abc[1], xyz]
    boost::tokenizer<boost::escaped_list_separator<char>> tok(path_string,
                                                              boost::escaped_list_separator<char>("\\", ".", "\""));
    std::vector<std::string> paths(tok.begin(), tok.end());
    return _get_parsed_paths(paths, parsed_paths);
}

std::string JsonFunctions::jsonpaths_to_string(const std::vector<SimpleJsonPath>& paths, size_t sub_index) {
    std::string output{"$"};
    size_t sz = (sub_index == -1 || sub_index >= paths.size()) ? paths.size() - 1 : sub_index;
    for (size_t i = 1; i <= sz; ++i) {
        output.append(".").append(paths[i].to_string());
    }
    return output;
}

StatusOr<ColumnPtr> JsonFunctions::get_json_int(FunctionContext* context, const Columns& columns) {
    return _get_json_value<TYPE_INT>(context, columns);
}

StatusOr<ColumnPtr> JsonFunctions::get_json_bigint(FunctionContext* context, const Columns& columns) {
    return _get_json_value<TYPE_BIGINT>(context, columns);
}

StatusOr<ColumnPtr> JsonFunctions::get_json_double(FunctionContext* context, const Columns& columns) {
    return _get_json_value<TYPE_DOUBLE>(context, columns);
}

StatusOr<ColumnPtr> JsonFunctions::get_json_string(FunctionContext* context, const Columns& columns) {
    return _get_json_value<TYPE_VARCHAR>(context, columns);
}

StatusOr<ColumnPtr> JsonFunctions::get_native_json_bool(FunctionContext* context, const Columns& columns) {
    return _json_query_impl<TYPE_BOOLEAN>(context, columns);
}

StatusOr<ColumnPtr> JsonFunctions::get_native_json_int(FunctionContext* context, const Columns& columns) {
    return _json_query_impl<TYPE_INT>(context, columns);
}

StatusOr<ColumnPtr> JsonFunctions::get_native_json_bigint(FunctionContext* context, const Columns& columns) {
    return _json_query_impl<TYPE_BIGINT>(context, columns);
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

StatusOr<ColumnPtr> _string_json(FunctionContext* context, const Columns& columns) {
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

template <LogicalType ResultType>
StatusOr<ColumnPtr> JsonFunctions::_get_json_value(FunctionContext* context, const Columns& columns) {
    ASSIGN_OR_RETURN(auto jsons, _string_json(context, columns));
    const auto& paths = columns[1];
    return _full_json_query_impl<ResultType>(context, Columns{jsons, paths});
}

//////////////////////////// User visiable functions /////////////////////////////////
struct NativeJsonState {
public:
    JsonPath json_path;

    // flat json used
    std::once_flag init_flat_once;
    bool init_flat = false;
    bool is_partial_match = false;
    LogicalType flat_column_type;
    std::string flat_path;
    JsonPath real_path;

    // support cast expr
    ObjectPool pool;
    Expr* ref;
    Expr* cast_expr;
};

static NativeJsonState* get_native_json_state(FunctionContext* context) {
    return reinterpret_cast<NativeJsonState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
}

static StatusOr<JsonPath*> get_prepared_or_parse(FunctionContext* context, Slice slice, JsonPath* out) {
    auto* prepared = reinterpret_cast<NativeJsonState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (prepared != nullptr && !prepared->json_path.is_empty()) {
        return &prepared->json_path;
    }
    auto res = JsonPath::parse(slice);
    RETURN_IF(!res.ok(), res.status());
    out->reset(std::move(res.value()));
    return out;
}

Status JsonFunctions::native_json_path_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (context->is_notnull_constant_column(1)) {
        auto path_column = context->get_constant_column(1);
        Slice path_value = ColumnHelper::get_const_value<TYPE_VARCHAR>(path_column);
        auto json_path = JsonPath::parse(path_value);
        RETURN_IF(!json_path.ok(), json_path.status());

        auto* state = new NativeJsonState();
        state->json_path.reset(std::move(json_path.value()));
        state->init_flat = false;
        context->set_function_state(scope, state);
        VLOG(10) << "prepare json path: " << path_value;
    } else {
        auto* state = new NativeJsonState();
        state->init_flat = false;
        context->set_function_state(scope, state);
    }
    return Status::OK();
}

Status JsonFunctions::native_json_path_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<NativeJsonState*>(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

StatusOr<ColumnPtr> JsonFunctions::json_query(FunctionContext* context, const Columns& columns) {
    return _json_query_impl<TYPE_JSON>(context, columns);
}

template <LogicalType ResultType>
StatusOr<ColumnPtr> JsonFunctions::_json_query_impl(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const auto* cc = ColumnHelper::get_data_column(columns[0].get());
    const JsonColumn* js = down_cast<const JsonColumn*>(cc);
    if (js->is_flat_json()) {
        return _flat_json_query_impl<ResultType>(context, columns);
    }
    return _full_json_query_impl<ResultType>(context, columns);
}

template <LogicalType TargetType>
static StatusOr<ColumnPtr> _extract_with_cast(FunctionContext* context, NativeJsonState* state, const std::string& path,
                                              const JsonColumn* json_column) {
    if (state->init_flat) {
        DCHECK_EQ(json_column->get_flat_field_type(state->flat_path), state->flat_column_type);
        if (state->is_partial_match) {
            DCHECK_EQ(state->flat_column_type, TYPE_JSON);
        }
        return json_column->get_flat_field(state->flat_path);
    }

    // flat json path must be constant
    JsonPath required_path;
    JsonPath* required_path_ptr = &required_path;
    ASSIGN_OR_RETURN(required_path_ptr, get_prepared_or_parse(context, path, required_path_ptr));

    JsonPath real_path;
    for (const auto& flat_path : json_column->flat_column_paths()) {
        ASSIGN_OR_RETURN(auto flat_json_path, JsonPath::parse(flat_path));
        // flat path's depth must less than required_path
        if (required_path_ptr->starts_with(&flat_json_path)) {
            RETURN_IF_ERROR(required_path_ptr->relativize(&flat_json_path, &real_path));

            std::call_once(state->init_flat_once, [&] {
                state->is_partial_match = !real_path.paths.empty();
                state->flat_column_type = json_column->get_flat_field_type(flat_path);
                state->flat_path = flat_path;
                state->real_path.reset(real_path);
                if (TargetType != TYPE_UNKNOWN && real_path.paths.empty() && state->flat_column_type != TargetType) {
                    // full match, check target type is match flat type, need cast again
                    state->ref = state->pool.add(new ColumnRef(TypeDescriptor(state->flat_column_type), 0));
                    state->cast_expr =
                            VectorizedCastExprFactory::from_type(TypeDescriptor(state->flat_column_type),
                                                                 TypeDescriptor(TargetType), state->ref, &state->pool);
                }
                state->init_flat = true;
            });

            return json_column->get_flat_field(flat_path);
        }
    }
    // not found, only should hit here in ut test
    return Status::JsonFormatError(fmt::format("flat json not found json path: {}", path));
}

template <LogicalType TargetType>
static StatusOr<ColumnPtr> _extract_with_hyper(NativeJsonState* state, const std::string& path,
                                               const JsonColumn* json_column) {
    if (!state->init_flat) {
        ASSIGN_OR_RETURN(auto flat_json_path, JsonPath::parse(path));
        std::call_once(state->init_flat_once, [&] {
            std::string flat_path = "";
            bool in_flat = true;
            for (size_t k = 0; k < flat_json_path.paths.size(); k++) {
                auto& p = flat_json_path.paths[k];
                if (p.key == "$" && p.array_selector->type == NONE) {
                    state->real_path.paths.emplace_back(p);
                    continue;
                }
                if (p.key.find('.') != std::string::npos) {
                    in_flat = false;
                }
                if (in_flat) {
                    flat_path += "." + p.key;
                    if (p.array_selector->type != NONE) {
                        state->real_path.paths.emplace_back("", p.array_selector);
                        in_flat = false;
                    }
                    continue;
                }
                state->real_path.paths.emplace_back(p);
            }

            if (in_flat) {
                state->is_partial_match = false;
                state->flat_column_type = TargetType;
            } else {
                state->is_partial_match = true;
                state->flat_column_type = TYPE_JSON;
            }
            state->flat_path = flat_path.substr(1);
            state->init_flat = true;
        });
    }
    std::vector<std::string> dst_path{state->flat_path};
    LogicalType dtype = state->flat_column_type;
    if constexpr (TargetType == TYPE_UNKNOWN) {
        if (dtype == TYPE_UNKNOWN) {
            dtype = TYPE_JSON;
            const auto& paths = json_column->flat_column_paths();
            for (size_t i = 0; i < paths.size(); i++) {
                if (paths[i] == state->flat_path) {
                    dtype = json_column->get_flat_field_type(paths[i]);
                    break;
                }
            }
        }
    }
    std::vector<LogicalType> dst_type{dtype};
    HyperJsonTransformer transform(dst_path, dst_type, false);
    transform.init_read_task(json_column->flat_column_paths(), json_column->flat_column_types(),
                             json_column->has_remain());

    RETURN_IF_ERROR(transform.trans(json_column->get_flat_fields_ptrs()));
    auto res = transform.mutable_result();
    DCHECK_EQ(1, res.size());
    res[0]->check_or_die();
    return res[0];
}

template <LogicalType TargetType>
static StatusOr<ColumnPtr> _extract_from_flat_json(FunctionContext* context, const Columns& columns) {
    if (UNLIKELY(columns[0]->is_constant())) {
        return Status::JsonFormatError("flat json doesn't support constant json");
    }

    auto* state = get_native_json_state(context);
    if (UNLIKELY(state == nullptr)) {
        // ut test may be hit here, the json path is invaild
        return Status::JsonFormatError("flat json required prepare status");
    }

    const JsonColumn* json_column;
    if (columns[0]->is_nullable()) {
        auto* nullable = down_cast<const NullableColumn*>(columns[0].get());
        json_column = down_cast<const JsonColumn*>(nullable->data_column().get());
    } else {
        json_column = down_cast<const JsonColumn*>(columns[0].get());
    }

    // flat json path must be constant
    std::string path;
    if (!state->init_flat) {
        if (columns[1]->only_null()) {
            // only null path, return null
            return ColumnHelper::create_const_null_column(columns[0]->size());
        } else if (LIKELY(columns[1]->is_constant())) {
            path = ColumnHelper::get_const_value<TYPE_VARCHAR>(columns[1].get()).to_string();
        } else {
            // just for compatible
            ColumnViewer<TYPE_VARCHAR> viewer(columns[1]);
            if (viewer.is_null(0) || (columns[1]->size() > 1 && viewer.is_null(1))) {
                return Status::JsonFormatError("flat json doesn't support null json path");
            }
            path = viewer.value(0).to_string();
            if (columns[1]->size() > 1 && path != viewer.value(1).to_string()) {
                return Status::JsonFormatError("flat json doesn't support variables json path");
            }
        }
    } else {
        path = state->flat_path;
    }

    if (config::enable_lazy_dynamic_flat_json) {
        return _extract_with_hyper<TargetType>(state, path, json_column);
    } else {
        return _extract_with_cast<TargetType>(context, state, path, json_column);
    }
}

template <LogicalType ResultType>
StatusOr<ColumnPtr> JsonFunctions::_flat_json_query_impl(FunctionContext* context, const Columns& columns) {
    ASSIGN_OR_RETURN(auto flat_column, _extract_from_flat_json<ResultType>(context, columns));
    auto* state = get_native_json_state(context);
    if (state->is_partial_match) {
        // partial match, must be json type
        auto num_rows = flat_column->size();
        auto json_viewer = ColumnViewer<TYPE_JSON>(flat_column);
        ColumnBuilder<ResultType> result(num_rows);

        JsonPath stored_path;
        vpack::Builder builder;
        for (int row = 0; row < num_rows; ++row) {
            if (json_viewer.is_null(row)) {
                result.append_null();
                continue;
            }
            JsonValue* json_value = json_viewer.value(row);
            builder.clear();
            vpack::Slice slice = JsonPath::extract(json_value, state->real_path, &builder);
            Status st = cast_vpjson_to<ResultType, false>(slice, result);
            if (!st.ok()) {
                result.append_null();
                continue;
            }
        }
        return result.build(ColumnHelper::is_all_const(columns));

    } else {
        // full match
        StatusOr<ColumnPtr> ret;
        if (ResultType != state->flat_column_type) {
            DCHECK(state->cast_expr != nullptr);
            Chunk chunk;
            chunk.append_column(flat_column, 0);
            ret = state->cast_expr->evaluate_checked(nullptr, &chunk);
        } else {
            ret = std::move(flat_column->clone());
        }
        if (ret.ok()) {
            ret.value()->check_or_die();
            return Column::mutate(std::move(ret.value()));
        } else {
            return ret;
        }
    }
}

template <LogicalType ResultType>
StatusOr<ColumnPtr> JsonFunctions::_full_json_query_impl(FunctionContext* context, const Columns& columns) {
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
        Status st = cast_vpjson_to<ResultType, false>(slice, result);
        if (!st.ok()) {
            result.append_null();
            continue;
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> JsonFunctions::json_exists(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const auto* cc = ColumnHelper::get_data_column(columns[0].get());
    const JsonColumn* js = down_cast<const JsonColumn*>(cc);
    if (js->is_flat_json()) {
        return _flat_json_exists(context, columns);
    }
    return _full_json_exists(context, columns);
}

StatusOr<ColumnPtr> JsonFunctions::_flat_json_exists(FunctionContext* context, const Columns& columns) {
    // exists is don't care flat type
    ASSIGN_OR_RETURN(auto flat_column, _extract_from_flat_json<TYPE_UNKNOWN>(context, columns));
    size_t rows = columns[0]->size();
    auto* state = get_native_json_state(context);
    if (state->is_partial_match) {
        auto json_viewer = ColumnViewer<TYPE_JSON>(flat_column);
        ColumnBuilder<TYPE_BOOLEAN> result(rows);

        JsonPath stored_path;
        for (int row = 0; row < rows; row++) {
            if (columns[0]->is_null(row)) {
                result.append_null();
                continue;
            }
            if (json_viewer.is_null(row) || json_viewer.value(row) == nullptr) {
                result.append(0);
                continue;
            }
            JsonValue* json_value = json_viewer.value(row);
            vpack::Builder builder;
            vpack::Slice slice = JsonPath::extract(json_value, state->real_path, &builder);
            result.append(!slice.isNone());
        }
        return result.build(ColumnHelper::is_all_const(columns));
    } else {
        ColumnBuilder<TYPE_BOOLEAN> result(rows);
        for (size_t row = 0; row < rows; ++row) {
            if (columns[0]->is_null(row)) {
                // only the json value is null, return null
                result.append_null();
                continue;
            }
            result.append(!flat_column->is_null(row));
        }
        return result.build(ColumnHelper::is_all_const(columns));
    }
}

StatusOr<ColumnPtr> JsonFunctions::_full_json_exists(FunctionContext* context, const Columns& columns) {
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

StatusOr<ColumnPtr> JsonFunctions::json_contains(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto num_rows = columns[0]->size();
    auto target_viewer = ColumnViewer<TYPE_JSON>(columns[0]);
    auto candidate_viewer = ColumnViewer<TYPE_JSON>(columns[1]);
    ColumnBuilder<TYPE_BOOLEAN> result(num_rows);

    for (int row = 0; row < num_rows; row++) {
        if (target_viewer.is_null(row) || target_viewer.value(row) == nullptr || candidate_viewer.is_null(row) ||
            candidate_viewer.value(row) == nullptr) {
            result.append_null();
            continue;
        }

        JsonValue* target_json = target_viewer.value(row);
        JsonValue* candidate_json = candidate_viewer.value(row);

        // Check if target contains candidate
        bool contains = json_value_contains(target_json, candidate_json);
        result.append(contains);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> JsonFunctions::json_array_empty(FunctionContext* context, const Columns& columns) {
    RETURN_IF(columns.size() != 0, Status::InvalidArgument("json_array_empty requires none parameter"));
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
        viewers.emplace_back(col);
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
    RETURN_IF(0 != columns.size(), Status::InvalidArgument("json_object_empty requires 0 arguments"));
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
        viewers.emplace_back(col);
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
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    const auto* cc = ColumnHelper::get_data_column(columns[0].get());
    const JsonColumn* js = down_cast<const JsonColumn*>(cc);
    if (js->is_flat_json()) {
        return _flat_json_length(context, columns);
    }
    return _full_json_length(context, columns);
}

StatusOr<ColumnPtr> JsonFunctions::_flat_json_length(FunctionContext* context, const Columns& columns) {
    ASSIGN_OR_RETURN(auto flat_column, _extract_from_flat_json<TYPE_JSON>(context, columns));
    size_t rows = columns[0]->size();

    auto* state = get_native_json_state(context);
    if (state->is_partial_match) {
        ColumnBuilder<TYPE_INT> result(rows);
        ColumnViewer<TYPE_JSON> json_viewer(flat_column);

        JsonPath stored_path;
        for (size_t row = 0; row < rows; row++) {
            if (json_viewer.is_null(row)) {
                result.append_null();
                continue;
            }

            JsonValue* json = json_viewer.value(row);
            vpack::Slice target_slice;
            vpack::Builder builder;
            target_slice = JsonPath::extract(json, state->real_path, &builder);

            if (target_slice.isObject() || target_slice.isArray()) {
                result.append(target_slice.length());
            } else if (target_slice.isNone()) {
                result.append(0);
            } else {
                result.append(1);
            }
        }
        return result.build(ColumnHelper::is_all_const(columns));
    } else {
        // full match
        ColumnViewer<TYPE_JSON> viewer(flat_column);
        ColumnBuilder<TYPE_INT> result(rows);
        DCHECK_EQ(state->flat_column_type, TYPE_JSON);
        for (size_t row = 0; row < rows; ++row) {
            if (columns[0]->is_null(row)) {
                // only the json value is null, return null
                result.append_null();
                continue;
            }
            if (viewer.is_null(row)) {
                result.append(0);
                continue;
            }
            vpack::Slice slice = viewer.value(row)->to_vslice();
            if (slice.isObject() || slice.isArray()) {
                result.append(slice.length());
            } else if (slice.isNone()) {
                result.append(0);
            } else {
                result.append(1);
            }
        }
        return result.build(ColumnHelper::is_all_const(columns));
    }
}

StatusOr<ColumnPtr> JsonFunctions::_full_json_length(FunctionContext* context, const Columns& columns) {
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
    if (columns.size() < 2) {
        return _json_keys_without_path(context, columns);
    }

    const auto* cc = ColumnHelper::get_data_column(columns[0].get());
    const JsonColumn* js = down_cast<const JsonColumn*>(cc);
    if (js->is_flat_json()) {
        return _flat_json_keys_with_path(context, columns);
    }

    return _full_json_keys_with_path(context, columns);
}

StatusOr<ColumnPtr> JsonFunctions::_flat_json_keys_with_path(FunctionContext* context, const Columns& columns) {
    ASSIGN_OR_RETURN(auto flat_column, _extract_from_flat_json<TYPE_JSON>(context, columns));
    auto* state = get_native_json_state(context);
    size_t rows = columns[0]->size();
    if (state->is_partial_match) {
        ColumnViewer<TYPE_JSON> json_viewer(flat_column);
        ColumnBuilder<TYPE_JSON> result(rows);

        for (size_t row = 0; row < rows; ++row) {
            if (columns[0]->is_null(row) || json_viewer.is_null(row)) {
                result.append_null();
                continue;
            }

            JsonValue* json = json_viewer.value(row);
            vpack::Builder builder;
            auto slice = JsonPath::extract(json, state->real_path, &builder);

            if (!slice.isObject()) {
                result.append_null();
            } else {
                vpack::Builder builder;
                {
                    vpack::ArrayBuilder ab(&builder);
                    for (const auto& iter : vpack::ObjectIterator(slice)) {
                        std::string key = iter.key.copyString();
                        ab->add(vpack::Value(key));
                    }
                }
                vpack::Slice json_array = builder.slice();
                result.append(JsonValue(json_array));
            }
        }
        return result.build(ColumnHelper::is_all_const(columns));
    } else {
        // full match
        ColumnViewer<TYPE_JSON> json_viewer(flat_column);
        ColumnBuilder<TYPE_JSON> result(rows);

        for (size_t row = 0; row < rows; ++row) {
            if (columns[0]->is_null(row) || json_viewer.is_null(row)) {
                result.append_null();
                continue;
            }
            vpack::Slice slice = json_viewer.value(row)->to_vslice();
            if (!slice.isObject()) {
                result.append_null();
            } else {
                vpack::Builder builder;
                {
                    vpack::ArrayBuilder ab(&builder);
                    for (const auto& iter : vpack::ObjectIterator(slice)) {
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
}

StatusOr<ColumnPtr> JsonFunctions::_full_json_keys_with_path(FunctionContext* context, const Columns& columns) {
    auto rows = columns[0]->size();
    auto json_viewer = ColumnViewer<TYPE_JSON>(columns[0]);
    ColumnBuilder<TYPE_JSON> result(rows);
    ColumnViewer<TYPE_VARCHAR> path_viewer(columns[1]);
    JsonPath stored_path;

    for (size_t row = 0; row < rows; row++) {
        if (json_viewer.is_null(row) || json_viewer.value(row) == nullptr) {
            result.append_null();
            continue;
        }

        JsonValue* json_value = json_viewer.value(row);
        vpack::Slice vslice;
        vpack::Builder extract_builder;
        Slice path_str = path_viewer.value(row);
        auto jsonpath = get_prepared_or_parse(context, path_str, &stored_path);
        if (UNLIKELY(!jsonpath.ok())) {
            result.append_null();
            continue;
        }

        vslice = JsonPath::extract(json_value, *jsonpath.value(), &extract_builder);

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

StatusOr<ColumnPtr> JsonFunctions::_json_keys_without_path(FunctionContext* context, const Columns& columns) {
    auto rows = columns[0]->size();
    auto json_viewer = ColumnViewer<TYPE_JSON>(columns[0]);
    ColumnBuilder<TYPE_JSON> result(rows);

    JsonPath stored_path;

    for (size_t row = 0; row < rows; row++) {
        if (json_viewer.is_null(row) || json_viewer.value(row) == nullptr) {
            result.append_null();
            continue;
        }

        JsonValue* json_value = json_viewer.value(row);
        vpack::Slice vslice;
        vpack::Builder extract_builder;
        vslice = json_value->to_vslice();
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

static void build_path_lookup_structures(const std::vector<JsonPath>& valid_paths,
                                         std::unordered_set<std::string>& exact_paths_to_remove,
                                         std::unordered_set<std::string>& prefix_paths_to_remove);

static StatusOr<JsonValue> _remove_json_paths_core(JsonValue* json_value,
                                                   const std::unordered_set<std::string>& exact_paths_to_remove,
                                                   const std::unordered_set<std::string>& prefix_paths_to_remove,
                                                   vpack::Builder* builder);

StatusOr<ColumnPtr> JsonFunctions::json_remove(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    RETURN_IF(columns.size() < 2,
              Status::InvalidArgument("json_remove requires at least 2 arguments: json_doc and path"));

    size_t rows = columns[0]->size();
    ColumnBuilder<TYPE_JSON> result(rows);
    ColumnViewer<TYPE_JSON> json_viewer(columns[0]);

    // Get all path arguments
    std::vector<ColumnViewer<TYPE_VARCHAR>> path_viewers;
    for (size_t i = 1; i < columns.size(); i++) {
        path_viewers.emplace_back(columns[i]);
    }

    // Parse all valid paths once at the beginning to amortize parsing overhead
    std::vector<JsonPath> valid_paths;
    bool all_paths_constant = true;

    for (size_t path_idx = 0; path_idx < path_viewers.size(); path_idx++) {
        // Check if this path column is constant
        bool is_constant = columns[path_idx + 1]->is_constant();

        if (is_constant) {
            // For constant paths, parse once and reuse
            if (!path_viewers[path_idx].is_null(0)) {
                Slice path_str = path_viewers[path_idx].value(0);
                auto jsonpath = JsonPath::parse(path_str);
                if (jsonpath.ok()) {
                    valid_paths.emplace_back(jsonpath.value());
                }
            }
        } else {
            all_paths_constant = false;
        }
    }

    // Build lookup structures once for constant paths to amortize construction overhead
    std::unordered_set<std::string> exact_paths_to_remove;
    std::unordered_set<std::string> prefix_paths_to_remove;
    build_path_lookup_structures(valid_paths, exact_paths_to_remove, prefix_paths_to_remove);

    for (size_t row = 0; row < rows; row++) {
        if (json_viewer.is_null(row) || json_viewer.value(row) == nullptr) {
            result.append_null();
            continue;
        }
        JsonValue* json_value = json_viewer.value(row);

        // Create new JSON with paths removed
        vpack::Builder builder;

        if (all_paths_constant) {
            // All paths are constant, use pre-built lookup structures directly
            ASSIGN_OR_RETURN(auto removed_json, _remove_json_paths_core(json_value, exact_paths_to_remove,
                                                                        prefix_paths_to_remove, &builder));
            result.append(std::move(removed_json));
        } else {
            // Some paths are non-constant, parse them for each row
            std::vector<JsonPath> row_paths = valid_paths; // Start with pre-parsed constant paths

            for (size_t path_idx = 0; path_idx < path_viewers.size(); path_idx++) {
                bool is_constant = columns[path_idx + 1]->is_constant();

                if (!is_constant) {
                    // Parse non-constant paths for each row
                    if (!path_viewers[path_idx].is_null(row)) {
                        Slice path_str = path_viewers[path_idx].value(row);
                        auto jsonpath = JsonPath::parse(path_str);
                        if (jsonpath.ok()) {
                            row_paths.emplace_back(jsonpath.value());
                        }
                    }
                }
            }

            // Build lookup structures for this row (including non-constant paths)
            std::unordered_set<std::string> row_exact_paths = exact_paths_to_remove;
            std::unordered_set<std::string> row_prefix_paths = prefix_paths_to_remove;

            // Add lookup structures for non-constant paths
            std::vector<JsonPath> non_constant_paths;
            for (size_t i = valid_paths.size(); i < row_paths.size(); i++) {
                non_constant_paths.push_back(row_paths[i]);
            }
            build_path_lookup_structures(non_constant_paths, row_exact_paths, row_prefix_paths);

            ASSIGN_OR_RETURN(auto removed_json,
                             _remove_json_paths_core(json_value, row_exact_paths, row_prefix_paths, &builder));
            result.append(std::move(removed_json));
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// Helper function to build lookup structures for path removal
static void build_path_lookup_structures(const std::vector<JsonPath>& valid_paths,
                                         std::unordered_set<std::string>& exact_paths_to_remove,
                                         std::unordered_set<std::string>& prefix_paths_to_remove) {
    for (const auto& remove_path : valid_paths) {
        std::string path_str = remove_path.to_string();
        exact_paths_to_remove.insert(path_str);

        // Build prefix paths for quick recursion decision by iterating JsonPath::paths
        // instead of string operations to handle escaped dots correctly
        std::string current_prefix = "$";
        for (size_t i = 0; i < remove_path.paths.size(); i++) {
            const auto& piece = remove_path.paths[i];

            // Add the key part (skip the root "$" piece)
            if (!piece.key.empty() && piece.key != "$") {
                current_prefix += "." + piece.key;
            }

            // Add the path before array selector
            prefix_paths_to_remove.insert(current_prefix);

            // Add array selector if present
            if (piece.array_selector) {
                current_prefix += piece.array_selector->to_string();
                prefix_paths_to_remove.insert(current_prefix);
            }
        }
    }
}

// Core function that performs the actual JSON removal work
static StatusOr<JsonValue> _remove_json_paths_core(JsonValue* json_value,
                                                   const std::unordered_set<std::string>& exact_paths_to_remove,
                                                   const std::unordered_set<std::string>& prefix_paths_to_remove,
                                                   vpack::Builder* builder) {
    namespace vpack = arangodb::velocypack;

    vpack::Slice original_slice = json_value->to_vslice();

    // New recursive writers that build directly into `builder` to avoid returning slices
    std::function<void(vpack::Builder*, vpack::Slice, const std::string&)> append_object_fields;
    std::function<void(vpack::Builder*, vpack::Slice, const std::string&)> append_array_elements;

    append_object_fields = [&](vpack::Builder* out, vpack::Slice obj_slice, const std::string& current_path) {
        for (auto it : vpack::ObjectIterator(obj_slice)) {
            std::string key = it.key.copyString();
            std::string child_path = current_path.empty() ? ("$." + key) : (current_path + "." + key);

            // Exact match: drop the key
            if (exact_paths_to_remove.find(child_path) != exact_paths_to_remove.end()) {
                continue;
            }

            vpack::Slice value = it.value;
            if (value.isNone()) {
                continue;
            }

            bool needs_recursion = (value.isObject() || value.isArray()) &&
                                   (prefix_paths_to_remove.find(child_path) != prefix_paths_to_remove.end());

            if (!needs_recursion) {
                out->add(key, value);
                continue;
            }

            if (value.isObject()) {
                vpack::ObjectBuilder child(out, key);
                append_object_fields(out, value, child_path);
            } else {
                vpack::ArrayBuilder child(out, key);
                append_array_elements(out, value, child_path);
            }
        }
    };

    append_array_elements = [&](vpack::Builder* out, vpack::Slice arr_slice, const std::string& current_path) {
        size_t array_size = arr_slice.length();
        for (size_t index = 0; index < array_size; ++index) {
            std::string child_path = current_path + "[" + std::to_string(index) + "]";

            // Exact match: drop the element
            if (exact_paths_to_remove.find(child_path) != exact_paths_to_remove.end()) {
                continue;
            }

            vpack::Slice element = arr_slice.at(index);
            if (element.isNone()) {
                continue;
            }

            bool needs_recursion = (element.isObject() || element.isArray()) &&
                                   (prefix_paths_to_remove.find(child_path) != prefix_paths_to_remove.end());

            if (!needs_recursion) {
                out->add(element);
                continue;
            }

            if (element.isObject()) {
                vpack::ObjectBuilder child(out);
                append_object_fields(out, element, child_path);
            } else {
                vpack::ArrayBuilder child(out);
                append_array_elements(out, element, child_path);
            }
        }
    };

    if (original_slice.isObject()) {
        vpack::ObjectBuilder ob(builder);
        append_object_fields(builder, original_slice, "$");
    } else if (original_slice.isArray()) {
        vpack::ArrayBuilder ab(builder);
        append_array_elements(builder, original_slice, "$");
    } else {
        builder->add(original_slice);
    }

    return JsonValue(builder->slice());
}

StatusOr<ColumnPtr> JsonFunctions::to_json(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    return cast_nested_to_json(columns[0], context->allow_throw_exception());
}

bool JsonFunctions::json_value_contains(JsonValue* target, JsonValue* candidate) {
    if (target == nullptr || candidate == nullptr) {
        return false;
    }

    vpack::Slice target_slice = target->to_vslice();
    vpack::Slice candidate_slice = candidate->to_vslice();

    return json_slice_contains(target_slice, candidate_slice);
}

/**
 * Class to handle JSON containment logic with recursive helper methods.
 * 
 * This class implements the JSON_CONTAINS function logic, which checks if a target JSON
 * document contains a candidate JSON value or subdocument. The containment rules are:
 * 
 * 1. For scalar values: exact equality
 * 2. For objects: target must contain all key-value pairs from candidate
 * 3. For arrays: 
 *    - If candidate is array: target must contain all elements from candidate
 *    - If candidate is object: target must contain all key-value pairs from candidate
 *      (distributed across array elements)
 *    - If candidate is scalar: target must contain the scalar value
 * 4. For nested structures: recursive containment checking
 */
class JsonContainmentChecker {
public:
    // Main recursive function to check if target slice contains candidate slice
    static bool contains(const arangodb::velocypack::Slice& target, const arangodb::velocypack::Slice& candidate) {
        // Handle null cases
        if (candidate.isNull()) {
            return true;
        }
        if (target.isNull()) {
            return false;
        }

        // Direct equality check
        if (JsonValue::compare(target, candidate) == 0) {
            return true;
        }

        // Handle different type combinations using recursion
        if (target.isObject() && candidate.isObject()) {
            return check_object_contains_object(target, candidate);
        }

        if (target.isArray()) {
            if (candidate.isArray()) {
                return check_array_contains_array(target, candidate);
            } else if (candidate.isObject()) {
                return check_array_contains_distributed_object(target, candidate);
            } else {
                return check_array_contains_value(target, candidate);
            }
        }

        // For scalar values, they must be equal (already checked above)
        return false;
    }

private:
    // Check if an object contains all key-value pairs from another object
    static bool check_object_contains_object(const arangodb::velocypack::Slice& target,
                                             const arangodb::velocypack::Slice& candidate) {
        for (auto const& item : arangodb::velocypack::ObjectIterator(candidate)) {
            std::string key = item.key.copyString();
            if (!target.hasKey(key) || JsonValue::compare(target.get(key), item.value) != 0) {
                return false;
            }
        }
        return true;
    }

    // Check if an array contains all elements from another array
    static bool check_array_contains_array(const arangodb::velocypack::Slice& target,
                                           const arangodb::velocypack::Slice& candidate) {
        for (auto const& cand_item : arangodb::velocypack::ArrayIterator(candidate)) {
            bool found = false;
            for (auto const& target_item : arangodb::velocypack::ArrayIterator(target)) {
                if (contains(target_item, cand_item)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    // Check if an array contains a single value (scalar or object)
    static bool check_array_contains_value(const arangodb::velocypack::Slice& target,
                                           const arangodb::velocypack::Slice& candidate) {
        for (auto const& target_item : arangodb::velocypack::ArrayIterator(target)) {
            if (contains(target_item, candidate)) {
                return true;
            }
        }
        return false;
    }

    // Check distributed containment: if all key-value pairs from candidate object
    // can be found somewhere in the target array
    static bool check_array_contains_distributed_object(const arangodb::velocypack::Slice& target,
                                                        const arangodb::velocypack::Slice& candidate) {
        for (auto const& cand_item : arangodb::velocypack::ObjectIterator(candidate)) {
            std::string key = cand_item.key.copyString();
            bool found = false;
            for (auto const& target_item : arangodb::velocypack::ArrayIterator(target)) {
                if (target_item.isObject() && target_item.hasKey(key) &&
                    JsonValue::compare(target_item.get(key), cand_item.value) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }
};

// Main recursive function to check if target slice contains candidate slice
static bool json_slice_contains(const arangodb::velocypack::Slice& target,
                                const arangodb::velocypack::Slice& candidate) {
    return JsonContainmentChecker::contains(target, candidate);
}

} // namespace starrocks

#include "gen_cpp/opcode/JsonFunctions.inc"
