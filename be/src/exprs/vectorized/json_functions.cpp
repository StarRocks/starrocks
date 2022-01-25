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
#include "glog/logging.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "util/json.h"
#include "velocypack/Builder.h"
#include "velocypack/Exception.h"
#include "velocypack/Iterator.h"
#include "velocypack/Slice.h"
#include "velocypack/Value.h"
#include "velocypack/ValueType.h"
#include "velocypack/vpack.h"

namespace starrocks::vectorized {

// static const re2::RE2 JSON_PATTERN("^([a-zA-Z0-9_\\-\\:\\s#\\|\\.]*)(?:\\[([0-9]+)\\])?");
// json path cannot contains: ", [, ]
static const re2::RE2 JSON_PATTERN(R"(^([^\"\[\]]*)(?:\[([0-9\:\*]+)\])?)");
static const re2::RE2 ARRAY_SINGLE_SELECTOR(R"(\d+)");
static const re2::RE2 ARRAY_SLICE_SELECTOR(R"(\d+\:\d+)");

bool ArraySelectorSingle::match(const std::string& input) {
    return RE2::FullMatch(input, ARRAY_SINGLE_SELECTOR);
}

bool ArraySelectorWildcard::match(const std::string& input) {
    return input == "*";
}

bool ArraySelectorSlice::match(const std::string& input) {
    return RE2::FullMatch(input, ARRAY_SLICE_SELECTOR);
}

void ArraySelectorSingle::iterate(vpack::Slice array_slice, std::function<void(vpack::Slice)> callback) {
    try {
        callback(array_slice.at(index));
    } catch (const vpack::Exception& e) {
        if (e.errorCode() == vpack::Exception::IndexOutOfBounds) {
            callback(noneJsonSlice());
        }
    }
}

void ArraySelectorWildcard::iterate(vpack::Slice array_slice, std::function<void(vpack::Slice)> callback) {
    for (auto item : vpack::ArrayIterator(array_slice)) {
        callback(item);
    }
}

void ArraySelectorSlice::iterate(vpack::Slice array_slice, std::function<void(vpack::Slice)> callback) {
    int index = 0;
    for (auto item : vpack::ArrayIterator(array_slice)) {
        if (left <= index && index < right) {
            callback(item);
        } else if (index >= right) {
            break;
        }
        index++;
    }
}

// 1. arr[x] select the x th element
// 2. arr[*] select all elements
// 3. arr[1:3] select slice of elements
Status ArraySelector::parse(const std::string& index, std::unique_ptr<ArraySelector>* output) {
    if (index.empty()) {
        output->reset(new ArraySelectorNone());
        return Status::OK();
    } else if (ArraySelectorSingle::match(index)) {
        StringParser::ParseResult result;
        int index_int = StringParser::string_to_int<int>(index.c_str(), index.length(), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", index));
        }
        output->reset(new ArraySelectorSingle(index_int));
        return Status::OK();
    } else if (ArraySelectorWildcard::match(index)) {
        output->reset(new ArraySelectorWildcard());
        return Status::OK();
    } else if (ArraySelectorSlice::match(index)) {
        std::vector<std::string> slices = strings::Split(index, ":");
        if (slices.size() != 2) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", index));
        }

        StringParser::ParseResult result;
        int left = StringParser::string_to_int<int>(slices[0].c_str(), slices[0].length(), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", index));
        }
        int right = StringParser::string_to_int<int>(slices[1].c_str(), slices[1].length(), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", index));
        }

        output->reset(new ArraySelectorSlice(left, right));
        return Status::OK();
    }

    return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", index));
}

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

        if (UNLIKELY(!RE2::FullMatch(path_exprs[i], JSON_PATTERN, &col, &index))) {
            parsed_paths->emplace_back("", -1, false);
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", path_exprs[i]));
        } else {
            int idx = -1;
            if (!index.empty()) {
                // TODO(mofei) support wildcard
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

    boost::tokenizer<boost::escaped_list_separator<char>> tok(path_str,
                                                              boost::escaped_list_separator<char>("\\", ".", "\""));
    std::vector<std::string> path_exprs(tok.begin(), tok.end());
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

bool JsonFunctions::extract_from_object(simdjson::ondemand::object& obj, const std::vector<SimpleJsonPath>& jsonpath,
                                        simdjson::ondemand::value& value) {
    simdjson::ondemand::value tvalue;
    bool ok = false;

    // Skip the first $.
    for (int i = 1; i < jsonpath.size(); i++) {
        if (UNLIKELY(!jsonpath[i].is_valid)) {
            return false;
        }

        const std::string& col = jsonpath[i].key;
        int index = jsonpath[i].idx;

        if (i == 1) {
            auto err = obj.find_field_unordered(col).get(tvalue);
            if (err) {
                return false;
            }
            ok = true;
        } else {
            auto err = tvalue.find_field_unordered(col).get(tvalue);
            if (err) {
                return false;
            }
            ok = true;
        }

        if (ok && index != -1) {
            auto arr = tvalue.get_array();
            if (arr.error()) {
                return false;
            }

            int idx = 0;
            for (auto a : arr) {
                if (a.error()) {
                    return false;
                }

                if (idx++ == index) {
                    a.get(tvalue);
                    break;
                }
            }
        }
    }

    std::swap(value, tvalue);

    return ok;
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

        std::vector<SimpleJsonPath> jsonpath;
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
            if (!extract_from_object(obj, jsonpath, value)) {
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
                if (!extract_from_object(obj, jsonpath, value)) {
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
}

template <PrimitiveType primitive_type>
void JsonFunctions::_build_column(ColumnBuilder<primitive_type>& result, simdjson::ondemand::value& value) {
    if constexpr (primitive_type == TYPE_INT) {
        int64_t i64;
        auto err = value.get_int64().get(i64);
        if (UNLIKELY(err)) {
            result.append_null();
            return;
        }

        result.append(i64);
        return;

    } else if constexpr (primitive_type == TYPE_DOUBLE) {
        double d;
        auto err = value.get_double().get(d);
        if (UNLIKELY(err)) {
            result.append_null();
            return;
        }

        result.append(d);
        return;

    } else if constexpr (primitive_type == TYPE_VARCHAR) {
        std::string_view sv;
        auto err = value.get_string().get(sv);
        if (UNLIKELY(err)) {
            result.append_null();
            return;
        }

        result.append(Slice{sv.data(), sv.size()});
        return;
    } else {
        result.append_null();
    }
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
            result.append(&json.value());
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
                result.append(json_str.value());
            }
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

Status JsonFunctions::parse_full_json_paths(const std::string& path_string, std::vector<JsonPath>* parsed_paths) {
    // split path by ".", and escape quota by "\"
    // eg:
    //    '$.text#abc.xyz'  ->  [$, text#abc, xyz]
    //    '$."text.abc".xyz'  ->  [$, text.abc, xyz]
    //    '$."text.abc"[1].xyz'  ->  [$, text.abc[1], xyz]
    boost::tokenizer<boost::escaped_list_separator<char>> tok(path_string,
                                                              boost::escaped_list_separator<char>("\\", ".", "\""));
    std::vector<std::string> path_exprs(tok.begin(), tok.end());

    for (int i = 0; i < path_exprs.size(); i++) {
        std::string col;
        std::string index;
        auto& current = path_exprs[i];

        if (i == 0) {
            std::unique_ptr<ArraySelector> selector(new ArraySelectorNone());
            if (current != "$") {
                parsed_paths->emplace_back(JsonPath("", std::move(selector)));
            } else {
                parsed_paths->emplace_back(JsonPath("$", std::move(selector)));
                continue;
            }
        }

        if (UNLIKELY(!RE2::FullMatch(path_exprs[i], JSON_PATTERN, &col, &index))) {
            parsed_paths->emplace_back("", std::unique_ptr<ArraySelector>(new ArraySelectorNone()));
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", path_exprs[i]));
        } else {
            std::unique_ptr<ArraySelector> selector;
            RETURN_IF_ERROR(ArraySelector::parse(index, &selector));
            parsed_paths->emplace_back(JsonPath(col, std::move(selector)));
        }
    }

    return Status::OK();
}

vpack::Slice JsonFunctions::_extract_full_from_object(const JsonValue* json, const std::vector<JsonPath>& jsonpath,
                                                      vpack::Builder* builder) {
    return _extract_full_from_object(json->to_vslice(), jsonpath, 1, builder);
}

// TODO(mofei) it's duplicated with extract_from_object
vpack::Slice JsonFunctions::_extract_full_from_object(vpack::Slice root, const std::vector<JsonPath>& jsonpath,
                                                      int path_index, vpack::Builder* builder) {
    vpack::Slice current_value = root;

    for (int i = path_index; i < jsonpath.size(); i++) {
        auto& path_item = jsonpath[i];
        auto item_key = path_item.key;
        auto& array_selector = path_item.array_selector;

        // iterate the path key
        if (!current_value.isObject()) {
            return noneJsonSlice();
        }
        vpack::Slice next_item = current_value.get(item_key);
        if (next_item.isNone()) {
            return noneJsonSlice();
        }

        // TODO(mofei) refactor it to ArraySelector
        switch (array_selector->type) {
        case INVALID:
            DCHECK(false);
        case NONE:
            break;
        case SINGLE: {
            if (!next_item.isArray()) {
                return noneJsonSlice();
            }
            array_selector->iterate(next_item, [&](vpack::Slice array_item) { next_item = array_item; });
            break;
        }
        case WILDCARD:
        case SLICE: {
            if (!next_item.isArray()) {
                return noneJsonSlice();
            }
            {
                builder->clear();
                vpack::ArrayBuilder ab(builder);
                array_selector->iterate(next_item, [&](vpack::Slice array_item) {
                    auto sub = _extract_full_from_object(array_item, jsonpath, i + 1, builder);
                    builder->add(sub);
                });
            }
            return builder->slice();
        }
        }

        current_value = next_item;
    }

    return current_value;
}

//////////////////////////// User visiable functions /////////////////////////////////

ColumnPtr JsonFunctions::json_query(FunctionContext* context, const Columns& columns) {
    auto num_rows = columns[0]->size();
    auto json_viewer = ColumnViewer<TYPE_JSON>(columns[0]);
    auto path_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    ColumnBuilder<TYPE_JSON> result(num_rows);

    for (int row = 0; row < num_rows; ++row) {
        JsonValue* json_value = json_viewer.value(row);
        auto path_value = path_viewer.value(row);
        std::string path_str(path_value.get_data(), path_value.get_size());

        std::vector<JsonPath> jsonpath;
        if (!parse_full_json_paths(path_str, &jsonpath).ok()) {
            VLOG(2) << "parse json path failed: " << path_str;
            result.append_null();
            continue;
        }
        VLOG(2) << "parsed json path for " << path_str;

        vpack::Builder builder;
        vpack::Slice slice = _extract_full_from_object(json_value, jsonpath, &builder);
        if (slice.isNone()) {
            result.append_null();
        } else {
            JsonValue value(slice);
            result.append(&value);
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr JsonFunctions::json_exists(FunctionContext* context, const Columns& columns) {
    auto num_rows = columns[0]->size();
    auto json_viewer = ColumnViewer<TYPE_JSON>(columns[0]);
    auto path_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    ColumnBuilder<TYPE_BOOLEAN> result(num_rows);

    for (int row = 0; row < num_rows; row++) {
        if (json_viewer.is_null(row) || json_viewer.value(row) == nullptr) {
            result.append_null();
            continue;
        }

        JsonValue* json_value = json_viewer.value(row);
        std::string path_str = path_viewer.value(row).to_string();

        std::vector<JsonPath> paths;
        if (!parse_full_json_paths(path_str, &paths).ok()) {
            result.append_null();
            VLOG(2) << "parse json path failed: " << path_str;
            continue;
        }
        VLOG(2) << "json_exists for  " << path_str << " of " << json_value->to_string().value();
        vpack::Builder builder;
        vpack::Slice slice = _extract_full_from_object(json_value, paths, &builder);
        result.append(!slice.isNone());
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr JsonFunctions::json_array(FunctionContext* context, const Columns& columns) {
    namespace vpack = arangodb::velocypack;

    // empty arguments create an empty json array
    if (columns.size() == 0) {
        ColumnBuilder<TYPE_JSON> result(1);
        vpack::Builder builder;
        builder.openArray();
        builder.close();
        vpack::Slice json_slice = builder.slice();
        JsonValue json(json_slice);
        result.append(&json);
        return result.build(ColumnHelper::is_all_const(columns));
    }

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
        result.append(&json);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr JsonFunctions::json_object(FunctionContext* context, const Columns& columns) {
    namespace vpack = arangodb::velocypack;

    // empty arguments create an empty object
    if (columns.size() == 0) {
        ColumnBuilder<TYPE_JSON> result(1);
        vpack::Builder builder;
        builder.openObject();
        builder.close();
        vpack::Slice json_slice = builder.slice();
        JsonValue json(json_slice);
        result.append(&json);
        return result.build(ColumnHelper::is_all_const(columns));
    }

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
                JsonValue* field_name = viewers[i].value(row);
                vpack::Slice field_name_slice = field_name->to_vslice();
                DCHECK_NOTNULL(field_name);

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
                if (i + 1 < viewers.size()) {
                    JsonValue* field_value = viewers[i + 1].value(row);
                    DCHECK_NOTNULL(field_value);
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
            result.append(&json);
        } else {
            result.append_null();
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks::vectorized
