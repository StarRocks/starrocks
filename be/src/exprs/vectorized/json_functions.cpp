// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/json_functions.h"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <re2/re2.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/status.h"
#include "rapidjson/error/en.h"

namespace starrocks::vectorized {

// static const re2::RE2 JSON_PATTERN("^([a-zA-Z0-9_\\-\\:\\s#\\|\\.]*)(?:\\[([0-9]+)\\])?");
// json path cannot contains: ", [, ]
static const re2::RE2 JSON_PATTERN(R"(^([^\"\[\]]*)(?:\[([0-9]+|\*)\])?)");

void JsonFunctions::get_parsed_paths(const std::vector<std::string>& path_exprs, std::vector<JsonPath>* parsed_paths) {
    if (path_exprs[0] != "$") {
        parsed_paths->emplace_back("", -1, false);
    } else {
        parsed_paths->emplace_back("$", -1, true);
    }

    for (int i = 1; i < path_exprs.size(); i++) {
        std::string col;
        std::string index;
        if (UNLIKELY(!RE2::FullMatch(path_exprs[i], JSON_PATTERN, &col, &index))) {
            parsed_paths->emplace_back("", -1, false);
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
    get_parsed_paths(path_exprs, parsed_paths);

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

rapidjson::Value* JsonFunctions::get_json_array_from_parsed_json(const std::string& json_path,
                                                                 rapidjson::Value* document,
                                                                 rapidjson::Document::AllocatorType& mem_allocator) {
    std::vector<JsonPath> vec;
    parse_json_paths(json_path, &vec);
    return get_json_array_from_parsed_json(vec, document, mem_allocator);
}

rapidjson::Value* JsonFunctions::get_json_array_from_parsed_json(const std::vector<JsonPath>& parsed_paths,
                                                                 rapidjson::Value* document,
                                                                 rapidjson::Document::AllocatorType& mem_allocator) {
    if (parsed_paths.empty() || !parsed_paths[0].is_valid) {
        return nullptr;
    }

    rapidjson::Value* root = match_value(parsed_paths, document, mem_allocator, true);
    if (root == nullptr || root == document) { // not found
        return nullptr;
    } else if (!root->IsArray()) {
        rapidjson::Value* array_obj = nullptr;
        array_obj = static_cast<rapidjson::Value*>(mem_allocator.Malloc(sizeof(rapidjson::Value)));
        array_obj->SetArray();
        array_obj->PushBack(*root, mem_allocator);
        return array_obj;
    }
    return root;
}

rapidjson::Value* JsonFunctions::get_json_object_from_parsed_json(const std::vector<JsonPath>& parsed_paths,
                                                                  rapidjson::Value* document,
                                                                  rapidjson::Document::AllocatorType& mem_allocator) {
    if (parsed_paths.empty() || !parsed_paths[0].is_valid) {
        return nullptr;
    }

    rapidjson::Value* root = match_value(parsed_paths, document, mem_allocator, true);
    if (root == nullptr || root == document) { // not found
        return nullptr;
    }
    return root;
}

rapidjson::Value* JsonFunctions::match_value(const std::vector<JsonPath>& parsed_paths, rapidjson::Value* document,
                                             rapidjson::Document::AllocatorType& mem_allocator, bool is_insert_null) {
    rapidjson::Value* root = document;
    rapidjson::Value* array_obj = nullptr;
    for (int i = 1; i < parsed_paths.size(); i++) {
        VLOG(10) << "parsed_paths: " << parsed_paths[i].debug_string();

        if (root == nullptr || root->IsNull()) {
            return nullptr;
        }

        if (UNLIKELY(!parsed_paths[i].is_valid)) {
            return nullptr;
        }

        const std::string& col = parsed_paths[i].key;
        int index = parsed_paths[i].idx;
        if (LIKELY(!col.empty())) {
            if (root->IsArray()) {
                array_obj = static_cast<rapidjson::Value*>(mem_allocator.Malloc(sizeof(rapidjson::Value)));
                array_obj->SetArray();
                bool is_null = true;

                // if array ,loop the array,find out all Objects,then find the results from the objects
                for (int j = 0; j < root->Size(); j++) {
                    rapidjson::Value* json_elem = &((*root)[j]);

                    if (json_elem->IsArray() || json_elem->IsNull()) {
                        continue;
                    } else {
                        if (!json_elem->IsObject()) {
                            continue;
                        }
                        if (!json_elem->HasMember(col.c_str())) {
                            if (is_insert_null) { // not found item, then insert a null object.
                                is_null = false;
                                rapidjson::Value nullObject(rapidjson::kNullType);
                                array_obj->PushBack(nullObject, mem_allocator);
                            }
                            continue;
                        }
                        rapidjson::Value* obj = &((*json_elem)[col.c_str()]);
                        if (obj->IsArray()) {
                            is_null = false;
                            for (int k = 0; k < obj->Size(); k++) {
                                array_obj->PushBack((*obj)[k], mem_allocator);
                            }
                        } else if (!obj->IsNull()) {
                            is_null = false;
                            array_obj->PushBack(*obj, mem_allocator);
                        }
                    }
                }

                root = is_null ? &(array_obj->SetNull()) : array_obj;
            } else if (root->IsObject()) {
                if (!root->HasMember(col.c_str())) {
                    return nullptr;
                } else {
                    root = &((*root)[col.c_str()]);
                }
            } else {
                // root is not a nested type, return NULL
                return nullptr;
            }
        }

        if (UNLIKELY(index != -1)) {
            // judge the rapidjson:Value, which base the top's result,
            // if not array return NULL;else get the index value from the array
            if (root->IsArray()) {
                if (root->IsNull()) {
                    return nullptr;
                } else if (index == -2) {
                    // [*]
                    array_obj = static_cast<rapidjson::Value*>(mem_allocator.Malloc(sizeof(rapidjson::Value)));
                    array_obj->SetArray();

                    for (int j = 0; j < root->Size(); j++) {
                        rapidjson::Value v;
                        v.CopyFrom((*root)[j], mem_allocator);
                        array_obj->PushBack(v, mem_allocator);
                    }
                    root = array_obj;
                } else if (index >= root->Size()) {
                    return nullptr;
                } else {
                    root = &((*root)[index]);
                }
            } else {
                return nullptr;
            }
        }
    }
    return root;
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
    get_parsed_paths(paths, parsed_paths);
}

rapidjson::Value* JsonFunctions::get_json_object(FunctionContext* context, const std::string& json_string,
                                                 const std::string& path_string, const JsonFunctionType& fntype,
                                                 rapidjson::Document* document) {
    // split path by ".", and escape quota by "\"
    // eg:
    //    '$.text#abc.xyz'  ->  [$, text#abc, xyz]
    //    '$."text.abc".xyz'  ->  [$, text.abc, xyz]
    //    '$."text.abc"[1].xyz'  ->  [$, text.abc[1], xyz]
    std::vector<JsonPath>* parsed_paths;
    std::vector<JsonPath> tmp_parsed_paths;
#ifndef BE_TEST
    parsed_paths =
            reinterpret_cast<std::vector<JsonPath>*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (parsed_paths == nullptr) {
        boost::tokenizer<boost::escaped_list_separator<char> > tok(
                path_string, boost::escaped_list_separator<char>("\\", ".", "\""));
        std::vector<std::string> paths(tok.begin(), tok.end());
        get_parsed_paths(paths, &tmp_parsed_paths);
        parsed_paths = &tmp_parsed_paths;
    }
#else
    boost::tokenizer<boost::escaped_list_separator<char> > tok(path_string,
                                                               boost::escaped_list_separator<char>("\\", ".", "\""));
    std::vector<std::string> paths(tok.begin(), tok.end());
    get_parsed_paths(paths, &tmp_parsed_paths);
    parsed_paths = &tmp_parsed_paths;
#endif

    VLOG(10) << "first parsed path: " << (*parsed_paths)[0].debug_string();

    if (!(*parsed_paths)[0].is_valid) {
        return document;
    }

    if (UNLIKELY((*parsed_paths).size() == 1)) {
        if (fntype == JSON_FUN_STRING) {
            document->SetString(json_string.c_str(), document->GetAllocator());
        } else {
            return document;
        }
    }

    //rapidjson::Document document;
    document->Parse(json_string.c_str());
    if (UNLIKELY(document->HasParseError())) {
        VLOG(1) << "Error at offset " << document->GetErrorOffset() << ": "
                << GetParseError_En(document->GetParseError());
        document->SetNull();
        return document;
    }
    return match_value(*parsed_paths, document, document->GetAllocator());
}

JsonFunctionType JsonTypeTraits<TYPE_INT>::JsonType = JSON_FUN_INT;
JsonFunctionType JsonTypeTraits<TYPE_DOUBLE>::JsonType = JSON_FUN_DOUBLE;
JsonFunctionType JsonTypeTraits<TYPE_VARCHAR>::JsonType = JSON_FUN_STRING;

template <PrimitiveType primitive_type>
ColumnPtr JsonFunctions::iterate_rows(FunctionContext* context, const Columns& columns) {
    auto json_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto path_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

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

        rapidjson::Document document;
        rapidjson::Value* root = JsonFunctions::get_json_object(context, json_string, path_string,
                                                                JsonTypeTraits<primitive_type>::JsonType, &document);

        if constexpr (primitive_type == TYPE_INT) {
            if (root != nullptr && root->IsInt()) {
                result.append(root->GetInt());
            } else {
                result.append_null();
            }
        } else if constexpr (primitive_type == TYPE_DOUBLE) {
            if (root == nullptr || root->IsNull()) {
                result.append_null();
            } else if (root->IsInt()) {
                result.append(static_cast<double>(root->GetInt()));
            } else if (root->IsDouble()) {
                result.append(root->GetDouble());
            } else {
                result.append_null();
            }
        } else if constexpr (primitive_type == TYPE_VARCHAR) {
            if (root == nullptr || root->IsNull()) {
                result.append_null();
            } else if (root->IsString()) {
                result.append(Slice(root->GetString()));
            } else {
                rapidjson::StringBuffer buf;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
                root->Accept(writer);
                result.append(Slice(buf.GetString()));
            }
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr JsonFunctions::get_json_int(FunctionContext* context, const Columns& columns) {
    return JsonFunctions::template iterate_rows<TYPE_INT>(context, columns);
}

ColumnPtr JsonFunctions::get_json_double(FunctionContext* context, const Columns& columns) {
    return JsonFunctions::template iterate_rows<TYPE_DOUBLE>(context, columns);
}

ColumnPtr JsonFunctions::get_json_string(FunctionContext* context, const Columns& columns) {
    return JsonFunctions::template iterate_rows<TYPE_VARCHAR>(context, columns);
}

std::string JsonFunctions::get_raw_json_string(const rapidjson::Value& value) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    value.Accept(writer);
    return std::string(buffer.GetString());
}

} // namespace starrocks::vectorized
