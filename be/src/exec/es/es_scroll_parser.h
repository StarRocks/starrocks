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

#pragma once
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <string>

#include "column/chunk.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "http/http_client.h"
#include "runtime/descriptors.h"
#include "types/logical_type.h"

namespace starrocks {
class ScrollParser {
public:
    ScrollParser(bool doc_value_mode);
    ~ScrollParser() = default;

    Status parse(const std::string& scroll_result, bool exactly_once = false);
    Status fill_chunk(RuntimeState* state, ChunkPtr* chunk, bool* line_eos);

    const std::string& get_scroll_id() { return _scroll_id; }
    int get_size() { return _size; }
    bool current_eos() { return _cur_line == _size; }

    void set_params(const TupleDescriptor* descs, const std::map<std::string, std::string>* docvalue_context,
                    std::string& timezone);

private:
    static bool _is_pure_doc_value(const rapidjson::Value& obj);

    template <LogicalType type, class CppType = RunTimeCppType<type>>
    static void _append_data(Column* column, CppType& value);

    static void _append_null(Column* column);

    Status _append_value_from_json_val(Column* column, const TypeDescriptor& type_desc, const rapidjson::Value& col,
                                       bool pure_doc_value);

    Slice _json_val_to_slice(const rapidjson::Value& val);

    template <LogicalType type, typename T = RunTimeCppType<type>>
    static Status _append_int_val(const rapidjson::Value& col, Column* column, bool pure_doc_value);

    template <LogicalType type, typename T = RunTimeCppType<type>>
    static Status _append_float_val(const rapidjson::Value& col, Column* column, bool pure_doc_value);

    static Status _append_bool_val(const rapidjson::Value& col, Column* column, bool pure_doc_value);

    template <LogicalType type, typename T = RunTimeCppType<type>>
    static Status _append_date_val(const rapidjson::Value& col, Column* column, bool pure_doc_value,
                                   const std::string& timezone);

    // The representation of array value in _source and doc_value (column storage) is inconsistent
    // in Elasticsearch, we need to do different processing to show consistent behavior.
    // For examples:
    // origin array: [1] -> _source: "1" -> doc_values: "[1]"
    // origin array: [1, 2] -> _source: "[1, 2]" -> doc_values: "[1, 2]"
    // origin array: [1, [2, 3]] -> _source: "[1, [2, 3]]" -> doc_values: "[1, 2, 3]"
    // origin array: [1, [null, 3]] -> _source: "[1, [null, 3]]" -> doc_values: "[1, 3]"
    Status _append_array_val(const rapidjson::Value& col, const TypeDescriptor& type_desc, Column* column,
                             bool pure_doc_value);

    Status _append_json_val(const rapidjson::Value& col, const TypeDescriptor& type_desc, Column* column,
                            bool pure_doc_value);

    Status _append_array_val_from_docvalue(const rapidjson::Value& val, const TypeDescriptor& child_type_desc,
                                           Column* column);

    // This is a recursive function.
    Status _append_array_val_from_source(const rapidjson::Value& val, const TypeDescriptor& child_type_desc,
                                         Column* column);

    const TupleDescriptor* _tuple_desc;
    const std::map<std::string, std::string>* _doc_value_context;
    std::string _timezone;

    std::string _scroll_id;
    size_t _size;
    rapidjson::SizeType _cur_line;
    rapidjson::Document _document_node;
    rapidjson::Value _inner_hits_node;

    rapidjson::StringBuffer _scratch_buffer;
    rapidjson::Writer<rapidjson::StringBuffer> _temp_writer;
};
} // namespace starrocks
