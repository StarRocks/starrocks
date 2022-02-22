// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include <string>

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
DIAGNOSTIC_POP

#include "column/chunk.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "http/http_client.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {
class ScrollParser {
public:
    ScrollParser(bool doc_value_mode);
    ~ScrollParser() = default;

    Status parse(const std::string& scroll_result, bool exactly_once = false);
    Status fill_chunk(RuntimeState* state, ChunkPtr* chunk, bool* line_eos);

    const std::string& get_scroll_id() { return _scroll_id; }
    int get_size() { return _size; }
    bool current_eos() { return _cur_line == _size; }

    void set_params(const TupleDescriptor* descs, const std::map<std::string, std::string>* docvalue_context);

private:
    static bool _pure_doc_value(const rapidjson::Value& obj);

    template <PrimitiveType type, class CppType = RunTimeCppType<type>>
    static void _append_data(Column* column, CppType& value);

    static void _append_null(Column* column);

    Status _append_value_from_json_val(Column* column, PrimitiveType type, const rapidjson::Value& col,
                                       bool pure_doc_value);

    Slice _json_val_to_slice(const rapidjson::Value& val);

    template <PrimitiveType type, typename T = RunTimeCppType<type>>
    Status _append_int_val(const rapidjson::Value& col, Column* column, bool pure_doc_value);

    template <PrimitiveType type, typename T = RunTimeCppType<type>>
    Status _append_float_val(const rapidjson::Value& col, Column* column, bool pure_doc_value);

    Status _append_bool_val(const rapidjson::Value& col, Column* column, bool pure_doc_value);

    template <PrimitiveType type, typename T = RunTimeCppType<type>>
    Status _append_date_val(const rapidjson::Value& col, Column* column, bool pure_doc_value);

private:
    const TupleDescriptor* _tuple_desc;
    const std::map<std::string, std::string>* _docvalue_context;

    std::string _scroll_id;
    size_t _size;
    rapidjson::SizeType _cur_line;
    rapidjson::Document _document_node;
    rapidjson::Value _inner_hits_node;
    bool _doc_value_mode;

    rapidjson::StringBuffer _scratch_buffer;
    rapidjson::Writer<rapidjson::StringBuffer> _temp_writer;
};
} // namespace starrocks::vectorized