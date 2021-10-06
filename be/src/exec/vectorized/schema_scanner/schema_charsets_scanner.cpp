// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/schema_scanner/schema_charsets_scanner.h"

#include "column/chunk.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaCharsetsScanner::_s_css_columns[] = {
        //   name,       type,          size
        {"CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DEFAULT_COLLATE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DESCRIPTION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"MAXLEN", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaCharsetsScanner::CharsetStruct SchemaCharsetsScanner::_s_charsets[] = {
        {"utf8", "utf8_general_ci", "UTF-8 Unicode", 3},
        {nullptr, nullptr, nullptr},
};

SchemaCharsetsScanner::SchemaCharsetsScanner()
        : SchemaScanner(_s_css_columns, sizeof(_s_css_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaCharsetsScanner::~SchemaCharsetsScanner() = default;

Status SchemaCharsetsScanner::fill_chunk(ChunkPtr* chunk) {
    // variables names
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[0]->id());
        Slice value(_s_charsets[_index].charset, strlen(_s_charsets[_index].charset));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // DEFAULT_COLLATE_NAME
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[1]->id());
        Slice value(_s_charsets[_index].default_collation, strlen(_s_charsets[_index].default_collation));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // DESCRIPTION
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[2]->id());
        Slice value(_s_charsets[_index].description, strlen(_s_charsets[_index].description));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // maxlen
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[3]->id());
        fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&_s_charsets[_index].maxlen);
    }
    _index++;
    return Status::OK();
}

Status SchemaCharsetsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (nullptr == _s_charsets[_index].charset) {
        *eos = true;
        return Status::OK();
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks::vectorized
