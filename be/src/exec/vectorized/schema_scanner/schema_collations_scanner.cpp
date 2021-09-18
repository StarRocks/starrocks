// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/schema_scanner/schema_collations_scanner.h"

#include "column/chunk.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaCollationsScanner::_s_cols_columns[] = {
        //   name,       type,          size
        {"COLLATION_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"IS_DEFAULT", TYPE_VARCHAR, sizeof(StringValue), false},
        {"IS_COMPILED", TYPE_VARCHAR, sizeof(StringValue), false},
        {"SORTLEN", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaCollationsScanner::CollationStruct SchemaCollationsScanner::_s_collations[] = {
        {"utf8_general_ci", "utf8", 33, "Yes", "Yes", 1},
        {nullptr, nullptr, 0, nullptr, nullptr, 0},
};

SchemaCollationsScanner::SchemaCollationsScanner()
        : SchemaScanner(_s_cols_columns, sizeof(_s_cols_columns) / sizeof(SchemaScanner::ColumnDesc)), _index(0) {}

SchemaCollationsScanner::~SchemaCollationsScanner() {}

Status SchemaCollationsScanner::fill_chunk(ChunkPtr* chunk) {
    // COLLATION_NAME
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[0]->id());
        Slice value(_s_collations[_index].name, strlen(_s_collations[_index].name));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // charset
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[1]->id());
        Slice value(_s_collations[_index].charset, strlen(_s_collations[_index].charset));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // id
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[2]->id());
        fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&_s_collations[_index].id);
    }
    // is_default
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[3]->id());
        Slice value(_s_collations[_index].is_default, strlen(_s_collations[_index].is_default));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // IS_COMPILED
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[4]->id());
        Slice value(_s_collations[_index].is_compile, strlen(_s_collations[_index].is_compile));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // sortlen
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[5]->id());
        fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&_s_collations[_index].sortlen);
    }
    _index++;
    return Status::OK();
}

Status SchemaCollationsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (nullptr == _s_collations[_index].name) {
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
