// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/schema_scanner/schema_charsets_scanner.h"

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
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // variables names
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                Slice value(_s_charsets[_index].charset, strlen(_s_charsets[_index].charset));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // DEFAULT_COLLATE_NAME
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                Slice value(_s_charsets[_index].default_collation, strlen(_s_charsets[_index].default_collation));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 3: {
            // DESCRIPTION
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                Slice value(_s_charsets[_index].description, strlen(_s_charsets[_index].description));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 4: {
            // maxlen
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&_s_charsets[_index].maxlen);
            }
            break;
        }
        default:
            break;
        }
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
