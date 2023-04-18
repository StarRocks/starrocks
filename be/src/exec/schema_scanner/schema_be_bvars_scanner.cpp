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

#include "exec/schema_scanner/schema_be_bvars_scanner.h"

#include <bvar/bvar.h>
#include <fmt/format.h>

#include "agent/master_info.h"
#include "column/binary_column.h"
#include "exec/schema_scanner/schema_helper.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

class MyDumper : public bvar::Dumper {
public:
    explicit MyDumper(ColumnPtr name_col, ColumnPtr desc_col)
            : _name_column(std::move(name_col)), _desc_column(std::move(desc_col)) {}

    bool dump(const std::string& name, const butil::StringPiece& desc) override {
        down_cast<BinaryColumn*>(_name_column.get())->append(name);
        down_cast<BinaryColumn*>(_desc_column.get())->append(Slice(desc.data(), desc.size()));
        return true;
    }

private:
    ColumnPtr _name_column;
    ColumnPtr _desc_column;
};

SchemaScanner::ColumnDesc SchemaBeBvarsScanner::_s_columns[] = {
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DESC", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaBeBvarsScanner::SchemaBeBvarsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaBeBvarsScanner::~SchemaBeBvarsScanner() = default;

Status SchemaBeBvarsScanner::start(RuntimeState* state) {
    auto o_id = get_backend_id();
    _be_id = o_id.has_value() ? o_id.value() : -1;
    _cur_idx = 0;
    _columns[0] = BinaryColumn::create();
    _columns[1] = BinaryColumn::create();
    _chunk_size = state->chunk_size();
    MyDumper dumper(_columns[0], _columns[1]);
    bvar::Variable::dump_exposed(&dumper, nullptr);
    return Status::OK();
}

Status SchemaBeBvarsScanner::fill_chunk(ChunkPtr* chunk) {
    const static int kSlotBeId = 1;
    const static int kSlotName = 2;
    const static int kSlotDesc = 3;

    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        if (slot_id < kSlotBeId || slot_id > kSlotDesc) {
            return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
        }
    }

    const auto copy_size = std::min<size_t>(_chunk_size, _columns[0]->size() - _cur_idx);

    if (slot_id_to_index_map.count(kSlotBeId) > 0) {
        auto column = (*chunk)->get_column_by_slot_id(kSlotBeId);
        down_cast<Int64Column*>(column.get())->append_value_multiple_times(&_be_id, copy_size);
    }

    if (slot_id_to_index_map.count(kSlotName) > 0) {
        auto column = (*chunk)->get_column_by_slot_id(kSlotName);
        if (_cur_idx == 0 && copy_size == _columns[0]->size() && column->size() == 0) {
            column->swap_column(*_columns[0]);
        } else {
            column->append(*_columns[0], _cur_idx, copy_size);
        }
    }

    if (slot_id_to_index_map.count(kSlotDesc) > 0) {
        auto column = (*chunk)->get_column_by_slot_id(kSlotDesc);
        if (_cur_idx == 0 && copy_size == _columns[1]->size() && column->size() == 0) {
            column->swap_column(*_columns[1]);
        } else {
            column->append(*_columns[1], _cur_idx, copy_size);
        }
    }
    _cur_idx += copy_size;

    return Status::OK();
}

Status SchemaBeBvarsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_cur_idx >= _columns[0]->size()) {
        *eos = true;
        return Status::OK();
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks
