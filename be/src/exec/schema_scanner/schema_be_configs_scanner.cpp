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

#include "exec/schema_scanner/schema_be_configs_scanner.h"

#include "agent/master_info.h"
#include "exec/schema_scanner/schema_helper.h"
#include "gutil/strings/substitute.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"
#include "util/metrics.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaBeConfigsScanner::_s_columns[] = {
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},        {"NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"VALUE", TYPE_VARCHAR, sizeof(StringValue), false},   {"TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DEFAULT", TYPE_VARCHAR, sizeof(StringValue), false}, {"MUTABLE", TYPE_BOOLEAN, sizeof(bool), false},
};

SchemaBeConfigsScanner::SchemaBeConfigsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaBeConfigsScanner::~SchemaBeConfigsScanner() = default;

Status SchemaBeConfigsScanner::start(RuntimeState* state) {
    auto o_id = get_backend_id();
    _be_id = o_id.has_value() ? o_id.value() : -1;
    _cur_idx = 0;
    _infos = config::list_configs();
    return Status::OK();
}

Status SchemaBeConfigsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    auto end = _cur_idx + 1;
    for (; _cur_idx < end; _cur_idx++) {
        auto& info = _infos[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // be id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&_be_id);
                break;
            }
            case 2: {
                // name
                Slice v(info.name);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 3: {
                // value
                Slice v(info.value);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 4: {
                // type
                Slice v(info.type);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 5: {
                // default
                Slice v(info.defval);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 6: {
                // mutable
                bool v(info.valmutable);
                fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&v);
                break;
            }
            default:
                return Status::InternalError(strings::Substitute("invalid slot id:$0", slot_id));
            }
        }
    }
    return Status::OK();
}

Status SchemaBeConfigsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_cur_idx >= _infos.size()) {
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
