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

#include "exec/vectorized/schema_scanner/schema_be_txns_scanner.h"

#include "agent/master_info.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "gutil/strings/substitute.h"
#include "runtime/string_value.h"
#include "storage/storage_engine.h"
#include "storage/txn_manager.h"
#include "util/metrics.h"

namespace starrocks {

using vectorized::fill_column_with_slot;

vectorized::SchemaScanner::ColumnDesc SchemaBeTxnsScanner::_s_columns[] = {
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},          {"LOAD_ID", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TXN_ID", TYPE_BIGINT, sizeof(int64_t), false},         {"PARTITION_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"TABLET_ID", TYPE_BIGINT, sizeof(int64_t), false},      {"CREATE_TIME", TYPE_BIGINT, sizeof(int64_t), false},
        {"COMMIT_TIME", TYPE_BIGINT, sizeof(int64_t), false},    {"PUBLISH_TIME", TYPE_BIGINT, sizeof(int64_t), false},
        {"ROWSET_ID", TYPE_VARCHAR, sizeof(StringValue), false}, {"NUM_SEGMENT", TYPE_BIGINT, sizeof(int64_t), false},
        {"NUM_DELFILE", TYPE_BIGINT, sizeof(int64_t), false},    {"NUM_ROW", TYPE_BIGINT, sizeof(int64_t), false},
        {"DATA_SIZE", TYPE_BIGINT, sizeof(int64_t), false},      {"VERSION", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaBeTxnsScanner::SchemaBeTxnsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaBeTxnsScanner::~SchemaBeTxnsScanner() = default;

Status SchemaBeTxnsScanner::start(RuntimeState* state) {
    auto o_id = get_backend_id();
    _be_id = o_id.has_value() ? o_id.value() : -1;
    _infos.clear();
    StorageEngine::instance()->txn_manager()->get_txn_infos(_param->txn_id, _param->tablet_id, _infos);
    _cur_idx = 0;
    return Status::OK();
}

Status SchemaBeTxnsScanner::fill_chunk(vectorized::ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
<<<<<<< HEAD
    for (; _cur_idx < _infos.size(); _cur_idx++) {
=======
    auto end = _cur_idx + 1;
    for (; _cur_idx < end; _cur_idx++) {
>>>>>>> 2.5.18
        auto& info = _infos[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 14) {
                return Status::InternalError(strings::Substitute("invalid slot id:$0", slot_id));
            }
            vectorized::ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // be id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&_be_id);
                break;
            }
            case 2: {
                // load id
                auto load_id_str = info.load_id.to_string();
                Slice v(load_id_str);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 3: {
                // txn id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.txn_id);
                break;
            }
            case 4: {
                // partition id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.partition_id);
                break;
            }
            case 5: {
                // tablet id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.tablet_id);
                break;
            }
            case 6: {
                // create time
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.create_time);
                break;
            }
            case 7: {
                // commit time
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.commit_time);
                break;
            }
            case 8: {
                // publish time
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.publish_time);
                break;
            }
            case 9: {
                // rowset id
                Slice v(info.rowset_id);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 10: {
                // num segment
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_segment);
                break;
            }
            case 11: {
                // num delfile
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_delfile);
                break;
            }
            case 12: {
                // num row
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_row);
                break;
            }
            case 13: {
                // data size
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.data_size);
                break;
            }
            case 14: {
                // version
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.version);
                break;
            }
            default:
                break;
            }
        }
    }
    return Status::OK();
}

Status SchemaBeTxnsScanner::get_next(vectorized::ChunkPtr* chunk, bool* eos) {
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
