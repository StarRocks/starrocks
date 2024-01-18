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

#include "exec/vectorized/schema_scanner/schema_be_tablets_scanner.h"

#include "agent/master_info.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "gutil/strings/substitute.h"
#include "runtime/string_value.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"

using starrocks::vectorized::fill_column_with_slot;

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaBeTabletsScanner::_s_columns[] = {
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},         {"TABLE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"PARTITION_ID", TYPE_BIGINT, sizeof(int64_t), false},  {"TABLET_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"NUM_VERSION", TYPE_BIGINT, sizeof(int64_t), false},   {"MAX_VERSION", TYPE_BIGINT, sizeof(int64_t), false},
        {"MIN_VERSION", TYPE_BIGINT, sizeof(int64_t), false},   {"NUM_ROWSET", TYPE_BIGINT, sizeof(int64_t), false},
        {"NUM_ROW", TYPE_BIGINT, sizeof(int64_t), false},       {"DATA_SIZE", TYPE_BIGINT, sizeof(int64_t), false},
        {"INDEX_MEM", TYPE_BIGINT, sizeof(int64_t), false},     {"CREATE_TIME", TYPE_BIGINT, sizeof(int64_t), false},
        {"STATE", TYPE_VARCHAR, sizeof(StringValue), false},    {"TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DATA_DIR", TYPE_VARCHAR, sizeof(StringValue), false}, {"SHARD_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"SCHEMA_HASH", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaBeTabletsScanner::SchemaBeTabletsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaBeTabletsScanner::~SchemaBeTabletsScanner() = default;

Status SchemaBeTabletsScanner::start(RuntimeState* state) {
    auto o_id = get_backend_id();
    _be_id = o_id.has_value() ? o_id.value() : -1;
    _infos.clear();
    auto manager = StorageEngine::instance()->tablet_manager();
    manager->get_tablets_basic_infos(_param->table_id, _param->partition_id, _param->tablet_id, _infos);
    LOG(INFO) << strings::Substitute("get_tablets_basic_infos table_id:$0 partition:$1 tablet:$2 #info:$3",
                                     _param->table_id, _param->partition_id, _param->tablet_id, _infos.size());
    _cur_idx = 0;
    return Status::OK();
}

static const char* tablet_state_to_string(TabletState state) {
    switch (state) {
    case TabletState::TABLET_NOTREADY:
        return "NOTREADY";
    case TabletState::TABLET_RUNNING:
        return "RUNNING";
    case TabletState::TABLET_TOMBSTONED:
        return "TOMBSTONED";
    case TabletState::TABLET_STOPPED:
        return "STOPPED";
    case TabletState::TABLET_SHUTDOWN:
        return "SHUTDOWN";
    default:
        return "UNKNOWN";
    }
}

static const char* keys_type_to_string(KeysType type) {
    switch (type) {
    case KeysType::DUP_KEYS:
        return "DUP";
    case KeysType::UNIQUE_KEYS:
        return "UNIQUE";
    case KeysType::AGG_KEYS:
        return "AGG";
    case KeysType::PRIMARY_KEYS:
        return "PRIMARY";
    default:
        return "UNKNOWN";
    }
}

Status SchemaBeTabletsScanner::fill_chunk(vectorized::ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
<<<<<<< HEAD
    for (; _cur_idx < _infos.size(); _cur_idx++) {
=======
    auto end = _cur_idx + 1;
    for (; _cur_idx < end; _cur_idx++) {
>>>>>>> 2.5.18
        auto& info = _infos[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 17) {
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
                // table id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.table_id);
                break;
            }
            case 3: {
                // partition id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.partition_id);
                break;
            }
            case 4: {
                // tablet id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.tablet_id);
                break;
            }
            case 5: {
                // num version
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_version);
                break;
            }
            case 6: {
                // max version
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.max_version);
                break;
            }
            case 7: {
                // min version
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.min_version);
                break;
            }
            case 8: {
                // num rowset
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_rowset);
                break;
            }
            case 9: {
                // num rowt
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_row);
                break;
            }
            case 10: {
                // data size
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.data_size);
                break;
            }
            case 11: {
                // index mem
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.index_mem);
                break;
            }
            case 12: {
                // create time
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.create_time);
                break;
            }
            case 13: {
                // state
                Slice state = Slice(tablet_state_to_string((TabletState)info.state));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&state);
                break;
            }
            case 14: {
                // type
                Slice type = Slice(keys_type_to_string((KeysType)info.type));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&type);
                break;
            }
            case 15: {
                Slice type = Slice(info.data_dir);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&type);
                break;
            }
            case 16: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.shard_id);
                break;
            }
            case 17: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.schema_hash);
                break;
            }
            default:
                break;
            }
        }
    }
    return Status::OK();
}

Status SchemaBeTabletsScanner::get_next(vectorized::ChunkPtr* chunk, bool* eos) {
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

} // namespace starrocks::vectorized
