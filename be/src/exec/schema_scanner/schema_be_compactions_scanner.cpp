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

#include "exec/schema_scanner/schema_be_compactions_scanner.h"

#include "agent/master_info.h"
#include "exec/schema_scanner/schema_helper.h"
#include "gutil/strings/substitute.h"
#include "runtime/string_value.h"
#include "storage/compaction_manager.h"
#include "storage/storage_engine.h"
#include "types/logical_type.h"
#include "util/metrics.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaBeCompactionsScanner::_s_columns[] = {
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"CANDIDATES_NUM", TYPE_BIGINT, sizeof(int64_t), false},
        {"BASE_COMPACTION_CONCURRENCY", TYPE_BIGINT, sizeof(int64_t), false},
        {"CUMULATIVE_COMPACTION_CONCURRENCY", TYPE_BIGINT, sizeof(int64_t), false},
        {"LATEST_COMPACTION_SCORE", TYPE_DOUBLE, sizeof(double), false},
        {"CANDIDATE_MAX_SCORE", TYPE_DOUBLE, sizeof(double), false},
        {"MANUAL_COMPACTION_CONCURRENCY", TYPE_BIGINT, sizeof(int64_t), false},
        {"MANUAL_COMPACTION_CANDIDATES_NUM", TYPE_BIGINT, sizeof(int64_t), false}};

SchemaBeCompactionsScanner::SchemaBeCompactionsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaBeCompactionsScanner::~SchemaBeCompactionsScanner() = default;

Status SchemaBeCompactionsScanner::start(RuntimeState* state) {
    auto o_id = get_backend_id();
    _be_id = o_id.has_value() ? o_id.value() : -1;
    _infos.clear();
    CompactionInfos info;
    auto compaction_manager = StorageEngine::instance()->compaction_manager();
    info.candidates_num = compaction_manager->candidates_size();
    info.base_compaction_concurrency = compaction_manager->base_compaction_concurrency();
    info.cumulative_compaction_concurrency = compaction_manager->cumulative_compaction_concurrency();
    info.last_score = compaction_manager->last_score();
    info.max_score = compaction_manager->max_score();
    _infos.emplace_back(std::move(info));
    _cur_idx = 0;
    return Status::OK();
}

Status SchemaBeCompactionsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    auto end = _cur_idx + 1;
    for (; _cur_idx < end; _cur_idx++) {
        auto& info = _infos[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 8) {
                return Status::InternalError(strings::Substitute("invalid slot id:$0", slot_id));
            }
            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // be id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&_be_id);
                break;
            }
            case 2: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.candidates_num);
                break;
            }
            case 3: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.base_compaction_concurrency);
                break;
            }
            case 4: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.cumulative_compaction_concurrency);
                break;
            }
            case 5: {
                fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&info.last_score);
                break;
            }
            case 6: {
                fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&info.max_score);
                break;
            }
            case 7: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.manual_compaction_concurrency);
                break;
            }
            case 8: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.manual_compaction_candidates_num);
                break;
            }
            default:
                break;
            }
        }
    }
    return Status::OK();
}

Status SchemaBeCompactionsScanner::get_next(ChunkPtr* chunk, bool* eos) {
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
