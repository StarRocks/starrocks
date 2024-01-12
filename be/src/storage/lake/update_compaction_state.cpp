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

#include "storage/lake/update_compaction_state.h"

#include "gutil/strings/substitute.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/lake/rowset.h"
#include "storage/lake/update_manager.h"
#include "storage/primary_key_encoder.h"
#include "storage/tablet_manager.h"
#include "util/trace.h"

namespace starrocks::lake {

CompactionState::~CompactionState() {
    if (_update_manager != nullptr) {
        _update_manager->compaction_state_mem_tracker()->release(_memory_usage);
    }
}

Status CompactionState::load_segments(Rowset* rowset, UpdateManager* update_manager,
                                      const TabletSchemaCSPtr& tablet_schema, uint32_t segment_id) {
    TRACE_COUNTER_SCOPE_LATENCY_US("load_segments_latency_us");
    std::lock_guard<std::mutex> lg(_state_lock);
    if (pk_cols.empty() && rowset->num_segments() > 0) {
        pk_cols.resize(rowset->num_segments());
    } else {
        DCHECK(pk_cols.size() == rowset->num_segments());
    }
    _update_manager = update_manager;
    _tablet_id = rowset->tablet_id();
    if (segment_id >= pk_cols.size() && pk_cols.size() != 0) {
        std::string msg = strings::Substitute("Error segment id: $0 vs $1", segment_id, pk_cols.size());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    if (pk_cols.size() == 0 || pk_cols[segment_id] != nullptr) {
        return Status::OK();
    }
    return _load_segments(rowset, tablet_schema, segment_id);
}

static const size_t large_compaction_memory_threshold = 1000000000;

Status CompactionState::_load_segments(Rowset* rowset, const TabletSchemaCSPtr& tablet_schema, uint32_t segment_id) {
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back(static_cast<uint32_t>(i));
    }

    Schema pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);

    std::unique_ptr<Column> pk_column;
    CHECK(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column, true).ok());

    OlapReaderStatistics stats;
    if (_segment_iters.empty()) {
        ASSIGN_OR_RETURN(_segment_iters, rowset->get_each_segment_iterator(pkey_schema, &stats));
    }
    CHECK_EQ(_segment_iters.size(), rowset->num_segments());

    // only hold pkey, so can use larger chunk size
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, config::vector_chunk_size);
    auto chunk = chunk_shared_ptr.get();

    auto itr = _segment_iters[segment_id].get();
    if (itr == nullptr) {
        return Status::OK();
    }
    auto& dest = pk_cols[segment_id];
    auto col = pk_column->clone();
    if (itr != nullptr) {
        while (true) {
            chunk->reset();
            auto st = itr->get_next(chunk);
            if (st.is_end_of_file()) {
                break;
            } else if (!st.ok()) {
                return st;
            } else {
                PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), col.get());
            }
        }
        itr->close();
    }
    dest = std::move(col);
    dest->raw_data();
    _memory_usage += dest->memory_usage();
    _update_manager->compaction_state_mem_tracker()->consume(dest->memory_usage());
    return Status::OK();
}

void CompactionState::release_segments(uint32_t segment_id) {
    std::lock_guard<std::mutex> lg(_state_lock);
    if (segment_id >= pk_cols.size() || pk_cols[segment_id] == nullptr) {
        return;
    }
    _memory_usage -= pk_cols[segment_id]->memory_usage();
    _update_manager->compaction_state_mem_tracker()->release(pk_cols[segment_id]->memory_usage());
    // reset ptr to release memory immediately
    pk_cols[segment_id].reset();
}

std::string CompactionState::to_string() const {
    return strings::Substitute("CompactionState tablet:$0", _tablet_id);
}

} // namespace starrocks::lake
