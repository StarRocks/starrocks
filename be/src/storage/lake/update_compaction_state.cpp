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
#include "storage/primary_key_encoder.h"
#include "storage/tablet_manager.h"

namespace starrocks::lake {

CompactionState::CompactionState(Rowset* rowset) {
    if (rowset->num_segments() > 0) {
        pk_cols.resize(rowset->num_segments());
    }
}

CompactionState::~CompactionState() {}

Status CompactionState::load_segments(Rowset* rowset, const TabletSchema& tablet_schema, uint32_t segment_id) {
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

Status CompactionState::_load_segments(Rowset* rowset, const TabletSchema& tablet_schema, uint32_t segment_id) {
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < tablet_schema.num_key_columns(); i++) {
        pk_columns.push_back(static_cast<uint32_t>(i));
    }

    Schema pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);

    std::unique_ptr<Column> pk_column;
    CHECK(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok());

    OlapReaderStatistics stats;
    auto res = rowset->get_each_segment_iterator(pkey_schema, &stats);
    if (!res.ok()) {
        return res.status();
    }

    auto& itrs = res.value();
    CHECK_EQ(itrs.size(), rowset->num_segments());

    // only hold pkey, so can use larger chunk size
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, config::vector_chunk_size);
    auto chunk = chunk_shared_ptr.get();

    auto itr = itrs[segment_id].get();
    if (itr == nullptr) {
        return Status::OK();
    }
    auto& dest = pk_cols[segment_id];
    auto col = pk_column->clone();
    if (itr != nullptr) {
        const auto num_rows = rowset->num_rows();
        col->reserve(num_rows);
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

    return Status::OK();
}

void CompactionState::release_segments(uint32_t segment_id) {
    if (segment_id >= pk_cols.size() || pk_cols[segment_id] == nullptr) {
        return;
    }
    pk_cols[segment_id]->reset_column();
}

} // namespace starrocks::lake
