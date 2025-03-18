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

#include "storage/primary_key_recover.h"

#include "serde/column_array_serde.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"

namespace starrocks {

Status PrimaryKeyRecover::recover() {
    // 1. clean up old primary index and delvec
    RETURN_IF_ERROR(pre_cleanup());
    LOG(INFO) << "PrimaryKeyRecover pre clean up finish. tablet_id: " << tablet_id();

    // 2. generate primary key schema
    MutableColumnPtr pk_column;
    auto pkey_schema = generate_pkey_schema();
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }

    // For simplicity, we use temp pk index for generate delvec, and then rebuild real pk index when retry publish
    PrimaryIndex index(pkey_schema);

    OlapReaderStatistics stats;
    std::vector<uint32_t> rowids;
    rowids.reserve(DEFAULT_CHUNK_SIZE);
    PrimaryIndex::DeletesMap new_deletes;
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, DEFAULT_CHUNK_SIZE);
    auto chunk = chunk_shared_ptr.get();
    // 3. scan all rowsets and segments to build primary index
    RETURN_IF_ERROR(rowset_iterator(
            pkey_schema, stats,
            [&](const std::vector<ChunkIteratorPtr>& itrs,
                const std::vector<std::unique_ptr<RandomAccessFile>>& del_rfs, const std::vector<uint32_t>& delidxs,
                uint32_t rowset_id) {
                // handle upsert
                if (del_rfs.size() != delidxs.size()) {
                    return Status::InternalError(
                            fmt::format("invalid delete files when recover, del files {} vs del idxs {}",
                                        del_rfs.size(), delidxs.size()));
                }
                const size_t total_size = itrs.size() + del_rfs.size();
                size_t itr_index = 0;
                size_t del_index = 0;
                for (size_t i = 0; i < total_size; i++) {
                    if (del_index < delidxs.size() && i == delidxs[del_index]) {
                        // this one is delete op
                        pk_column->reset_column();
                        const auto& read_file = del_rfs[del_index];
                        ASSIGN_OR_RETURN(auto file_size, read_file->get_size());
                        std::vector<uint8_t> read_buffer(file_size);
                        RETURN_IF_ERROR(read_file->read_at_fully(0, read_buffer.data(), read_buffer.size()));
                        auto col = pk_column->clone();
                        if (serde::ColumnArraySerde::deserialize(read_buffer.data(), col.get()) == nullptr) {
                            return Status::InternalError("column deserialization failed");
                        }
                        RETURN_IF_ERROR(index.erase(*col, &new_deletes));
                        del_index++;
                    } else {
                        // this one is upsert op
                        itr_index = i - del_index;
                        auto itr = itrs[itr_index].get();
                        if (itr == nullptr) {
                            continue;
                        }
                        uint32_t rssid = rowset_id + itr_index;
                        uint32_t row_id_start = 0;
                        new_deletes[rssid] = {};
                        while (true) {
                            chunk->reset();
                            rowids.clear();
                            auto st = itr->get_next(chunk, &rowids);
                            if (st.is_end_of_file()) {
                                break;
                            } else if (!st.ok()) {
                                return st;
                            } else {
                                pk_column->reset_column();
                                PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), pk_column.get());
                                // upsert and generate new deletes
                                RETURN_IF_ERROR(index.upsert(rssid, row_id_start, *pk_column, &new_deletes));
                                row_id_start += pk_column->size();
                            }
                        }
                        itr->close();
                    }
                }

                return Status::OK();
            }));
    LOG(INFO) << "PrimaryKeyRecover rebuild index finish. tablet_id: " << tablet_id();

    // 4. sync delete vector
    return finalize_delvec(new_deletes);
}

} // namespace starrocks