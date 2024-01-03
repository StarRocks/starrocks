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

#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"

namespace starrocks {

Status PrimaryKeyRecover::recover() {
    // 1. clean up old primary index and delvec
    RETURN_IF_ERROR(pre_cleanup());
    LOG(INFO) << "PrimaryKeyRecover pre clean up finish. tablet_id: " << tablet_id();

    // 2. generate primary key schema
    std::unique_ptr<Column> pk_column;
    auto pkey_schema = generate_pkey_schema();

    if (pkey_schema.num_fields() > 1) {
        // more than one key column
        if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
            CHECK(false) << "create column for primary key encoder failed";
        }
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
    RETURN_IF_ERROR(
            rowset_iterator(pkey_schema, stats, [&](const std::vector<ChunkIteratorPtr>& itrs, uint32_t rowset_id) {
                for (size_t i = 0; i < itrs.size(); i++) {
                    auto itr = itrs[i].get();
                    if (itr == nullptr) {
                        continue;
                    }
                    uint32_t rssid = rowset_id + i;
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
                            Column* pkc = nullptr;
                            if (pk_column) {
                                pk_column->reset_column();
                                PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), pk_column.get());
                                pkc = pk_column.get();
                            } else {
                                pkc = chunk->columns()[0].get();
                            }
                            // upsert and generate new deletes
                            RETURN_IF_ERROR(index.upsert(rssid, row_id_start, *pkc, &new_deletes));
                            row_id_start += pkc->size();
                        }
                    }
                    itr->close();
                }
                return Status::OK();
            }));
    LOG(INFO) << "PrimaryKeyRecover rebuild index finish. tablet_id: " << tablet_id();

    // 4. sync delete vector
    return finalize_delvec(new_deletes);
}

} // namespace starrocks