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

#include "storage/primary_key_compaction_conflict_resolver.h"

#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/primary_index.h"
#include "storage/primary_key_encoder.h"
#include "storage/rows_mapper.h"
#include "storage/tablet_schema.h"
#include "util/trace.h"

namespace starrocks {

Status PrimaryKeyCompactionConflictResolver::execute() {
    Schema pkey_schema = generate_pkey_schema();

    MutableColumnPtr pk_column;
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column, true));

    // init rows mapper iter
    ASSIGN_OR_RETURN(auto filename, filename());
    RowsMapperIterator mapper_iter;
    RETURN_IF_ERROR(mapper_iter.open(filename));

    // iterate all segment in output rowset
    RETURN_IF_ERROR(segment_iterator(
            [&](const CompactConflictResolveParams& params, const std::vector<ChunkIteratorPtr>& segment_iters,
                const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>& handle_delvec_result_func) {
                std::map<uint32_t, DelVectorPtr> rssid_to_delvec;
                for (size_t segment_id = 0; segment_id < segment_iters.size(); segment_id++) {
                    RETURN_IF_ERROR(breakpoint_check());
                    // only hold pkey, so can use larger chunk size
                    ChunkUniquePtr chunk_shared_ptr;
                    TRY_CATCH_BAD_ALLOC(chunk_shared_ptr =
                                                ChunkHelper::new_chunk(pkey_schema, config::vector_chunk_size));
                    auto chunk = chunk_shared_ptr.get();
                    auto col = pk_column->clone();
                    vector<uint32_t> tmp_deletes;
                    uint32_t current_rowid = 0;

                    auto itr = segment_iters[segment_id].get();
                    if (itr != nullptr) {
                        while (true) {
                            chunk->reset();
                            col->reset_column();
                            auto st = Status::OK();
                            // 4. get chunk
                            {
                                TRACE_COUNTER_SCOPE_LATENCY_US("compaction_get_next_latency_us");
                                st = itr->get_next(chunk);
                            }

                            if (st.is_end_of_file()) {
                                break;
                            } else if (!st.ok()) {
                                return st;
                            } else {
                                // 5. get input rssid & rowids, so we can generate delvec
                                std::vector<uint64_t> rssid_rowids;
                                std::vector<uint32_t> replace_indexes;
                                RETURN_IF_ERROR(mapper_iter.next_values(chunk->num_rows(), &rssid_rowids));
                                DCHECK(chunk->num_rows() == rssid_rowids.size());
                                for (int i = 0; i < rssid_rowids.size(); i++) {
                                    const uint32_t rssid = rssid_rowids[i] >> 32;
                                    const uint32_t rowid = rssid_rowids[i] & 0xffffffff;
                                    if (rssid_to_delvec.count(rssid) == 0) {
                                        // get delvec by loader
                                        DelVectorPtr delvec_ptr;
                                        {
                                            TRACE_COUNTER_SCOPE_LATENCY_US("compaction_delvec_loader_latency_us");
                                            RETURN_IF_ERROR(params.delvec_loader->load(
                                                    {params.tablet_id, rssid}, params.base_version, &delvec_ptr));
                                        }
                                        rssid_to_delvec[rssid] = delvec_ptr;
                                    }
                                    if (!rssid_to_delvec[rssid]->empty() &&
                                        rssid_to_delvec[rssid]->roaring()->contains(rowid)) {
                                        // Input row had been deleted, so we need to delete it from output rowset
                                        tmp_deletes.push_back(current_rowid + i);
                                    } else {
                                        // replace pk index
                                        replace_indexes.push_back(i);
                                    }
                                }
                                // 6. replace pk index
                                TRACE_COUNTER_SCOPE_LATENCY_US("compaction_replace_index_latency_us");
                                TRY_CATCH_BAD_ALLOC(PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(),
                                                                              col.get()));
                                RETURN_IF_ERROR(params.index->replace(params.rowset_id + segment_id, current_rowid,
                                                                      replace_indexes, *col));
                                current_rowid += chunk->num_rows();
                            }
                        }
                        itr->close();
                        // 7. generate final delvec
                        DelVectorPtr dv = std::make_shared<DelVector>();
                        if (tmp_deletes.empty()) {
                            dv->init(params.new_version, nullptr, 0);
                        } else {
                            dv->init(params.new_version, tmp_deletes.data(), tmp_deletes.size());
                        }
                        handle_delvec_result_func(params.rowset_id + segment_id, dv, tmp_deletes.size());
                    }
                }
                return Status::OK();
            }));

    return mapper_iter.status();
}

} // namespace starrocks