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

#include "base/debug/trace.h"
#include "common/config_exec_fwd.h"
#include "runtime/current_thread.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/primary_index.h"
#include "storage/primary_key_encoder.h"
#include "storage/rows_mapper.h"
#include "storage/tablet_schema.h"

namespace starrocks {

Status PrimaryKeyCompactionConflictResolver::execute() {
    Schema pkey_schema = generate_pkey_schema();
    ASSIGN_OR_RETURN(auto encoding_type, primary_key_encoding_type());

    MutableColumnPtr pk_column;
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column, encoding_type, true));

    // init rows mapper iter
    ASSIGN_OR_RETURN(auto filename, filename());
    RowsMapperIterator mapper_iter;
    RETURN_IF_ERROR(mapper_iter.open(filename));

    // iterate all segment in output rowset
    RETURN_IF_ERROR(segment_iterator(
            [&](const CompactConflictResolveParams& params, const std::vector<ChunkIteratorPtr>& segment_iters,
                const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>& handle_delvec_result_func) {
                std::map<uint32_t, DelVectorPtr> rssid_to_delvec;
                // Accumulate multiple chunks' PKs into a single replace() call to reduce per-chunk
                // overhead: each `params.index->replace()` ends with a memtable flush check + lock
                // round-trip in LakePersistentIndex::replace(). For a 1 M-row segment with the default
                // 4 K chunk, that is ~250 such calls per segment. Batching N chunks amortises the
                // per-call setup, vector allocations, and memtable bookkeeping by ~N×. 0 disables
                // batching (one replace per chunk).
                const size_t batch_rows_threshold =
                        std::max<size_t>(1, static_cast<size_t>(config::primary_key_compaction_replace_batch_rows));
                for (size_t segment_id = 0; segment_id < segment_iters.size(); segment_id++) {
                    RETURN_IF_ERROR(breakpoint_check());
                    // only hold pkey, so can use larger chunk size
                    ChunkUniquePtr chunk_shared_ptr;
                    TRY_CATCH_BAD_ALLOC(chunk_shared_ptr =
                                                ChunkHelper::new_chunk(pkey_schema, config::vector_chunk_size));
                    auto chunk = chunk_shared_ptr.get();
                    auto batch_col = pk_column->clone();
                    vector<uint32_t> tmp_deletes;
                    std::vector<uint32_t> batch_replace_indexes;
                    uint32_t current_rowid = 0;
                    uint32_t batch_start_rowid = 0; // segment offset of the first row in batch_col
                    uint32_t batch_acc_rows = 0;    // rows already accumulated in batch_col

                    auto flush_replace_batch = [&]() -> Status {
                        if (batch_acc_rows == 0) {
                            return Status::OK();
                        }
                        if (!batch_replace_indexes.empty()) {
                            TRACE_COUNTER_SCOPE_LATENCY_US("compaction_replace_index_latency_us");
                            RETURN_IF_ERROR(params.index->replace(params.rowset_id + segment_id, batch_start_rowid,
                                                                  batch_replace_indexes, *batch_col));
                        }
                        batch_start_rowid += batch_acc_rows;
                        batch_acc_rows = 0;
                        batch_col->reset_column();
                        batch_replace_indexes.clear();
                        return Status::OK();
                    };

                    auto itr = segment_iters[segment_id].get();
                    if (itr != nullptr) {
                        while (true) {
                            chunk->reset();
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
                                        // Index into batch_col after this chunk's encode has appended.
                                        // batch_acc_rows currently holds the count BEFORE appending this
                                        // chunk, so this is the absolute position in batch_col.
                                        batch_replace_indexes.push_back(batch_acc_rows + i);
                                    }
                                }
                                // 6. accumulate encoded PKs into batch_col (encode appends).
                                TRY_CATCH_BAD_ALLOC(PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(),
                                                                              batch_col.get(), encoding_type));
                                current_rowid += chunk->num_rows();
                                batch_acc_rows += chunk->num_rows();

                                if (batch_acc_rows >= batch_rows_threshold) {
                                    RETURN_IF_ERROR(flush_replace_batch());
                                }
                            }
                        }
                        // Flush any trailing rows that did not reach the threshold.
                        RETURN_IF_ERROR(flush_replace_batch());
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

Status PrimaryKeyCompactionConflictResolver::execute_without_update_index() {
    // init rows mapper iter
    ASSIGN_OR_RETURN(auto filename, filename());
    RowsMapperIterator mapper_iter;
    RETURN_IF_ERROR(mapper_iter.open(filename));

    // 1. iterate all segment in output rowset
    RETURN_IF_ERROR(segment_iterator(
            [&](const CompactConflictResolveParams& params, const std::vector<std::shared_ptr<Segment>>& segments,
                const std::function<void(uint32_t, const DelVectorPtr&, uint32_t)>& handle_delvec_result_func) {
                std::map<uint32_t, DelVectorPtr> rssid_to_delvec;
                for (size_t segment_id = 0; segment_id < segments.size(); segment_id++) {
                    RETURN_IF_ERROR(breakpoint_check());
                    // 2. get input rssid & rowids, so we can generate delvec
                    vector<uint32_t> tmp_deletes;
                    std::vector<uint64_t> rssid_rowids;
                    RETURN_IF_ERROR(mapper_iter.next_values(segments[segment_id]->num_rows(), &rssid_rowids));
                    DCHECK(segments[segment_id]->num_rows() == rssid_rowids.size());
                    for (int i = 0; i < rssid_rowids.size(); i++) {
                        const uint32_t rssid = rssid_rowids[i] >> 32;
                        const uint32_t rowid = rssid_rowids[i] & 0xffffffff;
                        if (rssid_to_delvec.count(rssid) == 0) {
                            // get delvec by loader
                            DelVectorPtr delvec_ptr;
                            {
                                TRACE_COUNTER_SCOPE_LATENCY_US("compaction_delvec_loader_latency_us");
                                RETURN_IF_ERROR(params.delvec_loader->load({params.tablet_id, rssid},
                                                                           params.base_version, &delvec_ptr));
                            }
                            rssid_to_delvec[rssid] = delvec_ptr;
                        }
                        if (!rssid_to_delvec[rssid]->empty() && rssid_to_delvec[rssid]->roaring()->contains(rowid)) {
                            // Input row had been deleted, so we need to delete it from output rowset
                            tmp_deletes.push_back(i);
                        }
                    }
                    // 3. generate final delvec
                    DelVectorPtr dv = std::make_shared<DelVector>();
                    if (tmp_deletes.empty()) {
                        dv->init(params.new_version, nullptr, 0);
                    } else {
                        dv->init(params.new_version, tmp_deletes.data(), tmp_deletes.size());
                    }
                    handle_delvec_result_func(params.rowset_id + segment_id, dv, tmp_deletes.size());
                }
                return Status::OK();
            }));

    return mapper_iter.status();
}

} // namespace starrocks
