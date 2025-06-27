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

#include "exec/pipeline/lookup_operator.h"

#include <memory>
#include <variant>

#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/global_types.h"
#include "common/status.h"
#include "exec/olap_scan_node.h"
#include "exec/pipeline/fetch_processor.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/scan/olap_scan_context.h"
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exec/workgroup/work_group.h"
#include "gutil/integral_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "runtime/lookup_stream_mgr.h"
#include "serde/column_array_serde.h"
#include "serde/protobuf_serde.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/rowset/common.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "util/defer_op.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"
#include "util/time.h"

namespace starrocks::pipeline {

class LookUpProcessor {
public:
    LookUpProcessor(LookUpOperator* parent) : _parent(parent) {}
    ~LookUpProcessor() = default;

    void close();

    bool is_running() const { return _is_running; }
    void set_running(bool running) { _is_running = running; }

    bool need_input() const { return false; }

    Status process(RuntimeState* state);

    void set_ctx(std::shared_ptr<LookUpContext> ctx) {
        DCHECK(ctx != nullptr && !ctx->requests.empty()) << "request should not be empty";
        _ctx = std::move(ctx);
    }

private:
    typedef phmap::flat_hash_map<SlotId, RowIdColumn::Ptr> RowIdColumnMap;
    Status _collect_request_row_id_columns(LookUpContext* ctx, RowIdColumnMap* row_id_columns);

    Status _lookup_by_row_ids(RuntimeState* state, const RowIdColumnMap& row_id_columns);

    // seg_id -> scan range
    typedef std::map<int32_t, std::shared_ptr<SparseRange<rowid_t>>> SegmentRangeMap;
    StatusOr<ChunkPtr> _calculate_scan_ranges(const RowIdColumn::Ptr& row_id_column, SlotId row_id_slot_id,
                                              SegmentRangeMap* segment_ranges, Buffer<uint32_t>* replicated_offsets);

    StatusOr<ChunkPtr> _get_data_from_storage(RuntimeState* state, const std::vector<SlotDescriptor*>& slots,
                                              const SegmentRangeMap& segment_ranges);

    Status _fill_response(const ChunkPtr& result_chunk, SlotId row_id_slot, const std::vector<SlotDescriptor*>& slots);

    Status _deserialize_row_id_columns(RemoteLookUpRequest& request, RowIdColumnMap* row_id_columns);

    StatusOr<ChunkPtr> _sort_chunk(RuntimeState* state, const ChunkPtr& chunk, const Columns& order_by_columns);

    std::shared_ptr<LookUpContext> _ctx;
    std::atomic_bool _is_running = false;

    Permutation _permutation;
    raw::RawString _serialize_buffer;

    LookUpOperator* _parent = nullptr;

    OlapReaderStatistics _stats;
};

void LookUpProcessor::close() {
    // update parent counter
    COUNTER_UPDATE(_parent->_bytes_read_counter, _stats.bytes_read);
    COUNTER_UPDATE(_parent->_io_timer, _stats.io_ns);
    COUNTER_UPDATE(_parent->_read_compressed_counter, _stats.compressed_bytes_read);
    COUNTER_UPDATE(_parent->_decompress_timer, _stats.decompress_ns);
    COUNTER_UPDATE(_parent->_read_uncompressed_counter, _stats.uncompressed_bytes_read);

    COUNTER_UPDATE(_parent->_block_load_timer, _stats.block_load_ns);
    COUNTER_UPDATE(_parent->_block_load_counter, _stats.blocks_load);
    COUNTER_UPDATE(_parent->_block_fetch_timer, _stats.block_fetch_ns);
    COUNTER_UPDATE(_parent->_block_seek_timer, _stats.block_seek_ns);
    COUNTER_UPDATE(_parent->_block_seek_counter, _stats.block_seek_num);
    COUNTER_UPDATE(_parent->_raw_rows_counter, _stats.raw_rows_read);

    COUNTER_UPDATE(_parent->_read_pages_num_counter, _stats.total_pages_num);
    COUNTER_UPDATE(_parent->_cached_pages_num_counter, _stats.cached_pages_num);
    COUNTER_UPDATE(_parent->_total_columns_data_page_count, _stats.total_columns_data_page_count);
}

Status LookUpProcessor::_collect_request_row_id_columns(LookUpContext* ctx, RowIdColumnMap* row_id_columns) {
    SCOPED_TIMER(_parent->_collect_request_row_id_columns_timer);
    for (auto& request : ctx->requests) {
        RETURN_IF_ERROR(std::visit(
                [&](auto& request) {
                    int64_t pending_time = MonotonicNanos() - request.receive_ts;
                    COUNTER_UPDATE(_parent->_request_pending_time, pending_time);
                    if constexpr (std::is_same_v<std::decay_t<decltype(request)>, LocalLookUpRequest>) {
                        VLOG_ROW << "[GLM] LocalLookUpRequest pending time: " << (pending_time * 1.0) / 1000000
                                 << "ms, plan_node_id:" << _parent->_plan_node_id;

                        for (const auto& [slot_id, columns] : *(request.fetch_ctx->request_columns)) {
                            auto row_id_column = RowIdColumn::static_pointer_cast(columns.first);
                            auto [iter, _] = row_id_columns->try_emplace(slot_id, RowIdColumn::create());
                            iter->second->append(*row_id_column, 0, row_id_column->size());
                        }
                        return Status::OK();
                    } else if constexpr (std::is_same_v<std::decay_t<decltype(request)>, RemoteLookUpRequest>) {
                        VLOG_ROW << "[GLM] RemoteLookUpRequest pending time: " << (pending_time * 1.0) / 1000000
                                 << "ms, plan_node_id:" << _parent->_plan_node_id << ", " << (void*)_parent
                                 << ", dispatcher: " << (void*)_parent->_dispatcher.get();
                        return _deserialize_row_id_columns(request, row_id_columns);
                    } else {
                        DCHECK(false) << "unknown request type";
                        return Status::InternalError("unknown request type");
                    }
                },
                request));
    }

    return Status::OK();
}

StatusOr<ChunkPtr> LookUpProcessor::_calculate_scan_ranges(const RowIdColumn::Ptr& row_id_column, SlotId row_id_slot_id,
                                                           SegmentRangeMap* segment_ranges,
                                                           Buffer<uint32_t>* replicate_offsets) {
    SCOPED_TIMER(_parent->_calculate_scan_ranges_timer);
    UInt32Column::Ptr position_column = UInt32Column::create();
    position_column->resize_uninitialized(row_id_column->size());
    auto& position_data = position_column->get_data();
    for (size_t i = 0; i < row_id_column->size(); i++) {
        position_data[i] = i;
    }

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(row_id_column, row_id_slot_id);
    chunk->append_column(position_column, Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
    chunk->check_or_die();

    // sort chunk by seg_id and ord_id
    ASSIGN_OR_RETURN(auto sorted_chunk,
                     _sort_chunk(_parent->runtime_state(), chunk,
                                 {row_id_column->seg_ids_column(), row_id_column->ord_ids_column()}));

    const auto& ordered_row_id_column =
            down_cast<RowIdColumn*>(sorted_chunk->get_column_by_slot_id(row_id_slot_id).get());
    const auto& seg_ids = UInt32Column::static_pointer_cast(ordered_row_id_column->seg_ids_column())->get_data();
    const auto& ord_ids = UInt32Column::static_pointer_cast(ordered_row_id_column->ord_ids_column())->get_data();
    size_t num_rows = ordered_row_id_column->size();

    // build sparse range for each segment
    uint32_t cur_seg_id = seg_ids[0];
    uint32_t cur_ord_id = ord_ids[0];
    Range<rowid_t> cur_range(cur_ord_id, cur_ord_id + 1);

    replicate_offsets->emplace_back(0);
    replicate_offsets->emplace_back(1);

    bool has_duplicated_row = false;
    // @TODO(silverbullet233): `try_emplace` will be called so many times in the following loop,
    // we should use a better data structure.
    for (size_t i = 1; i < num_rows; i++) {
        uint32_t seg_id = seg_ids[i];
        uint32_t ord_id = ord_ids[i];
        if (seg_id == cur_seg_id) {
            // same segment, check if need add a new range
            if (ord_id == cur_range.end() - 1) {
                // duplicated ord_ids, do nothing, we should mark which idx is duplicated
                replicate_offsets->back()++;
                has_duplicated_row = true;
                continue;
            }
            if (ord_id == cur_range.end()) {
                // continous range, just expand current range
                cur_range.expand(1);
            } else {
                // not continous, add the old one into seg_ranges
                auto [iter, _] = segment_ranges->try_emplace(cur_seg_id, std::make_shared<SparseRange<rowid_t>>());
                iter->second->add(cur_range);
                cur_range = Range<rowid_t>(ord_id, ord_id + 1);
            }
        } else {
            // move to next segment, we should add the old range into seg_ranges
            auto [iter, _] = segment_ranges->try_emplace(cur_seg_id, std::make_shared<SparseRange<rowid_t>>());
            iter->second->add(cur_range);
            // reset all
            cur_seg_id = seg_id;
            cur_range = Range<rowid_t>(ord_id, ord_id + 1);
        }
        replicate_offsets->emplace_back(replicate_offsets->back() + 1);
    }
    // handle the last one
    auto [iter, _] = segment_ranges->try_emplace(cur_seg_id, std::make_shared<SparseRange<rowid_t>>());
    iter->second->add(cur_range);

    // if there is no duplicated row, clear replicate_offsets
    if (!has_duplicated_row) {
        replicate_offsets->clear();
    }

    return sorted_chunk;
}

StatusOr<ChunkPtr> LookUpProcessor::_get_data_from_storage(RuntimeState* state,
                                                           const std::vector<SlotDescriptor*>& slots,
                                                           const SegmentRangeMap& segment_ranges) {
    SCOPED_TIMER(_parent->_get_data_from_storage_timer);
    ChunkPtr result_chunk;

    auto* glm_ctx = state->query_ctx()->global_late_materialization_ctx();
    for (const auto& [seg_id, range] : segment_ranges) {
        const auto& segment_info = glm_ctx->get_segment(seg_id);
        auto segment = segment_info.segment;
        DCHECK(segment_info.fs != nullptr) << "segment fs should not be null, seg_id: " << seg_id;
        auto tablet_schema = segment->tablet_schema_share_ptr();
        const auto& global_dict_map = state->get_query_global_dict_map();
        std::unique_ptr<ColumnIdToGlobalDictMap> global_dict(new ColumnIdToGlobalDictMap());

        // @TODO(silverbullet233): can we cache these info for each segment
        std::vector<ColumnId> cids;
        phmap::flat_hash_map<ColumnId, SlotId> cid_to_slot_id;
        for (const auto& slot : slots) {
            auto idx = tablet_schema->field_index(slot->col_name());
            DCHECK(idx != static_cast<size_t>(-1)) << "column not found: " << slot->col_name();
            cids.push_back(idx);
            cid_to_slot_id[idx] = slot->id();
            if (auto iter = global_dict_map.find(slot->id()); iter != global_dict_map.end()) {
                auto& dict_map = iter->second.first;
                global_dict->emplace(idx, const_cast<GlobalDictMap*>(&dict_map));
            }
        }
        Schema schema = ChunkHelper::convert_schema(tablet_schema, cids);
        SegmentReadOptions options;
        options.fs = segment_info.fs;
        options.tablet_schema = tablet_schema;
        options.stats = &_stats;
        options.global_dictmaps = global_dict.get();
        options.rowid_range_option = range;
        auto segment_iterator = new_segment_iterator(segment, schema, options);
        RETURN_IF_ERROR(segment_iterator->init_encoded_schema(*global_dict));

        ChunkPtr tmp;
        do {
            tmp.reset(ChunkHelper::new_chunk_pooled(segment_iterator->output_schema(), state->chunk_size()));
            auto status = segment_iterator->get_next(tmp.get());
            if (status.is_end_of_file()) {
                break;
            } else if (!status.ok()) {
                LOG(WARNING) << "get next segment iterator failed: " << status.to_string();
                return status;
            }

            if (tmp->is_empty()) {
                break;
            }

            if (result_chunk == nullptr) {
                result_chunk = std::make_shared<Chunk>();
                for (const auto& [cid, idx] : tmp->get_column_id_to_index_map()) {
                    auto column = tmp->get_column_by_index(idx);
                    SlotId slot_id = cid_to_slot_id[cid];
                    result_chunk->append_or_update_column(std::move(column), slot_id);
                }
            } else {
                for (const auto& [cid, idx] : tmp->get_column_id_to_index_map()) {
                    auto column = tmp->get_column_by_index(idx);
                    SlotId slot_id = cid_to_slot_id[cid];
                    result_chunk->get_column_by_slot_id(slot_id)->append(*column);
                }
            }

        } while (true);
    }

    return result_chunk;
}

Status LookUpProcessor::_fill_response(const ChunkPtr& result_chunk, SlotId row_id_slot,
                                       const std::vector<SlotDescriptor*>& slots) {
    SCOPED_TIMER(_parent->_fill_response_timer);
    size_t offset = 0;
    for (auto& request : _ctx->requests) {
        RETURN_IF_ERROR(std::visit(
                [&](auto& request) {
                    if constexpr (std::is_same_v<std::decay_t<decltype(request)>, LocalLookUpRequest>) {
                        auto& fetch_ctx = request.fetch_ctx;
                        if (!fetch_ctx->request_columns->contains(row_id_slot)) {
                            return Status::OK();
                        }

                        auto [row_id_column, _] = fetch_ctx->request_columns->at(row_id_slot);
                        size_t num_rows = row_id_column->size();
                        for (const auto& slot : slots) {
                            auto src_column = result_chunk->get_column_by_slot_id(slot->id());
                            auto dst_column = src_column->clone_empty();
                            dst_column->append(*src_column, offset, num_rows);
                            fetch_ctx->response_columns[slot->id()] = std::move(dst_column);
                        }
                        offset += num_rows;
                        return Status::OK();
                    } else if constexpr (std::is_same_v<std::decay_t<decltype(request)>, RemoteLookUpRequest>) {
                        if (!request.request_columns.contains(row_id_slot)) {
                            return Status::OK();
                        }
                        size_t num_rows = request.request_columns.at(row_id_slot)->size();

                        std::vector<ColumnPtr> columns;
                        size_t max_serialize_size = 0;

                        for (const auto& slot : slots) {
                            auto src_column = result_chunk->get_column_by_slot_id(slot->id());
                            auto dst_column = src_column->clone_empty();
                            dst_column->append(*src_column, offset, num_rows);
                            max_serialize_size += serde::ColumnArraySerde::max_serialized_size(*dst_column);
                            columns.emplace_back(std::move(dst_column));
                        }

                        _serialize_buffer.resize(max_serialize_size);
                        uint8_t* buff = reinterpret_cast<uint8_t*>(_serialize_buffer.data());
                        uint8_t* begin = buff;
                        for (size_t i = 0; i < slots.size(); i++) {
                            auto column = columns[i];
                            auto pcolumn = request.response->add_columns();
                            pcolumn->set_slot_id(slots[i]->id());
                            uint8_t* start = buff;
                            buff = serde::ColumnArraySerde::serialize(*column, buff);
                            pcolumn->set_data_size(buff - start);
                        }
                        size_t actual_serialize_size = buff - begin;
                        auto* cntl = static_cast<brpc::Controller*>(request.cntl);
                        cntl->response_attachment().append(_serialize_buffer.data(), actual_serialize_size);
                        offset += num_rows;

                        return Status::OK();
                    } else {
                        DCHECK(false) << "unknown request type";
                        return Status::InternalError("unknown request type");
                    }
                },
                request));
    }
    DCHECK_EQ(offset, result_chunk->num_rows())
            << "offset not match, offset: " << offset << ", num_rows: " << result_chunk->num_rows();
    return Status::OK();
}

StatusOr<ChunkPtr> LookUpProcessor::_sort_chunk(RuntimeState* state, const ChunkPtr& chunk,
                                                const Columns& order_by_columns) {
    // @TODO(silverbullet233): reuse sort descs
    SortDescs sort_descs;
    sort_descs.descs.reserve(order_by_columns.size());
    for (size_t i = 0; i < order_by_columns.size(); i++) {
        sort_descs.descs.emplace_back(true, true);
    }
    _permutation.resize(0);

    RETURN_IF_ERROR(sort_and_tie_columns(state->cancelled_ref(), order_by_columns, sort_descs, &_permutation));
    auto sorted_chunk = chunk->clone_empty_with_slot(chunk->num_rows());
    materialize_by_permutation(sorted_chunk.get(), {chunk}, _permutation);

    return sorted_chunk;
}

Status LookUpProcessor::_lookup_by_row_ids(RuntimeState* state, const RowIdColumnMap& row_id_columns) {
    SCOPED_TIMER(_parent->_lookup_by_row_ids_timer);
    for (const auto& [tuple_id, slot_id] : _parent->_row_id_slots) {
        if (!row_id_columns.contains(slot_id)) {
            continue;
        }
        const auto& row_id_column = row_id_columns.at(slot_id);
        SegmentRangeMap segment_ranges;
        Buffer<uint32_t> replicate_offsets;

        // sort by row_id
        ASSIGN_OR_RETURN(auto sorted_chunk,
                         _calculate_scan_ranges(row_id_column, slot_id, &segment_ranges, &replicate_offsets));
        const auto& tuple_desc = state->desc_tbl().get_tuple_descriptor(tuple_id);
        const auto& slots = tuple_desc->slots();

        ASSIGN_OR_RETURN(auto result_chunk, _get_data_from_storage(state, slots, segment_ranges));

        auto unordered_position_column = sorted_chunk->get_column_by_slot_id(Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
        if (!replicate_offsets.empty()) {
            // if there are duplicated rows, we need to replicate the data
            for (const auto& [slot_id, _] : result_chunk->get_slot_id_to_index_map()) {
                auto old_column = result_chunk->get_column_by_slot_id(slot_id);
                ASSIGN_OR_RETURN(auto new_column, old_column->replicate(replicate_offsets));
                result_chunk->append_or_update_column(std::move(new_column), slot_id);
            }
            result_chunk->check_or_die();
        }
        DCHECK_EQ(row_id_column->size(), result_chunk->num_rows());

        // add position column
        result_chunk->append_column(unordered_position_column, Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
        result_chunk->check_or_die();

        // re-sort by position
        ASSIGN_OR_RETURN(auto sorted_result_chunk, _sort_chunk(state, result_chunk, {unordered_position_column}));
        // fill response
        RETURN_IF_ERROR(_fill_response(sorted_result_chunk, slot_id, slots));
    }
    return Status::OK();
}

Status LookUpProcessor::process(RuntimeState* state) {
    SCOPED_TIMER(_parent->_process_time);
    Status status;
    DeferOp op([&]() {
        for (auto& request : _ctx->requests) {
            std::visit(
                    [&](auto& request) {
                        if constexpr (std::is_same_v<std::decay_t<decltype(request)>, LocalLookUpRequest>) {
                            request.callback(status);
                        } else if constexpr (std::is_same_v<std::decay_t<decltype(request)>, RemoteLookUpRequest>) {
                            status.to_protobuf(request.response->mutable_status());
                            request.done->Run();
                        } else {
                            DCHECK(false) << "unknown request type";
                        }
                    },
                    request);
        }
    });
    COUNTER_UPDATE(_parent->_received_request_count, _ctx->requests.size());
    COUNTER_UPDATE(_parent->_process_counter, 1);

    phmap::flat_hash_map<SlotId, RowIdColumn::Ptr> row_id_columns;
    RETURN_IF_ERROR(status = _collect_request_row_id_columns(_ctx.get(), &row_id_columns));
    RETURN_IF_ERROR(status = _lookup_by_row_ids(state, row_id_columns));

    return Status::OK();
}

Status LookUpProcessor::_deserialize_row_id_columns(RemoteLookUpRequest& request, RowIdColumnMap* row_id_columns) {
    request.request_columns.clear();
    for (size_t i = 0; i < request.request->row_id_columns_size(); i++) {
        const auto& pcolumn = request.request->row_id_columns(i);
        int32_t slot_id = pcolumn.slot_id();
        [[maybe_unused]] int64_t data_size = pcolumn.data_size();
        RowIdColumn::Ptr row_id_column = RowIdColumn::create();
        const uint8_t* buff = reinterpret_cast<const uint8_t*>(pcolumn.data().data());
        auto ret = serde::ColumnArraySerde::deserialize(buff, row_id_column.get());
        if (ret == nullptr) {
            LOG(WARNING) << "deserialize row id column error, slot_id: " << slot_id;
            return Status::InternalError("deserialize row id column error");
        }
        //
        request.request_columns.emplace(slot_id, row_id_column);
        auto [iter, _] = row_id_columns->try_emplace(slot_id, RowIdColumn::create());
        iter->second->append(*row_id_column, 0, row_id_column->size());
    }
    return Status::OK();
}

LookUpOperator::LookUpOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                               const phmap::flat_hash_map<TupleId, SlotId>& row_id_slots,
                               std::shared_ptr<LookUpDispatcher> dispatcher, int32_t max_io_tasks)
        : SourceOperator(factory, id, "look_up", plan_node_id, true, driver_sequence),
          _row_id_slots(row_id_slots),
          _dispatcher(std::move(dispatcher)),
          _max_io_tasks(max_io_tasks) {
    for (int32_t i = 0; i < _max_io_tasks; i++) {
        _processors.emplace_back(std::make_shared<LookUpProcessor>(this));
    }
}

Status LookUpOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _query_ctx = state->query_ctx()->get_shared_ptr();
    // init counter
    _init_counter(state);
    _dispatcher->attach_query_ctx(state->query_ctx());
    _dispatcher->attach_observer(state, observer());

    return Status::OK();
}

void LookUpOperator::_init_counter(RuntimeState* state) {
    _bytes_read_counter = ADD_COUNTER(_unique_metrics, "BytesRead", TUnit::BYTES);
    _rows_read_counter = ADD_COUNTER(_unique_metrics, "RowsRead", TUnit::UNIT);

    _read_compressed_counter = ADD_COUNTER(_unique_metrics, "CompressedBytesRead", TUnit::BYTES);
    _read_uncompressed_counter = ADD_COUNTER(_unique_metrics, "UncompressedBytesRead", TUnit::BYTES);

    _raw_rows_counter = ADD_COUNTER(_unique_metrics, "RawRowsRead", TUnit::UNIT);
    _read_pages_num_counter = ADD_COUNTER(_unique_metrics, "ReadPagesNum", TUnit::UNIT);
    _cached_pages_num_counter = ADD_COUNTER(_unique_metrics, "CachedPagesNum", TUnit::UNIT);

    _io_task_exec_timer = ADD_TIMER(_unique_metrics, "IOTaskExecTime");
    // SegmentInit

    // SegmentRead
    const std::string segment_read_name = "SegmentRead";
    _block_load_timer = ADD_CHILD_TIMER(_unique_metrics, segment_read_name, IO_TASK_EXEC_TIMER_NAME);
    _block_fetch_timer = ADD_CHILD_TIMER(_unique_metrics, "BlockFetch", segment_read_name);
    _block_load_counter = ADD_CHILD_COUNTER(_unique_metrics, "BlockFetchCount", TUnit::UNIT, segment_read_name);
    _block_seek_timer = ADD_CHILD_TIMER(_unique_metrics, "BlockSeek", segment_read_name);
    _block_seek_counter = ADD_CHILD_COUNTER(_unique_metrics, "BlockSeekCount", TUnit::UNIT, segment_read_name);
    _decompress_timer = ADD_CHILD_TIMER(_unique_metrics, "DecompressT", segment_read_name);
    _total_columns_data_page_count =
            ADD_CHILD_COUNTER(_unique_metrics, "TotalColumnsDataPageCount", TUnit::UNIT, segment_read_name);

    // IOTime
    _io_timer = ADD_CHILD_TIMER(_unique_metrics, "IOTime", IO_TASK_EXEC_TIMER_NAME);

    _collect_request_row_id_columns_timer = ADD_TIMER(_unique_metrics, "CollectRequestRowIdColumnsTime");
    _lookup_by_row_ids_timer = ADD_TIMER(_unique_metrics, "LookupByRowIdsTime");
    _calculate_scan_ranges_timer = ADD_TIMER(_unique_metrics, "CalculateScanRangesTime");
    _get_data_from_storage_timer = ADD_TIMER(_unique_metrics, "GetDataFromStorageTime");
    _fill_response_timer = ADD_TIMER(_unique_metrics, "FillResponseTime");
    _process_counter = ADD_COUNTER(_unique_metrics, "ProcessCount", TUnit::UNIT);
    _process_time = ADD_TIMER(_unique_metrics, "ProcessTime");
    _received_request_count = ADD_COUNTER(_unique_metrics, "ReceivedRequestCount", TUnit::UNIT);
    _request_pending_time = ADD_TIMER(_unique_metrics, "RequestPendingTime");

    _submit_io_task_counter = ADD_COUNTER(_unique_metrics, "SubmitIOTaskCount", TUnit::UNIT);
    _peak_scan_task_queue_size_counter = _unique_metrics->AddHighWaterMarkCounter(
            "PeakScanTaskQueueSize", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));
    _io_task_wait_timer = ADD_TIMER(_unique_metrics, "IOTaskWaitTime");
}

void LookUpOperator::close(RuntimeState* state) {
    VLOG_ROW << "[GLM] LookUpOperator::close, " << (void*)this << ", target_node_id: " << _plan_node_id;
    for (auto& processor : _processors) {
        processor->close();
    }
    Operator::close(state);
}

bool LookUpOperator::has_output() const {
    if (_is_finished) {
        return false;
    }
    if (!_get_io_task_status().ok()) {
        return true;
    }
    if (_num_running_io_tasks >= _max_io_tasks) {
        return false;
    }
    for (size_t i = 0; i < _max_io_tasks; i++) {
        auto& processor = _processors[i];
        if (!processor->is_running() && _dispatcher->has_data(_driver_sequence)) {
            // we can trigger io task
            return true;
        }
    }

    return false;
}

bool LookUpOperator::is_finished() const {
    return _is_finished && _num_running_io_tasks == 0;
}

bool LookUpOperator::pending_finish() const {
    return !is_finished();
}

Status LookUpOperator::set_finishing(RuntimeState* state) {
    VLOG_ROW << "[GLM] LookUpOperator::set_finishing, " << (void*)this << ", target_node_id: " << _plan_node_id
             << ", dispatcher: " << (void*)_dispatcher.get();
    _is_finished = true;
    return _clean_request_queue(state);
}

StatusOr<ChunkPtr> LookUpOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_ERROR(_get_io_task_status());
    RETURN_IF_ERROR(_try_to_trigger_io_task(state));
    return nullptr;
}

Status LookUpOperator::_try_to_trigger_io_task(RuntimeState* state) {
    for (int32_t i = 0; i < _max_io_tasks; i++) {
        auto processor = _processors[i];

        auto lookup_ctx = std::make_shared<LookUpContext>();
        bool is_running = processor->is_running();
        if (!is_running && _dispatcher->try_get(_driver_sequence, config::max_lookup_batch_request, lookup_ctx.get())) {
            processor->set_ctx(lookup_ctx);
            COUNTER_UPDATE(_submit_io_task_counter, 1);
            workgroup::ScanTask task;
            task.workgroup = state->fragment_ctx()->workgroup();
            task.priority = OlapScanNode::compute_priority(_submit_io_task_counter->value());
            task.task_group = down_cast<const LookUpOperatorFactory*>(_factory)->io_task_group();
            task.peak_scan_task_queue_size_counter = _peak_scan_task_queue_size_counter;
            task.work_function = [wp = _query_ctx, this, state, idx = i, create_ts = MonotonicNanos()](auto& ctx) {
                if (auto sp = wp.lock()) {
                    auto& processor = _processors[idx];
                    [[maybe_unused]] int64_t start_time = MonotonicNanos();
                    DeferOp defer([&] {
                        _num_running_io_tasks--;
                        processor->set_running(false);
                        [[maybe_unused]] int64_t end_time = MonotonicNanos();
                        VLOG_ROW << "[GLM] LookUpOperator::process, num_running_io_tasks: " << _num_running_io_tasks
                                 << ", " << (void*)this << ", process time: " << (end_time - start_time) / 1000000
                                 << "ms"
                                 << ", queue time: " << (start_time - create_ts) / 1000000 << "ms"
                                 << ", dispatcher: " << (void*)_dispatcher.get();
                        this->defer_notify();
                    });
                    COUNTER_UPDATE(_io_task_wait_timer, MonotonicNanos() - create_ts);
                    Status status = processor->process(state);
                    if (!status.ok()) {
                        LOG(WARNING) << "process error: " << status.to_string();
                        _set_io_task_status(status);
                    }
                }
            };
            _num_running_io_tasks++;
            VLOG_ROW << "[GLM] LookUpOperator::submit_io_task, num_running_io_tasks: " << _num_running_io_tasks << ", "
                     << (void*)this << ", dispatcher: " << (void*)_dispatcher.get() << ", idx: " << i;
            processor->set_running(true);
            task.workgroup->executors()->scan_executor()->submit(std::move(task));
        }
    }

    return Status::OK();
}

Status LookUpOperator::_clean_request_queue(RuntimeState* state) {
    DCHECK(_is_finished) << "LookUpOperator should be finished before clean request queue, " << (void*)this;
    do {
        auto ctx = std::make_shared<LookUpContext>();
        if (!_dispatcher->try_get(_driver_sequence, config::max_lookup_batch_request, ctx.get())) {
            // no more request
            break;
        }
        for (auto& request : ctx->requests) {
            std::visit(
                    [&](auto& request) {
                        if constexpr (std::is_same_v<std::decay_t<decltype(request)>, LocalLookUpRequest>) {
                            request.callback(Status::Cancelled("LookUpOperator is finished"));
                        } else if constexpr (std::is_same_v<std::decay_t<decltype(request)>, RemoteLookUpRequest>) {
                            Status status = Status::Cancelled("LookUpOperator is finished");
                            status.to_protobuf(request.response->mutable_status());
                            request.done->Run();
                        } else {
                            DCHECK(false) << "unknown request type";
                        }
                    },
                    request);
        }
    } while (true);
    return Status::OK();
}

LookUpOperatorFactory::LookUpOperatorFactory(int32_t id, int32_t plan_node_id,
                                             phmap::flat_hash_map<TupleId, SlotId> row_id_slots,
                                             std::shared_ptr<LookUpDispatcher> dispatcher, int32_t max_io_tasks)
        : SourceOperatorFactory(id, "lookup", plan_node_id),
          _row_id_slots(std::move(row_id_slots)),
          _dispatcher(std::move(dispatcher)),
          _max_io_tasks(max_io_tasks),
          _io_task_group(std::make_shared<workgroup::ScanTaskGroup>()) {}

} // namespace starrocks::pipeline