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

#include "exec/pipeline/lookup_request.h"

#include <brpc/controller.h>

#include <memory>

#include "base/container/raw_container.h"
#include "base/failpoint/fail_point.h"
#include "base/status.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/column_helper.h"
#include "column/serde/column_array_serde.h"
#include "column/sorting/sort_permute.h"
#include "column/sorting/sorting.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "connector/hive_connector.h"
#include "exec/pipeline/lookup_operator.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/scan/glm_manager.h"
#include "runtime/chunk_accumulator.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/primitive/range.h"

namespace starrocks::pipeline {

DEFINE_FAIL_POINT(lookup_request_failed);

// Deserialize remote request payload into a reusable chunk for processing.
Status RemoteLookUpRequestContext::collect_input_columns(ChunkPtr chunk) {
    request_chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < request->request_columns_size(); i++) {
        const auto& pcolumn = request->request_columns(i);
        SlotId slot_id = pcolumn.slot_id();
        int64_t data_size = pcolumn.data_size();
        auto dst_col = chunk->get_column_by_slot_id(slot_id)->as_mutable_raw_ptr();
        auto col = dst_col->clone_empty();
        VLOG_FILE << "deserialize column, slot_id: " << slot_id << ", data_size: " << data_size
                  << ", column: " << col->get_name();
        const uint8_t* buff = reinterpret_cast<const uint8_t*>(pcolumn.data().data());
        const auto* end = buff + data_size;
        auto ret = serde::ColumnArraySerde::deserialize(buff, end, col.get());
        if (!ret.ok()) {
            auto msg = fmt::format("deserialize column failed, slot_id: {}, data_size: {}", slot_id, data_size);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        dst_col->append(*col, 0, col->size());
        request_chunk->append_column(std::move(col), slot_id);
    }
    chunk->check_or_die();
    VLOG_FILE << "RemoteLookUpRequestContext collect input columns: " << chunk->debug_columns();
    return Status::OK();
}

// Serialize the result subset and write it back to the RPC response attachment.
StatusOr<size_t> RemoteLookUpRequestContext::fill_response(const ChunkPtr& result_chunk,
                                                           const std::vector<SlotDescriptor*>& slots,
                                                           size_t start_offset) {
    size_t num_rows = request_chunk->num_rows();
    VLOG_FILE << "RemoteLookUpRequestContext fill response, num_rows: " << num_rows << ", slots: " << slots.size();
    std::vector<ColumnPtr> columns;
    size_t max_serialized_size = 0;
    for (const auto& slot : slots) {
        auto src_col = result_chunk->get_column_by_slot_id(slot->id());
        auto dst_col = src_col->clone_empty();
        dst_col->append(*src_col, start_offset, num_rows);
        max_serialized_size += serde::ColumnArraySerde::max_serialized_size(*dst_col);
        columns.emplace_back(std::move(dst_col));
    }
    // @TODO reuse serialize buffer
    raw::RawString serialize_buffer;
    serialize_buffer.resize(max_serialized_size);
    uint8_t* buff = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    uint8_t* begin = buff;
    for (size_t i = 0; i < slots.size(); i++) {
        auto column = columns[i];
        auto pcolumn = response->add_columns();
        pcolumn->set_slot_id(slots[i]->id());
        uint8_t* start = buff;
        ASSIGN_OR_RETURN(buff, serde::ColumnArraySerde::serialize(*column, buff));
        pcolumn->set_data_size(buff - start);
        VLOG_FILE << "serialize column: " << slots[i]->id() << ", " << column->get_name()
                  << ", data_size: " << (buff - start);
    }
    size_t actual_serialize_size = buff - begin;
    auto* brpc_cntl = static_cast<brpc::Controller*>(cntl);
    brpc_cntl->response_attachment().append(serialize_buffer.data(), actual_serialize_size);

    return num_rows;
}

void RemoteLookUpRequestContext::callback(const Status& status) {
    VLOG_FILE << "RemoteLookUpRequestContext callback: " << status.to_string();
    status.to_protobuf(response->mutable_status());
    done->Run();
}

StatusOr<ChunkPtr> LookUpTask::_sort_chunk(RuntimeState* state, const ChunkPtr& chunk,
                                           const Columns& order_by_columns) {
    SortDescs sort_descs;
    sort_descs.descs.reserve(order_by_columns.size());
    for (size_t i = 0; i < order_by_columns.size(); i++) {
        sort_descs.descs.emplace_back(true, true);
    }
    _ctx->permutation.resize(0);

    RETURN_IF_ERROR(sort_and_tie_columns(state->cancelled_ref(), order_by_columns, sort_descs, &_ctx->permutation));
    auto sorted_chunk = chunk->clone_empty_with_slot(chunk->num_rows());
    materialize_by_permutation(sorted_chunk.get(), {chunk}, _ctx->permutation);

    return sorted_chunk;
}

// Derives row-id ranges for the incoming batch and records duplicates so
// downstream data can be replicated to match request cardinality.
StatusOr<ChunkPtr> LookUpTask::_calculate_row_id_range(
        RuntimeState* state, const ChunkPtr& request_chunk,
        phmap::flat_hash_map<int32_t, std::shared_ptr<SparseRange<int64_t>>>* row_id_ranges,
        Buffer<uint32_t>* replicated_offsets) {
    SCOPED_TIMER(_ctx->parent->_calculate_row_id_range_timer);
    // Step 1: Add position column to track original row order
    UInt32Column::MutablePtr position_column = UInt32Column::create();
    position_column->resize_uninitialized(request_chunk->num_rows());
    auto& position_data = position_column->get_data();
    for (size_t i = 0; i < request_chunk->num_rows(); i++) {
        position_data[i] = i;
    }
    request_chunk->append_column(std::move(position_column), Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
    request_chunk->check_or_die();

    // Step 2: Sort by scan_range_id and row_id for efficient range calculation
    auto scan_range_id_column = request_chunk->get_column_by_slot_id(_ctx->fetch_ref_slot_ids[0]);
    auto row_id_column = request_chunk->get_column_by_slot_id(_ctx->fetch_ref_slot_ids[1]);

    ASSIGN_OR_RETURN(auto sorted_chunk, _sort_chunk(state, request_chunk, {scan_range_id_column, row_id_column}));

    // Step 3: Calculate row_id ranges and replicated_offsets for duplicate handling
    const auto& nullable_scan_range_id_column = down_cast<NullableColumn*>(
            sorted_chunk->get_column_by_slot_id(_ctx->fetch_ref_slot_ids[0])->as_mutable_raw_ptr());
    DCHECK(!nullable_scan_range_id_column->has_null()) << "scan_range_id column should not have null";
    auto ordered_scan_range_id_column = Int32Column::static_pointer_cast(nullable_scan_range_id_column->data_column());
    const auto ordered_scan_range_ids = ordered_scan_range_id_column->immutable_data();

    const auto& nullable_row_id_column = down_cast<NullableColumn*>(
            sorted_chunk->get_column_by_slot_id(_ctx->fetch_ref_slot_ids[1])->as_mutable_raw_ptr());
    DCHECK(!nullable_row_id_column->has_null()) << "row_id column should not have null";
    auto ordered_row_id_column = Int64Column::static_pointer_cast(nullable_row_id_column->data_column());
    const auto ordered_row_ids = ordered_row_id_column->immutable_data();

    size_t num_rows = ordered_scan_range_id_column->size();

    int32_t cur_scan_range_id = ordered_scan_range_ids[0];
    int64_t cur_row_id = ordered_row_ids[0];
    Range<int64_t> cur_range(cur_row_id, cur_row_id + 1);

    replicated_offsets->emplace_back(0);
    replicated_offsets->emplace_back(1);

    bool has_duplicated_row = false;
    for (size_t i = 1; i < num_rows; i++) {
        int32_t scan_range_id = ordered_scan_range_ids[i];
        int64_t row_id = ordered_row_ids[i];
        if (scan_range_id == cur_scan_range_id) {
            // same scan range, check if need add a new range
            if (row_id == cur_range.end() - 1) {
                // Duplicate row_id found, increment replication count
                replicated_offsets->back()++;
                has_duplicated_row = true;
                continue;
            }
            if (row_id == cur_range.end()) {
                // Continuous range, expand current range
                cur_range.expand(1);
            } else {
                // Non-continuous, add current range to row_id_ranges
                auto [iter, _] =
                        row_id_ranges->try_emplace(cur_scan_range_id, std::make_shared<SparseRange<int64_t>>());
                iter->second->add(cur_range);
                cur_range = Range<int64_t>(row_id, row_id + 1);
            }
        } else {
            // Move to next scan range, add current range to row_id_ranges
            auto [iter, _] = row_id_ranges->try_emplace(cur_scan_range_id, std::make_shared<SparseRange<int64_t>>());
            iter->second->add(cur_range);
            cur_scan_range_id = scan_range_id;
            cur_range = Range<int64_t>(row_id, row_id + 1);
        }
        replicated_offsets->emplace_back(replicated_offsets->back() + 1);
    }
    // Add the last range
    auto [iter, _] = row_id_ranges->try_emplace(cur_scan_range_id, std::make_shared<SparseRange<int64_t>>());
    iter->second->add(cur_range);
    for (const auto& [scan_range_id, range] : *row_id_ranges) {
        VLOG_FILE << "scan_range_id: " << scan_range_id << ", range: " << range->to_string();
    }

    if (!has_duplicated_row) {
        replicated_offsets->clear();
    }
    return sorted_chunk;
}

// Reads the fetched columns for the calculated row-position ranges. Hands the file-local
// row positions to the scan natively (set_row_id_ranges) so the reader skips non-matching
// row groups and pages instead of evaluating a row-id predicate.
StatusOr<ChunkPtr> IcebergLookUpTask::_get_data_from_storage(
        RuntimeState* state,
        const phmap::flat_hash_map<int32_t, std::shared_ptr<SparseRange<int64_t>>>& row_id_ranges) {
    SCOPED_TIMER(_ctx->parent->_get_data_from_storage_timer);
    ChunkPtr result_chunk;
    // Iterate scan ranges in ascending scan_range_id order so the accumulated rows match the
    // (scan_range_id, row_id)-sorted request chunk. The hash-map's arbitrary iteration order would
    // misalign the SORT_ORDINAL position column appended in process() and the replicated_offsets,
    // attaching fetched payloads to the wrong request rows.
    std::vector<int32_t> ordered_scan_range_ids;
    ordered_scan_range_ids.reserve(row_id_ranges.size());
    for (const auto& [scan_range_id, _] : row_id_ranges) {
        ordered_scan_range_ids.push_back(scan_range_id);
    }
    std::sort(ordered_scan_range_ids.begin(), ordered_scan_range_ids.end());
    for (int32_t scan_range_id : ordered_scan_range_ids) {
        const auto& row_id_range = row_id_ranges.at(scan_range_id);
        // The reader works in unsigned file-local positions; these are non-negative row ids.
        // `native_range` must outlive the data source below (it is read during open/get_next).
        SparseRange<uint64_t> native_range;
        for (size_t i = 0; i < row_id_range->size(); i++) {
            const Range<int64_t>& r = (*row_id_range)[i];
            native_range.add(Range<uint64_t>(static_cast<uint64_t>(r.begin()), static_cast<uint64_t>(r.end())));
        }

        auto glm_ctx = down_cast<IcebergGlobalLateMaterilizationContext*>(
                state->query_runtime_state()->global_late_materialization_ctx_mgr()->get_ctx(_ctx->scan_id));
        if (glm_ctx == nullptr) {
            return Status::InternalError("GlobalLateMaterilizationContext not found for scan_id: " +
                                         std::to_string(_ctx->scan_id));
        }
        auto hdfs_scan_node = glm_ctx->hdfs_scan_node;
        hdfs_scan_node.tuple_id = _ctx->request_tuple_id;

        auto provider = std::make_unique<connector::HiveDataSourceProvider>(nullptr, _ctx->scan_id, hdfs_scan_node);
        const auto& scan_range = glm_ctx->get_hdfs_scan_range(scan_range_id);
        auto data_source = std::make_shared<connector::HiveDataSource>(provider.get(), scan_range);
        data_source->set_runtime_profile(_ctx->profile);
        data_source->set_row_id_ranges(&native_range);

        RETURN_IF_ERROR(data_source->open(state));
        do {
            ChunkPtr chunk = std::make_shared<Chunk>();
            auto status = data_source->get_next(state, &chunk);
            if (status.is_end_of_file()) {
                break;
            }
            RETURN_IF_ERROR(status);
            if (chunk->num_rows() == 0) {
                // An empty chunk is not end-of-stream for a restricted (sparse) read: a page/range
                // can yield no surviving rows while later ranges still have data. Keep reading until
                // EOF, otherwise we stop short and return fewer rows than requested positions.
                continue;
            }

            // Accumulate data from multiple chunks, excluding row_id columns
            if (result_chunk == nullptr) {
                result_chunk = std::make_shared<Chunk>();
                for (const auto& [slot_id, idx] : chunk->get_slot_id_to_index_map()) {
                    if (slot_id == _ctx->lookup_ref_slot_ids[0] || slot_id == _ctx->lookup_ref_slot_ids[1]) {
                        continue;
                    }
                    auto src_col = chunk->get_column_by_index(idx);
                    result_chunk->append_column(std::move(src_col), slot_id);
                }
            } else {
                for (const auto& [slot_id, idx] : chunk->get_slot_id_to_index_map()) {
                    if (slot_id == _ctx->lookup_ref_slot_ids[0] || slot_id == _ctx->lookup_ref_slot_ids[1]) {
                        continue;
                    }
                    auto src_col = chunk->get_column_by_index(idx);
                    auto dst_col = result_chunk->get_column_by_slot_id(slot_id)->as_mutable_raw_ptr();
                    dst_col->append(*src_col, 0, chunk->num_rows());
                }
            }
        } while (true);
        data_source->close(state);
    }
    return result_chunk;
}

// Executes the Iceberg lookup: build row-id ranges, fetch data natively by position, reorder to
// the original request layout, and feed responses back to each waiting context.
Status IcebergLookUpTask::process(RuntimeState* state, const ChunkPtr& request_chunk) {
    if (_ctx->request_ctxs.empty()) {
        return Status::OK();
    }

    phmap::flat_hash_map<int32_t, std::shared_ptr<SparseRange<int64_t>>> row_id_ranges;
    Buffer<uint32_t> replicated_offsets;
    ASSIGN_OR_RETURN(auto sorted_chunk,
                     _calculate_row_id_range(state, request_chunk, &row_id_ranges, &replicated_offsets));
    ASSIGN_OR_RETURN(auto result_chunk, _get_data_from_storage(state, row_id_ranges));

    if (result_chunk == nullptr) {
        // A non-empty request must fetch at least one row; returning OK here would leave
        // response_columns empty and crash FetchProcessor::_build_output_chunk. Fail soft instead.
        return Status::InternalError("Iceberg lookup fetched no rows for a non-empty request");
    }

    {
        auto unordered_position_column = sorted_chunk->get_column_by_slot_id(Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
        if (!replicated_offsets.empty()) {
            // Replicate data for duplicate row_ids
            for (const auto& [slot_id, _] : result_chunk->get_slot_id_to_index_map()) {
                auto old_column = result_chunk->get_column_by_slot_id(slot_id)->as_mutable_raw_ptr();
                ASSIGN_OR_RETURN(auto new_column, old_column->replicate(replicated_offsets));
                result_chunk->append_or_update_column(std::move(new_column), slot_id);
            }
            result_chunk->check_or_die();
        }
        // The fetched payload must have exactly one row per requested position; otherwise the
        // position-column pairing below is misaligned. check_or_die() is a no-op in release builds,
        // so guard explicitly to fail soft instead of overrunning the permutation in _sort_chunk.
        if (result_chunk->num_rows() != unordered_position_column->size()) {
            return Status::InternalError(fmt::format("Iceberg lookup fetched {} rows but {} positions were requested",
                                                     result_chunk->num_rows(), unordered_position_column->size()));
        }
        result_chunk->append_column(unordered_position_column, Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
        result_chunk->check_or_die();
        ASSIGN_OR_RETURN(auto sorted_result_chunk, _sort_chunk(state, result_chunk, {unordered_position_column}));
        result_chunk = sorted_result_chunk;
    }

    auto tuple_desc = state->desc_tbl().get_tuple_descriptor(_ctx->request_tuple_id);
    std::vector<SlotDescriptor*> slots;
    for (const auto& slot : tuple_desc->slots()) {
        if (slot->id() == _ctx->lookup_ref_slot_ids[0] || slot->id() == _ctx->lookup_ref_slot_ids[1]) {
            continue;
        }
        slots.emplace_back(slot);
    }
    {
        SCOPED_TIMER(_ctx->parent->_fill_response_timer);
        size_t start_offset = 0;
        for (const auto& request_ctx : _ctx->request_ctxs) {
            ASSIGN_OR_RETURN(auto num_rows, request_ctx->fill_response(result_chunk, slots, start_offset));
            start_offset += num_rows;
        }
    }

    return Status::OK();
}

Status NativeLookUpTask::process(RuntimeState* state, const ChunkPtr& request_chunk) {
    FAIL_POINT_TRIGGER_RETURN_ERROR(lookup_request_failed);

    // For native lookup, the request chunk is already in the expected order and contains all necessary columns,
    // so we can directly fill the response without additional processing.
    if (_ctx->request_ctxs.empty()) {
        return Status::OK();
    }

    SmallPermutation permutation;
    Buffer<uint32_t> replicated;

    Columns columns;

    for (size_t i = 0; i < _ctx->fetch_ref_slot_ids.size(); i++) {
        columns.push_back(request_chunk->get_column_by_slot_id(_ctx->fetch_ref_slot_ids[i]));
    }

    ASSIGN_OR_RETURN(auto locators, _build_row_id_range(state, columns, &permutation, &replicated));

    auto tuple_desc = state->desc_tbl().get_tuple_descriptor(_ctx->request_tuple_id);

    std::vector<SlotDescriptor*> query_slots;

    // skip all request column (they have existed in request chunk)
    auto should_keep = [&](SlotDescriptor* slot) {
        int id = slot->id();
        return !std::ranges::any_of(_ctx->lookup_ref_slot_ids, [&](int ref_id) { return id == ref_id; });
    };

    const auto& slots = tuple_desc->slots();

    for (auto* slot : slots | std::views::filter(should_keep)) {
        query_slots.push_back(slot);
    }

    ASSIGN_OR_RETURN(auto late_materialized_chunk,
                     _late_materialize_by_row_locators(state, query_slots, locators, request_chunk));

    if (!replicated.empty()) {
        auto chunk = std::make_shared<Chunk>();
        for (const auto& [slot_id, _] : late_materialized_chunk->get_slot_id_to_index_map()) {
            auto old_column = late_materialized_chunk->get_column_by_slot_id(slot_id)->as_mutable_raw_ptr();
            ASSIGN_OR_RETURN(auto new_column, old_column->replicate(replicated));
            chunk->append_column(std::move(new_column), slot_id);
        }
        late_materialized_chunk = std::move(chunk);
    }
    {
        size_t num_rows = late_materialized_chunk->num_rows();
        auto sorted_chunk = late_materialized_chunk->clone_empty_with_slot(num_rows);
        materialize_by_permutation_single(sorted_chunk.get(), late_materialized_chunk, permutation);
        late_materialized_chunk = std::move(sorted_chunk);
    }

    {
        size_t start_offset = 0;
        for (const auto& request_ctx : _ctx->request_ctxs) {
            ASSIGN_OR_RETURN(auto num_rows,
                             request_ctx->fill_response(late_materialized_chunk, query_slots, start_offset));
            start_offset += num_rows;
        }
    }

    return Status::OK();
}

// Generate the inverse of a given permutation.
SmallPermutation transform(const SmallPermutation& origin) {
    SmallPermutation result;
    result.resize(origin.size());

    for (size_t i = 0; i < origin.size(); ++i) {
        result[origin[i].index_in_chunk].index_in_chunk = i;
    }

    return result;
}

auto NativeLookUpTask::_build_row_id_range(RuntimeState* state, const Columns& row_locator, SmallPermutation* output,
                                           Buffer<uint32_t>* replicated) -> StatusOr<RowLocators> {
    DCHECK_EQ(row_locator[0]->size(), row_locator[1]->size());
    DCHECK_EQ(row_locator[0]->size(), row_locator[2]->size());

    for (const auto& col : row_locator) {
        DCHECK(!col->is_constant()) << "row locator columns should not be constant";
        DCHECK(!col->is_nullable() || !col->has_null());
    }

    ASSIGN_OR_RETURN(auto permutation, _sort_columns(state, row_locator));

    auto chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < row_locator.size(); ++i) {
        chunk->append_column(row_locator[i], i);
    }

    auto sorted_chunk = chunk->clone_empty_with_slot(chunk->num_rows());

    materialize_by_permutation_single(sorted_chunk.get(), chunk, permutation);

    {
        // right now we only have semantics a[i] = b[c[i]] (in materialize_by_permutation_single). so we need to transform permutation.
        // if we implement semantics a[c[i]] = b[i], then we can directly use the permutation from sort_and_tie_columns without transformation.
        *output = transform(permutation);
    }

    size_t num_rows = sorted_chunk->num_rows();
    const auto& sorted_columns = sorted_chunk->columns();

    const auto& tablet_id_column = ColumnHelper::get_data_column_by_type<TYPE_BIGINT>(sorted_columns[0]);
    const auto& rss_id_column = ColumnHelper::get_data_column_by_type<TYPE_INT>(sorted_columns[1]);
    const auto& row_id_column = ColumnHelper::get_data_column_by_type<TYPE_BIGINT>(sorted_columns[2]);

    const auto tablet_ids = tablet_id_column->immutable_data();
    const auto rss_ids = rss_id_column->immutable_data();
    const auto row_ids = row_id_column->immutable_data();

    std::vector<RowLocatorTuple> locators;

    if (num_rows == 0) {
        return Status::OK();
    }

    int64_t cur_tablet = tablet_ids[0];
    uint32_t cur_rss_id = rss_ids[0];

    SparseRange<rowid_t> ranges;
    Range<rowid_t> cur_range(row_ids[0], row_ids[0] + 1);

    replicated->push_back(0);
    uint32_t current_group_size = 1;

    for (size_t i = 1; i < num_rows; ++i) {
        int64_t tablet = tablet_ids[i];
        uint32_t rssid = rss_ids[i];
        rowid_t rowid = row_ids[i];

        bool same_locator = tablet == tablet_ids[i - 1] && rssid == rss_ids[i - 1] && rowid == row_ids[i - 1];

        bool same_segment = tablet == cur_tablet && rssid == cur_rss_id;

        if (same_locator) {
            current_group_size++;
        } else {
            replicated->push_back(replicated->back() + current_group_size);
            current_group_size = 1;
        }

        // not the same segment file
        if (!same_segment) {
            ranges.add(cur_range);
            locators.emplace_back(cur_tablet, cur_rss_id, std::move(ranges));

            // reset
            ranges = SparseRange<rowid_t>();
            cur_range = Range<rowid_t>(rowid, rowid + 1);

            cur_tablet = tablet;
            cur_rss_id = rssid;

            continue;
        }

        // rowid continuous -> expand current range
        if (rowid == cur_range.end()) {
            cur_range.expand(1);
        }

        // new rowid -> new range
        else if (rowid != cur_range.end() - 1) {
            ranges.add(cur_range);
            cur_range = Range<rowid_t>(rowid, rowid + 1);
        }
    }

    // flush the last range
    ranges.add(cur_range);
    locators.emplace_back(cur_tablet, cur_rss_id, std::move(ranges));
    replicated->push_back(replicated->back() + current_group_size);

    return locators;
}

StatusOr<SmallPermutation> NativeLookUpTask::_sort_columns(RuntimeState* state, const Columns& columns) {
    SortDescs sort_descs;
    sort_descs.descs.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); i++) {
        sort_descs.descs.emplace_back(true, true);
    }
    SmallPermutation permutation = create_small_permutation(columns[0]->size());
    RETURN_IF_ERROR(sort_and_tie_columns(state->cancelled_ref(), columns, sort_descs, permutation));
    return permutation;
}

auto NativeLookUpTask::_late_materialize_by_row_locators(RuntimeState* state, const std::vector<SlotDescriptor*>& slots,
                                                         const RowLocators& row_locators, const ChunkPtr& chunk)
        -> StatusOr<ChunkPtr> {
    ChunkAccumulator accumulator(chunk->num_rows());

    // acquire global late materialization context
    auto scan_id = _ctx->scan_id;
    auto glm_ctx = state->query_runtime_state()->global_late_materialization_ctx_mgr()->get_ctx(_ctx->scan_id);
    if (glm_ctx == nullptr) {
        return Status::InternalError("GlobalLateMaterilizationContext not found for scan_id: " +
                                     std::to_string(scan_id));
    }

    ObjectPool obj_pool;

    for (const auto& row_locator : row_locators) {
        auto [tablet_id, rssid, row_id_ranges] = row_locator;
        RETURN_IF_ERROR(_tablet_adaptor->capture(glm_ctx));

        RETURN_IF_ERROR(_tablet_adaptor->init(tablet_id));

        RETURN_IF_ERROR(_tablet_adaptor->init_schema(state));

        // init access path
        RETURN_IF_ERROR(_tablet_adaptor->init_access_path(state, &obj_pool));
        // init global dicts
        RETURN_IF_ERROR(_tablet_adaptor->init_global_dicts(state, &obj_pool, slots));
        RETURN_IF_ERROR(_tablet_adaptor->init_read_columns(slots));

        // Do read
        ASSIGN_OR_RETURN(auto iterator, _tablet_adaptor->get_iterator(rssid, row_id_ranges));

        // init chunk iterator
        do {
            ChunkPtr chunk(ChunkFactory::new_chunk_pooled(iterator->output_schema(), row_id_ranges.span_size()));
            auto status = iterator->get_next(chunk.get());
            if (status.is_end_of_file()) {
                break;
            }
            RETURN_IF_ERROR(status);
            RETURN_IF_ERROR(accumulator.push(std::move(chunk)));
        } while (true);
    }

    accumulator.finalize();
    auto accumulated = accumulator.pull();

    for (const auto& slot : slots) {
        size_t column_index = accumulated->schema()->get_field_index_by_name(slot->col_name());
        accumulated->set_slot_id_to_index(slot->id(), column_index);
        auto column = accumulated->get_column_by_slot_id(slot->id());
        if (slot->is_nullable() && !column->is_nullable()) {
            auto nullable_column = NullableColumn::wrap_if_necessary(std::move(column));
            accumulated->update_column(std::move(nullable_column), slot->id());
        }
    }

    return accumulated;
}

} // namespace starrocks::pipeline
