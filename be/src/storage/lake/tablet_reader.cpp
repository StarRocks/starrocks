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

#include "storage/lake/tablet_reader.h"

#include <algorithm>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <tuple>
#include <unordered_set>
#include <utility>

#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/datum_convert.h"
#include "column/flat_json/json_flat_path.h"
#include "common/config_ingest_fwd.h"
#include "common/config_json_flat_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/config_staros_worker_fwd.h"
#include "common/status.h"
#include "common/thread/threadpool.h"
#include "exec_primitive/pipeline/scan/scan_morsel.h"
#include "fs/fs_factory.h"
#include "fs/remote_read_context.h"
#include "gutil/stl_util.h"
#include "runtime/runtime_env.h"
#include "runtime/runtime_state.h"
#include "runtime/type_info_allocator_adapter.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/flat_json_metrics.h"
#include "storage/json_path_deriver.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/rowset.h"
#include "storage/lake/segment_metadata_filter.h"
#include "storage/lake/utils.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/predicate_parser.h"
#include "storage/query/range_group_task.h"
#include "storage/query/split_morsel_queue.h"
#include "storage/query/split_scan_morsel.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/seek_range.h"
#include "storage/tablet_schema_map.h"
#include "storage/types.h"
#include "storage_primitive/aggregate_iterator.h"
#include "storage_primitive/conjunctive_predicates.h"
#include "storage_primitive/empty_iterator.h"
#include "storage_primitive/merge_iterator.h"
#include "storage_primitive/row_source_mask_buffer.h"
#include "storage_primitive/schema_helper.h"
#include "storage_primitive/union_iterator.h"

namespace starrocks::lake {

// The open loops below can run for minutes on a tablet with many segments, so they stop as soon as the
// query is cancelled instead of opening the rest. A read without a RuntimeState (compaction, publish) is
// never cancelled here.
static Status check_cancelled(const TabletReaderParams& params, int64_t tablet_id) {
    if (params.runtime_state != nullptr && params.runtime_state->is_cancelled()) {
        return Status::Cancelled(fmt::format("cancelled while opening tablet:{}", tablet_id));
    }
    return Status::OK();
}

static bool cancel_requested(const std::atomic<bool>* flag) {
    return flag != nullptr && flag->load(std::memory_order_acquire);
}

// lake_scan_parallel_segment_open applies to the query scans of a tablet while the remote I/O scheduler is on.
// Loads, compaction and the other readers keep following enable_load_segment_parallel.
static bool should_open_segments_in_parallel(ReaderType reader_type) {
    return config::lake_scan_parallel_segment_open && reader_type == ReaderType::READER_QUERY &&
           remote_io_scheduler_enabled();
}

// Runs every task on the load segment thread pool, or on this thread when the pool refuses it, and waits for all of
// them, also when it returns early. The tasks read on behalf of the query of |query_remote_read|, or of the scan
// running on this thread when it is not set: the pool threads do not see this thread's scoped context. A task still
// queued once |is_cancelled| is set does not start its IO. Returns the first error in task order.
static Status run_on_load_segment_pool(std::vector<std::function<Status()>>& tasks,
                                       const RemoteReadContext& query_remote_read,
                                       const std::atomic<bool>* is_cancelled) {
    const RemoteReadContext remote_read = ScopedRemoteReadContext::or_current(query_remote_read);
    std::vector<std::future<Status>> futures;
    futures.reserve(tasks.size());
    // The tasks refer to |tasks| and |remote_read|: none may outlive this call.
    DeferOp wait_futures([&futures]() {
        for (auto& future : futures) {
            if (future.valid()) {
                future.wait();
            }
        }
    });
    for (auto& task : tasks) {
        auto packaged = std::make_shared<std::packaged_task<Status()>>([&task, &remote_read, is_cancelled]() {
            if (cancel_requested(is_cancelled)) {
                return Status::Cancelled("cancelled while opening segments");
            }
            ScopedRemoteReadContext remote_read_scope(remote_read);
            return task();
        });
        futures.emplace_back(packaged->get_future());
        Status submit_st;
        TEST_SYNC_POINT_CALLBACK("TabletReader::open_segments_in_parallel::submit", &submit_st);
        if (submit_st.ok()) {
            submit_st =
                    RuntimeEnv::GetInstance()->load_segment_thread_pool()->submit_func([packaged]() { (*packaged)(); });
        }
        if (!submit_st.ok()) {
            (*packaged)();
        }
    }
    Status first_error;
    for (auto& future : futures) {
        auto st = future.get();
        if (first_error.ok() && !st.ok()) {
            first_error = std::move(st);
        }
    }
    return first_error;
}

// Adds the fields the tablet preparation reports to |to|.
static void add_tablet_prepare_io_stats(const OlapReaderStatistics& from, OlapReaderStatistics* to) {
    to->segments_opened += from.segments_opened;
    to->io_ns += from.io_ns;
    to->io_count += from.io_count;
    to->io_ns_remote += from.io_ns_remote;
    to->io_count_remote += from.io_count_remote;
    to->compressed_bytes_read_remote += from.compressed_bytes_read_remote;
}

// The predicate tree of the segment metadata filter that Rowset::read applies with |options|, or nullptr when it
// applies none. Rowset::read skips a segment this filter excludes before loading it.
static const PredicateTree* metadata_prune_tree(const RowsetReadOptions& options) {
    if (!config::enable_lake_segment_metadata_filter || options.pred_tree_for_zone_map.empty() ||
        options.tablet_schema == nullptr) {
        return nullptr;
    }
    return &options.pred_tree_for_zone_map;
}

// lake_scan_parallel_segment_open: opens the segments of all |rowsets| at once, one task per segment, and puts
// them in metadata order whatever order the tasks finish in. A segment whose footer is already open in the metadata
// cache is taken as it is, without a task. If |cached_rowsets| is not null, it tells per rowset whether every segment
// was taken that way, as found before any task ran.
// With a non-null |prune_tree|, a segment SegmentMetadataFilter excludes by |prune_tree| is neither looked up nor
// opened: its slot stays nullptr, which Rowset::read skips, and it does not count against |cached_rowsets|.
static Status open_rowset_segments_in_parallel(const std::vector<RowsetPtr>& rowsets, const LakeIOOptions& lake_io_opts,
                                               const std::atomic<bool>* is_cancelled,
                                               OlapReaderStatistics* prepare_stats,
                                               std::vector<std::vector<SegmentPtr>>* rowset_segments,
                                               std::vector<bool>* cached_rowsets = nullptr,
                                               const PredicateTree* prune_tree = nullptr,
                                               const TabletSchema* tablet_schema = nullptr) {
    rowset_segments->resize(rowsets.size());
    if (cached_rowsets != nullptr) {
        cached_rowsets->assign(rowsets.size(), false);
    }
    std::vector<std::function<Status()>> tasks;
    std::vector<std::unique_ptr<OlapReaderStatistics>> task_stats;
    for (size_t rowset_idx = 0; rowset_idx < rowsets.size(); ++rowset_idx) {
        const RowsetPtr& rowset = rowsets[rowset_idx];
        auto& segments = (*rowset_segments)[rowset_idx];
        if (lake_io_opts.hold_segments || !rowset->can_hold_segments()) {
            // Such a rowset loads its segments by its own rules: open it in one task.
            auto* stats = task_stats.emplace_back(std::make_unique<OlapReaderStatistics>()).get();
            tasks.emplace_back([rowset, segments = &segments, &lake_io_opts, stats, is_cancelled]() -> Status {
                ASSIGN_OR_RETURN(*segments, rowset->segments(lake_io_opts, stats, is_cancelled));
                return Status::OK();
            });
            continue;
        }
        segments.resize(rowset->metadata().segment_metas_size());
        // Slots are metadata positions here, the positions whose segments Rowset::read filters in a full set.
        const bool prune = prune_tree != nullptr && tablet_schema != nullptr &&
                           rowset->num_segments() == rowset->metadata().segment_metas_size();
        bool all_cached = true;
        for (int32_t pos = 0; pos < static_cast<int32_t>(segments.size()); ++pos) {
            if (prune && !SegmentMetadataFilter::may_contain(rowset->metadata().segment_metas(pos), *prune_tree,
                                                             *tablet_schema)) {
                prepare_stats->lake_prepared_tablet_warm_index_pruned_segments++;
                continue;
            }
            // Keeps the opened cache entry through iterator construction, even if the LRU evicts it.
            // An entry whose open has not finished goes through a task, which waits for that open.
            segments[pos] = rowset->lookup_opened_segment_at(pos, lake_io_opts);
            if (segments[pos] != nullptr) {
                prepare_stats->lake_prepared_tablet_cached_segments++;
                continue;
            }
            all_cached = false;
            auto* stats = task_stats.emplace_back(std::make_unique<OlapReaderStatistics>()).get();
            tasks.emplace_back([rowset, segment = &segments[pos], &lake_io_opts, stats, pos]() -> Status {
#ifdef BE_TEST
                Status injected_st;
                TEST_SYNC_POINT_CALLBACK("TabletReader::open_segment_task", &injected_st);
                RETURN_IF_ERROR(injected_st);
#endif
                ASSIGN_OR_RETURN(*segment, rowset->load_segment_at(pos, lake_io_opts, stats));
                return Status::OK();
            });
        }
        if (cached_rowsets != nullptr) {
            (*cached_rowsets)[rowset_idx] = all_cached;
        }
    }
    auto st = run_on_load_segment_pool(tasks, lake_io_opts.remote_read, is_cancelled);
    // Every task that ran may have read a footer, including the ones after the first error.
    for (const auto& stats : task_stats) {
        add_tablet_prepare_io_stats(*stats, prepare_stats);
    }
    return st;
}

using ConjunctivePredicates = starrocks::ConjunctivePredicates;
using Datum = starrocks::Datum;
using Field = starrocks::Field;
using PredicateParser = starrocks::PredicateParser;
using ZonemapPredicatesRewriter = starrocks::ZonemapPredicatesRewriter;

static Schema build_prepare_pruning_schema(const RowsetPtr& rowset, const Schema& read_schema,
                                           const RowsetReadOptions& rowset_read_options,
                                           const DisjunctivePredicates& delete_predicates) {
    Schema prepare_schema = rowset->build_segment_schema(read_schema, rowset_read_options, delete_predicates);
    if (rowset_read_options.tablet_schema == nullptr) {
        return prepare_schema;
    }

    for (ColumnId cid : rowset_read_options.pred_tree_for_zone_map.column_ids()) {
        const TabletColumn& col = rowset_read_options.tablet_schema->column(cid);
        if (prepare_schema.get_field_by_name(std::string(col.name())) != nullptr) {
            continue;
        }
        prepare_schema.append(std::make_shared<Field>(StorageSchemaHelper::convert_field(cid, col)));
    }
    return prepare_schema;
}

static Status prepare_segment_pruned_scan_range(const SegmentPtr& segment, const Schema& schema,
                                                const SegmentReadOptions& segment_read_options,
                                                const PreparedSegmentReadStatePtr& prepared_segment_read_state) {
    if (segment == nullptr || prepared_segment_read_state == nullptr) {
        return Status::InvalidArgument("invalid prepared segment read state");
    }

    auto range_or = get_prepared_pruned_row_ranges(segment, schema, segment_read_options);
    if (!range_or.ok()) {
        return range_or.status();
    }
    prepared_segment_read_state->publish_pruned_scan_range(
            std::make_shared<SparseRange<>>(std::move(range_or).value()));
    return Status::OK();
}

static Status prepare_segment_pruned_scan_range_for_split(
        const RowsetPtr& rowset, size_t segment_idx, const SegmentPtr& segment,
        const PreparedSegmentReadStatePtr& segment_state, const Schema& read_schema,
        const RowsetReadOptions& rowset_read_options, const LakeIOOptions& lake_io_opts,
        const std::optional<SeekRange>& shared_segment_range, OlapReaderStatistics* prepare_stats) {
    const auto delete_predicates = rowset_read_options.delete_predicates != nullptr
                                           ? rowset_read_options.delete_predicates->get_predicates(rowset->index())
                                           : DisjunctivePredicates{};
    SegmentReadOptions segment_read_options;
    RETURN_IF_ERROR(rowset->init_segment_read_options(rowset_read_options, lake_io_opts, delete_predicates,
                                                      prepare_stats, &segment_read_options));
    RETURN_IF_ERROR(rowset->set_segment_tablet_range(segment_idx, shared_segment_range, &segment_read_options));
    std::vector<SeekRange> ranges_to_resolve = segment_read_options.ranges;
    std::optional<size_t> tablet_range_offset;
    if (segment_read_options.tablet_range.has_value() && !segment_read_options.tablet_range->all_range()) {
        tablet_range_offset = ranges_to_resolve.size();
        ranges_to_resolve.emplace_back(segment_read_options.tablet_range.value());
    }
    ASSIGN_OR_RETURN(auto resolved_ranges,
                     segment_seek_ranges_to_rowid_ranges(segment, ranges_to_resolve, segment_read_options.lake_io_opts,
                                                         prepare_stats));
    std::vector<std::optional<Range<rowid_t>>> seek_ranges_rowid_bounds(
            resolved_ranges.begin(), resolved_ranges.begin() + segment_read_options.ranges.size());
    std::optional<Range<rowid_t>> tablet_range_rowid_bounds;
    if (tablet_range_offset.has_value()) {
        tablet_range_rowid_bounds = resolved_ranges[*tablet_range_offset];
    }
    segment_state->publish_rowid_bounds_cache(std::move(seek_ranges_rowid_bounds), tablet_range_rowid_bounds);
    segment_read_options.read_state_cache.seek_range_rowid_ranges = &segment_state->seek_ranges_rowid_bounds;
    segment_read_options.read_state_cache.tablet_rowid_range = &segment_state->tablet_range_rowid_bounds;
    auto prepare_schema = build_prepare_pruning_schema(rowset, read_schema, rowset_read_options, delete_predicates);
    return prepare_segment_pruned_scan_range(segment, prepare_schema, segment_read_options, segment_state);
}

static void append_refined_segment_split_tasks(const RowsetPtr& rowset, const SegmentPtr& segment,
                                               const PreparedTabletReadStatePtr& prepared_tablet_read_state,
                                               const PreparedSegmentReadStatePtr& segment_state, size_t rowset_idx,
                                               size_t segment_idx, const SparseRange<>& refined_scan_range,
                                               rowid_t rows_per_split, bool is_first_split_of_segment,
                                               std::vector<pipeline::ScanSplitContextPtr>* split_tasks) {
    if (refined_scan_range.span_size() == 0) {
        return;
    }

    auto range_iter = refined_scan_range.new_iterator();
    while (range_iter.has_more()) {
        SparseRange<> split_range;
        range_iter.next_range(rows_per_split, &split_range);
        if (split_range.span_size() == 0) {
            break;
        }

        auto rowid_range = std::make_shared<RowidRangeOption>();
        rowid_range->add(rowset.get(), segment.get(), std::make_shared<SparseRange<>>(std::move(split_range)),
                         is_first_split_of_segment);
        is_first_split_of_segment = false;

        auto ctx = std::make_unique<pipeline::LakeSplitContext>();
        ctx->rowid_range = std::move(rowid_range);
        ctx->rowid_range_source = pipeline::LakeSplitContext::RowidRangeSource::REFINED;
        ctx->prepared_tablet_read_state = prepared_tablet_read_state;
        ctx->prepared_segment_read_state = segment_state;
        ctx->rowset_index = rowset_idx;
        ctx->segment_index = segment_idx;
        split_tasks->emplace_back(std::move(ctx));
    }
}

static StatusOr<SparseRange<>> build_initial_coarse_scan_range(const SegmentPtr& segment,
                                                               const SegmentReadOptions& segment_read_options) {
    const bool need_short_key_index =
            !segment_read_options.ranges.empty() ||
            (segment_read_options.tablet_range.has_value() && !segment_read_options.tablet_range->all_range());
    if (need_short_key_index) {
        RETURN_IF_ERROR(segment->load_index(segment_read_options.lake_io_opts, segment_read_options.stats));
    }
    ASSIGN_OR_RETURN(auto scan_range,
                     block_aligned_rowid_range_from_seek_ranges(segment.get(), segment_read_options.ranges));
    if (segment_read_options.tablet_range.has_value() && !segment_read_options.tablet_range->all_range()) {
        ASSIGN_OR_RETURN(auto tablet_scan_range, block_aligned_rowid_range_from_seek_ranges(
                                                         segment.get(), {segment_read_options.tablet_range.value()}));
        scan_range &= tablet_scan_range;
    }
    return scan_range;
}

// Exposed (declared in tablet_reader.h) for unit testing; see subtract_sparse_ranges there.
SparseRange<> subtract_sparse_ranges(const SparseRange<>& lhs, const SparseRange<>& rhs) {
    SparseRange<> result;
    size_t rhs_idx = 0;
    for (size_t lhs_idx = 0; lhs_idx < lhs.size(); ++lhs_idx) {
        auto cur = lhs[lhs_idx];
        while (rhs_idx < rhs.size() && rhs[rhs_idx].end() <= cur.begin()) {
            ++rhs_idx;
        }
        size_t probe = rhs_idx;
        while (probe < rhs.size() && rhs[probe].begin() < cur.end()) {
            const auto& cut = rhs[probe];
            if (cut.begin() > cur.begin()) {
                result.add(Range<>(cur.begin(), std::min(cur.end(), cut.begin())));
            }
            if (cut.end() >= cur.end()) {
                cur = Range<>(cur.end(), cur.end());
                break;
            }
            cur = Range<>(std::max(cur.begin(), cut.end()), cur.end());
            ++probe;
        }
        if (!cur.empty()) {
            result.add(cur);
        }
    }
    return result;
}

static void init_coarse_split_allocation_state(const PreparedSegmentReadStatePtr& segment_state,
                                               const SparseRange<>& coarse_scan_range) {
    std::lock_guard<std::mutex> guard(segment_state->coarse_range_lock);
    segment_state->coarse_scan_range = coarse_scan_range;
    segment_state->coarse_scan_range_iter = segment_state->coarse_scan_range.new_iterator();
    segment_state->allocated_coarse_ranges.clear();
    segment_state->coarse_split_allocation_closed = false;
    segment_state->clear_pruned_scan_range();
    segment_state->clear_rowid_bounds_cache();
}

static bool allocate_initial_coarse_split(const PreparedSegmentReadStatePtr& segment_state, Rowset* rowset,
                                          Segment* segment, rowid_t rows_per_split, RowidRangeOptionPtr* rowid_range) {
    *rowid_range = nullptr;
    std::lock_guard<std::mutex> guard(segment_state->coarse_range_lock);
    if (segment_state->coarse_split_allocation_closed || !segment_state->coarse_scan_range_iter.has_more()) {
        return false;
    }

    auto result = std::make_shared<RowidRangeOption>();
    size_t num_taken_rows = 0;
    while (segment_state->coarse_scan_range_iter.has_more() && num_taken_rows < rows_per_split) {
        const size_t remaining_rows = segment_state->coarse_scan_range_iter.remaining_rows();
        size_t rows_to_take = std::min<size_t>(rows_per_split - num_taken_rows, remaining_rows);
        if (remaining_rows > rows_to_take && remaining_rows - rows_to_take < rows_per_split) {
            rows_to_take = remaining_rows;
        }

        SparseRange<> taken_range;
        segment_state->coarse_scan_range_iter.next_range(rows_to_take, &taken_range);
        if (taken_range.span_size() == 0) {
            break;
        }
        const bool is_first_split_of_segment = segment_state->allocated_coarse_ranges.span_size() == 0;
        segment_state->allocated_coarse_ranges |= taken_range;
        num_taken_rows += taken_range.span_size();
        result->add(rowset, segment, std::make_shared<SparseRange<>>(std::move(taken_range)),
                    is_first_split_of_segment);
    }

    if (result->rowid_range_per_segment_per_rowset.empty()) {
        return false;
    }
    *rowid_range = std::move(result);
    return true;
}

// lake_scan_warm_index: starts filling the local cache with the index pages the scan of |state| reads next, and
// returns without waiting. Per segment: the short key index, the zone map and bloom filter roots of |predicate_cids|
// and the ordinal index roots of |read_cids|. An index root that is not the sole data page of its index sits right
// after that index's other pages, so the block before it is warmed as well. The fills are scheduled as a demand; the
// reads that follow join them. Nothing here is required for correctness: a segment whose file cannot warm is read as
// before.
// With a non-null |prune_tree|, a segment that SegmentMetadataFilter excludes is not warmed: Rowset::read skips it by
// the same test before loading any segment. A rowset whose opened segments are not indexed by metadata position, or a
// segment without sort key bounds, is warmed as before.
static void warm_prepared_indexes(PreparedTabletReadState* state, const TabletSchema& tablet_schema,
                                  const std::vector<ColumnId>& predicate_cids, const std::vector<ColumnId>& read_cids,
                                  const PredicateTree* prune_tree, const LakeIOOptions& query_io_opts,
                                  OlapReaderStatistics* stats) {
    LakeIOOptions lake_io_opts = query_io_opts;
    lake_io_opts.remote_read = ScopedRemoteReadContext::or_current(query_io_opts.remote_read);
    const int64_t block_size = std::max<int64_t>(1, config::starlet_star_cache_block_size_bytes);
    for (size_t rowset_idx = 0; rowset_idx < state->rowset_segments.size(); ++rowset_idx) {
        const auto& segments = state->rowset_segments[rowset_idx];
        const RowsetMetadataPB* rowset_meta = nullptr;
        if (prune_tree != nullptr && rowset_idx < state->rowsets.size() && state->rowsets[rowset_idx] != nullptr &&
            static_cast<int64_t>(segments.size()) == state->rowsets[rowset_idx]->metadata().segment_metas_size() &&
            static_cast<int64_t>(segments.size()) == state->rowsets[rowset_idx]->num_segments()) {
            rowset_meta = &state->rowsets[rowset_idx]->metadata();
        }
        for (size_t pos = 0; pos < segments.size(); ++pos) {
            const auto& segment = segments[pos];
            if (segment == nullptr || segment->num_rows() == 0) {
                continue;
            }
            if (rowset_meta != nullptr &&
                !SegmentMetadataFilter::may_contain(rowset_meta->segment_metas(static_cast<int>(pos)), *prune_tree,
                                                    tablet_schema)) {
                stats->lake_prepared_tablet_warm_index_pruned_segments++;
                continue;
            }
            // Block-aligned [begin, end) ranges in physical file coordinates: a bundled segment starts at its
            // offset in the bundle file, and the cache blocks are aligned to the physical file.
            std::vector<std::pair<int64_t, int64_t>> blocks;
            const int64_t object_offset = segment->file_info().bundle_file_offset.value_or(0);
            auto add = [&](const PagePointer& pp, bool with_previous_block) {
                if (pp.size == 0) {
                    return;
                }
                const int64_t begin = object_offset + static_cast<int64_t>(pp.offset);
                int64_t first = begin / block_size * block_size;
                if (with_previous_block) {
                    first = std::max<int64_t>(object_offset / block_size * block_size, first - block_size);
                }
                const int64_t last = (begin + pp.size + block_size - 1) / block_size * block_size;
                blocks.emplace_back(first, last);
            };
            auto add_unloaded = [&](const PagePointer& pp, bool loaded, bool with_previous_block) {
                if (pp.size != 0 && loaded) {
                    stats->lake_prepared_tablet_warm_index_skipped++;
                } else {
                    add(pp, with_previous_block);
                }
            };
            add_unloaded(segment->short_key_index_page(), segment->has_loaded_index(), false);
            auto reader_of = [&](ColumnId cid) -> const ColumnReader* {
                if (cid >= tablet_schema.num_columns()) {
                    return nullptr;
                }
                return segment->column_with_uid(tablet_schema.column(cid).unique_id());
            };
            for (ColumnId cid : predicate_cids) {
                if (const auto* reader = reader_of(cid); reader != nullptr) {
                    const auto& roots = reader->index_root_pages();
                    add_unloaded(roots.zone_map, reader->has_loaded_zone_map_index(), !roots.zone_map_root_is_data);
                    // A loaded BloomFilterIndexReader keeps its ordinal index, not its filter data pages, so the
                    // root and the block before it are warmed whether it is loaded or not.
                    add(roots.bloom_filter, !roots.bloom_filter_root_is_data);
                }
            }
            for (ColumnId cid : read_cids) {
                if (const auto* reader = reader_of(cid); reader != nullptr) {
                    const auto& roots = reader->index_root_pages();
                    // A one-page column synthesizes its ordinal index from the footer without reading the root,
                    // which is its only data page: its loaded flag says nothing about that page, so warm it.
                    add_unloaded(roots.ordinal, !roots.ordinal_root_is_data && reader->has_loaded_ordinal_index(),
                                 false);
                }
            }
            if (blocks.empty()) {
                continue;
            }
            std::sort(blocks.begin(), blocks.end());
            std::vector<std::pair<int64_t, int64_t>> merged;
            for (const auto& b : blocks) {
                if (!merged.empty() && b.first <= merged.back().second) {
                    merged.back().second = std::max(merged.back().second, b.second);
                } else {
                    merged.push_back(b);
                }
            }
            // The segment's own read file applies the bundle offset and the encryption of the segment, so the
            // ranges below are passed relative to the segment. A fill does not need the file once started, so the
            // file is closed when this segment is done. It is not opened retainable: the index blocks go to the
            // cache, and are never kept in memory for the handles.
            auto file_or = segment->new_segment_read_file(lake_io_opts);
            if (!file_or.ok()) {
                VLOG(2) << "skip warming the indexes of " << segment->file_name() << ": " << file_or.status();
                continue;
            }
            std::unique_ptr<RandomAccessFile> file = std::move(file_or).value();
            const int64_t file_end = object_offset + static_cast<int64_t>(segment->file_info().size.value_or(
                                                             std::numeric_limits<int64_t>::max() - object_offset));
            bool warmed = false;
            for (const auto& [begin, end] : merged) {
                const int64_t from = std::max(begin, object_offset);
                const int64_t to = std::min(end, file_end);
                if (to <= from) {
                    continue;
                }
                const int64_t offset = from - object_offset;
                const auto length = static_cast<size_t>(to - from);
                auto handle_or = file->warm_cache_async(offset, length, true /* urgent */);
#ifdef BE_TEST
                // Tests see the segment-relative range and may replace the result.
                std::tuple<int64_t, size_t, StatusOr<std::shared_ptr<io::CacheWarmHandle>>*> warm_call{offset, length,
                                                                                                       &handle_or};
                TEST_SYNC_POINT_CALLBACK("TabletReader::warm_prepared_indexes::warm", &warm_call);
#endif
                if (!handle_or.ok()) {
                    // A stream that cannot warm at all will not start to: stop asking for this segment.
                    VLOG(2) << "stop warming the indexes of " << segment->file_name() << ": " << handle_or.status();
                    break;
                }
                stats->lake_prepared_tablet_warm_index_ranges++;
                stats->lake_prepared_tablet_warm_index_blocks += static_cast<int64_t>((*handle_or)->submitted_blocks());
                state->index_warm_handles.emplace_back(std::move(handle_or).value());
                warmed = true;
            }
            if (warmed) {
                stats->lake_prepared_tablet_warm_index_segments++;
            }
        }
    }
}

// lake_scan_warm_index applies to the query scans of a tablet, split or not, while the remote I/O scheduler is on.
// It does not depend on lake_scan_parallel_segment_open.
static bool should_warm_index_pages(ReaderType reader_type) {
    return config::lake_scan_warm_index && reader_type == ReaderType::READER_QUERY && remote_io_scheduler_enabled();
}

static rowid_t rows_per_split(const TabletReaderParams& params) {
    return params.splitted_scan_rows > 0 ? static_cast<rowid_t>(std::min<int64_t>(params.splitted_scan_rows,
                                                                                  std::numeric_limits<rowid_t>::max()))
                                         : std::numeric_limits<rowid_t>::max();
}

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema)
        : ChunkIterator(std::move(schema)), _tablet_mgr(tablet_mgr), _tablet_metadata(std::move(metadata)) {}

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                           bool need_split, bool could_split_physically)
        : ChunkIterator(std::move(schema)),
          _tablet_mgr(tablet_mgr),
          _tablet_metadata(std::move(metadata)),
          _need_split(need_split),
          _could_split_physically(could_split_physically) {}

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                           bool need_split, bool could_split_physically, std::vector<RowsetPtr> rowsets)
        : ChunkIterator(std::move(schema)),
          _tablet_mgr(tablet_mgr),
          _tablet_metadata(std::move(metadata)),
          _need_split(need_split),
          _could_split_physically(could_split_physically) {
    if (!rowsets.empty()) {
        _rowsets_inited = true;
        _rowsets = std::move(rowsets);
    }
}

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                           std::vector<RowsetPtr> rowsets, std::shared_ptr<const TabletSchema> tablet_schema)
        : ChunkIterator(std::move(schema)),
          _tablet_mgr(tablet_mgr),
          _tablet_metadata(std::move(metadata)),
          _tablet_schema(std::move(tablet_schema)),
          _rowsets_inited(true),
          _rowsets(std::move(rowsets)) {}

TabletReader::TabletReader(TabletManager* tablet_mgr, std::shared_ptr<const TabletMetadataPB> metadata, Schema schema,
                           std::vector<RowsetPtr> rowsets, bool is_key, RowSourceMaskBuffer* mask_buffer,
                           std::shared_ptr<const TabletSchema> tablet_schema, RowSourceMaskBuffer* selection_buffer)
        : ChunkIterator(std::move(schema)),
          _tablet_mgr(tablet_mgr),
          _tablet_metadata(std::move(metadata)),
          _tablet_schema(std::move(tablet_schema)),
          _rowsets_inited(true),
          _rowsets(std::move(rowsets)),
          _is_vertical_merge(true),
          _is_key(is_key),
          _mask_buffer(mask_buffer),
          _selection_buffer(selection_buffer) {
    DCHECK(_mask_buffer);
}

TabletReader::~TabletReader() {
    close();
}

const OlapReaderStatistics& TabletReader::stats() const {
    if (_parallel_rowset_stats.empty()) {
        return _stats;
    }
    _combined_stats = _stats;
    for (const auto& rowset_stats : _parallel_rowset_stats) {
        _combined_stats.merge_from(*rowset_stats);
    }
    return _combined_stats;
}

const OlapReaderStatistics& TabletReader::compaction_stats() const {
    if (_parallel_rowset_stats.empty()) {
        return _stats;
    }
    // Compaction refreshes progress and task statistics for every chunk. Its collector reads only
    // these scalar fields, so avoid rebuilding the complete profile and JSON hit maps here.
    _compaction_stats = OlapReaderStatistics{};
    auto add_counters = [this](const OlapReaderStatistics& from) {
        _compaction_stats.create_segment_iter_ns += from.create_segment_iter_ns;
        _compaction_stats.decompress_ns += from.decompress_ns;
        _compaction_stats.block_load_ns += from.block_load_ns;
        _compaction_stats.block_fetch_ns += from.block_fetch_ns;
        _compaction_stats.block_seek_ns += from.block_seek_ns;
        _compaction_stats.block_seek_num += from.block_seek_num;
        _compaction_stats.decode_dict_ns += from.decode_dict_ns;
        _compaction_stats.get_rowsets_ns += from.get_rowsets_ns;
        _compaction_stats.get_delvec_ns += from.get_delvec_ns;
        _compaction_stats.get_delta_column_group_ns += from.get_delta_column_group_ns;
        _compaction_stats.del_filter_ns += from.del_filter_ns;
        _compaction_stats.blocks_load += from.blocks_load;
        _compaction_stats.raw_rows_read += from.raw_rows_read;
        _compaction_stats.compressed_bytes_read += from.compressed_bytes_read;
        _compaction_stats.uncompressed_bytes_read += from.uncompressed_bytes_read;
        _compaction_stats.io_ns_remote += from.io_ns_remote;
        _compaction_stats.io_ns_read_local_disk += from.io_ns_read_local_disk;
        _compaction_stats.compressed_bytes_read_remote += from.compressed_bytes_read_remote;
        _compaction_stats.compressed_bytes_read_local_disk += from.compressed_bytes_read_local_disk;
        _compaction_stats.segment_init_ns += from.segment_init_ns;
        _compaction_stats.column_iterator_init_ns += from.column_iterator_init_ns;
        _compaction_stats.io_count_local_disk += from.io_count_local_disk;
        _compaction_stats.io_count_remote += from.io_count_remote;
    };
    add_counters(_stats);
    for (const auto& rowset_stats : _parallel_rowset_stats) {
        add_counters(*rowset_stats);
    }
    return _compaction_stats;
}

TabletReader::RealtimeStats TabletReader::realtime_stats() const {
    RealtimeStats result{_stats.raw_rows_read, _stats.bytes_read, _stats.decompress_ns, _stats.vec_cond_ns,
                         _stats.del_filter_ns};
    for (const auto& rowset_stats : _parallel_rowset_stats) {
        result.raw_rows_read += rowset_stats->raw_rows_read;
        result.bytes_read += rowset_stats->bytes_read;
        result.decompress_ns += rowset_stats->decompress_ns;
        result.vec_cond_ns += rowset_stats->vec_cond_ns;
        result.del_filter_ns += rowset_stats->del_filter_ns;
    }
    return result;
}

void TabletReader::reset_stats() {
    _stats = OlapReaderStatistics{};
    // Reusable readers may still hold iterators with these pointers until the next open().
    for (auto& rowset_stats : _parallel_rowset_stats) {
        *rowset_stats = OlapReaderStatistics{};
    }
}

Status TabletReader::prepare() {
    if (_tablet_schema == nullptr) {
        _tablet_schema = GlobalTabletSchemaMap::Instance()->emplace(_tablet_metadata->schema()).first;
    }
    if (UNLIKELY(_tablet_schema == nullptr)) {
        return Status::InternalError("failed to construct tablet schema");
    }
    if (!_rowsets_inited) {
        _rowsets = Rowset::get_rowsets(_tablet_mgr, _tablet_metadata);
        _rowsets_inited = true;
    }
    return Status::OK();
}

Status TabletReader::open(const TabletReaderParams& read_params) {
    if (read_params.grouped_range_task != nullptr) {
        // Grouped roots may plan ordinary physical splits; children retain independent rowid and C clips.
        // Prepared/reusable state and logical ranges cannot prove the same ownership contract.
        if ((_need_split && (!_could_split_physically || read_params.rowid_range_option != nullptr)) ||
            read_params.enable_prepared_physical_split_scan || read_params.prepared_tablet_read_state != nullptr ||
            read_params.prepared_segment_read_state != nullptr ||
            read_params.refine_initial_coarse_split_and_append_refined_tasks ||
            read_params.short_key_ranges_option != nullptr) {
            return Status::NotSupported("grouped range task does not support prepared or logical split readers");
        }
        RETURN_IF_ERROR(validate_range_group_schema(*read_params.grouped_range_task, *_tablet_schema));
    }
    if (read_params.reader_type != ReaderType::READER_QUERY && read_params.reader_type != ReaderType::READER_CHECKSUM &&
        read_params.reader_type != ReaderType::READER_ALTER_TABLE && !is_compaction(read_params.reader_type) &&
        read_params.reader_type != ReaderType::READER_BYPASS_QUERY) {
        return Status::NotSupported("reader type not supported now");
    }
    RETURN_IF_ERROR(init_compaction_column_paths(read_params));
    // Only a reader over the whole tablet (unsplit, or the one planning the splits) counts its rowsets;
    // every split child carries a rowid or short-key range.
    if (read_params.rowid_range_option == nullptr && read_params.short_key_ranges_option == nullptr &&
        read_params.prepared_tablet_read_state == nullptr) {
        _stats.phy_rowsets_count += _rowsets.size();
    }
    if (_collect_iter != nullptr) {
        _collect_iter->close();
        _collect_iter.reset();
    }
    // Keep totals across reopens, including vertical compaction's column-group passes.
    // release_for_reuse() resets both the base and private statistics after reporting them.
    for (const auto& rowset_stats : _parallel_rowset_stats) {
        _stats.merge_from(*rowset_stats);
    }
    // The old iterators no longer refer to their task-local statistics.
    _parallel_rowset_stats.clear();
    _nosplit_warm_state.reset();

    if (_need_split) {
        std::vector<BaseTabletSharedPtr> tablets;
        auto tablet_shared_ptr = std::make_shared<Tablet>(_tablet_mgr, _tablet_metadata->id());
        // to avoid list tablet metadata by set version_hint
        tablet_shared_ptr->set_version_hint(_tablet_metadata->version());
        tablets.emplace_back(tablet_shared_ptr);

        std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets;
        tablet_rowsets.emplace_back();
        auto& rss = tablet_rowsets.back();
        int64_t tablet_num_rows = 0;
        for (auto& rowset : _rowsets) {
            tablet_num_rows += rowset->num_rows();
            rss.emplace_back(rowset);
        }

        // Do not split if tablet has no rowsets (empty tablet)
        if (_rowsets.empty()) {
            _need_split = false;
            return init_collector(read_params);
        }

        // not split for data skew between tablet
        if (tablet_num_rows < read_params.splitted_scan_rows * config::lake_tablet_rows_splitted_ratio) {
            // set _need_split false to make iterator can get data this round if split do not happen,
            // otherwise, iterator will return empty.
            _need_split = false;
            return init_collector(read_params);
        }

        RowidRangeOptionPtr grouped_candidates;
        if (read_params.grouped_range_task != nullptr) {
            // Validate and clip the same memoized segments consumed by physical split planning.
            // Exact rowids are needed when the short-key index omits a varying group-key suffix.
            grouped_candidates = std::make_shared<RowidRangeOption>();
            const std::atomic<bool>* is_cancelled =
                    read_params.runtime_state != nullptr ? &read_params.runtime_state->cancelled_ref() : nullptr;
            for (const auto& rowset : _rowsets) {
                RETURN_IF_ERROR(check_cancelled(read_params, _tablet_metadata->id()));
                RETURN_IF_ERROR(validate_range_group_schema(*read_params.grouped_range_task, *rowset->tablet_schema()));
                ASSIGN_OR_RETURN(auto segments,
                                 rowset->get_segments_checked(read_params.lake_io_opts, &_stats, is_cancelled));
                for (const auto& segment : segments) {
                    if (segment != nullptr) {
                        RETURN_IF_ERROR(
                                validate_range_group_schema(*read_params.grouped_range_task, segment->tablet_schema()));
                        RETURN_IF_ERROR(check_cancelled(read_params, _tablet_metadata->id()));
                        SparseRange<> candidates;
                        if (segment->num_rows() != 0) {
                            ASSIGN_OR_RETURN(auto seek, decode_range_group_seek(*read_params.grouped_range_task,
                                                                                segment->tablet_schema_share_ptr()));
                            ASSIGN_OR_RETURN(auto rowids, segment_seek_ranges_to_rowid_ranges(
                                                                  segment, {std::move(seek)}, read_params.lake_io_opts,
                                                                  &_stats, read_params.use_page_cache, is_cancelled));
                            RETURN_IF_ERROR(check_cancelled(read_params, _tablet_metadata->id()));
                            if (rowids.front().has_value()) candidates.add(*rowids.front());
                        }
                        grouped_candidates->add(rowset.get(), segment.get(),
                                                std::make_shared<SparseRange<>>(std::move(candidates)), true);
                    }
                }
            }
        }

        if (_could_split_physically && read_params.enable_prepared_physical_split_scan) {
            auto prepared_tablet_read_state = std::make_shared<PreparedTabletReadState>();
            OlapReaderStatistics prepare_stats;
            {
                // Keep reads already performed even if a later preparation step fails.
                DeferOp record_prepare_stats([&] { record_tablet_prepare_stats(prepare_stats); });
                SCOPED_RAW_TIMER(&_stats.lake_prepared_tablet_prepare_ns);
                RETURN_IF_ERROR(build_prepared_tablet_read_state(read_params, prepared_tablet_read_state.get(),
                                                                 &prepare_stats));
                RETURN_IF_ERROR(
                        build_initial_coarse_split_tasks(read_params, prepared_tablet_read_state, &prepare_stats));
            }
            if (_split_tasks.empty()) {
                _need_split = false;
                return init_collector(read_params);
            }
            return Status::OK();
        }

        // lake_scan_parallel_segment_open: open the segments of every rowset at once before the splitter loads
        // them one rowset after another, and let the splits read those very Segment objects. A grouped range task
        // has already opened them above.
        // lake_scan_warm_index also opens them here, serially without parallel open, so that it can warm them.
        PreparedTabletReadStatePtr classic_state;
        if (_could_split_physically && read_params.grouped_range_task == nullptr &&
            (should_open_segments_in_parallel(read_params.reader_type) ||
             should_warm_index_pages(read_params.reader_type)) &&
            std::all_of(_rowsets.begin(), _rowsets.end(),
                        [](const RowsetPtr& rowset) { return rowset->can_hold_segments(); })) {
            classic_state = std::make_shared<PreparedTabletReadState>();
            OlapReaderStatistics prepare_stats;
            // Keep reads already performed even if a later preparation step fails.
            DeferOp record_prepare_stats([&] { record_tablet_prepare_stats(prepare_stats); });
            SCOPED_RAW_TIMER(&_stats.lake_prepared_tablet_prepare_ns);
            RETURN_IF_ERROR(build_prepared_tablet_read_state(read_params, classic_state.get(), &prepare_stats));
            for (size_t i = 0; i < classic_state->rowsets.size(); ++i) {
                RETURN_IF_ERROR(classic_state->rowsets[i]->set_segments_for_split(classic_state->rowset_segments[i]));
            }
            // lake_scan_warm_index: the splits share these segments, and the state keeps the warm handles until the
            // last split that holds it is done.
            RowsetReadOptions classic_read_options;
            LakeIOOptions classic_io_options;
            RETURN_IF_ERROR(
                    init_rowset_read_options_for_split(read_params, &classic_read_options, &classic_io_options));
            warm_index_pages(classic_state.get(), read_params.reader_type, classic_read_options, classic_io_options);
        }

        pipeline::Morsels morsels;
        morsels.emplace_back(
                std::make_unique<pipeline::ScanMorsel>(read_params.plan_node_id, *(read_params.scan_range)));

        std::shared_ptr<pipeline::SplitMorselQueue> split_morsel_queue = nullptr;

        if (_could_split_physically) {
            auto physical_split_morsel_queue = std::make_shared<pipeline::PhysicalSplitMorselQueue>(
                    std::move(morsels), read_params.scan_dop, read_params.splitted_scan_rows);
            physical_split_morsel_queue->set_candidate_rowid_ranges(std::move(grouped_candidates));
            split_morsel_queue = std::move(physical_split_morsel_queue);
        } else {
            // logical
            split_morsel_queue = std::make_shared<pipeline::LogicalSplitMorselQueue>(
                    std::move(morsels), read_params.scan_dop, read_params.splitted_scan_rows);
        }

        // do prepare
        split_morsel_queue->set_tablets(tablets);
        split_morsel_queue->set_tablet_rowsets(tablet_rowsets);
        split_morsel_queue->set_key_ranges(read_params.range, read_params.end_range, read_params.start_key,
                                           read_params.end_key);
        split_morsel_queue->set_tablet_schema(_tablet_schema);

        while (true) {
            // Every split can load a rowset's segments (PhysicalSplitMorselQueue::_init_segment).
            RETURN_IF_ERROR(check_cancelled(read_params, _tablet_metadata->id()));
            auto split_status_or = split_morsel_queue->try_get();
            if (UNLIKELY(!split_status_or.ok())) {
                LOG(WARNING) << "failed to get split morsel: " << split_status_or.status()
                             << ", query_id: " << print_id(read_params.runtime_state->query_id())
                             << ", tablet_id: " << tablet_shared_ptr->tablet_id();
                // clear split tasks, and fallback to non-split mode
                _split_tasks.clear();
                _need_split = false;
                return init_collector(read_params);
            }

            auto split = std::move(split_status_or.value());
            if (split != nullptr) {
                auto ctx = std::make_unique<pipeline::LakeSplitContext>();

                if (_could_split_physically) {
                    auto physical_split = dynamic_cast<pipeline::PhysicalSplitScanMorsel*>(split.get());
                    ctx->rowid_range = physical_split->get_rowid_range_option();
                } else {
                    auto logical_split = dynamic_cast<pipeline::LogicalSplitScanMorsel*>(split.get());
                    ctx->short_key_range = logical_split->get_short_key_ranges_option();
                }
                ctx->split_morsel_queue = split_morsel_queue;
                ctx->classic_tablet_read_state = classic_state;
                _split_tasks.emplace_back(std::move(ctx));
            } else {
                break;
            }
        }

    } else {
        TabletReaderParams effective_params = read_params;
        if (effective_params.refine_initial_coarse_split_and_append_refined_tasks) {
            RETURN_IF_ERROR(refine_initial_coarse_split_and_append_refined_tasks(effective_params,
                                                                                 &effective_params.rowid_range_option));
        }
        return init_collector(effective_params);
    }

    return Status::OK();
}

Status TabletReader::init_compaction_column_paths(const TabletReaderParams& read_params) {
    if (!config::enable_compaction_flat_json || !is_compaction(read_params.reader_type) ||
        read_params.column_access_paths == nullptr) {
        return Status::OK();
    }

    // A vertical compaction reads its column groups in turn and keeps the paths derived by earlier groups in this
    // list: derive the JSON columns of this read that have none yet, rather than only the first group's.
    std::unordered_set<uint32_t> columns_with_path;
    for (const auto& path : *read_params.column_access_paths) {
        columns_with_path.insert(path->index());
    }
    // Bail out before touching any segment when this read has no JSON column: get_segments() below
    // loads every input segment, so on JSON-free tables this path would fully load the whole input
    // set for nothing (500 wide segments ≈ 350MB). Without a held set it also loads them with
    // fill_metadata_cache=true, pushing that whole set into the shared metacache and evicting
    // neighbors' entries; a compaction task that holds its inputs reuses them here instead
    // (Rowset::get_segments_checked).
    bool has_json_column = false;
    for (size_t i = 0; i < _tablet_schema->num_columns(); i++) {
        const auto& col = _tablet_schema->column(i);
        if (col.type() == LogicalType::TYPE_JSON && _schema.get_field_by_name(std::string(col.name())) != nullptr &&
            !columns_with_path.contains(i)) {
            has_json_column = true;
            break;
        }
    }
    if (!has_json_column) {
        return Status::OK();
    }

    // get_non_null_segments() drops lost-segment placeholders (experimental_lake_ignore_lost_segment).
    int num_readers = 0;
    for (const auto& rowset : _rowsets) {
        auto segments = rowset->get_non_null_segments();
        std::for_each(segments.begin(), segments.end(),
                      [&](const auto& segment) { num_readers += segment->num_rows() > 0 ? 1 : 0; });
    }

    std::shared_ptr<FlatJsonConfig> flat_json_config;
    if (auto metadata = _tablet_mgr->get_latest_cached_tablet_metadata(_tablet_metadata->id());
        metadata && metadata->has_flat_json_config()) {
        flat_json_config = std::make_shared<FlatJsonConfig>();
        flat_json_config->update(metadata->flat_json_config());
    }
    // Only a table that flattens JSON writes the output flat; otherwise inputs read flat are merged back.
    const bool mixed_inputs =
            config::enable_compaction_flat_json_mixed_inputs &&
            (flat_json_config != nullptr ? flat_json_config->is_flat_json_enabled() : config::enable_json_flat);

    std::vector<const ColumnReader*> readers;
    for (size_t i = 0; i < _tablet_schema->num_columns(); i++) {
        const auto& col = _tablet_schema->column(i);
        auto col_name = std::string(col.name());
        if (_schema.get_field_by_name(col_name) == nullptr || col.type() != LogicalType::TYPE_JSON ||
            columns_with_path.contains(i)) {
            continue;
        }
        readers.clear();
        int num_plain = 0;
        for (const auto& rowset : _rowsets) {
            for (const auto& segment : rowset->get_non_null_segments()) {
                if (segment->num_rows() == 0) {
                    continue;
                }
                auto reader = segment->column_with_uid(col.unique_id());
                if (reader != nullptr && reader->column_type() == LogicalType::TYPE_JSON &&
                    nullptr != reader->sub_readers() && !reader->sub_readers()->empty()) {
                    readers.emplace_back(reader);
                } else if (reader != nullptr && reader->column_type() == LogicalType::TYPE_JSON) {
                    num_plain++;
                }
            }
        }
        // Some inputs flat, the others whole JSON: a segment without the column reads as defaults that are
        // never flat, so it is left to the whole-JSON path.
        const bool mixed = !readers.empty() && readers.size() + num_plain == num_readers && num_plain > 0;
        if (readers.size() == num_readers || (mixed && mixed_inputs)) {
            JsonPathDeriver deriver;
            deriver.init_flat_json_config(flat_json_config.get());
            deriver.derived(readers);
            auto paths = deriver.flat_paths();
            auto types = deriver.flat_types();
            if (mixed && !paths.empty()) {
                // A whole-JSON input may hold any value at any path; JSON keeps each as it is, and the value
                // stats of the JSON sub-columns written let a later compaction type the paths by their values.
                types.assign(types.size(), LogicalType::TYPE_JSON);
                FlatJsonMetrics::instance()->flat_json_compaction_mixed_inputs_total.increment(1);
            }
            VLOG(3) << "Lake Compaction flat json column: " << JsonFlatPath::debug_flat_json(paths, types, true);
            ASSIGN_OR_RETURN(auto res, ColumnAccessPath::create(TAccessPathType::ROOT, std::string(col.name()), i));
            for (size_t j = 0; j < paths.size(); j++) {
                ColumnAccessPath::insert_json_path(res.get(), types[j], paths[j]);
            }
            res->set_from_compaction(true);
            read_params.column_access_paths->emplace_back(std::move(res));
        }
    }
    return Status::OK();
}

void TabletReader::flush_reusable_iterator_io_stats() {
    for (const auto& [rowset_idx, segment_idx] : _used_reusable_slots) {
        if (rowset_idx < _reusable_rowset_iterators.size() &&
            segment_idx < _reusable_rowset_iterators[rowset_idx].size()) {
            if (const auto& iter = _reusable_rowset_iterators[rowset_idx][segment_idx]; iter != nullptr) {
                flush_segment_iterator_io_stats(iter.get());
            }
        }
    }
    _used_reusable_slots.clear();
}

void TabletReader::close() {
    if (_collect_iter != nullptr) {
        _collect_iter->close();
        _collect_iter.reset();
    }
    for (auto& rowset_iters : _reusable_rowset_iterators) {
        for (auto& iter : rowset_iters) {
            if (iter != nullptr) {
                iter->close();
                iter.reset();
            }
        }
    }
    _reusable_rowset_iterators.clear();
    _used_reusable_slots.clear();
    _nosplit_warm_state.reset();
    STLDeleteElements(&_predicate_free_list);
    _rowsets.clear();
    _obj_pool.clear();
}

Status TabletReader::do_get_next(Chunk* chunk) {
    DCHECK(!_is_vertical_merge);
    if (_need_split) {
        // return eof
        return Status::EndOfFile("split morsel");
    }
    RETURN_IF_ERROR(_collect_iter->get_next(chunk));
    return Status::OK();
}

Status TabletReader::do_get_next(Chunk* chunk, std::vector<uint64_t>* rssid_rowids) {
    return _collect_iter->get_next(chunk, rssid_rowids);
}

Status TabletReader::do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) {
    DCHECK(_is_vertical_merge);
    if (_need_split) {
        // return eof
        return Status::EndOfFile("split morsel");
    }
    RETURN_IF_ERROR(_collect_iter->get_next(chunk, source_masks));
    if (_is_key && _selection_buffer != nullptr && source_masks->empty() && chunk->num_rows() > 0) {
        // A one-child heap merge returns the child iterator directly, which does not
        // produce source masks. UNSHARE still needs one mask per physical row so value
        // groups can replay the selection stream.
        source_masks->insert(source_masks->end(), chunk->num_rows(), RowSourceMask{0, false});
    }
    return Status::OK();
}

Status TabletReader::do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks,
                                 std::vector<uint64_t>* rssid_rowids) {
    DCHECK(_is_vertical_merge);
    RETURN_IF_ERROR(_collect_iter->get_next(chunk, source_masks, rssid_rowids));
    if (_is_key && _selection_buffer != nullptr && source_masks->empty() && chunk->num_rows() > 0) {
        source_masks->insert(source_masks->end(), chunk->num_rows(), RowSourceMask{0, false});
    }
    return Status::OK();
}

void TabletReader::record_tablet_prepare_stats(const OlapReaderStatistics& prepare_stats) {
    _stats.lake_prepared_tablet_segments_opened += prepare_stats.segments_opened;
    _stats.lake_prepared_tablet_cached_segments += prepare_stats.lake_prepared_tablet_cached_segments;
    _stats.lake_prepared_tablet_io_ns += prepare_stats.io_ns;
    _stats.lake_prepared_tablet_io_count += prepare_stats.io_count;
    _stats.lake_prepared_tablet_io_remote_ns += prepare_stats.io_ns_remote;
    _stats.lake_prepared_tablet_io_count_remote += prepare_stats.io_count_remote;
    _stats.lake_prepared_tablet_bytes_read_remote += prepare_stats.compressed_bytes_read_remote;
}

// The one entry point of index warming for the query scans of a tablet, split or not: called once the scan has opened
// the segments of |state|.
void TabletReader::warm_index_pages(PreparedTabletReadState* state, ReaderType reader_type,
                                    const RowsetReadOptions& options, const LakeIOOptions& lake_io_opts) {
    if (!should_warm_index_pages(reader_type) || options.tablet_schema == nullptr) {
        return;
    }
    const auto& predicate_ids = options.pred_tree_for_zone_map.column_ids();
    std::vector<ColumnId> predicate_cids(predicate_ids.begin(), predicate_ids.end());
    std::vector<ColumnId> read_cids;
    for (const auto& field : schema().fields()) {
        if (!field->is_virtual()) {
            read_cids.push_back(field->id());
        }
    }
    warm_prepared_indexes(state, *options.tablet_schema, predicate_cids, read_cids, metadata_prune_tree(options),
                          lake_io_opts, &_stats);
}

Status TabletReader::build_prepared_tablet_read_state(const TabletReaderParams& params, PreparedTabletReadState* state,
                                                      OlapReaderStatistics* prepare_stats) {
    if (state == nullptr) {
        return Status::InvalidArgument("prepared tablet read state is null");
    }
    if (!state->rowsets.empty()) {
        return Status::OK();
    }
    if (!_rowsets_inited) {
        _rowsets = Rowset::get_rowsets(_tablet_mgr, _tablet_metadata);
        _rowsets_inited = true;
    }

    std::vector<RowsetPtr> rowsets = _rowsets;
    std::vector<std::vector<SegmentPtr>> rowset_segments;
    std::vector<std::vector<PreparedSegmentReadStatePtr>> rowset_prepared_states;
    rowset_segments.reserve(rowsets.size());
    rowset_prepared_states.reserve(rowsets.size());
    SCOPED_RAW_TIMER(&_stats.lake_prepared_tablet_segment_open_ns);
    const std::atomic<bool>* is_cancelled =
            params.runtime_state != nullptr ? &params.runtime_state->cancelled_ref() : nullptr;
    if (should_open_segments_in_parallel(params.reader_type)) {
        RETURN_IF_ERROR(check_cancelled(params, _tablet_metadata->id()));
        // Include the candidate slots of every rowset even if only some footers can be opened.
        for (const auto& rowset : rowsets) {
            _stats.lake_prepared_tablet_segments += rowset->num_segments();
        }
        RETURN_IF_ERROR(open_rowset_segments_in_parallel(rowsets, params.lake_io_opts, is_cancelled, prepare_stats,
                                                         &rowset_segments));
        // The tasks that had started their IO when the query was cancelled may all have succeeded.
        RETURN_IF_ERROR(check_cancelled(params, _tablet_metadata->id()));
    } else {
        for (const auto& rowset : rowsets) {
            RETURN_IF_ERROR(check_cancelled(params, _tablet_metadata->id()));
            // Include the failing rowset's candidate slots even if only some footers can be opened.
            _stats.lake_prepared_tablet_segments += rowset->num_segments();
            ASSIGN_OR_RETURN(auto segments, rowset->segments(params.lake_io_opts, prepare_stats, is_cancelled));
            rowset_segments.emplace_back(std::move(segments));
        }
    }
    for (const auto& segments : rowset_segments) {
        std::vector<PreparedSegmentReadStatePtr> prepared_states;
        prepared_states.reserve(segments.size());
        for (size_t i = 0; i < segments.size(); ++i) {
            prepared_states.emplace_back(std::make_shared<PreparedSegmentReadState>());
        }
        rowset_prepared_states.emplace_back(std::move(prepared_states));
    }

    state->rowsets = std::move(rowsets);
    state->rowset_segments = std::move(rowset_segments);
    state->rowset_prepared_states = std::move(rowset_prepared_states);
    return Status::OK();
}

Status TabletReader::init_rowset_read_options(const TabletReaderParams& params, RowsetReadOptions* options) {
    if (options == nullptr) {
        return Status::InvalidArgument("rowset read options is null");
    }
    KeysType keys_type = _tablet_schema->keys_type();
    RETURN_IF_ERROR(init_predicates(params));
    RETURN_IF_ERROR(init_delete_predicates(params, &_delete_predicates));
    // The seek range is invariant across this reader's per-split reopens, so parse it once and reuse the
    // cached result. This skips the per-reopen seek-tuple rebuild (convert_field + datum decode +
    // allocations) that dominated the lake split-scan open path.
    if (!_cached_seek_ranges) {
        std::vector<SeekRange> ranges;
        RETURN_IF_ERROR(parse_seek_range(*_tablet_schema, params.range, params.end_range, params.start_key,
                                         params.end_key, &ranges, &_mempool));
        _cached_seek_ranges = std::move(ranges);
    }
    options->ranges = *_cached_seek_ranges;
    options->grouped_range_task = params.grouped_range_task;
    options->pred_tree = params.pred_tree;
    options->runtime_filter_preds = params.runtime_filter_preds;
    RETURN_IF_ERROR(ZonemapPredicatesRewriter::rewrite_predicate_tree(&_obj_pool, options->pred_tree,
                                                                      options->pred_tree_for_zone_map));
    options->sorted = ((keys_type != DUP_KEYS && keys_type != PRIMARY_KEYS) && !params.skip_aggregation) ||
                      is_compaction(params.reader_type) || params.sorted_by_keys_per_tablet;
    options->reader_type = params.reader_type;
    options->chunk_size = params.chunk_size;
    options->delete_predicates = &_delete_predicates;
    options->stats = &_stats;
    options->runtime_state = params.runtime_state;
    options->profile = params.profile;
    options->use_page_cache = params.use_page_cache;
    options->tablet_schema = _tablet_schema;
    options->global_dictmaps = params.global_dictmaps;
    options->unused_output_column_ids = params.unused_output_column_ids;
    options->runtime_range_pruner = params.runtime_range_pruner;
    options->lake_io_opts = params.lake_io_opts;
    options->enable_join_runtime_filter_pushdown = params.enable_join_runtime_filter_pushdown;
    options->has_predicate_above_iterator = params.has_predicate_above_iterator;
    options->prune_column_after_index_filter = params.prune_column_after_index_filter;
    options->enable_gin_filter = params.enable_gin_filter;
    options->enable_predicate_col_late_materialize = params.enable_predicate_col_late_materialize;

    if (keys_type == KeysType::PRIMARY_KEYS) {
        options->is_primary_keys = true;
    }
    // Read version is required by the Index Delta Group (ADD INDEX fast-path)
    // visibility filter for ALL key types, not only PRIMARY_KEYS. Leaving it 0
    // for DUPLICATE / UNIQUE / AGGREGATE reads hides every .idx entry
    // (entry.version > 0 == query_version), so the index is silently skipped.
    options->version = _tablet_metadata->version();

    if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS) {
        options->asc_hint = _is_asc_hint;
    }

    options->rowid_range_option = params.rowid_range_option;
    options->short_key_ranges_option = params.short_key_ranges_option;
    options->column_access_paths = params.column_access_paths;
    options->use_vector_index = params.use_vector_index;
    options->vector_search_option = params.vector_search_option;
    options->sample_options = params.sample_options;
    options->has_preaggregation = true;
    if ((is_compaction(params.reader_type) || params.sorted_by_keys_per_tablet)) {
        options->has_preaggregation = true;
    } else if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS ||
               (keys_type == UNIQUE_KEYS && params.skip_aggregation)) {
        options->has_preaggregation = false;
    }
    return Status::OK();
}

Status TabletReader::init_rowset_read_options_for_split(const TabletReaderParams& params, RowsetReadOptions* options,
                                                        LakeIOOptions* lake_io_opts) {
    if (lake_io_opts == nullptr) {
        return Status::InvalidArgument("lake io options is null");
    }
    RETURN_IF_ERROR(init_rowset_read_options(params, options));

    *lake_io_opts = params.lake_io_opts;
    if (lake_io_opts->fs == nullptr) {
        auto root_loc = _tablet_mgr->tablet_root_location(_tablet_metadata->id());
        ASSIGN_OR_RETURN(lake_io_opts->fs, FileSystemFactory::CreateSharedFromString(root_loc));
    }
    if (lake_io_opts->location_provider == nullptr) {
        lake_io_opts->location_provider = _tablet_mgr->location_provider();
    }
    options->lake_io_opts = *lake_io_opts;
    return Status::OK();
}

Status TabletReader::build_initial_coarse_split_tasks(const TabletReaderParams& params,
                                                      const PreparedTabletReadStatePtr& prepared_tablet_read_state,
                                                      OlapReaderStatistics* prepare_stats) {
    if (prepared_tablet_read_state == nullptr) {
        return Status::InvalidArgument("prepared tablet read state is null");
    }
    if (prepared_tablet_read_state->rowsets.size() != prepared_tablet_read_state->rowset_segments.size() ||
        prepared_tablet_read_state->rowsets.size() != prepared_tablet_read_state->rowset_prepared_states.size()) {
        return Status::InvalidArgument("prepared tablet read state has inconsistent rowset state");
    }

    RowsetReadOptions rowset_read_options;
    LakeIOOptions lake_io_opts;
    RETURN_IF_ERROR(init_rowset_read_options_for_split(params, &rowset_read_options, &lake_io_opts));

    warm_index_pages(prepared_tablet_read_state.get(), params.reader_type, rowset_read_options, lake_io_opts);

    _split_tasks.clear();
    const auto split_rows = rows_per_split(params);
    for (size_t rowset_idx = 0; rowset_idx < prepared_tablet_read_state->rowsets.size(); ++rowset_idx) {
        const auto& rowset = prepared_tablet_read_state->rowsets[rowset_idx];
        if (rowset == nullptr) {
            continue;
        }
        const auto& segments = prepared_tablet_read_state->rowset_segments[rowset_idx];
        const auto& segment_states = prepared_tablet_read_state->rowset_prepared_states[rowset_idx];
        if (segments.size() != segment_states.size()) {
            return Status::InvalidArgument("prepared tablet read state has inconsistent segment state");
        }
        _stats.lake_prepared_rowsets++;
        ASSIGN_OR_RETURN(auto shared_segment_range, rowset->get_seek_range());

        for (size_t segment_idx = 0; segment_idx < segments.size(); ++segment_idx) {
            // Building the coarse range can load the segment's short key index.
            RETURN_IF_ERROR(check_cancelled(params, _tablet_metadata->id()));
            const auto& segment = segments[segment_idx];
            if (segment == nullptr || segment->num_rows() == 0) {
                continue;
            }
            const auto& segment_state = segment_states[segment_idx];
            const auto delete_predicates =
                    rowset_read_options.delete_predicates != nullptr
                            ? rowset_read_options.delete_predicates->get_predicates(rowset->index())
                            : DisjunctivePredicates{};
            SegmentReadOptions segment_read_options;
            RETURN_IF_ERROR(rowset->init_segment_read_options(rowset_read_options, lake_io_opts, delete_predicates,
                                                              prepare_stats, &segment_read_options));
            RETURN_IF_ERROR(rowset->set_segment_tablet_range(segment_idx, shared_segment_range, &segment_read_options));
            ASSIGN_OR_RETURN(auto coarse_scan_range, build_initial_coarse_scan_range(segment, segment_read_options));
            if (coarse_scan_range.span_size() == 0) {
                continue;
            }
            init_coarse_split_allocation_state(segment_state, coarse_scan_range);

            RowidRangeOptionPtr seed_rowid_range;
            if (!allocate_initial_coarse_split(segment_state, rowset.get(), segment.get(), split_rows,
                                               &seed_rowid_range)) {
                continue;
            }

            auto ctx = std::make_unique<pipeline::LakeSplitContext>();
            ctx->rowid_range = std::move(seed_rowid_range);
            ctx->rowid_range_source = pipeline::LakeSplitContext::RowidRangeSource::INITIAL_COARSE;
            ctx->prepared_tablet_read_state = prepared_tablet_read_state;
            ctx->prepared_segment_read_state = segment_state;
            ctx->rowset_index = rowset_idx;
            ctx->segment_index = segment_idx;
            _split_tasks.emplace_back(std::move(ctx));
        }
    }
    return Status::OK();
}

Status TabletReader::refine_initial_coarse_split_and_append_refined_tasks(const TabletReaderParams& params,
                                                                          RowidRangeOptionPtr* local_rowid_range) {
    if (local_rowid_range == nullptr) {
        return Status::InvalidArgument("local rowid range is null");
    }
    auto seed_rowid_range_option = params.rowid_range_option;
    *local_rowid_range = nullptr;
    if (params.prepared_tablet_read_state == nullptr || params.prepared_segment_read_state == nullptr ||
        params.prepared_rowset_index >= params.prepared_tablet_read_state->rowsets.size() ||
        params.prepared_rowset_index >= params.prepared_tablet_read_state->rowset_segments.size() ||
        params.prepared_rowset_index >= params.prepared_tablet_read_state->rowset_prepared_states.size()) {
        return Status::InvalidArgument("invalid initial coarse split context");
    }

    const auto& prepared_tablet_read_state = params.prepared_tablet_read_state;
    const auto& segment_state = params.prepared_segment_read_state;
    const auto rowset_idx = params.prepared_rowset_index;
    const auto segment_idx = params.prepared_segment_index;
    if (segment_idx >= prepared_tablet_read_state->rowset_segments[rowset_idx].size() ||
        segment_idx >= prepared_tablet_read_state->rowset_prepared_states[rowset_idx].size() ||
        prepared_tablet_read_state->rowset_prepared_states[rowset_idx][segment_idx] != segment_state) {
        return Status::InvalidArgument("initial coarse split context does not match prepared state");
    }

    const auto& rowset = prepared_tablet_read_state->rowsets[rowset_idx];
    const auto& segment = prepared_tablet_read_state->rowset_segments[rowset_idx][segment_idx];
    if (rowset == nullptr || segment == nullptr || segment->num_rows() == 0) {
        *local_rowid_range = std::make_shared<RowidRangeOption>();
        return Status::OK();
    }

    TabletReaderParams prepare_params = params;
    // The seed morsel carries only one coarse split. Segment prepare must compute the
    // final pruned range for the whole segment so refined children can cover the rest.
    prepare_params.rowid_range_option = nullptr;

    RowsetReadOptions rowset_read_options;
    LakeIOOptions lake_io_opts;
    RETURN_IF_ERROR(init_rowset_read_options_for_split(prepare_params, &rowset_read_options, &lake_io_opts));

    OlapReaderStatistics prepare_stats;
    DeferOp record_seed_stats([&] {
        // Preserve the seed breakdown when pruning fails after performing reads.
        _stats.lake_prepared_seed_io_ns += prepare_stats.io_ns;
        _stats.lake_prepared_seed_io_count += prepare_stats.io_count;
        _stats.lake_prepared_seed_io_remote_ns += prepare_stats.io_ns_remote;
        _stats.lake_prepared_seed_io_count_remote += prepare_stats.io_count_remote;
        _stats.lake_prepared_seed_bytes_read_remote += prepare_stats.compressed_bytes_read_remote;
        _stats.lake_prepared_seed_segment_init_ns += prepare_stats.segment_init_ns;
        _stats.lake_prepared_seed_vector_index_load_ns += prepare_stats.vector_index_load_ns;
        _stats.lake_prepared_seed_get_row_ranges_by_vector_index_ns +=
                prepare_stats.get_row_ranges_by_vector_index_timer;
        _stats.lake_prepared_seed_vector_index_cache_lookup_ns += prepare_stats.vector_index_cache_lookup_ns;
        _stats.lake_prepared_seed_vector_index_file_open_ns += prepare_stats.vector_index_file_open_ns;
        _stats.lake_prepared_seed_vector_index_read_file_ns += prepare_stats.vector_index_read_file_ns;
        _stats.lake_prepared_seed_vector_index_init_index_ns += prepare_stats.vector_index_init_index_ns;
        _stats.lake_prepared_seed_vector_index_searcher_init_ns += prepare_stats.vector_index_searcher_init_ns;
        _stats.lake_prepared_seed_vector_index_cache_hit_count += prepare_stats.vector_index_cache_hit_count;
        _stats.lake_prepared_seed_vector_index_cache_miss_count += prepare_stats.vector_index_cache_miss_count;
        _stats.lake_prepared_seed_vector_search_ns += prepare_stats.vector_search_timer;
        _stats.lake_prepared_seed_process_vector_distance_and_id_ns +=
                prepare_stats.process_vector_distance_and_id_timer;
        _stats.lake_prepared_seed_rows_vector_index_filtered += prepare_stats.rows_vector_index_filtered;
        _stats.lake_prepared_seed_zonemap_ns += prepare_stats.zone_map_filter_ns;
        _stats.lake_prepared_seed_zonemap_filtered_rows += prepare_stats.rows_stats_filtered;
        _stats.lake_prepared_seed_bf_ns += prepare_stats.bf_filter_ns;
        _stats.lake_prepared_seed_bf_filtered_rows += prepare_stats.rows_bf_filtered;
    });
    ASSIGN_OR_RETURN(auto shared_segment_range, rowset->get_seek_range());
    Status st;
    {
        TEST_SYNC_POINT("TabletReader::refine_initial_coarse_split::before_pruning");
        // Time the seed-only prepared-scan-range preparation (zonemap/bloom page-filter folding + seek-range
        // resolution). This otherwise hides inside IOTaskExecTime with no dedicated counter; for highly
        // selective scans over a big tablet it can dominate the whole scan.
        SCOPED_RAW_TIMER(&_stats.lake_prepared_seed_ns);
        st = prepare_segment_pruned_scan_range_for_split(rowset, segment_idx, segment, segment_state, schema(),
                                                         rowset_read_options, lake_io_opts, shared_segment_range,
                                                         &prepare_stats);
    }
    {
        std::lock_guard<std::mutex> guard(segment_state->coarse_range_lock);
        segment_state->coarse_split_allocation_closed = true;
    }
    RETURN_IF_ERROR(st);

    SparseRange<> pruned_scan_range;
    if (segment_state->pruned_scan_range != nullptr) {
        pruned_scan_range = *segment_state->pruned_scan_range;
        _stats.lake_prepared_scan_rows += segment_state->pruned_scan_range->span_size();
        _stats.lake_prepared_scan_ranges += segment_state->pruned_scan_range->size();
    } else {
        pruned_scan_range.add(Range<>(0, segment->num_rows()));
    }
    if (pruned_scan_range.span_size() == 0) {
        *local_rowid_range = std::make_shared<RowidRangeOption>();
        return Status::OK();
    }

    if (seed_rowid_range_option != nullptr) {
        auto seed_split = seed_rowid_range_option->get_segment_rowid_range(rowset.get(), segment.get());
        if (seed_split.row_id_range != nullptr) {
            SparseRange<> seed_scan_range = *seed_split.row_id_range;
            seed_scan_range &= pruned_scan_range;
            if (seed_scan_range.span_size() > 0) {
                auto rowid_range = std::make_shared<RowidRangeOption>();
                rowid_range->add(rowset.get(), segment.get(),
                                 std::make_shared<SparseRange<>>(std::move(seed_scan_range)),
                                 seed_split.is_first_split_of_segment);
                *local_rowid_range = std::move(rowid_range);
            } else if (seed_split.is_first_split_of_segment) {
                // The segment survives, but its statistics owner has no rows to read. Account for
                // the first-split contributions without opening an empty iterator. Other splits
                // subtract their row counts from rows_key_range_filtered as usual.
                _stats.phy_segments_count++;
                _stats.rows_key_range_filtered += segment->num_rows();
            }
        }
    }

    SparseRange<> allocated_coarse_ranges;
    {
        std::lock_guard<std::mutex> guard(segment_state->coarse_range_lock);
        allocated_coarse_ranges = segment_state->allocated_coarse_ranges;
    }
    SparseRange<> remaining_scan_range = subtract_sparse_ranges(pruned_scan_range, allocated_coarse_ranges);
    // Emit is_first_split_of_segment=true from the refined tasks only when no coarse split was allocated
    // for this segment. If a coarse split was allocated, the coarse/seed child already carries the flag
    // (allocate_initial_coarse_split / the seed child above), so letting the first refined split also carry
    // it would double-count segment-level stats. This keeps exactly one split per segment flagged.
    const bool refined_carries_first_split = allocated_coarse_ranges.span_size() == 0;
    append_refined_segment_split_tasks(rowset, segment, prepared_tablet_read_state, segment_state, rowset_idx,
                                       segment_idx, remaining_scan_range, rows_per_split(params),
                                       refined_carries_first_split, &_split_tasks);
    _stats.lake_prepared_segments++;

    if (*local_rowid_range == nullptr) {
        // Keep this seed child as an empty read if its coarse range was fully pruned.
        *local_rowid_range = std::make_shared<RowidRangeOption>();
    }
    return Status::OK();
}

Status TabletReader::get_segment_iterators(const TabletReaderParams& params, std::vector<ChunkIteratorPtr>* iters) {
    RowsetReadOptions rs_opts;
    RETURN_IF_ERROR(init_rowset_read_options(params, &rs_opts));
    // A rowset's own union of its segments would hide them from the union that reads ahead.
    rs_opts.union_segments = !_union_reads_ahead;

    SCOPED_RAW_TIMER(&_stats.create_segment_iter_ns);

    const auto& prepared_tablet_read_state = params.prepared_tablet_read_state;
    const bool use_prepared_segments = prepared_tablet_read_state != nullptr;
    if (use_prepared_segments &&
        (prepared_tablet_read_state->rowsets.size() != _rowsets.size() ||
         prepared_tablet_read_state->rowset_segments.size() != prepared_tablet_read_state->rowsets.size() ||
         prepared_tablet_read_state->rowset_prepared_states.size() != prepared_tablet_read_state->rowsets.size())) {
        return Status::InvalidArgument("prepared tablet read state does not match reader rowsets");
    }
    const bool use_prepared_segment_target = use_prepared_segments && params.prepared_segment_read_state != nullptr;
    if (use_prepared_segment_target) {
        if (params.prepared_rowset_index >= _rowsets.size()) {
            return Status::InvalidArgument("prepared target rowset index exceeds reader rowsets");
        }
        const auto rowset_idx = params.prepared_rowset_index;
        const auto segment_idx = params.prepared_segment_index;
        auto& rowset = _rowsets[rowset_idx];
        const auto& prepared_rowset = prepared_tablet_read_state->rowsets[rowset_idx];
        if (prepared_rowset == nullptr || prepared_rowset->rowset_id() != rowset->rowset_id()) {
            return Status::InvalidArgument("prepared tablet read state rowset order does not match reader rowsets");
        }
        const auto& prepared_segments = prepared_tablet_read_state->rowset_segments[rowset_idx];
        const auto& prepared_segment_states = prepared_tablet_read_state->rowset_prepared_states[rowset_idx];
        if (segment_idx >= prepared_segments.size() || segment_idx >= prepared_segment_states.size()) {
            return Status::InvalidArgument("prepared target segment index exceeds prepared segment state");
        }
        if (prepared_segment_states[segment_idx] != params.prepared_segment_read_state) {
            return Status::InvalidArgument("prepared target segment state does not match split context");
        }
        if (_reusable_rowset_iterators.size() < _rowsets.size()) {
            _reusable_rowset_iterators.resize(_rowsets.size());
        }
        ASSIGN_OR_RETURN(auto seg_iters,
                         enhance_error_prompt(rowset->read_prepared_segment(
                                 schema(), rs_opts, prepared_segments, segment_idx, params.prepared_segment_read_state,
                                 &_reusable_rowset_iterators[rowset_idx])));
        _used_reusable_slots.emplace_back(rowset_idx, segment_idx);
        iters->insert(iters->end(), seg_iters.begin(), seg_iters.end());
        _stats.rowsets_read_count++;
        return Status::OK();
    }
    if (use_prepared_segments) {
        return Status::InvalidArgument("prepared tablet read state requires a prepared segment target");
    }
    const auto& classic_state = params.classic_tablet_read_state;
    if (classic_state != nullptr) {
        if (classic_state->rowsets.size() != _rowsets.size() ||
            classic_state->rowset_segments.size() != _rowsets.size()) {
            return Status::InvalidArgument("CLASSIC tablet read state does not match reader rowsets");
        }
        for (size_t i = 0; i < _rowsets.size(); ++i) {
            if (classic_state->rowsets[i] == nullptr ||
                classic_state->rowsets[i]->rowset_id() != _rowsets[i]->rowset_id()) {
                return Status::InvalidArgument("CLASSIC tablet read state rowset order mismatch");
            }
        }
    }
    // Per rowset, whether all its segments were open in the metadata cache before this scan opened them. Filled only
    // when the segments are opened up front in parallel below.
    std::vector<bool> cached_rowsets;
    if (!_need_split && classic_state == nullptr && should_warm_index_pages(params.reader_type) &&
        params.rowid_range_option == nullptr && params.short_key_ranges_option == nullptr) {
        // lake_scan_warm_index: open the segments once, warm all of them before building any iterator, and build the
        // iterators on these same segments. The reader keeps the warm handles until close() or the next open(), so
        // the queued fills keep their demand while the scan reads.
        _nosplit_warm_state = std::make_shared<PreparedTabletReadState>();
        auto* state = _nosplit_warm_state.get();
        state->rowsets = _rowsets;
        const std::atomic<bool>* is_cancelled =
                params.runtime_state != nullptr ? &params.runtime_state->cancelled_ref() : nullptr;
        if (should_open_segments_in_parallel(params.reader_type)) {
            RETURN_IF_ERROR(check_cancelled(params, _tablet_metadata->id()));
            // Open only the segments Rowset::read will not skip by the segment metadata filter.
            RETURN_IF_ERROR(open_rowset_segments_in_parallel(
                    _rowsets, rs_opts.lake_io_opts, is_cancelled, &_stats, &state->rowset_segments, &cached_rowsets,
                    metadata_prune_tree(rs_opts), rs_opts.tablet_schema.get()));
        } else {
            for (const auto& rowset : _rowsets) {
                RETURN_IF_ERROR(check_cancelled(params, _tablet_metadata->id()));
                ASSIGN_OR_RETURN(auto segments, rowset->segments(rs_opts.lake_io_opts, &_stats, is_cancelled));
                state->rowset_segments.emplace_back(std::move(segments));
            }
        }
        RETURN_IF_ERROR(check_cancelled(params, _tablet_metadata->id()));
        warm_index_pages(state, params.reader_type, rs_opts, rs_opts.lake_io_opts);
    }
    auto read_rowset = [&](size_t rowset_idx, const RowsetReadOptions& options) {
        const auto& rowset = _rowsets[rowset_idx];
        if (classic_state != nullptr) {
            return rowset->read(schema(), options, classic_state->rowset_segments[rowset_idx]);
        }
        // The prepared-segments path indexes segments by metadata position: only a rowset that can hold its
        // segments, and whose segments all opened, reuses the warmed set. The others load theirs as before.
        if (_nosplit_warm_state != nullptr && rowset->can_hold_segments() &&
            _nosplit_warm_state->rowset_segments[rowset_idx].size() == static_cast<size_t>(rowset->num_segments())) {
            return rowset->read(schema(), options, _nosplit_warm_state->rowset_segments[rowset_idx]);
        }
        return rowset->read(schema(), options);
    };
    // A query scan under the remote I/O scheduler builds its rowsets' iterators in parallel with
    // lake_scan_parallel_segment_open; every other reader follows enable_load_segment_parallel.
    const bool parallel_rowset_read =
            config::enable_load_segment_parallel || should_open_segments_in_parallel(params.reader_type);
    // When every segment of the tablet was already open in the metadata cache, building a rowset's iterators only
    // constructs objects (the index and data reads stay in SegmentIterator's lazy initialization), so it is done
    // inline instead of as a task on the rowset thread pool plus a wait. Only when all rowsets are cached: mixing
    // inline rowsets with pool tasks would gain little. An explicit enable_load_segment_parallel keeps its pool.
    // A primary key segment iterator loads its delete vector and delta column groups when it is constructed, so a
    // primary key tablet is built inline only when that needs no read: the tablet has no delta column group, and
    // the delete vector of every segment is in the metadata cache (or the segment has none).
    auto pk_iterators_read_nothing = [&]() {
        if (_tablet_metadata->has_dcg_meta() && !_tablet_metadata->dcg_meta().dcgs().empty()) {
            return false;
        }
        for (size_t i = 0; i < _rowsets.size(); ++i) {
            for (const auto& segment : _nosplit_warm_state->rowset_segments[i]) {
                if (segment != nullptr &&
                    !del_vec_cached(_tablet_mgr, *_tablet_metadata, _rowsets[i]->metadata().id() + segment->id())) {
                    return false;
                }
            }
        }
        return true;
    };
    const bool inline_cached_rowsets =
            !config::enable_load_segment_parallel && _nosplit_warm_state != nullptr && !cached_rowsets.empty() &&
            std::all_of(cached_rowsets.begin(), cached_rowsets.end(), [](bool cached) { return cached; }) &&
            (!rs_opts.is_primary_keys || pk_iterators_read_nothing());

    // One entry per rowset read, in rowset order: its iterators, or the future that builds them. They are appended
    // to |iters| in this order whatever order the tasks finish in, because the order of the iterators is the merge
    // precedence of the key models that merge.
    struct RowsetIterators {
        std::vector<ChunkIteratorPtr> iters;
        std::future<StatusOr<std::vector<ChunkIteratorPtr>>> future;
    };
    std::vector<RowsetIterators> rowset_iters;
    rowset_iters.reserve(_rowsets.size());
    // The parallel tasks capture local variables and |this| by reference.
    // Must wait for all tasks to complete before returning, otherwise
    // dangling references to |rs_opts| and |this| cause use-after-free.
    DeferOp wait_futures([&rowset_iters]() {
        for (auto& entry : rowset_iters) {
            if (entry.future.valid()) {
                entry.future.wait();
            }
        }
    });
    for (size_t rowset_idx = 0; rowset_idx < _rowsets.size(); ++rowset_idx) {
        // Stop opening, and submitting, the remaining rowsets. Rowset::read() checks again per segment.
        RETURN_IF_ERROR(check_cancelled(params, _tablet_metadata->id()));
        auto& rowset = _rowsets[rowset_idx];
        if (params.rowid_range_option != nullptr && !params.rowid_range_option->contains_rowset(rowset.get())) {
            continue;
        }
        _stats.rowsets_read_count++;

        const bool inline_cached_rowset = inline_cached_rowsets && rowset->can_hold_segments();
        if (inline_cached_rowset) {
            _stats.lake_prepared_tablet_inline_rowsets++;
        }
        if (parallel_rowset_read && !inline_cached_rowset) {
            _parallel_rowset_stats.emplace_back(std::make_unique<OlapReaderStatistics>());
            RowsetReadOptions rowset_opts = rs_opts;
            rowset_opts.stats = _parallel_rowset_stats.back().get();
            auto task = std::make_shared<std::packaged_task<StatusOr<std::vector<ChunkIteratorPtr>>()>>(
                    [&, rowset_idx, rowset_opts]() {
#ifdef BE_TEST
                        Status injected_st;
                        TEST_SYNC_POINT_CALLBACK("TabletReader::get_segment_iterators::parallel_read", &injected_st);
                        if (!injected_st.ok()) {
                            return StatusOr<std::vector<ChunkIteratorPtr>>(injected_st);
                        }
#endif
                        return enhance_error_prompt(read_rowset(rowset_idx, rowset_opts));
                    });

            auto packaged_func = [task]() { (*task)(); };
            Status submit_st;
            TEST_SYNC_POINT_CALLBACK("TabletReader::get_segment_iterators::submit", &submit_st);
            if (submit_st.ok()) {
                submit_st = RuntimeEnv::GetInstance()->load_rowset_thread_pool()->submit_func(std::move(packaged_func));
            }
            if (!submit_st.ok()) {
                // Try this rowset serially if submission failed; other rowsets may still be running.
                LOG(WARNING) << "submit_func failed: " << submit_st.code_as_string()
                             << ", try to load rowset serially, rowset_id: " << rowset->id();
                ASSIGN_OR_RETURN(auto seg_iters, enhance_error_prompt(read_rowset(rowset_idx, rowset_opts)));
                rowset_iters.push_back(RowsetIterators{.iters = std::move(seg_iters)});
            } else {
                rowset_iters.push_back(RowsetIterators{.future = task->get_future()});
            }
        } else {
            ASSIGN_OR_RETURN(auto seg_iters, enhance_error_prompt(read_rowset(rowset_idx, rs_opts)));
            rowset_iters.push_back(RowsetIterators{.iters = std::move(seg_iters)});
        }
    }

    for (auto& entry : rowset_iters) {
        if (entry.future.valid()) {
            auto seg_iters_or = entry.future.get();
            if (!seg_iters_or.ok()) {
                return seg_iters_or.status();
            }
            entry.iters = std::move(seg_iters_or).value();
        }
        iters->insert(iters->end(), entry.iters.begin(), entry.iters.end());
    }
    return Status::OK();
}

Status TabletReader::init_predicates(const TabletReaderParams& params) {
    return Status::OK();
}

Status delete_predicate_column_ids(const TabletMetadataPB& metadata, const TabletSchema& schema,
                                   std::set<ColumnId>* column_ids) {
    auto keep = [&](const std::string& column_name) {
        const size_t index = schema.field_index(column_name);
        if (index < schema.num_columns()) {
            column_ids->insert(index);
        }
    };
    for (int index = 0, size = metadata.rowsets_size(); index < size; ++index) {
        const auto& rowset_metadata = metadata.rowsets(index);
        if (!rowset_metadata.has_delete_predicate()) {
            continue;
        }
        const auto& pred_pb = rowset_metadata.delete_predicate();
        for (int i = 0; i < pred_pb.binary_predicates_size(); ++i) {
            keep(pred_pb.binary_predicates(i).column_name());
        }
        for (int i = 0; i < pred_pb.is_null_predicates_size(); ++i) {
            keep(pred_pb.is_null_predicates(i).column_name());
        }
        for (int i = 0; i < pred_pb.in_predicates_size(); ++i) {
            keep(pred_pb.in_predicates(i).column_name());
        }
    }
    return Status::OK();
}

Status TabletReader::init_delete_predicates(const TabletReaderParams& params, DeletePredicates* dels) {
    if (UNLIKELY(_tablet_metadata == nullptr)) {
        return Status::InternalError("tablet metadata is null. forget or fail to call prepare()");
    }
    if (UNLIKELY(_tablet_schema == nullptr)) {
        return Status::InternalError("tablet schema is null. forget or fail to call prepare()");
    }
    OlapPredicateParser pred_parser(_tablet_schema);

    for (int index = 0, size = _tablet_metadata->rowsets_size(); index < size; ++index) {
        const auto& rowset_metadata = _tablet_metadata->rowsets(index);
        if (!rowset_metadata.has_delete_predicate()) {
            continue;
        }

        const auto& pred_pb = rowset_metadata.delete_predicate();
        std::vector<TCondition> conds;
        for (int i = 0; i < pred_pb.binary_predicates_size(); ++i) {
            const auto& binary_predicate = pred_pb.binary_predicates(i);
            TCondition cond;
            cond.__set_column_name(binary_predicate.column_name());
            auto& op = binary_predicate.op();
            if (op == "<") {
                cond.__set_condition_op("<<");
            } else if (op == ">") {
                cond.__set_condition_op(">>");
            } else {
                cond.__set_condition_op(op);
            }
            cond.condition_values.emplace_back(binary_predicate.value());
            conds.emplace_back(std::move(cond));
        }

        for (int i = 0; i < pred_pb.is_null_predicates_size(); ++i) {
            const auto& is_null_predicate = pred_pb.is_null_predicates(i);
            TCondition cond;
            cond.__set_column_name(is_null_predicate.column_name());
            cond.__set_condition_op("IS");
            cond.condition_values.emplace_back(is_null_predicate.is_not_null() ? "NOT NULL" : "NULL");
            conds.emplace_back(std::move(cond));
        }

        for (int i = 0; i < pred_pb.in_predicates_size(); ++i) {
            TCondition cond;
            const InPredicatePB& in_predicate = pred_pb.in_predicates(i);
            cond.__set_column_name(in_predicate.column_name());
            if (in_predicate.is_not_in()) {
                cond.__set_condition_op("!*=");
            } else {
                cond.__set_condition_op("*=");
            }
            for (const auto& value : in_predicate.values()) {
                cond.condition_values.push_back(value);
            }
            conds.emplace_back(std::move(cond));
        }

        ConjunctivePredicates conjunctions;
        for (const auto& cond : conds) {
            auto pred_or = pred_parser.parse_thrift_cond(cond);
            if (!pred_or.ok()) {
                if (LIKELY(!config::lake_tablet_ignore_invalid_delete_predicate)) {
                    return pred_or.status();
                } else {
                    LOG(WARNING) << "failed to parse delete condition.column_name[" << cond.column_name
                                 << "], condition_op[" << cond.condition_op << "], condition_values["
                                 << (cond.condition_values.empty() ? "<empty>" : cond.condition_values[0]) << "].";
                    continue;
                }
            }
            conjunctions.add(pred_or.value());
            // save for memory release.
            _predicate_free_list.emplace_back(pred_or.value());
        }
        if (!conjunctions.empty()) {
            dels->add(index, conjunctions);
        }
    }

    return Status::OK();
}

bool TabletReader::union_reads_ahead(const TabletReaderParams& params) const {
    // A query scan that walks all the segments of the tablet one after another, in ascending order, through the
    // union below starts the read-ahead of the next segments while it reads the current one; a split scan reads its
    // segments from several scan tasks at once already. Only the key types that always take the union branch
    // qualify, as the segments are known to reach it before they are read.
    const KeysType keys_type = _tablet_schema->keys_type();
    const bool union_branch =
            !params.sorted_by_keys_per_tablet && (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS ||
                                                  (keys_type == UNIQUE_KEYS && params.skip_aggregation));
    return union_branch && params.reader_type == ReaderType::READER_QUERY && !_need_split && _is_asc_hint &&
           params.rowid_range_option == nullptr && params.short_key_ranges_option == nullptr &&
           remote_io_scheduler_enabled() && config::lake_scan_read_ahead_segments > 0 &&
           config::remote_prefetch_window_blocks > 0;
}

Status TabletReader::init_collector(const TabletReaderParams& params) {
    _union_reads_ahead = union_reads_ahead(params);
    std::vector<ChunkIteratorPtr> seg_iters;
    RETURN_IF_ERROR(get_segment_iterators(params, &seg_iters));

    // Put each SegmentIterator into a TimedChunkIterator, if a profile is provided.
    if (params.profile != nullptr) {
        RuntimeProfile::Counter* scan_timer = params.profile->total_time_counter();
        std::vector<ChunkIteratorPtr> children;
        children.reserve(seg_iters.size());
        for (auto& seg_iter : seg_iters) {
            children.emplace_back(timed_chunk_iterator(seg_iter, scan_timer));
        }
        seg_iters.swap(children);
    }

    // If |keys_type| is UNIQUE_KEYS and |params.skip_aggregation| is true, must disable aggregate totally.
    // If |keys_type| is AGG_KEYS and |params.skip_aggregation| is true, aggregate is an optional operation.
    KeysType keys_type = _tablet_schema->keys_type();
    const auto skip_aggr = params.skip_aggregation;
    const auto select_all_keys = _schema.num_key_fields() == _tablet_schema->num_key_columns();
    DCHECK_LE(_schema.num_key_fields(), _tablet_schema->num_key_columns());

    if (seg_iters.empty()) {
        _collect_iter = new_empty_iterator(_schema, params.chunk_size);
    } else if (is_compaction(params.reader_type) && (keys_type == DUP_KEYS || keys_type == PRIMARY_KEYS)) {
        //             MergeIterator
        //                   |
        //       +-----------+-----------+
        //       |           |           |
        //     Timer        ...        Timer
        //       |           |           |
        // SegmentIterator  ...    SegmentIterator
        //
        if (_is_vertical_merge && !_is_key) {
            _collect_iter = new_mask_merge_iterator(seg_iters, _mask_buffer, _selection_buffer);
        } else {
            _collect_iter = new_heap_merge_iterator(seg_iters, keys_type == PRIMARY_KEYS);
        }
    } else if (params.sorted_by_keys_per_tablet && (keys_type == DUP_KEYS || keys_type == PRIMARY_KEYS) &&
               seg_iters.size() > 1) {
        if (params.profile != nullptr && (params.is_pipeline || params.profile->parent() != nullptr)) {
            RuntimeProfile* p;
            if (params.is_pipeline) {
                p = params.profile;
            } else {
                p = params.profile->parent()->create_child("MERGE", true, true);
            }
            RuntimeProfile::Counter* sort_timer = ADD_TIMER(p, "Sort");
            _collect_iter = new_heap_merge_iterator(seg_iters);
            _collect_iter = timed_chunk_iterator(_collect_iter, sort_timer);
        } else {
            _collect_iter = new_heap_merge_iterator(seg_iters);
        }
    } else if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS || (keys_type == UNIQUE_KEYS && skip_aggr) ||
               (select_all_keys && seg_iters.size() == 1)) {
        if (!_is_asc_hint) {
            std::reverse(seg_iters.begin(), seg_iters.end());
        }
        //             UnionIterator
        //                   |
        //       +-----------+-----------+
        //       |           |           |
        //     Timer        ...        Timer
        //       |           |           |
        // SegmentIterator  ...    SegmentIterator
        //
        // With _union_reads_ahead, each child is one segment (see union_reads_ahead()).
        if (_union_reads_ahead) {
            _collect_iter = new_union_iterator(std::move(seg_iters), config::lake_scan_read_ahead_segments,
                                               config::remote_prefetch_window_blocks);
        } else {
            _collect_iter = new_union_iterator(std::move(seg_iters));
        }
    } else if ((keys_type == AGG_KEYS || keys_type == UNIQUE_KEYS) && !skip_aggr) {
        //                 Timer
        //                   |
        //           AggregateIterator (factor = 0)
        //                   |
        //                 Timer
        //                   |
        //             MergeIterator
        //                   |
        //       +-----------+-----------+
        //       |           |           |
        //     Timer        ...        Timer
        //       |           |           |
        // SegmentIterator  ...    SegmentIterator
        //
        if (params.profile != nullptr && (params.is_pipeline || params.profile->parent() != nullptr)) {
            RuntimeProfile* p;
            if (params.is_pipeline) {
                p = params.profile;
            } else {
                p = params.profile->parent()->create_child("MERGE", true, true);
            }
            RuntimeProfile::Counter* sort_timer = ADD_TIMER(p, "Sort");
            RuntimeProfile::Counter* aggr_timer = ADD_TIMER(p, "Aggr");

            if (_is_vertical_merge && !_is_key) {
                _collect_iter = new_mask_merge_iterator(seg_iters, _mask_buffer, _selection_buffer);
            } else {
                _collect_iter = new_heap_merge_iterator(seg_iters);
            }
            _collect_iter = timed_chunk_iterator(_collect_iter, sort_timer);
            if (!_is_vertical_merge) {
                _collect_iter = new_aggregate_iterator(std::move(_collect_iter), 0);
            } else {
                _collect_iter = new_aggregate_iterator(std::move(_collect_iter), _is_key);
            }
            _collect_iter = timed_chunk_iterator(_collect_iter, aggr_timer);
        } else {
            if (_is_vertical_merge && !_is_key) {
                _collect_iter = new_mask_merge_iterator(seg_iters, _mask_buffer, _selection_buffer);
            } else {
                _collect_iter = new_heap_merge_iterator(seg_iters);
            }
            if (!_is_vertical_merge) {
                _collect_iter = new_aggregate_iterator(std::move(_collect_iter), 0);
            } else {
                _collect_iter = new_aggregate_iterator(std::move(_collect_iter), _is_key);
            }
        }
    } else if (keys_type == AGG_KEYS) {
        CHECK(skip_aggr);
        //                 Timer
        //                   |
        //          AggregateIterator (factor = config::pre_aggregate_factor)
        //                   |
        //                 Timer
        //                   |
        //             UnionIterator
        //                   |
        //       +-----------+-----------+
        //       |           |           |
        //     Timer        ...        Timer
        //       |           |           |
        // SegmentIterator  ...    SegmentIterator
        //
        int f = config::pre_aggregate_factor;
        if (params.profile != nullptr && (params.is_pipeline || params.profile->parent() != nullptr)) {
            RuntimeProfile* p;
            if (params.is_pipeline) {
                p = params.profile;
            } else {
                p = params.profile->parent()->create_child("MERGE", true, true);
            }
            RuntimeProfile::Counter* union_timer = ADD_TIMER(p, "Union");
            RuntimeProfile::Counter* aggr_timer = ADD_TIMER(p, "Aggr");

            _collect_iter = new_union_iterator(std::move(seg_iters));
            _collect_iter = timed_chunk_iterator(_collect_iter, union_timer);
            _collect_iter = new_aggregate_iterator(std::move(_collect_iter), f);
            _collect_iter = timed_chunk_iterator(_collect_iter, aggr_timer);
        } else {
            _collect_iter = new_union_iterator(std::move(seg_iters));
            _collect_iter = new_aggregate_iterator(std::move(_collect_iter), f);
        }
    } else {
        return Status::InternalError("Unknown keys type");
    }

    if (_collect_iter != nullptr) {
        RETURN_IF_ERROR(_collect_iter->init_encoded_schema(*params.global_dictmaps));
        RETURN_IF_ERROR(_collect_iter->init_output_schema(*params.unused_output_column_ids));
    }

    return Status::OK();
}

// convert an OlapTuple to SeekTuple.
Status TabletReader::to_seek_tuple(const TabletSchema& tablet_schema, const OlapTuple& input, SeekTuple* tuple,
                                   MemPool* mempool) {
    const TypeInfoAllocator* allocator = nullptr;
    TypeInfoAllocator type_info_allocator;
    if (mempool != nullptr) {
        type_info_allocator = make_type_info_allocator(mempool);
        allocator = &type_info_allocator;
    }

    Schema schema;
    std::vector<Datum> values;
    values.reserve(input.size());
    const auto& sort_key_idxes = tablet_schema.sort_key_idxes();
    DCHECK(sort_key_idxes.empty() || sort_key_idxes.size() >= input.size());

    if (sort_key_idxes.size() > 0) {
        for (auto idx : sort_key_idxes) {
            schema.append_sort_key_idx(idx);
        }
    }

    for (size_t i = 0; i < input.size(); i++) {
        int idx = sort_key_idxes.empty() ? i : sort_key_idxes[i];
        auto f = std::make_shared<Field>(StorageSchemaHelper::convert_field(idx, tablet_schema.column(idx)));
        schema.append(f);
        values.emplace_back();
        if (input.is_null(i)) {
            continue;
        }
        // If the type of the storage level is CHAR,
        // we treat it as VARCHAR, because the execution level CHAR is VARCHAR
        // CHAR type strings are truncated at the storage level after '\0'.
        if (f->type()->type() == TYPE_CHAR) {
            RETURN_IF_ERROR(datum_from_string(get_type_info(TYPE_VARCHAR).get(), &values.back(), input.get_value(i),
                                              allocator));
        } else {
            RETURN_IF_ERROR(datum_from_string(f->type().get(), &values.back(), input.get_value(i), allocator));
        }
    }
    *tuple = SeekTuple(std::move(schema), std::move(values));
    return Status::OK();
}

// convert vector<OlapTuple> to vector<SeekRange>
Status TabletReader::parse_seek_range(const TabletSchema& tablet_schema,
                                      TabletReaderParams::RangeStartOperation range_start_op,
                                      TabletReaderParams::RangeEndOperation range_end_op,
                                      const std::vector<OlapTuple>& range_start_key,
                                      const std::vector<OlapTuple>& range_end_key, std::vector<SeekRange>* ranges,
                                      MemPool* mempool) {
    if (range_start_key.empty()) {
        return {};
    }

    bool lower_inclusive = range_start_op == TabletReaderParams::RangeStartOperation::GE ||
                           range_start_op == TabletReaderParams::RangeStartOperation::EQ;
    bool upper_inclusive = range_end_op == TabletReaderParams::RangeEndOperation::LE ||
                           range_end_op == TabletReaderParams::RangeEndOperation::EQ;

    CHECK_EQ(range_start_key.size(), range_end_key.size());
    size_t n = range_start_key.size();
    ranges->reserve(n);
    for (size_t i = 0; i < n; i++) {
        SeekTuple lower;
        SeekTuple upper;
        RETURN_IF_ERROR(to_seek_tuple(tablet_schema, range_start_key[i], &lower, mempool));
        RETURN_IF_ERROR(to_seek_tuple(tablet_schema, range_end_key[i], &upper, mempool));
        ranges->emplace_back(std::move(lower), std::move(upper));
        ranges->back().set_inclusive_lower(lower_inclusive);
        ranges->back().set_inclusive_upper(upper_inclusive);
    }
    return Status::OK();
}

} // namespace starrocks::lake
