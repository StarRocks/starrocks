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

#include <future>
#include <set>
#include <thread>
#include <utility>

#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/datum_convert.h"
#include "common/config_ingest_fwd.h"
#include "common/config_json_flat_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/runtime_profile.h"
#include "common/status.h"
#include "fs/fs_factory.h"
#include "gutil/stl_util.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/aggregate_iterator.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/conjunctive_predicates.h"
#include "storage/empty_iterator.h"
#include "storage/lake/rowset.h"
#include "storage/lake/utils.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/merge_iterator.h"
#include "storage/predicate_parser.h"
#include "storage/row_source_mask.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/seek_range.h"
#include "storage/short_key_index.h"
#include "storage/tablet_schema_map.h"
#include "storage/type_info_allocator_adapter.h"
#include "storage/types.h"
#include "storage/union_iterator.h"
#include "util/json_flattener.h"

namespace starrocks::lake {

using ConjunctivePredicates = starrocks::ConjunctivePredicates;
using Datum = starrocks::Datum;
using Field = starrocks::Field;
using PredicateParser = starrocks::PredicateParser;
using ZonemapPredicatesRewriter = starrocks::ZonemapPredicatesRewriter;

namespace {

struct AdaptiveSplitStats {
    size_t segment_prepare_tasks = 0;
    size_t prepared_segments = 0;
    size_t direct_execute_segments = 0;
    size_t split_execute_segments = 0;
    size_t fully_pruned_segments = 0;
    size_t emitted_child_tasks = 0;
    size_t matched_rows = 0;
};

enum class LakeSplitPlanMode {
    DIRECT_BASELINE = 0,
    PREPARED_PHYSICAL_SPLIT = 1,
};

enum class AdaptiveSegmentPrepareDecision {
    USE = 0,
    SKIP_NO_SEGMENT = 1,
};

struct LakeSplitPlan {
    LakeSplitPlanMode mode = LakeSplitPlanMode::DIRECT_BASELINE;
    AdaptiveSegmentPrepareDecision decision = AdaptiveSegmentPrepareDecision::SKIP_NO_SEGMENT;
    size_t segment_count = 0;

    bool use_prepared_physical_split() const { return mode == LakeSplitPlanMode::PREPARED_PHYSICAL_SPLIT; }
};

size_t estimate_fanout(size_t rows, size_t split_rows) {
    if (rows == 0 || split_rows == 0) {
        return 0;
    }
    return (rows + split_rows - 1) / split_rows;
}

size_t count_adaptive_split_segments(const std::vector<std::shared_ptr<Rowset>>& rowsets) {
    size_t segment_count = 0;
    for (const auto& rowset : rowsets) {
        if (rowset == nullptr || rowset->num_rows() == 0) {
            continue;
        }
        segment_count += rowset->num_segments();
    }
    return segment_count;
}

LakeSplitPlan make_lake_split_plan(bool could_split_physically, const std::vector<std::shared_ptr<Rowset>>& rowsets) {
    LakeSplitPlan plan;
    if (!could_split_physically || !config::enable_lake_index_pruned_physical_split) {
        return plan;
    }

    plan.segment_count = count_adaptive_split_segments(rowsets);
    if (plan.segment_count == 0) {
        plan.decision = AdaptiveSegmentPrepareDecision::SKIP_NO_SEGMENT;
        return plan;
    }
    plan.decision = AdaptiveSegmentPrepareDecision::USE;
    plan.mode = LakeSplitPlanMode::PREPARED_PHYSICAL_SPLIT;
    return plan;
}

const char* adaptive_segment_prepare_decision_name(AdaptiveSegmentPrepareDecision decision) {
    switch (decision) {
    case AdaptiveSegmentPrepareDecision::USE:
        return "use";
    case AdaptiveSegmentPrepareDecision::SKIP_NO_SEGMENT:
        return "skip_no_segment";
    }
    return "unknown";
}

void record_adaptive_segment_prepare_profile(RuntimeProfile* profile, AdaptiveSegmentPrepareDecision decision,
                                             size_t segment_count, size_t segment_prepare_tasks) {
    if (profile == nullptr) {
        return;
    }

    COUNTER_UPDATE(ADD_COUNTER(profile, "LakeAdaptiveSegmentPrepareSegmentCount", TUnit::UNIT),
                   static_cast<int64_t>(segment_count));
    COUNTER_UPDATE(ADD_COUNTER(profile, "LakeAdaptiveSegmentPrepareScheduledTasks", TUnit::UNIT),
                   static_cast<int64_t>(segment_prepare_tasks));

    switch (decision) {
    case AdaptiveSegmentPrepareDecision::USE:
        COUNTER_UPDATE(ADD_COUNTER(profile, "LakeAdaptiveSegmentPrepareUsed", TUnit::UNIT), 1);
        break;
    case AdaptiveSegmentPrepareDecision::SKIP_NO_SEGMENT:
        COUNTER_UPDATE(ADD_COUNTER(profile, "LakeAdaptiveSegmentPrepareSkipNoSegment", TUnit::UNIT), 1);
        break;
    }

    profile->add_info_string("LakeAdaptiveSegmentPrepareLastDecision",
                             adaptive_segment_prepare_decision_name(decision));
}

Schema build_execution_pruning_schema(const TabletSchemaCSPtr& tablet_schema, const SegmentReadOptions& options) {
    std::set<ColumnId> column_ids;
    if (tablet_schema != nullptr) {
        for (ColumnId cid = 0; cid < tablet_schema->num_columns(); ++cid) {
            if (options.pred_tree_for_zone_map.contains_column(cid)) {
                column_ids.emplace(cid);
            }
        }
    }
    options.delete_predicates.get_column_ids(&column_ids);
    if (column_ids.empty()) {
        return Schema();
    }
    return ChunkHelper::convert_schema(tablet_schema, std::vector<ColumnId>(column_ids.begin(), column_ids.end()));
}

Status prepare_segment_execution_pruned_scan_range(const TabletSchemaCSPtr& tablet_schema, const SegmentPtr& segment,
                                                   SegmentReadOptions* options,
                                                   const PreparedSegmentReadStatePtr& pruning_state) {
    DCHECK(options != nullptr);
    DCHECK(pruning_state != nullptr);
    if (pruning_state == nullptr || options == nullptr || !options->short_key_ranges.empty()) {
        return Status::OK();
    }

    if (options->ranges.empty()) {
        pruning_state->seek_range_rowid_bounds.clear();
    } else {
        ASSIGN_OR_RETURN(pruning_state->seek_range_rowid_bounds,
                         get_segment_rowid_ranges_by_seek_ranges(segment, options->ranges, options->lake_io_opts));
    }
    pruning_state->seek_range_rowid_bounds_ready.store(true, std::memory_order_release);
    if (options->tablet_range.has_value() && !options->tablet_range->all_range()) {
        ASSIGN_OR_RETURN(
                pruning_state->tablet_range_rowid_range,
                get_segment_rowid_range_by_seek_range(segment, options->tablet_range.value(), options->lake_io_opts));
    } else {
        pruning_state->tablet_range_rowid_range.reset();
    }
    pruning_state->tablet_range_rowid_range_ready.store(true, std::memory_order_release);

    options->cached_seek_range_rowid_bounds = &pruning_state->seek_range_rowid_bounds;
    options->cached_tablet_range_rowid = &pruning_state->tablet_range_rowid_range;

    const Schema schema = build_execution_pruning_schema(tablet_schema, *options);
    ASSIGN_OR_RETURN(auto range, new_segment_iterator_for_prepare_pruning(segment, schema, *options));
    pruning_state->execution_pruned_range = std::make_shared<SparseRange<>>(std::move(range));
    return Status::OK();
}

Status init_lake_segment_read_options(const TabletSchemaCSPtr& tablet_schema, int64_t tablet_id, bool is_asc_hint,
                                      OlapReaderStatistics* stats, const TabletReaderParams& read_params,
                                      const RowsetReadOptions& rs_opts, const LakeIOOptions& lake_io_opts,
                                      const std::shared_ptr<Rowset>& rowset, size_t segment_idx,
                                      const DisjunctivePredicates& delete_preds, SegmentReadOptions* seg_options) {
    seg_options->fs = lake_io_opts.fs;
    seg_options->stats = stats;
    seg_options->ranges = rs_opts.ranges;
    seg_options->tablet_range = std::nullopt;
    seg_options->pred_tree = rs_opts.pred_tree;
    seg_options->pred_tree_for_zone_map = rs_opts.pred_tree_for_zone_map;
    seg_options->delete_predicates = delete_preds;
    seg_options->use_page_cache = rs_opts.use_page_cache;
    seg_options->profile = rs_opts.profile;
    seg_options->global_dictmaps = rs_opts.global_dictmaps;
    seg_options->runtime_range_pruner = rs_opts.runtime_range_pruner;
    seg_options->tablet_schema = tablet_schema;
    seg_options->lake_io_opts = lake_io_opts;
    seg_options->asc_hint = is_asc_hint;
    seg_options->tablet_id = tablet_id;
    seg_options->rowset_id = rowset->metadata().id();
    seg_options->rowsetid = rowset->rowset_id();
    seg_options->reader_type = read_params.reader_type;
    seg_options->chunk_size = read_params.chunk_size;
    seg_options->enable_join_runtime_filter_pushdown = rs_opts.enable_join_runtime_filter_pushdown;
    seg_options->enable_predicate_col_late_materialize = rs_opts.enable_predicate_col_late_materialize;
    seg_options->column_access_paths = read_params.column_access_paths;
    seg_options->is_first_split_of_segment = true;

    ASSIGN_OR_RETURN(auto shared_segment_range, rowset->get_shared_segment_seek_range());
    if (segment_idx < static_cast<size_t>(rowset->metadata().shared_segments_size()) &&
        rowset->metadata().shared_segments(segment_idx) && shared_segment_range.has_value()) {
        seg_options->tablet_range = *shared_segment_range;
    }
    return Status::OK();
}

std::unique_ptr<pipeline::LakeSplitContext> make_segment_prepare_context(
        const PreparedTabletReadStatePtr& prepared_read_state,
        const PreparedSegmentReadStatePtr& prepared_segment_state, size_t rowset_idx, size_t segment_idx) {
    auto ctx = std::make_unique<pipeline::LakeSplitContext>();
    ctx->task_type = pipeline::LakeSplitContext::TaskType::SEGMENT_PREPARE;
    ctx->prepared_read_state = prepared_read_state;
    ctx->prepared_segment_state = prepared_segment_state;
    ctx->rowset_index = rowset_idx;
    ctx->segment_index = segment_idx;
    return ctx;
}

std::unique_ptr<pipeline::LakeSplitContext> make_segment_seed_context(
        RowidRangeOptionPtr rowid_range, const PreparedTabletReadStatePtr& prepared_read_state,
        const PreparedSegmentReadStatePtr& prepared_segment_state, size_t rowset_idx, size_t segment_idx) {
    auto ctx = std::make_unique<pipeline::LakeSplitContext>();
    ctx->task_type = pipeline::LakeSplitContext::TaskType::SEGMENT_PREPARE;
    ctx->adaptive_task_source = pipeline::LakeSplitContext::AdaptiveTaskSource::SEED_LOCAL;
    ctx->rowid_range = std::move(rowid_range);
    ctx->prepared_read_state = prepared_read_state;
    ctx->prepared_segment_state = prepared_segment_state;
    ctx->rowset_index = rowset_idx;
    ctx->segment_index = segment_idx;
    return ctx;
}

std::unique_ptr<pipeline::LakeSplitContext> make_physical_split_context(
        RowidRangeOptionPtr rowid_range, const PreparedTabletReadStatePtr& prepared_read_state,
        const PreparedSegmentReadStatePtr& prepared_segment_state, size_t rowset_idx, size_t segment_idx,
        pipeline::LakeSplitContext::AdaptiveTaskSource adaptive_task_source =
                pipeline::LakeSplitContext::AdaptiveTaskSource::REFINED_CHILD) {
    auto ctx = std::make_unique<pipeline::LakeSplitContext>();
    ctx->task_type = pipeline::LakeSplitContext::TaskType::PHYSICAL_SPLIT;
    ctx->adaptive_task_source = adaptive_task_source;
    ctx->rowid_range = std::move(rowid_range);
    ctx->prepared_read_state = prepared_read_state;
    ctx->prepared_segment_state = prepared_segment_state;
    ctx->rowset_index = rowset_idx;
    ctx->segment_index = segment_idx;
    return ctx;
}

rowid_t lower_bound_block_aligned_rowid(Segment* segment, const SeekTuple& key, bool lower) {
    std::string index_key =
            key.short_key_encode(segment->num_short_keys(), lower ? KEY_MINIMAL_MARKER : KEY_MAXIMAL_MARKER);
    uint32_t start_block_id;
    auto start_iter = segment->lower_bound(index_key);
    if (start_iter.valid()) {
        // The previous block may still contain matching keys, so start from that block boundary.
        start_block_id = start_iter.ordinal();
        if (start_block_id > 0) {
            start_block_id--;
        }
    } else {
        start_block_id = segment->last_block();
    }
    return start_block_id * segment->num_rows_per_block();
}

rowid_t upper_bound_block_aligned_rowid(Segment* segment, const SeekTuple& key, bool lower, rowid_t end) {
    std::string index_key =
            key.short_key_encode(segment->num_short_keys(), lower ? KEY_MINIMAL_MARKER : KEY_MAXIMAL_MARKER);
    auto end_iter = segment->upper_bound(index_key);
    if (end_iter.valid()) {
        end = end_iter.ordinal() * segment->num_rows_per_block();
    }
    return end;
}

StatusOr<SparseRange<>> build_block_aligned_seek_scan_range(const SegmentPtr& segment,
                                                            const std::vector<SeekRange>& ranges,
                                                            const LakeIOOptions& lake_io_opts) {
    SparseRange<> scan_range;
    if (ranges.empty()) {
        scan_range.add(Range<>(0, segment->num_rows()));
        return scan_range;
    }

    RETURN_IF_ERROR(segment->load_index(lake_io_opts));
    for (const auto& range : ranges) {
        rowid_t lower_rowid = 0;
        rowid_t upper_rowid = segment->num_rows();
        if (!range.upper().empty()) {
            upper_rowid = upper_bound_block_aligned_rowid(segment.get(), range.upper(), !range.inclusive_upper(),
                                                          segment->num_rows());
        }
        if (!range.lower().empty() && upper_rowid > 0) {
            lower_rowid = lower_bound_block_aligned_rowid(segment.get(), range.lower(), range.inclusive_lower());
        }
        if (lower_rowid <= upper_rowid) {
            scan_range.add(Range<>(lower_rowid, upper_rowid));
        }
    }
    return scan_range;
}

StatusOr<SparseRange<>> build_adaptive_coarse_scan_range(const SegmentPtr& segment,
                                                         const SegmentReadOptions& seg_options) {
    ASSIGN_OR_RETURN(auto scan_range,
                     build_block_aligned_seek_scan_range(segment, seg_options.ranges, seg_options.lake_io_opts));
    if (seg_options.tablet_range.has_value() && !seg_options.tablet_range->all_range()) {
        ASSIGN_OR_RETURN(auto tablet_scan_range,
                         build_block_aligned_seek_scan_range(segment, {seg_options.tablet_range.value()},
                                                             seg_options.lake_io_opts));
        scan_range &= tablet_scan_range;
    }
    return scan_range;
}

void reset_adaptive_segment_issue_state(const PreparedSegmentReadStatePtr& prepared_segment_state,
                                        const SparseRange<>& coarse_scan_range) {
    std::lock_guard<std::mutex> guard(prepared_segment_state->adaptive_issue_lock);
    prepared_segment_state->adaptive_coarse_scan_range = coarse_scan_range;
    prepared_segment_state->adaptive_coarse_scan_range_iter =
            prepared_segment_state->adaptive_coarse_scan_range.new_iterator();
    prepared_segment_state->adaptive_issued_coarse_ranges.clear();
    prepared_segment_state->adaptive_coarse_scan_range_ready = true;
    prepared_segment_state->adaptive_pending_issue_closed = false;
    prepared_segment_state->adaptive_first_issued_split = true;
    prepared_segment_state->execution_pruned_range.reset();
    prepared_segment_state->seek_range_rowid_bounds.clear();
    prepared_segment_state->tablet_range_rowid_range.reset();
    prepared_segment_state->seek_range_rowid_bounds_ready.store(false, std::memory_order_release);
    prepared_segment_state->tablet_range_rowid_range_ready.store(false, std::memory_order_release);
    prepared_segment_state->final_pruned_rows = 0;
    prepared_segment_state->estimated_fanout = 0;
    prepared_segment_state->prepare_status = Status::OK();
    prepared_segment_state->lifecycle.store(static_cast<uint32_t>(PreparedSegmentReadState::Lifecycle::UNPREPARED),
                                            std::memory_order_release);
}

bool issue_one_adaptive_coarse_task(const PreparedSegmentReadStatePtr& prepared_segment_state, Rowset* rowset,
                                    Segment* segment, const TabletReaderParams& read_params,
                                    RowidRangeOptionPtr* rowid_range) {
    *rowid_range = nullptr;
    std::lock_guard<std::mutex> guard(prepared_segment_state->adaptive_issue_lock);
    if (!prepared_segment_state->adaptive_coarse_scan_range_ready ||
        prepared_segment_state->adaptive_pending_issue_closed ||
        !prepared_segment_state->adaptive_coarse_scan_range_iter.has_more()) {
        return false;
    }

    auto result = std::make_shared<RowidRangeOption>();
    size_t num_taken_rows = 0;
    while (prepared_segment_state->adaptive_coarse_scan_range_iter.has_more() &&
           num_taken_rows < read_params.splitted_scan_rows) {
        size_t remaining_in_segment = prepared_segment_state->adaptive_coarse_scan_range_iter.remaining_rows();
        size_t rows_to_take = std::min<size_t>(read_params.splitted_scan_rows - num_taken_rows, remaining_in_segment);
        if (remaining_in_segment > rows_to_take &&
            remaining_in_segment - rows_to_take < read_params.splitted_scan_rows) {
            rows_to_take = remaining_in_segment;
        }

        SparseRange<> taken_range;
        prepared_segment_state->adaptive_coarse_scan_range_iter.next_range(rows_to_take, &taken_range);
        if (taken_range.span_size() == 0) {
            break;
        }
        num_taken_rows += taken_range.span_size();
        prepared_segment_state->adaptive_issued_coarse_ranges |= taken_range;
        result->add(rowset, segment, std::make_shared<SparseRange<>>(std::move(taken_range)),
                    prepared_segment_state->adaptive_first_issued_split);
        prepared_segment_state->adaptive_first_issued_split = false;
    }

    if (result->rowid_range_per_segment_per_rowset.empty()) {
        return false;
    }
    *rowid_range = std::move(result);
    return true;
}

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

void split_segment_scan_range(Rowset* rowset, Segment* segment, const SparseRange<>& scan_range,
                              const TabletReaderParams& read_params,
                              const PreparedTabletReadStatePtr& prepared_read_state,
                              const PreparedSegmentReadStatePtr& prepared_segment_state, size_t rowset_idx,
                              size_t segment_idx, bool initial_is_first_split_of_segment,
                              RowidRangeOptionPtr* current_task,
                              std::vector<pipeline::ScanSplitContextPtr>* split_tasks, AdaptiveSplitStats* stats) {
    bool is_first_split_of_segment = initial_is_first_split_of_segment;
    auto range_iter = scan_range.new_iterator();
    while (range_iter.has_more()) {
        auto rowid_range = std::make_shared<RowidRangeOption>();
        size_t num_taken_rows = 0;
        while (range_iter.has_more() && num_taken_rows < read_params.splitted_scan_rows) {
            size_t remaining_in_segment = range_iter.remaining_rows();
            size_t rows_to_take =
                    std::min<size_t>(read_params.splitted_scan_rows - num_taken_rows, remaining_in_segment);
            if (remaining_in_segment > rows_to_take &&
                remaining_in_segment - rows_to_take < read_params.splitted_scan_rows) {
                rows_to_take = remaining_in_segment;
            }

            SparseRange<> taken_range;
            range_iter.next_range(rows_to_take, &taken_range);
            if (taken_range.span_size() == 0) {
                break;
            }
            num_taken_rows += taken_range.span_size();
            rowid_range->add(rowset, segment, std::make_shared<SparseRange<>>(std::move(taken_range)),
                             is_first_split_of_segment);
            is_first_split_of_segment = false;
        }

        if (rowid_range->rowid_range_per_segment_per_rowset.empty()) {
            continue;
        }
        if (*current_task == nullptr) {
            *current_task = std::move(rowid_range);
            continue;
        }
        split_tasks->emplace_back(make_physical_split_context(std::move(rowid_range), prepared_read_state,
                                                              prepared_segment_state, rowset_idx, segment_idx));
        ++stats->emitted_child_tasks;
    }
}

} // namespace

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
                           std::shared_ptr<const TabletSchema> tablet_schema)
        : ChunkIterator(std::move(schema)),
          _tablet_mgr(tablet_mgr),
          _tablet_metadata(std::move(metadata)),
          _tablet_schema(std::move(tablet_schema)),
          _rowsets_inited(true),
          _rowsets(std::move(rowsets)),
          _is_vertical_merge(true),
          _is_key(is_key),
          _mask_buffer(mask_buffer) {
    DCHECK(_mask_buffer);
}

TabletReader::~TabletReader() {
    close();
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
    _stats.rowsets_read_count += _rowsets.size();
    return Status::OK();
}

Status TabletReader::open(const TabletReaderParams& read_params) {
    if (read_params.reader_type != ReaderType::READER_QUERY && read_params.reader_type != ReaderType::READER_CHECKSUM &&
        read_params.reader_type != ReaderType::READER_ALTER_TABLE && !is_compaction(read_params.reader_type) &&
        read_params.reader_type != ReaderType::READER_BYPASS_QUERY) {
        return Status::NotSupported("reader type not supported now");
    }
    RETURN_IF_ERROR(init_compaction_column_paths(read_params));
    if (_collect_iter != nullptr) {
        _collect_iter->close();
        _collect_iter.reset();
    }

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

        pipeline::Morsels morsels;
        morsels.emplace_back(
                std::make_unique<pipeline::ScanMorsel>(read_params.plan_node_id, *(read_params.scan_range)));

        std::shared_ptr<pipeline::SplitMorselQueue> split_morsel_queue = nullptr;

        if (_could_split_physically) {
            auto split_plan = make_lake_split_plan(_could_split_physically, _rowsets);
            if (read_params.enable_lake_adaptive_split_morsel_queue && split_plan.use_prepared_physical_split()) {
                return _build_lake_adaptive_split_seed_tasks(read_params, split_plan.segment_count);
            }
            if (config::enable_lake_index_pruned_physical_split) {
                if (split_plan.use_prepared_physical_split()) {
                    auto st = _build_prepared_physical_split_tasks(read_params, split_plan.segment_count);
                    if (st.ok()) {
                        return Status::OK();
                    }
                    LOG(WARNING) << "failed to build adaptive Lake split tasks, fallback to generic physical split"
                                 << ", query_id: " << print_id(read_params.runtime_state->query_id())
                                 << ", tablet_id: " << tablet_shared_ptr->tablet_id()
                                 << ", rowsets: " << _rowsets.size() << ", start_keys: " << read_params.start_key.size()
                                 << ", end_keys: " << read_params.end_key.size()
                                 << ", range_start_op: " << read_params.range
                                 << ", range_end_op: " << read_params.end_range
                                 << ", splitted_scan_rows: " << read_params.splitted_scan_rows
                                 << ", lake_io_has_fs: " << (read_params.lake_io_opts.fs != nullptr)
                                 << ", lake_io_has_location_provider: "
                                 << (read_params.lake_io_opts.location_provider != nullptr) << ", status: " << st;
                    _split_tasks.clear();
                } else {
                    record_adaptive_segment_prepare_profile(read_params.profile, split_plan.decision,
                                                            split_plan.segment_count, 0);
                }
            }
            split_morsel_queue = std::make_shared<pipeline::PhysicalSplitMorselQueue>(
                    std::move(morsels), read_params.scan_dop, read_params.splitted_scan_rows);
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
                _split_tasks.emplace_back(std::move(ctx));
            } else {
                break;
            }
        }

    } else {
        return init_collector(read_params);
    }

    return Status::OK();
}

Status TabletReader::_build_prepared_physical_split_tasks(const TabletReaderParams& read_params, size_t segment_count) {
    LakeIOOptions lake_io_opts = read_params.lake_io_opts;
    if (lake_io_opts.fs == nullptr) {
        auto root_loc = _tablet_mgr->tablet_root_location(_tablet_metadata->id());
        ASSIGN_OR_RETURN(lake_io_opts.fs, FileSystemFactory::CreateSharedFromString(root_loc));
    }
    if (lake_io_opts.location_provider == nullptr) {
        lake_io_opts.location_provider = _tablet_mgr->location_provider();
    }

    PreparedReadStatePtr prepared_state =
            _prepared_read_state != nullptr ? _prepared_read_state : std::make_shared<PreparedReadState>();
    RETURN_IF_ERROR(build_prepared_read_state(read_params, prepared_state.get()));

    _split_tasks.clear();
    AdaptiveSplitStats stats;
    for (size_t rowset_idx = 0; rowset_idx < prepared_state->rowsets.size(); ++rowset_idx) {
        const auto& segments = prepared_state->rowset_segments[rowset_idx];
        const auto& prepared_segments = prepared_state->rowset_prepared_states[rowset_idx];
        for (size_t segment_idx = 0; segment_idx < segments.size(); ++segment_idx) {
            const auto& segment = segments[segment_idx];
            if (segment == nullptr || segment->num_rows() == 0) {
                continue;
            }
            _split_tasks.emplace_back(make_segment_prepare_context(prepared_state, prepared_segments[segment_idx],
                                                                   rowset_idx, segment_idx));
            ++stats.segment_prepare_tasks;
        }
    }

    LOG(INFO) << "Lake adaptive physical split scheduled segment-prepare tasks"
              << ", query_id: " << print_id(read_params.runtime_state->query_id())
              << ", tablet_id: " << _tablet_metadata->id() << ", segment_count: " << segment_count
              << ", split_tasks: " << stats.segment_prepare_tasks;
    record_adaptive_segment_prepare_profile(read_params.profile, AdaptiveSegmentPrepareDecision::USE, segment_count,
                                            stats.segment_prepare_tasks);
    return Status::OK();
}

Status TabletReader::_build_lake_adaptive_split_seed_tasks(const TabletReaderParams& read_params,
                                                           size_t segment_count) {
    PreparedReadStatePtr prepared_state =
            _prepared_read_state != nullptr ? _prepared_read_state : std::make_shared<PreparedReadState>();
    RETURN_IF_ERROR(build_prepared_read_state(read_params, prepared_state.get()));
    RowsetReadOptions rs_opts;
    RETURN_IF_ERROR(init_rowset_read_options(read_params, &rs_opts));
    LakeIOOptions lake_io_opts = read_params.lake_io_opts;
    if (lake_io_opts.fs == nullptr) {
        auto root_loc = _tablet_mgr->tablet_root_location(_tablet_metadata->id());
        ASSIGN_OR_RETURN(lake_io_opts.fs, FileSystemFactory::CreateSharedFromString(root_loc));
    }
    if (lake_io_opts.location_provider == nullptr) {
        lake_io_opts.location_provider = _tablet_mgr->location_provider();
    }
    rs_opts.lake_io_opts = lake_io_opts;

    _split_tasks.clear();
    AdaptiveSplitStats stats;
    for (size_t rowset_idx = 0; rowset_idx < prepared_state->rowsets.size(); ++rowset_idx) {
        auto& rowset = prepared_state->rowsets[rowset_idx];
        const auto& segments = prepared_state->rowset_segments[rowset_idx];
        const auto& prepared_segments = prepared_state->rowset_prepared_states[rowset_idx];
        const auto delete_preds = rs_opts.delete_predicates != nullptr
                                          ? rs_opts.delete_predicates->get_predicates(rowset_idx)
                                          : DisjunctivePredicates{};
        for (size_t segment_idx = 0; segment_idx < segments.size(); ++segment_idx) {
            const auto& segment = segments[segment_idx];
            const auto& segment_state = prepared_segments[segment_idx];
            if (segment == nullptr || segment->num_rows() == 0 || segment_state == nullptr) {
                continue;
            }
            SegmentReadOptions seg_options;
            RETURN_IF_ERROR(init_lake_segment_read_options(_tablet_schema, _tablet_metadata->id(), _is_asc_hint,
                                                           &_stats, read_params, rs_opts, lake_io_opts, rowset,
                                                           segment_idx, delete_preds, &seg_options));
            ASSIGN_OR_RETURN(auto coarse_scan_range, build_adaptive_coarse_scan_range(segment, seg_options));
            if (coarse_scan_range.span_size() == 0) {
                continue;
            }

            reset_adaptive_segment_issue_state(segment_state, coarse_scan_range);
            RowidRangeOptionPtr seed_rowid_range;
            if (!issue_one_adaptive_coarse_task(segment_state, rowset.get(), segment.get(), read_params,
                                                &seed_rowid_range)) {
                continue;
            }
            _split_tasks.emplace_back(make_segment_seed_context(std::move(seed_rowid_range), prepared_state,
                                                                segment_state, rowset_idx, segment_idx));
            ++stats.segment_prepare_tasks;
        }
    }

    LOG(INFO) << "Lake adaptive split scheduled segment seed tasks"
              << ", query_id: " << print_id(read_params.runtime_state->query_id())
              << ", tablet_id: " << _tablet_metadata->id() << ", segment_count: " << segment_count
              << ", split_tasks: " << stats.segment_prepare_tasks;
    record_adaptive_segment_prepare_profile(read_params.profile, AdaptiveSegmentPrepareDecision::USE, segment_count,
                                            stats.segment_prepare_tasks);
    return Status::OK();
}

Status TabletReader::_prepare_segment_split_task(const TabletReaderParams& read_params,
                                                 const pipeline::LakeSplitContext* split_context,
                                                 RowidRangeOptionPtr* local_rowid_range) {
    if (split_context == nullptr || split_context->task_type != pipeline::LakeSplitContext::TaskType::SEGMENT_PREPARE ||
        split_context->prepared_read_state == nullptr || split_context->prepared_segment_state == nullptr) {
        return Status::InvalidArgument("invalid Lake segment-prepare split context");
    }
    *local_rowid_range = nullptr;

    auto prepared_state = split_context->prepared_read_state;
    auto segment_state = split_context->prepared_segment_state;
    RETURN_IF_ERROR(build_prepared_read_state(read_params, prepared_state.get()));

    if (split_context->rowset_index >= prepared_state->rowsets.size() ||
        split_context->rowset_index >= prepared_state->rowset_segments.size() ||
        split_context->segment_index >= prepared_state->rowset_segments[split_context->rowset_index].size()) {
        return Status::InvalidArgument("Lake segment-prepare context index out of range");
    }

    RowsetReadOptions rs_opts;
    RETURN_IF_ERROR(init_rowset_read_options(read_params, &rs_opts));
    LakeIOOptions lake_io_opts = read_params.lake_io_opts;
    if (lake_io_opts.fs == nullptr) {
        auto root_loc = _tablet_mgr->tablet_root_location(_tablet_metadata->id());
        ASSIGN_OR_RETURN(lake_io_opts.fs, FileSystemFactory::CreateSharedFromString(root_loc));
    }
    if (lake_io_opts.location_provider == nullptr) {
        lake_io_opts.location_provider = _tablet_mgr->location_provider();
    }
    rs_opts.lake_io_opts = lake_io_opts;

    auto& rowset = prepared_state->rowsets[split_context->rowset_index];
    auto& segment = prepared_state->rowset_segments[split_context->rowset_index][split_context->segment_index];
    if (segment == nullptr || segment->num_rows() == 0) {
        return Status::OK();
    }

    auto lifecycle =
            static_cast<PreparedSegmentReadState::Lifecycle>(segment_state->lifecycle.load(std::memory_order_acquire));
    if (lifecycle == PreparedSegmentReadState::Lifecycle::FAILED) {
        return segment_state->prepare_status;
    }

    uint32_t expected = static_cast<uint32_t>(PreparedSegmentReadState::Lifecycle::UNPREPARED);
    if (segment_state->lifecycle.compare_exchange_strong(
                expected, static_cast<uint32_t>(PreparedSegmentReadState::Lifecycle::PREPARING),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
        auto fail_prepare = [&](const Status& st) -> Status {
            {
                std::lock_guard<std::mutex> guard(segment_state->adaptive_issue_lock);
                segment_state->adaptive_pending_issue_closed = true;
            }
            segment_state->prepare_status = st;
            segment_state->lifecycle.store(static_cast<uint32_t>(PreparedSegmentReadState::Lifecycle::FAILED),
                                           std::memory_order_release);
            return st;
        };
        const auto delete_preds = rs_opts.delete_predicates != nullptr
                                          ? rs_opts.delete_predicates->get_predicates(split_context->rowset_index)
                                          : DisjunctivePredicates{};
        SegmentReadOptions seg_options;
        RETURN_IF_ERROR(init_lake_segment_read_options(_tablet_schema, _tablet_metadata->id(), _is_asc_hint, &_stats,
                                                       read_params, rs_opts, lake_io_opts, rowset,
                                                       split_context->segment_index, delete_preds, &seg_options));

        auto st = prepare_segment_execution_pruned_scan_range(_tablet_schema, segment, &seg_options, segment_state);
        if (!st.ok()) {
            return fail_prepare(st);
        }
        {
            std::lock_guard<std::mutex> guard(segment_state->adaptive_issue_lock);
            segment_state->adaptive_pending_issue_closed = true;
        }
        if (segment_state->execution_pruned_range != nullptr) {
            segment_state->final_pruned_rows = segment_state->execution_pruned_range->span_size();
        } else {
            segment_state->final_pruned_rows = segment->num_rows();
        }
        segment_state->estimated_fanout =
                estimate_fanout(segment_state->final_pruned_rows, read_params.splitted_scan_rows);

        segment_state->prepare_status = Status::OK();
        segment_state->lifecycle.store(static_cast<uint32_t>(PreparedSegmentReadState::Lifecycle::PREPARED),
                                       std::memory_order_release);
    } else {
        lifecycle = static_cast<PreparedSegmentReadState::Lifecycle>(
                segment_state->lifecycle.load(std::memory_order_acquire));
        if (lifecycle == PreparedSegmentReadState::Lifecycle::FAILED) {
            return segment_state->prepare_status;
        }
    }

    SparseRangePtr final_scan_range = segment_state->execution_pruned_range;
    if (final_scan_range == nullptr) {
        final_scan_range = std::make_shared<SparseRange<>>(Range<>(0, segment->num_rows()));
    }

    _split_tasks.clear();
    if (final_scan_range->span_size() == 0) {
        return Status::OK();
    }

    AdaptiveSplitStats stats;
    stats.prepared_segments = 1;
    stats.matched_rows = final_scan_range->span_size();
    if (split_context->rowid_range != nullptr) {
        auto seed_split = split_context->rowid_range->get_segment_rowid_range(rowset.get(), segment.get());
        if (seed_split.row_id_range != nullptr) {
            SparseRange<> local_scan_range = *seed_split.row_id_range;
            local_scan_range &= *final_scan_range;
            if (local_scan_range.span_size() > 0) {
                auto rowid_range = std::make_shared<RowidRangeOption>();
                rowid_range->add(rowset.get(), segment.get(),
                                 std::make_shared<SparseRange<>>(std::move(local_scan_range)),
                                 seed_split.is_first_split_of_segment);
                *local_rowid_range = std::move(rowid_range);
            }
        }
    }

    SparseRange<> issued_coarse_ranges;
    {
        std::lock_guard<std::mutex> guard(segment_state->adaptive_issue_lock);
        issued_coarse_ranges = segment_state->adaptive_issued_coarse_ranges;
    }
    SparseRange<> remaining_scan_range = subtract_sparse_ranges(*final_scan_range, issued_coarse_ranges);
    RowidRangeOptionPtr current_task = *local_rowid_range;
    split_segment_scan_range(rowset.get(), segment.get(), remaining_scan_range, read_params, prepared_state,
                             segment_state, split_context->rowset_index, split_context->segment_index,
                             split_context->rowid_range == nullptr, &current_task, &_split_tasks, &stats);
    if (*local_rowid_range == nullptr && current_task != nullptr) {
        *local_rowid_range = std::move(current_task);
    }
    if (*local_rowid_range != nullptr && _split_tasks.empty()) {
        stats.direct_execute_segments = 1;
    } else if (!_split_tasks.empty()) {
        stats.split_execute_segments = 1;
    }

    LOG(INFO) << "Lake adaptive segment prepare finished"
              << ", query_id: " << print_id(read_params.runtime_state->query_id())
              << ", tablet_id: " << _tablet_metadata->id() << ", rowset_idx: " << split_context->rowset_index
              << ", segment_idx: " << split_context->segment_index
              << ", final_rows: " << segment_state->final_pruned_rows
              << ", estimated_fanout: " << segment_state->estimated_fanout
              << ", emitted_child_tasks: " << stats.emitted_child_tasks;
    return Status::OK();
}

Status TabletReader::init_compaction_column_paths(const TabletReaderParams& read_params) {
    if (!config::enable_compaction_flat_json || !is_compaction(read_params.reader_type) ||
        read_params.column_access_paths == nullptr) {
        return Status::OK();
    }

    if (!read_params.column_access_paths->empty()) {
        VLOG(3) << "Lake Compaction flat json paths exists: " << read_params.column_access_paths->size();
        return Status::OK();
    }

    DCHECK(is_compaction(read_params.reader_type) && read_params.column_access_paths != nullptr &&
           read_params.column_access_paths->empty());
    int num_readers = 0;
    for (const auto& rowset : _rowsets) {
        auto segments = rowset->get_segments();
        std::for_each(segments.begin(), segments.end(),
                      [&](const auto& segment) { num_readers += segment->num_rows() > 0 ? 1 : 0; });
    }

    std::vector<const ColumnReader*> readers;
    for (size_t i = 0; i < _tablet_schema->num_columns(); i++) {
        const auto& col = _tablet_schema->column(i);
        auto col_name = std::string(col.name());
        if (_schema.get_field_by_name(col_name) == nullptr || col.type() != LogicalType::TYPE_JSON) {
            continue;
        }
        readers.clear();
        for (const auto& rowset : _rowsets) {
            for (const auto& segment : rowset->get_segments()) {
                if (segment->num_rows() == 0) {
                    continue;
                }
                auto reader = segment->column_with_uid(col.unique_id());
                if (reader != nullptr && reader->column_type() == LogicalType::TYPE_JSON &&
                    nullptr != reader->sub_readers() && !reader->sub_readers()->empty()) {
                    readers.emplace_back(reader);
                }
            }
        }
        if (readers.size() == num_readers) {
            // must all be flat json type
            JsonPathDeriver deriver;

            if (auto metadata = _tablet_mgr->get_latest_cached_tablet_metadata(_tablet_metadata->id());
                metadata && metadata->has_flat_json_config()) {
                auto flat_json_config = std::make_shared<FlatJsonConfig>();
                flat_json_config->update(metadata->flat_json_config());
                deriver.init_flat_json_config(flat_json_config.get());
            }

            deriver.derived(readers);
            auto paths = deriver.flat_paths();
            auto types = deriver.flat_types();
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
    return Status::OK();
}

Status TabletReader::do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks,
                                 std::vector<uint64_t>* rssid_rowids) {
    DCHECK(_is_vertical_merge);
    RETURN_IF_ERROR(_collect_iter->get_next(chunk, source_masks, rssid_rowids));
    return Status::OK();
}

Status TabletReader::build_prepared_read_state(const TabletReaderParams& params, PreparedReadState* state) {
    if (!state->rowsets.empty()) {
        return Status::OK();
    }

    state->rowsets = _rowsets;
    state->rowset_segments.reserve(state->rowsets.size());
    state->rowset_prepared_states.reserve(state->rowsets.size());
    for (const auto& rowset : state->rowsets) {
        ASSIGN_OR_RETURN(auto segments, rowset->segments(params.lake_io_opts));
        std::vector<PreparedSegmentReadStatePtr> prepared_states;
        prepared_states.reserve(segments.size());
        for (size_t i = 0; i < segments.size(); ++i) {
            prepared_states.emplace_back(std::make_shared<PreparedSegmentReadState>());
        }
        state->rowset_segments.emplace_back(std::move(segments));
        state->rowset_prepared_states.emplace_back(std::move(prepared_states));
    }
    return Status::OK();
}

Status TabletReader::init_rowset_read_options(const TabletReaderParams& params, RowsetReadOptions* rs_opts) {
    KeysType keys_type = _tablet_schema->keys_type();
    RETURN_IF_ERROR(init_predicates(params));
    RETURN_IF_ERROR(init_delete_predicates(params, &_delete_predicates));
    rs_opts->pred_tree = params.pred_tree;
    rs_opts->runtime_filter_preds = params.runtime_filter_preds;
    RETURN_IF_ERROR(ZonemapPredicatesRewriter::rewrite_predicate_tree(&_obj_pool, rs_opts->pred_tree,
                                                                      rs_opts->pred_tree_for_zone_map));
    rs_opts->sorted = ((keys_type != DUP_KEYS && keys_type != PRIMARY_KEYS) && !params.skip_aggregation) ||
                      is_compaction(params.reader_type) || params.sorted_by_keys_per_tablet;
    rs_opts->reader_type = params.reader_type;
    rs_opts->chunk_size = params.chunk_size;
    rs_opts->delete_predicates = &_delete_predicates;
    rs_opts->stats = &_stats;
    rs_opts->runtime_state = params.runtime_state;
    rs_opts->profile = params.profile;
    rs_opts->use_page_cache = params.use_page_cache;
    rs_opts->tablet_schema = _tablet_schema;
    rs_opts->global_dictmaps = params.global_dictmaps;
    rs_opts->unused_output_column_ids = params.unused_output_column_ids;
    rs_opts->runtime_range_pruner = params.runtime_range_pruner;
    rs_opts->lake_io_opts = params.lake_io_opts;
    rs_opts->enable_join_runtime_filter_pushdown = params.enable_join_runtime_filter_pushdown;
    rs_opts->prune_column_after_index_filter = params.prune_column_after_index_filter;
    rs_opts->enable_gin_filter = params.enable_gin_filter;
    rs_opts->enable_predicate_col_late_materialize = params.enable_predicate_col_late_materialize;

    if (keys_type == KeysType::PRIMARY_KEYS) {
        rs_opts->is_primary_keys = true;
        rs_opts->version = _tablet_metadata->version();
    }
    rs_opts->reader_type = params.reader_type;

    if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS) {
        rs_opts->asc_hint = _is_asc_hint;
    }

    rs_opts->column_access_paths = params.column_access_paths;
    rs_opts->has_preaggregation = true;
    if ((is_compaction(params.reader_type) || params.sorted_by_keys_per_tablet)) {
        rs_opts->has_preaggregation = true;
    } else if (keys_type == PRIMARY_KEYS || keys_type == DUP_KEYS ||
               (keys_type == UNIQUE_KEYS && params.skip_aggregation)) {
        rs_opts->has_preaggregation = false;
    }
    RETURN_IF_ERROR(parse_seek_range(*_tablet_schema, params.range, params.end_range, params.start_key, params.end_key,
                                     &rs_opts->ranges, &_mempool));
    rs_opts->rowid_range_option = params.rowid_range_option;
    rs_opts->short_key_ranges_option = params.short_key_ranges_option;
    rs_opts->prepared_target_rowset_index = params.prepared_target_rowset_index;
    rs_opts->prepared_target_segment_index = params.prepared_target_segment_index;
    return Status::OK();
}

// TODO: support
//  1. rowid range and short key range
Status TabletReader::get_segment_iterators(const TabletReaderParams& params, std::vector<ChunkIteratorPtr>* iters) {
    PreparedReadState local_prepared_state;
    auto* prepared_state = _prepared_read_state != nullptr ? _prepared_read_state.get() : &local_prepared_state;
    const bool enable_reusable_segment_iters =
            config::enable_lake_scan_child_morsel_reuse && _prepared_read_state != nullptr;
    RETURN_IF_ERROR(build_prepared_read_state(params, prepared_state));
    if (enable_reusable_segment_iters && _reusable_rowset_iterators.size() < prepared_state->rowsets.size()) {
        _reusable_rowset_iterators.resize(prepared_state->rowsets.size());
    }

    RowsetReadOptions rs_opts;
    RETURN_IF_ERROR(init_rowset_read_options(params, &rs_opts));

    const bool enable_targeted_child_read =
            enable_reusable_segment_iters && config::enable_lake_scan_child_morsel_fast_reopen &&
            params.prepared_target_rowset_index >= 0 && params.prepared_target_segment_index >= 0;

    SCOPED_RAW_TIMER(&_stats.create_segment_iter_ns);

    if (enable_targeted_child_read) {
        const size_t rowset_idx = static_cast<size_t>(params.prepared_target_rowset_index);
        if (rowset_idx < prepared_state->rowsets.size()) {
            auto& rowset = prepared_state->rowsets[rowset_idx];
            if (params.rowid_range_option == nullptr || params.rowid_range_option->contains_rowset(rowset.get())) {
                ASSIGN_OR_RETURN(auto seg_iters, enhance_error_prompt(rowset->read(
                                                         schema(), rs_opts, prepared_state->rowset_segments[rowset_idx],
                                                         &_reusable_rowset_iterators[rowset_idx],
                                                         &prepared_state->rowset_prepared_states[rowset_idx])));
                iters->insert(iters->end(), seg_iters.begin(), seg_iters.end());
            }
            return Status::OK();
        }
    }

    std::vector<std::future<StatusOr<std::vector<ChunkIteratorPtr>>>> futures;
    // The parallel tasks capture local variables and |this| by reference.
    // Must wait for all tasks to complete before returning, otherwise
    // dangling references to |rs_opts| and |this| cause use-after-free.
    DeferOp wait_futures([&futures]() {
        for (auto& future : futures) {
            if (future.valid()) {
                future.wait();
            }
        }
    });
    DCHECK_EQ(prepared_state->rowsets.size(), prepared_state->rowset_segments.size());
    DCHECK_EQ(prepared_state->rowsets.size(), prepared_state->rowset_prepared_states.size());
    for (size_t rowset_idx = 0; rowset_idx < prepared_state->rowsets.size(); ++rowset_idx) {
        auto& rowset = prepared_state->rowsets[rowset_idx];
        if (params.rowid_range_option != nullptr && !params.rowid_range_option->contains_rowset(rowset.get())) {
            continue;
        }

        if (config::enable_load_segment_parallel) {
            auto task = std::make_shared<std::packaged_task<StatusOr<std::vector<ChunkIteratorPtr>>()>>([&, rowset,
                                                                                                         rowset_idx]() {
#ifdef BE_TEST
                Status injected_st;
                TEST_SYNC_POINT_CALLBACK("TabletReader::get_segment_iterators::parallel_read", &injected_st);
                if (!injected_st.ok()) {
                    return StatusOr<std::vector<ChunkIteratorPtr>>(injected_st);
                }
#endif
                if (enable_reusable_segment_iters) {
                    return enhance_error_prompt(rowset->read(schema(), rs_opts,
                                                             prepared_state->rowset_segments[rowset_idx],
                                                             &_reusable_rowset_iterators[rowset_idx],
                                                             &prepared_state->rowset_prepared_states[rowset_idx]));
                }
                return enhance_error_prompt(
                        rowset->read(schema(), rs_opts, prepared_state->rowset_segments[rowset_idx]));
            });

            auto packaged_func = [task]() { (*task)(); };
            if (auto st = ExecEnv::GetInstance()->load_rowset_thread_pool()->submit_func(std::move(packaged_func));
                !st.ok()) {
                // try load rowset serially if sumbit_func failed
                LOG(WARNING) << "sumbit_func failed: " << st.code_as_string()
                             << ", try to load rowset serially, rowset_id: " << rowset->id();
                ASSIGN_OR_RETURN(auto seg_iters,
                                 enable_reusable_segment_iters
                                         ? enhance_error_prompt(rowset->read(
                                                   schema(), rs_opts, prepared_state->rowset_segments[rowset_idx],
                                                   &_reusable_rowset_iterators[rowset_idx],
                                                   &prepared_state->rowset_prepared_states[rowset_idx]))
                                         : enhance_error_prompt(rowset->read(
                                                   schema(), rs_opts, prepared_state->rowset_segments[rowset_idx])));
                iters->insert(iters->end(), seg_iters.begin(), seg_iters.end());
            } else {
                futures.push_back(task->get_future());
            }
        } else {
            ASSIGN_OR_RETURN(auto seg_iters,
                             enable_reusable_segment_iters
                                     ? enhance_error_prompt(rowset->read(
                                               schema(), rs_opts, prepared_state->rowset_segments[rowset_idx],
                                               &_reusable_rowset_iterators[rowset_idx],
                                               &prepared_state->rowset_prepared_states[rowset_idx]))
                                     : enhance_error_prompt(rowset->read(schema(), rs_opts,
                                                                         prepared_state->rowset_segments[rowset_idx])));
            iters->insert(iters->end(), seg_iters.begin(), seg_iters.end());
        }
    }

    for (auto& future : futures) {
        auto seg_iters_or = future.get();
        if (!seg_iters_or.ok()) {
            return seg_iters_or.status();
        }
        iters->insert(iters->end(), seg_iters_or.value().begin(), seg_iters_or.value().end());
    }
    return Status::OK();
}

Status TabletReader::init_predicates(const TabletReaderParams& params) {
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

Status TabletReader::init_collector(const TabletReaderParams& params) {
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
            _collect_iter = new_mask_merge_iterator(seg_iters, _mask_buffer);
        } else {
            _collect_iter = new_heap_merge_iterator(
                    seg_iters,
                    (keys_type == PRIMARY_KEYS) && StorageEngine::instance()->enable_light_pk_compaction_publish());
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
        _collect_iter = new_union_iterator(std::move(seg_iters));
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
                _collect_iter = new_mask_merge_iterator(seg_iters, _mask_buffer);
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
                _collect_iter = new_mask_merge_iterator(seg_iters, _mask_buffer);
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
        auto f = std::make_shared<Field>(ChunkHelper::convert_field(idx, tablet_schema.column(idx)));
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
