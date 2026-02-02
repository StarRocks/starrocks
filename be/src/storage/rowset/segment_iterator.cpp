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

#include "segment_iterator.h"

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <utility>

#include "base/simd/simd.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "common/config.h"
#include "common/status.h"
#include "fs/fs.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "gutil/stl_util.h"
#include "io/shared_buffered_input_stream.h"
#include "segment_options.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/column_or_predicate.h"
#include "storage/column_predicate.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/del_vector.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/vector/tenann/del_id_filter.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "storage/index/vector/vector_index_reader.h"
#include "storage/index/vector/vector_index_reader_factory.h"
#include "storage/index/vector/vector_search_option.h"
#include "storage/lake/update_manager.h"
#include "storage/projection_iterator.h"
#include "storage/range.h"
#include "storage/record_predicate/record_predicate_helper.h"
#include "storage/roaring2range.h"
#include "storage/rowset/bitmap_index_evaluator.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/column_decoder.h"
#include "storage/rowset/common.h"
#include "storage/rowset/data_sample.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/dictcode_column_iterator.h"
#include "storage/rowset/fill_subfield_iterator.h"
#include "storage/rowset/rowid_column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/runtime_filter_predicate.h"
#include "storage/runtime_range_pruner.h"
#include "storage/runtime_range_pruner.hpp"
#include "storage/types.h"
#include "storage/update_manager.h"
#include "types/array_type_info.h"
#include "types/logical_type.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

constexpr static const LogicalType kDictCodeType = TYPE_INT;

// compare |tuple| with the first row of |chunk|.
// NULL will be treated as a minimal value.
static int compare(const SeekTuple& tuple, const Chunk& chunk) {
    DCHECK_LE(tuple.columns(), chunk.num_columns());
    const auto& schema = tuple.schema();
    const size_t n = tuple.columns();
    for (uint32_t i = 0; i < n; i++) {
        const Datum& v1 = tuple.get(i);
        const ColumnPtr& c = chunk.get_column_by_index(i);
        DCHECK_GE(c->size(), 1u);
        if (v1.is_null()) {
            if (c->is_null(0)) {
                continue;
            }
            return -1;
        }
        if (int r = schema.field(i)->type()->cmp(v1, c->get(0)); r != 0) {
            return r;
        }
    }
    return 0;
}

static int compare(const Slice& lhs_index_key, const Chunk& rhs_chunk, const Schema& short_key_schema) {
    DCHECK_GE(rhs_chunk.num_rows(), 1u);

    SeekTuple tuple(short_key_schema, rhs_chunk.get(0).datums());
    std::string rhs_index_key = tuple.short_key_encode(short_key_schema.num_fields(), 0);
    auto rhs = Slice(rhs_index_key);

    return lhs_index_key.compare(rhs);
}

class SegmentIterator final : public ChunkIterator {
public:
    SegmentIterator(std::shared_ptr<Segment> segment, Schema _schema, const SegmentReadOptions& options);

    ~SegmentIterator() override = default;

    void close() override;

protected:
    Status do_get_next(Chunk* chunk) override;
    Status do_get_next(Chunk* chunk, vector<uint32_t>* rowid) override;
    Status do_get_next(Chunk* chunk, vector<uint64_t>* rssid_rowids) override;
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override { return do_get_next(chunk); }
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks,
                       std::vector<uint64_t>* rssid_rowids) override {
        return do_get_next(chunk, rssid_rowids);
    }

private:
    struct ScanContext {
        class ScanStrategy {
        public:
            virtual ~ScanStrategy() = default;
            virtual Status seek_columns(ordinal_t pos) = 0;
            virtual Status read_columns(Chunk* chunk, const SparseRange<>& range, Buffer<uint8_t>* selection,
                                        Buffer<uint16_t>* selected_idx) = 0;
        };

        class NormalScanStrategy final : public ScanStrategy {
        public:
            explicit NormalScanStrategy(ScanContext* ctx) : _ctx(ctx) {}

            Status seek_columns(ordinal_t pos) override;

            Status read_columns(Chunk* chunk, const SparseRange<>& range, Buffer<uint8_t>* selection,
                                Buffer<uint16_t>* selected_idx) override;

        private:
            ScanContext* _ctx;
        };

        class PredicateLateMaterializationScanStrategy final : public ScanStrategy {
        public:
            explicit PredicateLateMaterializationScanStrategy(ScanContext* ctx) : _ctx(ctx) {}

            Status seek_columns(ordinal_t pos) override;

            Status read_columns(Chunk* chunk, const SparseRange<>& range, Buffer<uint8_t>* selection,
                                Buffer<uint16_t>* selected_idx) override;

        private:
            ScanContext* _ctx;
        };

        ScanContext() : _normal_scan_strategy(this), _predicate_late_materialization_scan_strategy(this) {}
        ~ScanContext() = default;

        // Release all chunk resources to free memory
        void close();
        // Seek all column iterators to the specified ordinal position
        Status seek_columns(ordinal_t pos, bool predicate_col_late_materialize_read);
        // Read column data from the specified range into the chunk
        // Handles column pruning and delete state tracking
        Status read_columns(Chunk* chunk, const SparseRange<>& range, bool predicate_col_late_materialize_read,
                            Buffer<uint8_t>* selection, Buffer<uint16_t>* selected_idx);
        // Calculate total memory usage of all chunks in this context
        int64_t memory_usage() const;
        // Get the number of column iterators in this context
        size_t column_size();
        // Generate a JSON string representation of the context state for debugging
        std::string to_string() const;

        OlapReaderStatistics* stats = nullptr;
        // Non-null when runtime filter pushdown is enabled. Points to SegmentIterator::_column_to_runtime_filters_map.
        // Used by predicate-column late materialization to include runtime filters in page-level predicate pushdown.
        std::unordered_map<ColumnId, RuntimeFilterPredicates>* runtime_filters_by_column = nullptr;

        Schema _read_schema;
        Schema _dict_decode_schema;
        std::vector<bool> _is_dict_column;
        std::vector<ColumnIterator*> _column_iterators;
        std::vector<ColumnId> _subfield_columns;
        std::vector<ColumnIterator*> _subfield_iterators;
        std::vector<ColumnIterator*> _column_iterators_for_predicate_late_materialize;
        std::vector<ColumnId> _column_id_for_predicate_late_materialize;
        std::map<ColumnId, ColumnIterator*> _column_ids_to_column_iterators;
        std::map<ColumnId, size_t> _column_ids_to_index;
        ColumnId _row_id_column_id;

        ScanContext* _next{nullptr};

        // index the column which only be used for filter
        // thus its not need do dict_decode_code
        std::vector<size_t> _skip_dict_decode_indexes;
        // index: output schema index, values: read schema index
        std::vector<size_t> _read_index_map;

        ChunkPtr _read_chunk;
        ChunkPtr _dict_chunk;
        ChunkPtr _final_chunk;
        ChunkPtr _adapt_global_dict_chunk;

        // true iff |_is_dict_column| contains at least one `true`, i.e,
        // |_column_iterators| contains at least one `DictCodeColumnIterator`.
        bool _has_dict_column{false};

        // if true, the last item of |_column_iterators| is a `RowIdColumnIterator` and
        // the last item of |_read_schema| and |_dict_decode_schema| is a row id field.
        bool _late_materialize{false};

        // not all dict encode
        bool _has_force_dict_encode{false};

        // If the column is pruneable, it means that we can skip the page read for it.
        // Currently, it only can be happend if the column is a pure pushdown predicate
        // for inverted index. Store ColumnId instead of column index so checks remain
        // valid even when iterator ordering changes.
        std::unordered_set<ColumnId> _prune_cols;
        bool _prune_column_after_index_filter = false;
        bool _enable_predicate_col_late_materialize = false;
        bool _only_output_one_predicate_col_with_filter_push_down = false;
        bool _support_push_down_predicate = false;
        std::vector<ColumnId> _predicate_order;
        ColumnPredicateMap _column_predicate_map;
        bool _is_filtered = false; // true if push down predicate into page level

        // Selectivity tracking for predicate column late materialization
        // Maps (score) to column_id, where score = read_time_ns * selectivity
        // Lower score = better (fast to read, high filtering rate)
        // Using multimap to allow multiple columns with same selectivity
        std::multimap<double, ColumnId> _predicate_selectivity_map;
        // Track evaluated chunk count for periodic selectivity updates (every 32 chunks)
        size_t _evaluated_chunk_nums = 0;

        // Continuous tracking of first column selectivity for sampling trigger
        // Accumulated since last reset (either initialization or predicate order change)
        size_t _first_column_total_rows_read = 0;   // Total rows read for first column
        size_t _first_column_total_rows_passed = 0; // Total rows passed first column filter

    private:
        ScanStrategy& _scan_strategy(bool predicate_col_late_materialize_read) {
            if (predicate_col_late_materialize_read) {
                return static_cast<ScanStrategy&>(_predicate_late_materialization_scan_strategy);
            }
            return static_cast<ScanStrategy&>(_normal_scan_strategy);
        }

        NormalScanStrategy _normal_scan_strategy;
        PredicateLateMaterializationScanStrategy _predicate_late_materialization_scan_strategy;
    };

    // Vector index related context, only created when needed
    struct VectorIndexContext {
        VectorIndexContext() = default;
        ~VectorIndexContext() = default;

        // Vector index parameters
        int64_t k = 0;
        bool use_vector_index = false;
        std::string vector_distance_column_name;
        int vector_column_id = -1;
        SlotId vector_slot_id = -1;
        std::unordered_map<rowid_t, float> id2distance_map;
        std::map<std::string, std::string> query_params;
        double vector_range = -1.0;
        int result_order = 0;
        bool use_ivfpq = false;

#ifdef WITH_TENANN
        tenann::PrimitiveSeqView query_view;
        std::shared_ptr<tenann::IndexMeta> index_meta;
#endif

        std::shared_ptr<VectorIndexReader> ann_reader;

        // Helper method to check if rowid should always be built
        bool always_build_rowid() const { return use_vector_index && !use_ivfpq; }
    };

    // Inverted index related context, only created when needed
    struct InvertedIndexContext {
        InvertedIndexContext() = default;
        ~InvertedIndexContext() = default;

        // Inverted index state
        bool has_inverted_index = false;
        std::vector<InvertedIndexIterator*> inverted_index_iterators;
        std::unordered_set<ColumnId> prune_cols_candidate_by_inverted_index;

        // Cleanup method to properly delete iterators
        void cleanup() {
            for (auto* iter : inverted_index_iterators) {
                if (iter != nullptr) {
                    delete iter;
                }
            }
            inverted_index_iterators.clear();
            has_inverted_index = false;
        }
    };

    Status _init();
    Status _try_to_update_ranges_by_runtime_filter();
    Status _do_get_next(Chunk* result, vector<rowid_t>* rowid);

    template <bool check_global_dict>
    Status _init_column_iterators(const Schema& schema);
    Status _get_row_ranges_by_keys();
    StatusOr<SparseRange<>> _get_row_ranges_by_key_ranges();
    StatusOr<SparseRange<>> _get_row_ranges_by_short_key_ranges();
    Status _get_row_ranges_by_zone_map();
    Status _get_row_ranges_by_vector_index();
    Status _get_row_ranges_by_bloom_filter();
    Status _get_row_ranges_by_rowid_range();
    Status _get_row_ranges_by_row_ids(std::vector<int64_t>* result_ids, SparseRange<>* r);

    Status _apply_tablet_range();
    StatusOr<std::optional<Range<>>> _seek_range_to_rowid_range(const SeekRange& range);

    uint32_t segment_id() const { return _segment->id(); }
    uint32_t num_rows() const { return _segment->num_rows(); }

    Status _lookup_ordinal(const SeekTuple& key, bool lower, rowid_t end, rowid_t* rowid);
    Status _lookup_ordinal(const Slice& index_key, const Schema& short_key_schema, bool lower, rowid_t end,
                           rowid_t* rowid);
    Status _seek_columns(const Schema& schema, rowid_t pos);
    Status _read_columns(const Schema& schema, Chunk* chunk, size_t nrows);

    StatusOr<uint16_t> _filter_by_non_expr_predicates(Chunk* chunk, vector<rowid_t>* rowid, uint16_t from, uint16_t to);
    StatusOr<uint16_t> _filter_by_expr_predicates(Chunk* chunk, vector<rowid_t>* rowid);
    StatusOr<uint16_t> _filter_by_record_predicate(Chunk* chunk, vector<rowid_t>* rowid);

    StatusOr<uint16_t> _filter_by_compound_and_predicates(Chunk* chunk, vector<rowid_t>* rowid, uint16_t from,
                                                          uint16_t to, ColumnId column_id,
                                                          const std::vector<const ColumnPredicate*>& and_predicates,
                                                          std::vector<Column*>& current_cols,
                                                          bool apply_runtime_filter = true);

    Status _evaluate_col_runtime_filters(ColumnId column_id, Column* col, uint8_t* selection, uint16_t from,
                                         uint16_t to);

    uint16_t _filter_chunk_by_selection(Chunk* chunk, vector<rowid_t>* rowid, uint16_t from, uint16_t to);

    uint16_t _filter_columns_by_selection(std::vector<Column*>& columns, vector<rowid_t>* rowid, uint16_t from,
                                          uint16_t to);

    void _init_column_predicates();

    StatusOr<size_t> trigger_sample_if_necessary(vector<rowid_t>* rowid);

    // Sample and measure all predicate columns to update selectivity and read time
    // Reads all predicate columns (like _predicate_evaluate_without_late_materialize)
    // and measures actual read time and filtering effectiveness for each column
    // Returns the sampled chunk size if successful
    StatusOr<size_t> _sample_predicate_columns(vector<rowid_t>* rowid);

    Status _init_context();

    template <bool late_materialization>
    Status _build_context(ScanContext* ctx);

    Status _init_global_dict_decoder();

    Status _rewrite_predicates();

    Status _decode_dict_codes(ScanContext* ctx);

    Status _check_low_cardinality_optimization();

    Status _finish_late_materialization(ScanContext* ctx);

    void _build_final_chunk(ScanContext* ctx);

    Status _encode_to_global_id(ScanContext* ctx);

    FieldPtr _make_field(size_t i);

    Status _switch_context(ScanContext* to);

    // `_check_low_cardinality_optimization` and `_init_column_iterators` must have been called
    // before you calling this method, otherwise the result is incorrect.
    bool _can_using_dict_code(const FieldPtr& field) const;

    // check field use low_cardinality global dict optimization
    bool _can_using_global_dict(const FieldPtr& field) const;

    Status _apply_bitmap_index();

    // Data sampling
    Status _apply_data_sampling();
    StatusOr<RowIdSparseRange> _sample_by_block();
    StatusOr<RowIdSparseRange> _sample_by_page();

    Status _apply_del_vector();

    Status _init_inverted_index_iterators();

    Status _apply_inverted_index();

    Status _read(Chunk* chunk, vector<rowid_t>* rowid, size_t n, bool predicate_col_late_materialize_read);

    void _init_column_access_paths();

    // search delta column group by column uniqueid, if this column exist in delta column group,
    // then return column iterator and delta column's fillname.
    // Or just return null
    StatusOr<std::unique_ptr<ColumnIterator>> _new_dcg_column_iterator(const TabletColumn& column,
                                                                       std::string* filename,
                                                                       FileEncryptionInfo* encryption_info,
                                                                       ColumnAccessPath* path);

    // This function is a unified entry for creating column iterators.
    // `ucid` means unique column id, use it for searching delta column group.
    Status _init_column_iterator_by_cid(const ColumnId cid, const ColumnUID ucid, bool check_dict_enc);

    ColumnAccessPath* _lookup_access_path(ColumnId cid, const TabletColumn& col);

    void _update_stats(io::SeekableInputStream* rfile);

    //  This function will search and build the segment from delta column group.
    StatusOr<std::shared_ptr<Segment>> _get_dcg_segment(uint32_t ucid);

    bool need_early_materialize_subfield(const FieldPtr& field);

    Status _init_ann_reader();

    IndexReadOptions _index_read_options(ColumnId cid) const;

    Status _init_reader_from_file(const std::string& index_path, const std::shared_ptr<TabletIndex>& tablet_index_meta,
                                  const std::map<std::string, std::string>& query_params);
    StatusOr<size_t> _predicate_evaluate(vector<rowid_t>* rowid);

    StatusOr<size_t> _predicate_evaluate_without_late_materialize(vector<rowid_t>* rowid);

    StatusOr<size_t> _predicate_evaluate_late_materialize(vector<rowid_t>* rowid);

    StatusOr<size_t> _predicate_evaluate_late_materialize_read_first_column(vector<rowid_t>* rowid,
                                                                            std::vector<Column*>& current_columns);
    Status _evaluate_late_materialize_read_other_columns(vector<rowid_t>* rowid, std::vector<Column*>& current_columns,
                                                         size_t& chunk_size);

    void _build_context_for_predicate(ScanContext* ctx);

    // Build column-specific RuntimeFilterPredicates for late materialization
    // Group runtime filter predicates by ColumnId
    void _build_column_oriented_rf(ScanContext* ctx);

private:
    using RawColumnIterators = std::vector<std::unique_ptr<ColumnIterator>>;
    using ColumnDecoders = std::vector<ColumnDecoder>;

    std::shared_ptr<Segment> _segment;
    std::unordered_map<std::string, std::shared_ptr<Segment>> _dcg_segments;
    SegmentReadOptions _opts;
    RawColumnIterators _column_iterators;
    std::vector<int> _io_coalesce_column_index;
    ColumnDecoders _column_decoders;
    BitmapIndexEvaluator _bitmap_index_evaluator;
    // delete predicates
    std::map<ColumnId, ColumnOrPredicate> _del_predicates;

    Status _get_del_vec_st;
    Status _get_dcg_st;
    DelVectorPtr _del_vec;
    DeltaColumnGroupList _dcgs;
    roaring::api::roaring_uint32_iterator_t _roaring_iter;

    std::unordered_map<ColumnId, std::unique_ptr<io::SeekableInputStream>> _column_files;

    SparseRange<> _scan_range;
    SparseRangeIterator<> _range_iter;

    PredicateTree _non_expr_pred_tree;
    PredicateTree _expr_pred_tree;
    RuntimeFilterPredicates _runtime_filter_preds;
    // Runtime filter predicates organized by column id for late materialization
    std::unordered_map<ColumnId, RuntimeFilterPredicates> _column_to_runtime_filters_map;

    // _selection is used to accelerate
    Buffer<uint8_t> _selection;

    // _selected_idx is used to store selected index when evaluating branchless predicate
    Buffer<uint16_t> _selected_idx;

    ScanContext _context_list[2];
    // points to |_context_list[0]| or |_context_list[1]| after `_init_context`.
    ScanContext* _context = nullptr;

    int _context_switch_count = 0;

    // a mapping from column id to a indicate whether it's predicate need rewrite.
    std::vector<uint8_t> _predicate_need_rewrite;

    ObjectPool _obj_pool;

    // initial number of columns of |_opts.pred_tree|.
    int _predicate_columns = 0;

    // the next rowid to read
    rowid_t _cur_rowid = 0;

    int _late_materialization_ratio = 0;

    int _reserve_chunk_size = 0;

    bool _inited = false;

    std::unordered_map<ColumnId, ColumnAccessPath*> _column_access_paths;
    std::unordered_map<ColumnId, ColumnAccessPath*> _predicate_column_access_paths;

    // Vector index context - only created when needed
    std::unique_ptr<VectorIndexContext> _vector_index_ctx;

    // Inverted index context - only created when needed
    std::unique_ptr<InvertedIndexContext> _inverted_index_ctx;

    bool _enable_predicate_col_late_materialize;
};

// ScanContext method implementations

void SegmentIterator::ScanContext::close() {
    _read_chunk.reset();
    _dict_chunk.reset();
    _final_chunk.reset();
    _adapt_global_dict_chunk.reset();
}

Status SegmentIterator::ScanContext::seek_columns(ordinal_t pos, bool predicate_col_late_materialize_read) {
    return _scan_strategy(predicate_col_late_materialize_read).seek_columns(pos);
}

Status SegmentIterator::ScanContext::read_columns(Chunk* chunk, const SparseRange<>& range,
                                                  bool predicate_col_late_materialize_read, Buffer<uint8_t>* selection,
                                                  Buffer<uint16_t>* selected_idx) {
    return _scan_strategy(predicate_col_late_materialize_read).read_columns(chunk, range, selection, selected_idx);
}

Status SegmentIterator::ScanContext::NormalScanStrategy::seek_columns(ordinal_t pos) {
    for (auto* iter : _ctx->_column_iterators) {
        RETURN_IF_ERROR(iter->seek_to_ordinal(pos));
    }
    return Status::OK();
}

Status SegmentIterator::ScanContext::NormalScanStrategy::read_columns(Chunk* chunk, const SparseRange<>& range,
                                                                      Buffer<uint8_t>* selection,
                                                                      Buffer<uint16_t>* selected_idx) {
    (void)selection;
    (void)selected_idx;

    // reset _is_filtered every time
    _ctx->_is_filtered = false;

    bool may_has_del_row = chunk->delete_state() != DEL_NOT_SATISFIED;
    std::vector<ColumnId> pruned_cols;
    size_t pruned_col_size = 0;
    for (size_t i = 0; i < _ctx->_column_iterators.size(); i++) {
        ColumnId column_id = _ctx->_read_schema.field(i)->id();
        if (_ctx->_prune_column_after_index_filter && _ctx->_prune_cols.count(column_id)) {
            pruned_cols.push_back(column_id);
            continue;
        }
        auto* col = chunk->get_column_raw_ptr_by_id(column_id);

        // for binary column, we must reserve enough memory to avoid extra memcpy
        // but if segment is small and there are lots of segments, we can't reserve too much unnecessary memory
        if (col->capacity() == 0) {
            _ctx->_column_iterators[i]->reserve_col(range.span_size(), col);
        }

        RETURN_IF_ERROR(_ctx->_column_iterators[i]->next_batch(range, col));

        if (pruned_col_size == 0) {
            pruned_col_size = col->size();
        }
        if (pruned_col_size != col->size()) {
            return Status::InternalError(
                    fmt::format("pruned_col_size {} != column size:{}", pruned_col_size, col->size()));
        }
        DCHECK_EQ(pruned_col_size, col->size());
        may_has_del_row |= (col->delete_state() != DEL_NOT_SATISFIED);
    }
    for (ColumnId cid : pruned_cols) {
        auto* col = chunk->get_column_raw_ptr_by_id(cid);
        // make sure each pruned column has the same size as the unpruneable one.
        col->resize(pruned_col_size);
    }
    chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    return Status::OK();
}

Status SegmentIterator::ScanContext::PredicateLateMaterializationScanStrategy::seek_columns(ordinal_t pos) {
    for (auto* iter : _ctx->_column_iterators_for_predicate_late_materialize) {
        RETURN_IF_ERROR(iter->seek_to_ordinal(pos));
    }
    return Status::OK();
}

Status SegmentIterator::ScanContext::PredicateLateMaterializationScanStrategy::read_columns(
        Chunk* chunk, const SparseRange<>& range, Buffer<uint8_t>* selection, Buffer<uint16_t>* selected_idx) {
    DCHECK(selection != nullptr);
    DCHECK(selected_idx != nullptr);
    DCHECK(!_ctx->_column_iterators_for_predicate_late_materialize.empty());
    DCHECK(!_ctx->_predicate_order.empty());

    auto& column_iterators = _ctx->_column_iterators_for_predicate_late_materialize;
    const ColumnId first_column_id = _ctx->_predicate_order[0];
    const bool first_col_has_runtime_filter =
            _ctx->runtime_filters_by_column != nullptr && _ctx->runtime_filters_by_column->contains(first_column_id);
    // TODO:support runtime bloom filter push down to page level
    bool first_col_supports_pushdown =
            !first_col_has_runtime_filter &&
            column_iterators[0]->support_push_down_predicate(_ctx->_column_predicate_map[first_column_id]);

    // reset _is_filtered every time
    _ctx->_is_filtered = false;

    bool may_has_del_row = chunk->delete_state() != DEL_NOT_SATISFIED;
    std::vector<ColumnId> pruned_cols;
    size_t pruned_col_size = 0;
    for (size_t i = 0; i < column_iterators.size(); i++) {
        ColumnId column_id = _ctx->_column_id_for_predicate_late_materialize[i];
        if (_ctx->_prune_column_after_index_filter && _ctx->_prune_cols.count(column_id)) {
            pruned_cols.push_back(column_id);
            continue;
        }
        auto* col = chunk->get_column_raw_ptr_by_id(column_id);

        // for binary column, we must reserve enough memory to avoid extra memcpy
        // but if segment is small and there are lots of segments, we can't reserve too much unnecessary memory
        if (col->capacity() == 0) {
            column_iterators[i]->reserve_col(range.span_size(), col);
        }

        if (!first_col_supports_pushdown) {
            RETURN_IF_ERROR(column_iterators[i]->next_batch(range, col));
        } else {
            size_t processed_rows = 0;
            if (i == 0) {
                size_t original_row_num = col->size();

                // for first predicate column, if can filter data in page level
                // _is_filtered is set to true
                // and selection is record for filter rowId column
                // and only append filtered data in col
                RETURN_IF_ERROR(column_iterators[i]->next_batch_with_filter(
                        range, col, _ctx->_column_predicate_map[first_column_id], selection, selected_idx,
                        &processed_rows));
                size_t appended_rows = col->size() - original_row_num;
                if (processed_rows >= appended_rows && _ctx->stats != nullptr) {
                    _ctx->stats->rows_vec_cond_filtered += (processed_rows - appended_rows);
                }
                _ctx->_first_column_total_rows_read += processed_rows;
                _ctx->_first_column_total_rows_passed += appended_rows;
                _ctx->_is_filtered = true;
            } else {
                // for rowId column iterator, apply selection if _is_filtered is true
                DCHECK(i == 1);
                RETURN_IF_ERROR(column_iterators[i]->next_batch_with_filter(range, col, ColumnPredicates(), selection,
                                                                            selected_idx, &processed_rows));
            }
        }

        if (pruned_col_size == 0) {
            pruned_col_size = col->size();
        }
        if (pruned_col_size != col->size()) {
            return Status::InternalError(
                    fmt::format("pruned_col_size {} != column size:{}", pruned_col_size, col->size()));
        }
        DCHECK_EQ(pruned_col_size, col->size());
        may_has_del_row |= (col->delete_state() != DEL_NOT_SATISFIED);
    }
    for (ColumnId cid : pruned_cols) {
        auto* col = chunk->get_column_raw_ptr_by_id(cid);
        // make sure each pruned column has the same size as the unpruneable one.
        col->resize(pruned_col_size);
    }
    chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    return Status::OK();
}

int64_t SegmentIterator::ScanContext::memory_usage() const {
    int64_t usage = 0;
    usage += (_read_chunk != nullptr) ? _read_chunk->memory_usage() : 0;
    usage += (_dict_chunk.get() != _read_chunk.get()) ? _dict_chunk->memory_usage() : 0;
    usage += (_final_chunk.get() != _dict_chunk.get()) ? _final_chunk->memory_usage() : 0;
    usage += (_adapt_global_dict_chunk.get() != _final_chunk.get()) ? _adapt_global_dict_chunk->memory_usage() : 0;
    return usage;
}

size_t SegmentIterator::ScanContext::column_size() {
    return _column_iterators.size();
}

std::string SegmentIterator::ScanContext::to_string() const {
    std::ostringstream oss;
    oss << "{";
    oss << "\"_read_schema\": " << _read_schema.to_string() << ",";
    oss << "\"_dict_decode_schema\": " << _dict_decode_schema.to_string() << ",";
    oss << "\"_is_dict_column\": [";
    for (size_t i = 0; i < _is_dict_column.size(); ++i) {
        oss << (_is_dict_column[i] ? "true" : "false");
        if (i + 1 < _is_dict_column.size()) oss << ", ";
    }
    oss << "],";
    oss << "\"_column_iterators\": " << _column_iterators.size() << ",";
    oss << "\"_subfield_columns\": [";
    for (size_t i = 0; i < _subfield_columns.size(); ++i) {
        oss << _subfield_columns[i];
        if (i + 1 < _subfield_columns.size()) oss << ", ";
    }
    oss << "],";
    oss << "\"_subfield_iterators\": " << _subfield_iterators.size() << ",";
    oss << "\"_skip_dict_decode_indexes\": [";
    for (size_t i = 0; i < _skip_dict_decode_indexes.size(); ++i) {
        oss << (_skip_dict_decode_indexes[i] ? "true" : "false");
        if (i + 1 < _skip_dict_decode_indexes.size()) oss << ", ";
    }
    oss << "],";
    oss << "\"_read_index_map\": [";
    for (size_t i = 0; i < _read_index_map.size(); ++i) {
        oss << _read_index_map[i];
        if (i + 1 < _read_index_map.size()) oss << ", ";
    }
    oss << "],";
    oss << "\"_has_dict_column\": " << (_has_dict_column ? "true" : "false") << ",";
    oss << "\"_late_materialize\": " << (_late_materialize ? "true" : "false") << ",";
    oss << "\"_has_force_dict_encode\": " << (_has_force_dict_encode ? "true" : "false") << ",";
    oss << "\"_prune_cols\": [";
    size_t prune_count = 0;
    for (auto cid : _prune_cols) {
        if (prune_count++ > 0) oss << ", ";
        oss << cid;
    }
    oss << "],";
    oss << "\"_prune_column_after_index_filter\": " << (_prune_column_after_index_filter ? "true" : "false");
    oss << "}";
    return oss.str();
}

SegmentIterator::SegmentIterator(std::shared_ptr<Segment> segment, Schema schema, const SegmentReadOptions& options)
        : ChunkIterator(std::move(schema), options.chunk_size),
          _segment(std::move(segment)),
          _opts(std::move(options)),
          _bitmap_index_evaluator(_schema, _opts.pred_tree),
          _predicate_columns(_opts.pred_tree.num_columns()),
          _enable_predicate_col_late_materialize(_opts.enable_predicate_col_late_materialize) {
    // Initialize vector index context only when needed
    if (_opts.use_vector_index) {
        _vector_index_ctx = std::make_unique<VectorIndexContext>();
        _vector_index_ctx->use_vector_index = true;
        _vector_index_ctx->vector_distance_column_name = _opts.vector_search_option->vector_distance_column_name;
        _vector_index_ctx->vector_column_id = _opts.vector_search_option->vector_column_id;
        _vector_index_ctx->vector_slot_id = _opts.vector_search_option->vector_slot_id;
        _vector_index_ctx->vector_range = _opts.vector_search_option->vector_range;
        _vector_index_ctx->result_order = _opts.vector_search_option->result_order;
        _vector_index_ctx->use_ivfpq = _opts.vector_search_option->use_ivfpq;
        _vector_index_ctx->query_params = _opts.vector_search_option->query_params;

        if (_vector_index_ctx->vector_range >= 0 && _vector_index_ctx->use_ivfpq) {
            _vector_index_ctx->k = _opts.vector_search_option->k * _opts.vector_search_option->pq_refine_factor *
                                   _opts.vector_search_option->k_factor;
        } else {
            _vector_index_ctx->k = _opts.vector_search_option->k * _opts.vector_search_option->k_factor;
        }
#ifdef WITH_TENANN
        _vector_index_ctx->query_view = tenann::PrimitiveSeqView{
                .data = reinterpret_cast<uint8_t*>(_opts.vector_search_option->query_vector.data()),
                .size = static_cast<uint32_t>(_opts.vector_search_option->query_vector.size()),
                .elem_type = tenann::PrimitiveType::kFloatType};
#endif
    }
    // For small segment file (the number of rows is less than chunk_size),
    // the segment iterator will reserve a large amount of memory,
    // especially when there are many columns, many small files, many versions,
    // a compaction task or query will consume a lot of memory,
    // increasing the burden on the memory allocator, while increasing memory consumption.
    // Therefore, when the segment file is relatively small, we should only reserve necessary memory.
    _reserve_chunk_size = static_cast<int32_t>(std::min(static_cast<uint32_t>(_opts.chunk_size), _segment->num_rows()));

    // for very long queries(>30min), delvec may got GCed, to prevent this, load delvec at query start, call stack:
    //   olap_chunk_source::prepare -> tablet_reader::open -> get_segment_iterators -> create SegmentIterator
    if (_opts.is_primary_keys && _opts.version > 0) {
        TabletSegmentId tsid;
        tsid.tablet_id = _opts.tablet_id;
        tsid.segment_id = _opts.rowset_id + segment_id();
        if (_opts.delvec_loader != nullptr) {
            SCOPED_RAW_TIMER(&_opts.stats->get_delvec_ns);
            _get_del_vec_st = _opts.delvec_loader->load(tsid, _opts.version, &_del_vec);
            if (_get_del_vec_st.ok()) {
                if (_del_vec && _del_vec->empty()) {
                    _del_vec.reset();
                }
            }
        }
    }
    if (_opts.dcg_loader != nullptr) {
        SCOPED_RAW_TIMER(&_opts.stats->get_delta_column_group_ns);
        // Load delta column groups based on table type
        // For PK tables: use tablet segment id and version
        // For other table types: use tablet id, rowset id, and segment id
        if (_opts.is_primary_keys) {
            TabletSegmentId tsid;
            tsid.tablet_id = _opts.tablet_id;
            tsid.segment_id = _opts.rowset_id + segment_id();
            _get_dcg_st = _opts.dcg_loader->load(tsid, _opts.version, &_dcgs);
        } else {
            int64_t tablet_id = _opts.tablet_id;
            RowsetId rowsetid = _opts.rowsetid;
            _get_dcg_st = _opts.dcg_loader->load(tablet_id, rowsetid, segment_id(), INT64_MAX, &_dcgs);
        }
    }
}

Status SegmentIterator::_init() {
    SCOPED_RAW_TIMER(&_opts.stats->segment_init_ns);
    if (_opts.is_cancelled != nullptr && _opts.is_cancelled->load(std::memory_order_acquire)) {
        return Status::Cancelled("Cancelled");
    }
    if (!_get_del_vec_st.ok()) {
        return _get_del_vec_st;
    }
    if (!_get_dcg_st.ok()) {
        return _get_dcg_st;
    }
    if (_opts.is_primary_keys && _opts.version > 0) {
        if (_del_vec) {
            if (_segment->num_rows() == _del_vec->cardinality()) {
                return Status::EndOfFile("all rows deleted");
            }
            VLOG(2) << "seg_iter init delvec tablet:" << _opts.tablet_id << " rowset:" << _opts.rowset_id
                    << " seg:" << segment_id() << " version req:" << _opts.version << " actual:" << _del_vec->version()
                    << " " << _del_vec->cardinality() << "/" << _segment->num_rows();
            roaring_iterator_init(&_del_vec->roaring()->roaring, &_roaring_iter);
        }
    }

    _selection.resize(_reserve_chunk_size);
    _selected_idx.resize(_reserve_chunk_size);

    StarRocksMetrics::instance()->segment_read_total.increment(1);

    /// the calling order matters, do not change unless you know why.

    _segment->turn_on_batch_update_cache_size();
    DeferOp op([&] { _segment->turn_off_batch_update_cache_size(); });
    // init stage
    // The main task is to do some initialization,
    // initialize the iterator and check if certain optimizations can be applied
    _init_column_access_paths();
    RETURN_IF_ERROR(_check_low_cardinality_optimization());
    RETURN_IF_ERROR(_init_column_iterators<true>(_schema));
    RETURN_IF_ERROR(_init_ann_reader());
    // filter by index stage
    // Use indexes and predicates to filter some data page
    RETURN_IF_ERROR(_get_row_ranges_by_rowid_range());
    RETURN_IF_ERROR(_get_row_ranges_by_keys());
    RETURN_IF_ERROR(_apply_tablet_range());
    bool apply_del_vec_after_all_index_filter = config::apply_del_vec_after_all_index_filter;
    if (!apply_del_vec_after_all_index_filter) {
        RETURN_IF_ERROR(_apply_del_vector());
    }
    // Support prefilter for now
    RETURN_IF_ERROR(_apply_bitmap_index());
    RETURN_IF_ERROR(_get_row_ranges_by_zone_map());
    RETURN_IF_ERROR(_get_row_ranges_by_bloom_filter());
    RETURN_IF_ERROR(_apply_inverted_index());
    if (apply_del_vec_after_all_index_filter) {
        RETURN_IF_ERROR(_apply_del_vector());
    }
    RETURN_IF_ERROR(_get_row_ranges_by_vector_index());
    RETURN_IF_ERROR(_apply_data_sampling());

    // rewrite stage
    // Rewriting predicates using segment dictionary codes
    RETURN_IF_ERROR(_rewrite_predicates());
    _init_column_predicates();
    RETURN_IF_ERROR(_init_context());

    // reverse scan_range
    if (!_opts.asc_hint) {
        _scan_range.split_and_reverse(config::desc_hint_split_range, config::vector_chunk_size);
    }

    _range_iter = _scan_range.new_iterator();

    for (auto column_index : _io_coalesce_column_index) {
        RETURN_IF_ERROR(_column_iterators[column_index]->convert_sparse_range_to_io_range(_scan_range));
    }

    return Status::OK();
}

inline Status SegmentIterator::_init_reader_from_file(const std::string& index_path,
                                                      const std::shared_ptr<TabletIndex>& tablet_index_meta,
                                                      const std::map<std::string, std::string>& query_params) {
#ifdef WITH_TENANN
    if (!_vector_index_ctx) {
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto meta, get_vector_meta(tablet_index_meta, query_params))
    _vector_index_ctx->index_meta = std::make_shared<tenann::IndexMeta>(std::move(meta));
    RETURN_IF_ERROR(VectorIndexReaderFactory::create_from_file(index_path, _vector_index_ctx->index_meta,
                                                               &_vector_index_ctx->ann_reader));
    auto status = _vector_index_ctx->ann_reader->init_searcher(*_vector_index_ctx->index_meta.get(), index_path);
    // means empty ann reader
    if (status.is_not_supported()) {
        _vector_index_ctx->use_vector_index = false;
        return Status::OK();
    }
    return status;
#else
    return Status::OK();
#endif
}

Status SegmentIterator::_init_ann_reader() {
#ifdef WITH_TENANN
    if (!_vector_index_ctx || !_vector_index_ctx->use_vector_index) {
        return Status::OK();
    }
    std::unordered_map<int32_t, TabletIndex> col_map_index;
    for (const auto& index : *_segment->tablet_schema().indexes()) {
        if (index.index_type() == VECTOR) {
            col_map_index.emplace(index.col_unique_ids()[0], index);
        }
    }

    std::vector<TabletIndex> hit_indexes;
    for (auto& field : _schema.fields()) {
        if (col_map_index.count(field->uid()) > 0) {
            hit_indexes.emplace_back(col_map_index.at(field->uid()));
        }
    }

    // TODO: Support more index in one segment iterator, only support one index for now
    DCHECK(hit_indexes.size() <= 1) << "Only support query no more than one index now";

    if (hit_indexes.empty()) {
        return Status::OK();
    }

    auto tablet_index_meta = std::make_shared<TabletIndex>(hit_indexes[0]);

    std::string index_path = IndexDescriptor::vector_index_file_path(_opts.rowset_path, _opts.rowsetid.to_string(),
                                                                     segment_id(), tablet_index_meta->index_id());

    return _init_reader_from_file(index_path, tablet_index_meta, _vector_index_ctx->query_params);
#else
    return Status::OK();
#endif
}

Status SegmentIterator::_get_row_ranges_by_vector_index() {
#ifdef WITH_TENANN
    if (!_vector_index_ctx || !_vector_index_ctx->use_vector_index) {
        return Status::OK();
    }
    RETURN_IF(_scan_range.empty(), Status::OK());

    SCOPED_RAW_TIMER(&_opts.stats->get_row_ranges_by_vector_index_timer);

    Status st;
    std::map<rowid_t, float> id2distance_map;
    std::vector<int64_t> result_ids;
    std::vector<float> result_distances;
    std::vector<int64_t> filtered_result_ids;
    DelIdFilter del_id_filter(_scan_range);

    {
        SCOPED_RAW_TIMER(&_opts.stats->vector_search_timer);
        if (_vector_index_ctx->vector_range >= 0) {
            st = _vector_index_ctx->ann_reader->range_search(
                    _vector_index_ctx->query_view, _vector_index_ctx->k, &result_ids, &result_distances, &del_id_filter,
                    static_cast<float>(_vector_index_ctx->vector_range), _vector_index_ctx->result_order);
        } else {
            result_ids.resize(_vector_index_ctx->k);
            result_distances.resize(_vector_index_ctx->k);
            st = _vector_index_ctx->ann_reader->search(
                    _vector_index_ctx->query_view, _vector_index_ctx->k, (result_ids.data()),
                    reinterpret_cast<uint8_t*>(result_distances.data()), &del_id_filter);
        }
    }

    if (!st.ok()) {
        LOG(WARNING) << "Vector index search failed: " << st.to_string();
        return Status::InternalError(st.to_string());
    }
    SCOPED_RAW_TIMER(&_opts.stats->process_vector_distance_and_id_timer);

    for (size_t i = 0; i < result_ids.size() && result_ids[i] != -1; i++) {
        id2distance_map[result_ids[i]] = result_distances[i];
    }

    SparseRange r;
    RETURN_IF_ERROR(_get_row_ranges_by_row_ids(&result_ids, &r));

    size_t prev_size = _scan_range.span_size();
    _scan_range = _scan_range.intersection(r);
    _opts.stats->rows_vector_index_filtered += (prev_size - _scan_range.span_size());

    SparseRangeIterator range_iter = _scan_range.new_iterator();
    size_t to_read = _scan_range.span_size();
    while (range_iter.has_more()) {
        Range r = range_iter.next(to_read);
        for (uint32_t i = r.begin(); i < r.end(); i++) {
            filtered_result_ids.emplace_back(i);
        }
    }

    _vector_index_ctx->id2distance_map.reserve(filtered_result_ids.size());
    for (size_t i = 0; i < filtered_result_ids.size(); i++) {
        _vector_index_ctx->id2distance_map[static_cast<rowid_t>(filtered_result_ids[i])] =
                id2distance_map[filtered_result_ids[i]];
    }
    return Status::OK();
#else
    return Status::OK();
#endif
}

Status SegmentIterator::_get_row_ranges_by_row_ids(std::vector<int64_t>* result_ids, SparseRange<>* r) {
    if (result_ids->empty()) {
        return Status::OK();
    }

    std::sort(result_ids->begin(), result_ids->end());

    // filter -1 above
    auto first_valid_id_iter = std::upper_bound(result_ids->begin(), result_ids->end(), -1);
    if (first_valid_id_iter == result_ids->end()) {
        // All elements are less than 0
        return Status::OK();
    }

    int64_t range_start = *first_valid_id_iter;
    int64_t range_end = range_start + 1;

    for (auto it = first_valid_id_iter + 1; it != result_ids->end(); ++it) {
        if (*it == range_end) {
            ++range_end;
        } else {
            r->add(Range<>(range_start, range_end));
            range_start = *it;
            range_end = range_start + 1;
        }
    }

    r->add(Range<>(range_start, range_end));

    return Status::OK();
}

Status SegmentIterator::_try_to_update_ranges_by_runtime_filter() {
    return _opts.runtime_range_pruner.update_range_if_arrived(
            _opts.global_dictmaps,
            [this](auto cid, const PredicateList& predicates) {
                const ColumnPredicate* del_pred;
                auto iter = _del_predicates.find(cid);
                del_pred = iter != _del_predicates.end() ? &(iter->second) : nullptr;
                SparseRange<> r;

                RETURN_IF_ERROR(_column_iterators[cid]->get_row_ranges_by_zone_map(predicates, del_pred, &r,
                                                                                   CompoundNodeType::AND));
                size_t prev_size = _range_iter.remaining_rows();
                SparseRange<> res;
                res.set_sorted(_scan_range.is_sorted());
                _range_iter = _range_iter.intersection(r, &res);
                std::swap(res, _scan_range);
                _range_iter.set_range(&_scan_range);
                _opts.stats->runtime_stats_filtered += (prev_size - _range_iter.remaining_rows());
                return Status::OK();
            },
            false, _opts.stats->raw_rows_read);
}

StatusOr<std::shared_ptr<Segment>> SegmentIterator::_get_dcg_segment(uint32_t ucid) {
    // iterate dcg from new ver to old ver
    for (const auto& dcg : _dcgs) {
        // cols file index -> column index in corresponding file
        std::pair<int32_t, int32_t> idx = dcg->get_column_idx(ucid);
        if (idx.first >= 0) {
            ASSIGN_OR_RETURN(auto column_file, dcg->column_file_by_idx(parent_name(_segment->file_name()), idx.first));
            if (_dcg_segments.count(column_file) == 0) {
                ASSIGN_OR_RETURN(auto dcg_segment, _segment->new_dcg_segment(*dcg, idx.first, _opts.tablet_schema));
                _dcg_segments[column_file] = dcg_segment;
            }
            return _dcg_segments[column_file];
        }
    }
    // the column not exist in delta column group
    return nullptr;
}

StatusOr<std::unique_ptr<ColumnIterator>> SegmentIterator::_new_dcg_column_iterator(const TabletColumn& column,
                                                                                    std::string* filename,
                                                                                    FileEncryptionInfo* encryption_info,
                                                                                    ColumnAccessPath* path) {
    // build column iter from delta column group
    ASSIGN_OR_RETURN(auto dcg_segment, _get_dcg_segment(column.unique_id()));
    if (dcg_segment != nullptr) {
        if (filename != nullptr) {
            *filename = dcg_segment->file_name();
        }
        if (encryption_info != nullptr && dcg_segment->encryption_info()) {
            *encryption_info = *dcg_segment->encryption_info();
        }
        return dcg_segment->new_column_iterator(column, path);
    }
    return nullptr;
}

void SegmentIterator::_init_column_access_paths() {
    if (_opts.column_access_paths == nullptr || _opts.column_access_paths->empty()) {
        return;
    }

    for (auto& column_access_path : *_opts.column_access_paths) {
        auto* path = column_access_path.get();

        // FIXME(murphy)
        if (path->is_from_predicate() || path->is_extended()) {
            _predicate_column_access_paths[path->index()] = path;
        } else {
            _column_access_paths[path->index()] = path;
        }
    }
}

// Attach the ColumnAccessPath
ColumnAccessPath* SegmentIterator::_lookup_access_path(ColumnId cid, const TabletColumn& col) {
    ColumnAccessPath* access_path = nullptr;
    if (col.is_extended()) {
        auto extended_info = col.extended_info();
        if (extended_info != nullptr && extended_info->source_column_uid >= 0) {
            ColumnId source_id = extended_info->source_column_uid;
            auto it = _column_access_paths.find(source_id);
            if (it != _column_access_paths.end() && it->second->is_extended()) {
                access_path = it->second;
            }
        }
    } else {
        auto it = _column_access_paths.find(cid);
        if (it != _column_access_paths.end()) {
            access_path = it->second;
        }
    }
    if (access_path) {
        VLOG(2) << fmt::format("attach column access path for column={} path={}", cid, access_path->to_string());
    }
    return access_path;
}

Status SegmentIterator::_init_column_iterator_by_cid(const ColumnId cid, const ColumnUID ucid, bool check_dict_enc) {
    ColumnIteratorOptions iter_opts;
    iter_opts.stats = _opts.stats;
    iter_opts.use_page_cache = _opts.use_page_cache;
    iter_opts.temporary_data = _opts.temporary_data;
    iter_opts.check_dict_encoding = check_dict_enc;
    iter_opts.reader_type = _opts.reader_type;
    iter_opts.lake_io_opts = _opts.lake_io_opts;
    iter_opts.has_preaggregation = _opts.has_preaggregation;

    RandomAccessFileOptions opts{.skip_fill_local_cache = !_opts.lake_io_opts.fill_data_cache,
                                 .buffer_size = _opts.lake_io_opts.buffer_size,
                                 .skip_disk_cache = _opts.lake_io_opts.skip_disk_cache};

    std::string dcg_filename;
    FileEncryptionInfo dcg_encryption_info;
    if (ucid < 0) {
        LOG(ERROR) << "invalid unique columnid in segment iterator, ucid: " << ucid
                   << ", segment: " << _segment->file_name();
    }
    auto tablet_schema = _opts.tablet_schema ? _opts.tablet_schema : _segment->tablet_schema_share_ptr();
    const auto& col = tablet_schema->column(cid);
    ColumnAccessPath* access_path = _lookup_access_path(cid, col);

    ASSIGN_OR_RETURN(auto col_iter, _new_dcg_column_iterator(col, &dcg_filename, &dcg_encryption_info, access_path));
    if (col_iter == nullptr) {
        // not found in delta column group, create normal column iterator
        ASSIGN_OR_RETURN(_column_iterators[cid], _segment->new_column_iterator_or_default(col, access_path));
        const auto encryption_info = _segment->encryption_info();
        if (encryption_info) {
            opts.encryption_info = *encryption_info;
        }
        ASSIGN_OR_RETURN(auto rfile, _opts.fs->new_random_access_file_with_bundling(opts, _segment->file_info()));
        if (config::io_coalesce_lake_read_enable && !_segment->is_default_column(col) &&
            _segment->lake_tablet_manager() != nullptr) {
            ASSIGN_OR_RETURN(auto file_size, rfile->get_size());
            auto shared_buffered_input_stream =
                    std::make_unique<io::SharedBufferedInputStream>(rfile->stream(), _segment->file_name(), file_size);
            auto options = io::SharedBufferedInputStream::CoalesceOptions{
                    .max_dist_size = config::io_coalesce_read_max_distance_size,
                    .max_buffer_size = config::io_coalesce_read_max_buffer_size};
            shared_buffered_input_stream->set_coalesce_options(options);
            iter_opts.read_file = shared_buffered_input_stream.get();
            iter_opts.is_io_coalesce = true;
            _column_files[cid] = std::move(shared_buffered_input_stream);
            _io_coalesce_column_index.emplace_back(cid);
        } else {
            iter_opts.read_file = rfile.get();
            _column_files[cid] = std::move(rfile);
        }
    } else {
        // create delta column iterator
        // TODO io_coalesce
        _column_iterators[cid] = std::move(col_iter);
        opts.encryption_info = dcg_encryption_info;
        ASSIGN_OR_RETURN(auto dcg_file, _opts.fs->new_random_access_file(opts, dcg_filename));
        iter_opts.read_file = dcg_file.get();
        _column_files[cid] = std::move(dcg_file);
    }
    RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts));
    return Status::OK();
}

template <bool check_global_dict>
Status SegmentIterator::_init_column_iterators(const Schema& schema) {
    SCOPED_RAW_TIMER(&_opts.stats->column_iterator_init_ns);
    const size_t n = std::max<size_t>(1 + ChunkHelper::max_column_id(schema), _column_iterators.size());
    _column_iterators.resize(n);
    if constexpr (check_global_dict) {
        _column_decoders.resize(n);
    }

    bool has_predicate = !_opts.pred_tree.empty();
    _predicate_need_rewrite.resize(n, false);
    for (const FieldPtr& f : schema.fields()) {
        const ColumnId cid = f->id();
        if (_column_iterators[cid] == nullptr) {
            bool check_dict_enc;
            if (_opts.global_dictmaps->count(cid)) {
                // if cid has global dict encode
                // we will force the use of dictionary codes
                check_dict_enc = true;
            } else if (_opts.pred_tree.contains_column(cid)) {
                // If there is an expression condition on the column
                // that can be optimized using low cardinality,
                // we will try to load the dictionary code
                check_dict_enc = _predicate_need_rewrite[cid];
            } else {
                check_dict_enc = has_predicate;
            }
            RETURN_IF_ERROR(_init_column_iterator_by_cid(cid, f->uid(), check_dict_enc));

            if constexpr (check_global_dict) {
                _column_decoders[cid].set_iterator(_column_iterators[cid].get());
                _column_decoders[cid].set_all_page_dict_encoded(_column_iterators[cid]->all_page_dict_encoded());
                if (_opts.global_dictmaps->count(cid)) {
                    _column_decoders[cid].set_global_dict(_opts.global_dictmaps->find(cid)->second);
                    _column_decoders[cid].check_global_dict();
                }
            }

            // turn off low cardinality if not all data pages are dict-encoded.
            _predicate_need_rewrite[cid] &= _column_iterators[cid]->all_page_dict_encoded();
        }
    }
    VLOG(2) << fmt::format("init_column_iterators schema={}", schema.to_string());
    VLOG(2) << fmt::format("init_column_iterators predicate_need_rewrite: {}", fmt::join(_predicate_need_rewrite, ","));
    return Status::OK();
}

bool SegmentIterator::need_early_materialize_subfield(const FieldPtr& field) {
    if (field->type()->type() != LogicalType::TYPE_STRUCT) {
        // @Todo: support json/map/array when support flat-column,
        // the performance improvement scenarios are too few now
        return false;
    }
    auto cid = field->id();
    if (_predicate_column_access_paths.find(cid) != _predicate_column_access_paths.end()) {
        return true;
    }
    return false;
}

// If this predicate is generated by join runtime filter,
// We only use it to compute segment row range.
struct IndexOnlyPredicateChecker {
    bool operator()(const PredicateColumnNode& node) const {
        const auto* col_pred = node.col_pred();
        return col_pred != nullptr && col_pred->is_index_filter_only();
    }

    template <CompoundNodeType Type>
    bool operator()(const PredicateCompoundNode<Type>& node) const {
        return std::all_of(node.children().begin(), node.children().end(),
                           [&](const auto& child) { return child.visit(*this); });
    }
};

struct ExprPredicateChecker {
    bool operator()(const PredicateColumnNode& node) const { return node.col_pred()->is_expr_predicate(); }

    template <CompoundNodeType Type>
    bool operator()(const PredicateCompoundNode<Type>& node) const {
        return std::any_of(node.children().begin(), node.children().end(),
                           [&](const auto& child) { return child.visit(*this); });
    }
};

void SegmentIterator::_init_column_predicates() {
    PredicateAndNode useless_pred_root;
    PredicateAndNode used_pred_root;
    _opts.pred_tree.root().partition_copy([](const auto& node) { return node.visit(IndexOnlyPredicateChecker()); },
                                          &useless_pred_root, &used_pred_root);
    PredicateAndNode expr_pred_root;
    PredicateAndNode non_expr_pred_root;
    used_pred_root.partition_move([](auto& node) { return node.visit(ExprPredicateChecker()); }, &expr_pred_root,
                                  &non_expr_pred_root);
    _expr_pred_tree = PredicateTree::create(std::move(expr_pred_root));
    _non_expr_pred_tree = PredicateTree::create(std::move(non_expr_pred_root));
    _runtime_filter_preds = _opts.runtime_filter_preds;
}

StatusOr<size_t> SegmentIterator::trigger_sample_if_necessary(vector<rowid_t>* rowid) {
    // Check every 16 chunks if we should trigger sampling based on selectivity
    // if only have one predicate column, do not sample
    const bool should_check_selectivity = (_context->_evaluated_chunk_nums++ % 16) == 0 &&
                                          (!_context->_only_output_one_predicate_col_with_filter_push_down);
    if (should_check_selectivity && _context->_enable_predicate_col_late_materialize) {
        // Calculate accumulated selectivity.
        // If selectivity > tigger_sample_selectivity or this is the first time, trigger sampling
        // to potentially find an even better predicate order
        double first_column_selectivity = _context->_first_column_total_rows_read == 0
                                                  ? 1
                                                  : static_cast<double>(_context->_first_column_total_rows_passed) /
                                                            _context->_first_column_total_rows_read;
        if (first_column_selectivity > config::predicate_sampling_trigger_selectivity_threshold) {
            ColumnId old_first_column_id = _context->_predicate_order[0];

            // Sample and update predicate selectivity
            ASSIGN_OR_RETURN(size_t sampled_chunk_size, _sample_predicate_columns(rowid));

            // Check if the first column changed after sampling
            ColumnId new_first_column_id = _context->_predicate_order.front();
            if (new_first_column_id != old_first_column_id) {
                // First column changed, reset selectivity statistics
                _context->_first_column_total_rows_read = 0;
                _context->_first_column_total_rows_passed = 0;
            }
            return sampled_chunk_size;
        }
    }
    return 0;
}

StatusOr<size_t> SegmentIterator::_sample_predicate_columns(vector<rowid_t>* rowid) {
    // Clear previous measurements
    _context->_predicate_selectivity_map.clear();

    // Read a sample chunk with predicate_col_late_materialize_read = false
    // This reads ALL predicate columns at once (not using late materialization)
    const uint32_t sample_size = _reserve_chunk_size;

    Chunk* chunk = _context->_read_chunk.get();

    if (!_range_iter.has_more()) {
        return 0;
    }

    // Call _read with predicate_col_late_materialize_read = false to read all columns
    RETURN_IF_ERROR(_read(chunk, rowid, sample_size, false));

    const size_t chunk_size = chunk->num_rows();
    if (chunk_size == 0) {
        return 0;
    }

    SCOPED_RAW_TIMER(&_opts.stats->vec_cond_ns);
    // Similar to RuntimeFilterProbeCollector::update_selectivity
    // Use two selections:
    // 1. _selection: for merged result of all predicates (merged_selection)
    // 2. current_selection: for current predicate result
    Buffer<uint8_t> current_selection;
    current_selection.resize(chunk_size);

    bool use_merged_selection = true; // Similar to RuntimeFilter's use_merged_selection flag

    // Evaluate each predicate column and measure its selectivity
    for (const auto& column_id : _context->_predicate_order) {
        auto* col = chunk->get_column_raw_ptr_by_id(column_id);

        const auto& predicates = _context->_column_predicate_map.at(column_id);
        bool has_compound_predicates = !predicates.empty();
        bool has_runtime_filters =
                _opts.enable_join_runtime_filter_pushdown && _column_to_runtime_filters_map.contains(column_id);

        if (!has_compound_predicates && !has_runtime_filters) {
            // Column may have been added for read-only; no predicates or runtime filters to evaluate.
            _context->_predicate_selectivity_map.emplace(1.0, column_id);
            continue;
        }

        // First predicate uses _selection (merged), subsequent use current_selection
        auto& selection = use_merged_selection ? _selection : current_selection;

        if (has_compound_predicates) {
            SCOPED_RAW_TIMER(&_opts.stats->vec_cond_evaluate_ns);
            // Use compound_and_predicates_evaluate to evaluate predicates
            // Similar to RuntimeFilter::evaluate
            RETURN_IF_ERROR(compound_and_predicates_evaluate(predicates, col, selection.data(), _selected_idx.data(), 0,
                                                             chunk_size));
        } else {
            DCHECK(has_runtime_filters);
            // No predicates but runtime filters exist, start with all rows selected.
            std::fill(selection.data(), selection.data() + chunk_size, 1);
        }

        if (has_runtime_filters) {
            RETURN_IF_ERROR(_evaluate_col_runtime_filters(column_id, col, selection.data(), 0, chunk_size));
        }

        // Count rows that passed predicates
        size_t true_count = SIMD::count_nonzero(selection.data(), chunk_size);
        double selectivity = static_cast<double>(true_count) / chunk_size;

        // Store selectivity directly as the sorting key (like RuntimeFilter)
        _context->_predicate_selectivity_map.emplace(selectivity, column_id);

        // Merge selections (similar to RuntimeFilter's logic)
        if (use_merged_selection) {
            // First predicate: result is already in _selection (merged_selection)
            use_merged_selection = false;
        } else {
            // Subsequent predicates: AND current selection with merged selection
            uint8_t* dest = _selection.data();             // merged_selection
            const uint8_t* src = current_selection.data(); // current result
            for (size_t j = 0; j < chunk_size; ++j) {
                dest[j] = src[j] & dest[j];
            }
        }
    }

    // Filter the entire chunk (including all columns and rowid) based on merged selection
    // _selection already contains the merged result, use it directly
    // This removes rows that don't pass all predicates
    size_t filtered_chunk_size = 0;
    {
        SCOPED_RAW_TIMER(&_opts.stats->vec_cond_chunk_copy_ns);
        filtered_chunk_size = _filter_chunk_by_selection(chunk, rowid, 0, chunk_size);
    }
    _opts.stats->rows_vec_cond_filtered += (chunk_size - filtered_chunk_size);

    // Update predicate order based on selectivity (lower selectivity = higher filtering = better)
    std::vector<ColumnId> new_order;
    new_order.reserve(_context->_predicate_order.size());

    for (const auto& kv : _context->_predicate_selectivity_map) {
        new_order.push_back(kv.second);
    }

    DCHECK(new_order.size() == _context->_predicate_order.size());
    if (new_order.size() != _context->_predicate_order.size()) {
        return Status::InternalError("scan predicate late materializationo error!");
    }

    const ColumnId old_first_column_id = _context->_predicate_order.front();

    // Update the predicate order
    _context->_predicate_order = std::move(new_order);

    // Update the first column iterator if it changed
    const ColumnId new_first_column_id = _context->_predicate_order.front();
    if (new_first_column_id != old_first_column_id) {
        _context->_column_iterators_for_predicate_late_materialize[0] =
                _context->_column_ids_to_column_iterators[new_first_column_id];
        _context->_column_id_for_predicate_late_materialize[0] = new_first_column_id;
    }

    // Return the filtered chunk size (after applying all predicates)
    return filtered_chunk_size;
}

Status SegmentIterator::_get_row_ranges_by_keys() {
    if (_opts.is_first_split_of_segment) {
        StarRocksMetrics::instance()->segment_row_total.increment(num_rows());
    }
    SCOPED_RAW_TIMER(&_opts.stats->rows_key_range_filter_ns);

    const uint32_t prev_num_rows = _scan_range.span_size();
    const bool is_logical_split = !_opts.short_key_ranges.empty();

    SparseRange<> scan_range_by_keys;
    if (is_logical_split) {
        ASSIGN_OR_RETURN(scan_range_by_keys, _get_row_ranges_by_short_key_ranges());
        _opts.stats->rows_key_range_num += _opts.short_key_ranges.size();
    } else {
        ASSIGN_OR_RETURN(scan_range_by_keys, _get_row_ranges_by_key_ranges());
    }

    _scan_range &= scan_range_by_keys;

    if (!is_logical_split) {
        _opts.stats->rows_key_range_filtered += prev_num_rows - _scan_range.span_size();
    } else {
        // For the multiple splits from the same segment, rows_key_range_filtered=N-n1-...-nk, where N denotes the
        // number of rows of the segment, and ni denotes the number of rows of i-th split after short key index.
        //             N rows
        // ┌─────────────────────────────┐
        // │  key range 1   key range 2  │ 2 key ranges
        // │  ┌────┬────┐   ┌────┬────┐  │
        // │  │ n1 │ n2 │   │ n3 │ n4 │  │ 4 splits
        // └──┴────┴────┴───┴────┴────┴──┘
        _opts.stats->rows_key_range_filtered += -static_cast<int64_t>(_scan_range.span_size());
        if (_opts.is_first_split_of_segment) {
            _opts.stats->rows_key_range_filtered += prev_num_rows;
        }
    }
    _opts.stats->rows_after_key_range += _scan_range.span_size();
    StarRocksMetrics::instance()->segment_rows_by_short_key.increment(_scan_range.span_size());

    return Status::OK();
}

Status SegmentIterator::_apply_tablet_range() {
    if (!_opts.tablet_range.has_value() || _opts.tablet_range.value().all_range()) {
        return Status::OK();
    }

    // _lookup_ordinal() relies on short key index.
    RETURN_IF_ERROR(_segment->load_index(_opts.lake_io_opts));
    ASSIGN_OR_RETURN(auto rowid_range_opt, _seek_range_to_rowid_range(_opts.tablet_range.value()));
    if (rowid_range_opt.has_value()) {
        _scan_range &= SparseRange<>(rowid_range_opt.value());
    } else {
        // no valid rowid range, clear the scan range
        _scan_range.clear();
    }
    return Status::OK();
}

StatusOr<SparseRange<>> SegmentIterator::_get_row_ranges_by_key_ranges() {
    DCHECK(_opts.short_key_ranges.empty());

    SparseRange<> res;

    if (_opts.ranges.empty()) {
        res.add(Range<>(0, num_rows()));
        return res;
    }

    RETURN_IF_ERROR(_segment->load_index(_opts.lake_io_opts));
    for (const SeekRange& range : _opts.ranges) {
        ASSIGN_OR_RETURN(auto rowid_range_opt, _seek_range_to_rowid_range(range));
        if (rowid_range_opt.has_value()) {
            res.add(rowid_range_opt.value());
        }
    }
    return res;
}

StatusOr<std::optional<Range<>>> SegmentIterator::_seek_range_to_rowid_range(const SeekRange& range) {
    rowid_t lower_rowid = 0;
    rowid_t upper_rowid = num_rows();

    if (!range.upper().empty()) {
        RETURN_IF_ERROR(_init_column_iterators<false>(range.upper().schema()));
        RETURN_IF_ERROR(_lookup_ordinal(range.upper(), !range.inclusive_upper(), num_rows(), &upper_rowid));
    }
    if (!range.lower().empty() && upper_rowid > 0) {
        RETURN_IF_ERROR(_init_column_iterators<false>(range.lower().schema()));
        RETURN_IF_ERROR(_lookup_ordinal(range.lower(), range.inclusive_lower(), upper_rowid, &lower_rowid));
    }

    if (lower_rowid <= upper_rowid) {
        return std::optional<Range<>>{Range{lower_rowid, upper_rowid}};
    }
    return std::nullopt;
}

StatusOr<SparseRange<>> SegmentIterator::_get_row_ranges_by_short_key_ranges() {
    DCHECK(!_opts.short_key_ranges.empty());

    SparseRange<> res;

    if (_opts.short_key_ranges.size() == 1 && _opts.short_key_ranges[0]->lower->is_infinite() &&
        _opts.short_key_ranges[0]->upper->is_infinite()) {
        res.add(Range<>(0, num_rows()));
        return res;
    }

    RETURN_IF_ERROR(_segment->load_index(_opts.lake_io_opts));
    for (const auto& short_key_range : _opts.short_key_ranges) {
        rowid_t lower_rowid = 0;
        rowid_t upper_rowid = num_rows();

        const auto& upper = short_key_range->upper;
        if (upper->tuple_key != nullptr) {
            RETURN_IF_ERROR(_init_column_iterators<false>(upper->tuple_key->schema()));
            RETURN_IF_ERROR(_lookup_ordinal(*(upper->tuple_key), !upper->inclusive, num_rows(), &upper_rowid));
        } else if (!upper->short_key.empty()) {
            RETURN_IF_ERROR(_init_column_iterators<false>(*(upper->short_key_schema)));
            RETURN_IF_ERROR(_lookup_ordinal(upper->short_key, *(upper->short_key_schema), !upper->inclusive, num_rows(),
                                            &upper_rowid));
        }

        if (upper_rowid > 0) {
            const auto& lower = short_key_range->lower;
            if (lower->tuple_key != nullptr) {
                RETURN_IF_ERROR(_init_column_iterators<false>(lower->tuple_key->schema()));
                RETURN_IF_ERROR(_lookup_ordinal(*(lower->tuple_key), lower->inclusive, upper_rowid, &lower_rowid));
            } else if (!lower->short_key.empty()) {
                RETURN_IF_ERROR(_init_column_iterators<false>(*(lower->short_key_schema)));
                RETURN_IF_ERROR(_lookup_ordinal(lower->short_key, *(lower->short_key_schema), lower->inclusive,
                                                upper_rowid, &lower_rowid));
            }
        }

        if (lower_rowid <= upper_rowid) {
            res.add(Range{lower_rowid, upper_rowid});
        }
    }

    return res;
}

struct ZoneMapFilterEvaluator {
    template <CompoundNodeType Type>
    StatusOr<std::optional<SparseRange<>>> operator()(const PredicateCompoundNode<Type>& node) {
        std::optional<SparseRange<>> row_ranges = std::nullopt;

        const auto& ctx = pred_tree.compound_node_context(node.id());
        const auto& cid_to_col_preds = ctx.cid_to_col_preds(node);

        for (const auto& [cid, col_preds] : cid_to_col_preds) {
            const auto iter = del_preds.find(cid);
            const ColumnPredicate* del_pred = iter != del_preds.end() ? &(iter->second) : nullptr;

            SparseRange<> cur_row_ranges;
            RETURN_IF_ERROR(column_iterators[cid]->get_row_ranges_by_zone_map(col_preds, del_pred, &cur_row_ranges,
                                                                              Type, &src_range));
            _merge_row_ranges<Type>(row_ranges, cur_row_ranges);
        }

        if constexpr (Type == CompoundNodeType::AND) {
            if (!has_apply_only_del_columns) {
                has_apply_only_del_columns = true;

                for (const auto& cid : del_columns) {
                    if (cid_to_col_preds.contains(cid)) {
                        continue;
                    }

                    const auto iter = del_preds.find(cid);
                    if (iter == del_preds.end()) {
                        continue;
                    }
                    const ColumnPredicate* del_pred = &(iter->second);

                    SparseRange<> cur_row_ranges;
                    RETURN_IF_ERROR(column_iterators[cid]->get_row_ranges_by_zone_map({}, del_pred, &cur_row_ranges,
                                                                                      Type, &src_range));
                    _merge_row_ranges<Type>(row_ranges, cur_row_ranges);
                }
            }
        }

        for (const auto& child : node.compound_children()) {
            ASSIGN_OR_RETURN(auto cur_row_ranges_opt, child.visit(*this));
            if (cur_row_ranges_opt.has_value()) {
                _merge_row_ranges<Type>(row_ranges, cur_row_ranges_opt.value());
            }
        }

        return row_ranges;
    }

    template <CompoundNodeType Type>
    void _merge_row_ranges(std::optional<SparseRange<>>& dest, SparseRange<>& source) {
        if (!dest.has_value()) {
            dest = std::move(source);
        } else {
            if constexpr (Type == CompoundNodeType::AND) {
                dest.value() &= source;
            } else {
                dest.value() |= source;
            }
        }
    }

    const PredicateTree& pred_tree;
    std::vector<std::unique_ptr<ColumnIterator>>& column_iterators;

    const std::map<ColumnId, ColumnOrPredicate>& del_preds;
    const std::set<ColumnId>& del_columns;
    const Range<> src_range;
    bool has_apply_only_del_columns = false;
};

Status SegmentIterator::_get_row_ranges_by_zone_map() {
    RETURN_IF(!config::enable_index_page_level_zonemap_filter, Status::OK());
    RETURN_IF(_scan_range.empty(), Status::OK());

    SCOPED_RAW_TIMER(&_opts.stats->zone_map_filter_ns);
    SparseRange<> zm_range(0, num_rows());

    // -------------------------------------------------------------
    // group delete predicates by column id.
    // -------------------------------------------------------------
    // e.g, if there are two (disjunctive) delete predicates:
    // `c1=1 and c2=100` and `c1=100 and c2=200`, the group result
    // will be a mapping of `c1` to predicate `c1=1 or c1=100` and a
    // mapping of `c2` to predicate `c2=100 or c2=200`.
    std::set<ColumnId> del_columns;
    _opts.delete_predicates.get_column_ids(&del_columns);
    for (ColumnId cid : del_columns) {
        std::vector<const ColumnPredicate*> preds;
        for (size_t i = 0; i < _opts.delete_predicates.size(); i++) {
            _opts.delete_predicates[i].predicates_of_column(cid, &preds);
        }
        DCHECK(!preds.empty());
        _del_predicates.insert({cid, ColumnOrPredicate(get_type_info(preds[0]->type_info()), cid, preds)});
    }

    // -------------------------------------------------------------
    // prune data pages by zone map index.
    // -------------------------------------------------------------

    ASSIGN_OR_RETURN(auto hit_row_ranges, _opts.pred_tree_for_zone_map.visit(ZoneMapFilterEvaluator{
                                                  _opts.pred_tree_for_zone_map, _column_iterators, _del_predicates,
                                                  del_columns, Range<>{_scan_range.begin(), _scan_range.end()}}));
    if (hit_row_ranges.has_value()) {
        zm_range &= hit_row_ranges.value();
    }

    StarRocksMetrics::instance()->segment_rows_read_by_zone_map.increment(zm_range.span_size());
    size_t prev_size = _scan_range.span_size();
    _scan_range = _scan_range.intersection(zm_range);
    _opts.stats->rows_stats_filtered += (prev_size - _scan_range.span_size());
    return Status::OK();
}

// if |lower| is true, return the first row in the range [0, end) that is not less than |key|,
// or end if no such row is found.
// if |lower| is false, return the first row in the range [0, end) that is greater than |key|,
// or end if no such row is found.
// |rowid| will be assigned to the id of found row or |end| if no such row is found.
Status SegmentIterator::_lookup_ordinal(const SeekTuple& key, bool lower, rowid_t end, rowid_t* rowid) {
    std::string index_key;
    index_key = lower ? key.short_key_encode(_segment->num_short_keys(), KEY_MINIMAL_MARKER)
                      : key.short_key_encode(_segment->num_short_keys(), KEY_MAXIMAL_MARKER);

    uint32_t start_block_id;
    auto start_iter = _segment->lower_bound(index_key);
    if (start_iter.valid()) {
        // Because previous block may contain this key, so we should set rowid to
        // last block's first row.
        start_block_id = start_iter.ordinal();
        if (start_block_id > 0) {
            start_block_id--;
        }
    } else {
        // When we don't find a valid index item, which means all short key is
        // smaller than input key, this means that this key may exist in the last
        // row block. so we set the rowid to first row of last row block.
        start_block_id = _segment->last_block();
    }
    rowid_t start = start_block_id * _segment->num_rows_per_block();

    auto end_iter = _segment->upper_bound(index_key);
    if (end_iter.valid()) {
        end = end_iter.ordinal() * _segment->num_rows_per_block();
    }

    // binary search to find the exact key
    ChunkPtr chunk = ChunkHelper::new_chunk(key.schema(), 1);
    if (lower) {
        while (start < end) {
            chunk->reset();
            rowid_t mid = start + (end - start) / 2;
            RETURN_IF_ERROR(_seek_columns(key.schema(), mid));
            RETURN_IF_ERROR(_read_columns(key.schema(), chunk.get(), 1));
            if (compare(key, *chunk) > 0) {
                start = mid + 1;
            } else {
                end = mid;
            }
        }
    } else {
        while (start < end) {
            chunk->reset();
            rowid_t mid = start + (end - start) / 2;
            RETURN_IF_ERROR(_seek_columns(key.schema(), mid));
            RETURN_IF_ERROR(_read_columns(key.schema(), chunk.get(), 1));
            if (compare(key, *chunk) < 0) {
                end = mid;
            } else {
                start = mid + 1;
            }
        }
    }
    *rowid = start;
    return Status::OK();
}

Status SegmentIterator::_lookup_ordinal(const Slice& index_key, const Schema& short_key_schema, bool lower, rowid_t end,
                                        rowid_t* rowid) {
    uint32_t start_block_id;
    auto start_iter = _segment->lower_bound(index_key);
    if (start_iter.valid()) {
        // Because previous block may contain this key, so we should set rowid to
        // last block's first row.
        start_block_id = start_iter.ordinal();
        if (start_block_id > 0) {
            start_block_id--;
        }
    } else {
        // When we don't find a valid index item, which means all short key is
        // smaller than input key, this means that this key may exist in the last
        // row block. so we set the rowid to first row of last row block.
        start_block_id = _segment->last_block();
    }
    rowid_t start = start_block_id * _segment->num_rows_per_block();

    auto end_iter = _segment->upper_bound(index_key);
    if (end_iter.valid()) {
        end = end_iter.ordinal() * _segment->num_rows_per_block();
    }

    // binary search to find the exact key
    ChunkPtr chunk = ChunkHelper::new_chunk(short_key_schema, 1);
    if (lower) {
        while (start < end) {
            chunk->reset();
            rowid_t mid = start + (end - start) / 2;
            RETURN_IF_ERROR(_seek_columns(short_key_schema, mid));
            RETURN_IF_ERROR(_read_columns(short_key_schema, chunk.get(), 1));
            if (compare(index_key, *chunk, short_key_schema) > 0) {
                start = mid + 1;
            } else {
                end = mid;
            }
        }
    } else {
        while (start < end) {
            chunk->reset();
            rowid_t mid = start + (end - start) / 2;
            RETURN_IF_ERROR(_seek_columns(short_key_schema, mid));
            RETURN_IF_ERROR(_read_columns(short_key_schema, chunk.get(), 1));
            if (compare(index_key, *chunk, short_key_schema) < 0) {
                end = mid;
            } else {
                start = mid + 1;
            }
        }
    }
    *rowid = start;
    return Status::OK();
}

Status SegmentIterator::_seek_columns(const Schema& schema, rowid_t pos) {
    SCOPED_RAW_TIMER(&_opts.stats->block_seek_ns);
    for (const FieldPtr& f : schema.fields()) {
        RETURN_IF_ERROR(_column_iterators[f->id()]->seek_to_ordinal(pos));
    }
    return Status::OK();
}

Status SegmentIterator::_read_columns(const Schema& schema, Chunk* chunk, size_t nrows) {
    SCOPED_RAW_TIMER(&_opts.stats->block_fetch_ns);
    const size_t n = schema.num_fields();
    bool may_has_del_row = chunk->delete_state() != DEL_NOT_SATISFIED;
    for (size_t i = 0; i < n; i++) {
        ColumnId cid = schema.field(i)->id();
        auto* column = chunk->get_column_raw_ptr_by_index(i);
        size_t nread = nrows;
        RETURN_IF_ERROR(_column_iterators[cid]->next_batch(&nread, column));
        may_has_del_row = may_has_del_row | (column->delete_state() != DEL_NOT_SATISFIED);
        DCHECK_EQ(nrows, nread);
    }
    chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    return Status::OK();
}

inline Status SegmentIterator::_read(Chunk* chunk, vector<rowid_t>* rowids, size_t n,
                                     bool predicate_col_late_materialize_read) {
    size_t read_num = 0;
    SparseRange<> range;

    // Always seek columns if switching between late materialization modes or position changed
    // This ensures all column iterators (_column_iterators vs _column_iterators_for_predicate_late_materialize)
    // are synchronized at the same position
    if (_cur_rowid != _range_iter.begin() || _cur_rowid == 0 ||
        (_context->_enable_predicate_col_late_materialize && !predicate_col_late_materialize_read)) {
        _cur_rowid = _range_iter.begin();
        _opts.stats->block_seek_num += 1;
        SCOPED_RAW_TIMER(&_opts.stats->block_seek_ns);
        RETURN_IF_ERROR(_context->seek_columns(_cur_rowid, predicate_col_late_materialize_read));
    }

    _range_iter.next_range(n, &range);
    read_num += range.span_size();
    {
        _opts.stats->blocks_load += 1;
        SCOPED_RAW_TIMER(&_opts.stats->block_fetch_ns);
        RETURN_IF_ERROR(
                _context->read_columns(chunk, range, predicate_col_late_materialize_read, &_selection, &_selected_idx));
        if (!predicate_col_late_materialize_read) {
            chunk->check_or_die();
        }
    }

    if (rowids != nullptr) {
        rowids->reserve(rowids->size() + n);
        SparseRangeIterator<> iter = range.new_iterator();
        while (iter.has_more()) {
            Range<> r = iter.next(n);
            for (uint32_t i = r.begin(); i < r.end(); i++) {
                rowids->push_back(i);
            }
        }
    }

    _cur_rowid = range.end();
    _opts.stats->raw_rows_read += read_num;
    return Status::OK();
}

Status SegmentIterator::do_get_next(Chunk* chunk) {
    if (!_inited) {
        RETURN_IF_ERROR(_init());
        _inited = true;
    }

    RETURN_IF_ERROR(_try_to_update_ranges_by_runtime_filter());

    DCHECK_EQ(0, chunk->num_rows());

    Status st;
    std::vector<uint32_t> rowids;
    std::vector<uint32_t>* p_rowids =
            (_vector_index_ctx && _vector_index_ctx->always_build_rowid()) ? &rowids : nullptr;
    do {
        st = _do_get_next(chunk, p_rowids);
    } while (st.ok() && chunk->num_rows() == 0);
    return st;
}

Status SegmentIterator::do_get_next(Chunk* chunk, vector<uint32_t>* rowid) {
    if (!_inited) {
        RETURN_IF_ERROR(_init());
        _inited = true;
    }

    RETURN_IF_ERROR(_try_to_update_ranges_by_runtime_filter());

    DCHECK_EQ(0, chunk->num_rows());

    Status st;
    do {
        st = _do_get_next(chunk, rowid);
    } while (st.ok() && chunk->num_rows() == 0);
    return st;
}

Status SegmentIterator::do_get_next(Chunk* chunk, vector<uint64_t>* rssid_rowids) {
    if (!_inited) {
        RETURN_IF_ERROR(_init());
        _inited = true;
    }

    RETURN_IF_ERROR(_try_to_update_ranges_by_runtime_filter());

    DCHECK_EQ(0, chunk->num_rows());

    Status st;
    vector<uint32_t> rowids;
    do {
        st = _do_get_next(chunk, &rowids);
    } while (st.ok() && chunk->num_rows() == 0);
    if (st.ok()) {
        // encode rssid with rowid
        // | rssid (32bit) | rowid (32bit) |
        uint64_t rssid_shift = (uint64_t)(_opts.rowset_id + segment_id()) << 32;
        for (uint32_t rowid : rowids) {
            rssid_rowids->push_back(rssid_shift | rowid);
        }
    }
    return st;
}

Status SegmentIterator::_do_get_next(Chunk* result, vector<rowid_t>* rowid) {
    MonotonicStopWatch sw;
    sw.start();

#ifdef USE_STAROS
    // only used for CACHE SELECT, do not form any chunk to save CPU time,
    // just read file content in `_scan_range`
    if (_opts.lake_io_opts.cache_file_only) {
        // read every column in this segment at once, maybe optimize this later
        size_t buf_size = config::starlet_fs_stream_buffer_size_bytes;
        if (buf_size <= 0) {
            buf_size = 1048576; // 1MB
        }
        _context->_read_chunk->reset();
        Chunk* chunk = _context->_read_chunk.get();
        size_t column_index = 0;
        for (size_t cid = 0; cid < _column_iterators.size(); ++cid) {
            if (_column_iterators[cid] == nullptr) {
                continue;
            }
            auto* col = chunk->get_column_raw_ptr_by_index(column_index++);
            if (!_scan_range.empty()) {
                RETURN_IF_ERROR(_column_iterators[cid]->seek_to_ordinal(_scan_range.begin()));
            }
            ASSIGN_OR_RETURN(auto vec, _column_iterators[cid]->get_io_range_vec(_scan_range, col));
            for (auto e : vec) {
                // if buf_size is 1MB, offset is 123, and size is 2MB
                // after calculation, offset will be 0, and size will be 2MB+123
                size_t offset = (e.first / buf_size) * buf_size;
                size_t size = e.second + (e.first % buf_size);
                while (size > 0) {
                    size_t cur_size = std::min(buf_size, size);
                    RETURN_IF_ERROR(_column_files[cid]->touch_cache(offset, cur_size));
                    offset += cur_size;
                    size -= cur_size;
                }
            }
        }

        _opts.stats->block_load_ns += sw.elapsed_time();

        return Status::EndOfFile("no more data in segment");
    }
#endif // USE_STAROS

    const int64_t prev_raw_rows_read = _opts.stats->raw_rows_read;

    _context->_read_chunk->reset();
    _context->_dict_chunk->reset();
    _context->_final_chunk->reset();
    _context->_adapt_global_dict_chunk->reset();

    Chunk* chunk = _context->_read_chunk.get();

    StatusOr<size_t> predicte_result = _predicate_evaluate(rowid);

    _opts.stats->block_load_ns += sw.elapsed_time();

    int64_t total_read = _opts.stats->raw_rows_read - prev_raw_rows_read;

    if (UNLIKELY(predicte_result.status().is_end_of_file() || !predicte_result.status().ok())) {
        return predicte_result.status();
    }

    size_t chunk_size = predicte_result.value();

    if (_context->_has_dict_column) {
        chunk = _context->_dict_chunk.get();
        RETURN_IF_ERROR(_decode_dict_codes(_context));
    }

    _build_final_chunk(_context);
    chunk = _context->_final_chunk.get();

    bool need_switch_context = false;
    if (_context->_late_materialize) {
        chunk = _context->_final_chunk.get();
        _opts.stats->late_materialize_rows += chunk_size;
        SCOPED_RAW_TIMER(&_opts.stats->late_materialize_ns);
        RETURN_IF_ERROR(_finish_late_materialization(_context));
        if (_context->_next != nullptr && (chunk_size * 1000 > total_read * _late_materialization_ratio)) {
            need_switch_context = true;
        }
    } else if (_context->_next != nullptr && _context_switch_count < 3 &&
               chunk_size * 1000 <= total_read * _late_materialization_ratio) {
        need_switch_context = true;
        _context_switch_count++;
    }

    // remove (logical) deleted rows.
    if (chunk_size > 0 && chunk->delete_state() != DEL_NOT_SATISFIED && !_opts.delete_predicates.empty()) {
        SCOPED_RAW_TIMER(&_opts.stats->del_filter_ns);
        size_t old_sz = chunk->num_rows();
        // NOTE: risk of using _selection.data() without initialization
        // if the delete_predicates do nothing to the selection.
        RETURN_IF_ERROR(_opts.delete_predicates.evaluate(chunk, _selection.data()));
        size_t deletes = SIMD::count_nonzero(_selection.data(), old_sz);
        if (deletes == old_sz) {
            chunk->set_num_rows(0);
            if (rowid != nullptr) {
                rowid->resize(0);
            }
            _opts.stats->rows_del_filtered += old_sz;
        } else if (deletes > 0) {
            // flip boolean result.
            for (size_t i = 0; i < old_sz; i++) {
                _selection[i] = !_selection[i];
            }
            size_t new_sz = chunk->filter_range(_selection, 0, old_sz);
            if (rowid != nullptr) {
                auto size = ColumnHelper::filter_range<uint32_t>(_selection, rowid->data(), 0, old_sz);
                rowid->resize(size);
            }
            _opts.stats->rows_del_filtered += old_sz - new_sz;
        }
    }

    ASSIGN_OR_RETURN(chunk_size, _filter_by_record_predicate(chunk, rowid));

    if (_context->_has_force_dict_encode) {
        RETURN_IF_ERROR(_encode_to_global_id(_context));
        chunk = _context->_adapt_global_dict_chunk.get();
    }

    if (_vector_index_ctx && _vector_index_ctx->use_vector_index && !_vector_index_ctx->use_ivfpq) {
        DCHECK(rowid != nullptr);
        FloatColumn::MutablePtr distance_column = FloatColumn::create();
        vector<rowid_t> rowids;
        for (const auto& rid : *rowid) {
            auto it = _vector_index_ctx->id2distance_map.find(rid);
            if (LIKELY(it != _vector_index_ctx->id2distance_map.end())) {
                rowids.emplace_back(it->first);
            } else {
                DCHECK(false) << "not found row id:" << rid << " in distance map";
                return Status::InternalError(fmt::format("not found row id:{} in distance map", rid));
            }
        }
        for (const auto& vrid : rowids) {
            distance_column->append(_vector_index_ctx->id2distance_map[vrid]);
        }

        // TODO: plan vector column in FE Planner
        chunk->append_vector_column(std::move(distance_column), _make_field(_vector_index_ctx->vector_column_id),
                                    _vector_index_ctx->vector_slot_id);
    }

    result->swap_chunk(*chunk);

    if (need_switch_context) {
        RETURN_IF_ERROR(_switch_context(_context->_next));
    }

    return Status::OK();
}

StatusOr<size_t> SegmentIterator::_predicate_evaluate(vector<rowid_t>* rowid) {
    if (_context->_enable_predicate_col_late_materialize ||
        _context->_only_output_one_predicate_col_with_filter_push_down) {
        return _predicate_evaluate_late_materialize(rowid);
    } else {
        return _predicate_evaluate_without_late_materialize(rowid);
    }
}

StatusOr<size_t> SegmentIterator::_predicate_evaluate_late_materialize_read_first_column(
        vector<rowid_t>* rowid, std::vector<Column*>& current_columns) {
    const uint32_t chunk_capacity = _reserve_chunk_size;
    const uint32_t return_chunk_threshold = std::max<uint32_t>(chunk_capacity - chunk_capacity / 4, 1);
    const bool has_non_expr_predicate = !_non_expr_pred_tree.empty();
    const bool scan_range_normalized = _scan_range.is_sorted();
    Chunk* chunk = _context->_read_chunk.get();
    current_columns.reserve(_context->_column_id_for_predicate_late_materialize.size());

    const ColumnId first_column_id = _context->_predicate_order.front();
    auto* first_col = chunk->get_column_raw_ptr_by_id(first_column_id);

    const ColumnPredicateMap& non_expr_column_predicate_map = _non_expr_pred_tree.get_immediate_column_predicate_map();
    const ColumnPredicateMap& expr_column_predicate_map = _expr_pred_tree.get_immediate_column_predicate_map();

    current_columns.emplace_back(first_col);
    if (_context->_enable_predicate_col_late_materialize) {
        DCHECK(!_context->_only_output_one_predicate_col_with_filter_push_down);
        auto* rowid_column = chunk->get_column_raw_ptr_by_id(_context->_row_id_column_id);
        current_columns.emplace_back(rowid_column);
    }
    uint16_t chunk_start = first_col->size();

    size_t original_chunk_size = chunk_start;
    while ((chunk_start < return_chunk_threshold) && _range_iter.has_more()) {
        RETURN_IF_ERROR(_read(chunk, rowid, chunk_capacity - chunk_start, true));
        // chunk->check_or_die();
        size_t next_start = first_col->size();

        // Count how many rowids were read by this _read() call
        if (!_context->_is_filtered) {
            size_t rows_read_this_iter = next_start - chunk_start;
            _context->_first_column_total_rows_read += rows_read_this_iter;
        }

        if (has_non_expr_predicate && !_context->_is_filtered &&
            non_expr_column_predicate_map.contains(first_column_id)) {
            // use first column's non-expr predicate to filter data
            // This will filter rowid_column, removing rows that don't match the predicate
            // column-expr-predicate doesn't support [begin, end] interface
            ASSIGN_OR_RETURN(next_start, _filter_by_compound_and_predicates(
                                                 chunk, rowid, chunk_start, next_start, first_column_id,
                                                 non_expr_column_predicate_map.at(first_column_id), current_columns));
        }

        chunk_start = next_start;

        if (chunk_start && !scan_range_normalized) {
            break;
        }
    }

    size_t chunk_size = first_col->size();

    if (expr_column_predicate_map.contains(first_column_id) && !_context->_is_filtered) {
        // column-expr-predicate doesn't support [begin, end] interface
        ASSIGN_OR_RETURN(chunk_size, _filter_by_compound_and_predicates(chunk, rowid, 0, chunk_size, first_column_id,
                                                                        expr_column_predicate_map.at(first_column_id),
                                                                        current_columns, false));
    }

    if (!_context->_is_filtered) {
        _context->_first_column_total_rows_passed += (chunk_size - original_chunk_size);
    }
    return chunk_size;
}

Status SegmentIterator::_evaluate_late_materialize_read_other_columns(vector<rowid_t>* rowid,
                                                                      std::vector<Column*>& current_columns,
                                                                      size_t& chunk_size) {
    Chunk* chunk = _context->_read_chunk.get();
    bool may_has_del_row = chunk->delete_state() != DEL_NOT_SATISFIED;
    for (int i = 1; i < _context->_predicate_order.size(); i++) {
        const ColumnId current_column_id = _context->_predicate_order[i];
        auto rowid_column = chunk->get_column_raw_ptr_by_id(_context->_row_id_column_id);
        // read column by row id if not read yet
        const auto* ordinals = down_cast<const FixedLengthColumn<rowid_t>*>(rowid_column);
        auto* col = chunk->get_column_raw_ptr_by_id(current_column_id);
        current_columns.emplace_back(col);

        col->resize(0);
        {
            SCOPED_RAW_TIMER(&_opts.stats->late_materialize_ns);
            // for dict column, no matter it's global or local
            // we should get its local dict values in predicate evaluation which is same as next_batch
            // because the corresponding predicates are already rewritten by local dictionary
            ColumnIterator* cur_iter = _context->_column_ids_to_column_iterators[current_column_id];
            cur_iter->reserve_col(chunk_size, col);
            RETURN_IF_ERROR(cur_iter->fetch_values_by_rowid_for_predicate_evaluate(*ordinals, col));
        }
        if (ordinals->size() != col->size()) {
            return Status::Corruption("_predicate_evaluate_late_materialize col size not equal to ordinal col size");
        }
        may_has_del_row |= (col->delete_state() != DEL_NOT_SATISFIED);

        // evaluate predicate on this column, including expr and non-expr predicates
        ASSIGN_OR_RETURN(chunk_size, _filter_by_compound_and_predicates(
                                             chunk, rowid, 0, chunk_size, current_column_id,
                                             _context->_column_predicate_map.at(current_column_id), current_columns));
    }

    DCHECK(current_columns.size() == chunk->num_columns())
            << "current column size:" << current_columns.size() << ", chunk column size:" << chunk->num_columns();

    // check chunk until every predicate column is read
    chunk->check_or_die();

    chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    return Status::OK();
}

StatusOr<size_t> SegmentIterator::_predicate_evaluate_late_materialize(vector<rowid_t>* rowid) {
    ASSIGN_OR_RETURN(size_t sampled_chunk_size, trigger_sample_if_necessary(rowid));
    if (sampled_chunk_size > 0) {
        // Sampling was triggered, return the sampled chunk size without further processing
        return sampled_chunk_size;
    }

    std::vector<Column*> current_columns;
    ASSIGN_OR_RETURN(size_t chunk_size, _predicate_evaluate_late_materialize_read_first_column(rowid, current_columns));

    RETURN_IF_ERROR(_evaluate_late_materialize_read_other_columns(rowid, current_columns, chunk_size));

    if (UNLIKELY(chunk_size == 0 && !_range_iter.has_more())) {
        // Return directly if chunk_start is zero, i.e, chunk is empty.
        // Otherwise, chunk will be swapped with result, which is incorrect
        // because the chunk is a pointer to _read_chunk instead of _final_chunk.
        return Status::EndOfFile("no more data in segment");
    }
    return chunk_size;
}

StatusOr<size_t> SegmentIterator::_predicate_evaluate_without_late_materialize(vector<rowid_t>* rowid) {
    const uint32_t chunk_capacity = _reserve_chunk_size;
    const uint32_t return_chunk_threshold = std::max<uint32_t>(chunk_capacity - chunk_capacity / 4, 1);
    const bool has_non_expr_predicate = !_non_expr_pred_tree.empty();
    const bool scan_range_normalized = _scan_range.is_sorted();

    Chunk* chunk = _context->_read_chunk.get();
    uint16_t chunk_start = chunk->num_rows();
    while ((chunk_start < return_chunk_threshold) & _range_iter.has_more()) {
        RETURN_IF_ERROR(_read(chunk, rowid, chunk_capacity - chunk_start, false));
        chunk->check_or_die();
        size_t next_start = chunk->num_rows();

        if (has_non_expr_predicate) {
            ASSIGN_OR_RETURN(next_start, _filter_by_non_expr_predicates(chunk, rowid, chunk_start, next_start));
            chunk->check_or_die();
        }
        chunk_start = next_start;
        DCHECK_EQ(chunk_start, chunk->num_rows());

        if (chunk_start && !scan_range_normalized) {
            break;
        }
    }

    size_t raw_chunk_size = chunk->num_rows();
    if (UNLIKELY(raw_chunk_size == 0)) {
        // Return directly if chunk_start is zero, i.e, chunk is empty.
        // Otherwise, chunk will be swapped with result, which is incorrect
        // because the chunk is a pointer to _read_chunk instead of _final_chunk.
        return Status::EndOfFile("no more data in segment");
    }

    ASSIGN_OR_RETURN(size_t chunk_size, _filter_by_expr_predicates(chunk, rowid));

    return chunk_size;
}

FieldPtr SegmentIterator::_make_field(size_t i) {
    DCHECK(_vector_index_ctx != nullptr);
    return std::make_shared<Field>(i, _vector_index_ctx->vector_distance_column_name, get_type_info(TYPE_FLOAT), false);
}

Status SegmentIterator::_switch_context(ScanContext* to) {
    if (_context != nullptr) {
        const ordinal_t ordinal = _cur_rowid;
        for (ColumnIterator* iter : to->_column_iterators) {
            RETURN_IF_ERROR(iter->seek_to_ordinal(ordinal));
        }
        _context->close();
    }

    if (to->_read_chunk == nullptr) {
        ASSIGN_OR_RETURN(to->_read_chunk, ChunkHelper::new_chunk_checked(to->_read_schema, _reserve_chunk_size));
    }

    if (to->_has_dict_column) {
        if (to->_dict_chunk == nullptr) {
            ASSIGN_OR_RETURN(to->_dict_chunk,
                             ChunkHelper::new_chunk_checked(to->_dict_decode_schema, _reserve_chunk_size));
        }
    } else {
        to->_dict_chunk = to->_read_chunk;
    }

    DCHECK_GT(this->output_schema().num_fields(), 0);

    if (to->_has_force_dict_encode) {
        // This branch may be caused by dictionary inconsistency (there is no local dictionary, but the
        // global dictionary exists), so our processing method is read->decode->materialize->encode.
        // the column after materialize is binary_column

        // rebuild encoded schema
        // If a column global dictionary cannot be applied to a local dictionary.
        // We need to disable the global dictionary for these columns first
        _encoded_schema.clear();
        for (const auto& field : schema().fields()) {
            if (_can_using_global_dict(field)) {
                _encoded_schema.append(Field::convert_to_dict_field(*field));
            } else {
                _encoded_schema.append(field);
            }
        }

        // Rebuilding final_chunk schema. filter_unused_columns will prune out useless columns in encode_schema
        Schema final_chunk_schema;
        DCHECK_GE(_encoded_schema.num_fields(), output_schema().num_fields());
        size_t output_schema_idx = 0;
        for (size_t i = 0; i < _encoded_schema.num_fields(); ++i) {
            if (_encoded_schema.field(i)->id() == output_schema().field(output_schema_idx)->id()) {
                final_chunk_schema.append(_encoded_schema.field(i));
                output_schema_idx++;
            }
        }
        ASSIGN_OR_RETURN(to->_final_chunk, ChunkHelper::new_chunk_checked(final_chunk_schema, _reserve_chunk_size));
    } else {
        ASSIGN_OR_RETURN(to->_final_chunk, ChunkHelper::new_chunk_checked(this->output_schema(), _reserve_chunk_size));
    }
    if (to->_has_force_dict_encode) {
        ASSIGN_OR_RETURN(to->_adapt_global_dict_chunk,
                         ChunkHelper::new_chunk_checked(this->output_schema(), _reserve_chunk_size));
    } else {
        to->_adapt_global_dict_chunk = to->_final_chunk;
    }

    _context = to;
    return Status::OK();
}

Status SegmentIterator::_evaluate_col_runtime_filters(ColumnId column_id, Column* col, uint8_t* selection,
                                                      uint16_t from, uint16_t to) {
    if (!_opts.enable_join_runtime_filter_pushdown) {
        return Status::OK();
    }

    auto iter = _column_to_runtime_filters_map.find(column_id);
    if (iter == _column_to_runtime_filters_map.end()) {
        return Status::OK();
    }

    SCOPED_RAW_TIMER(&_opts.stats->rf_cond_evaluate_ns);
    size_t input_count = SIMD::count_nonzero(&selection[from], to - from);

    Chunk chunk;
    // `column` is owned by storage layer, we don't have ownership
    ColumnPtr bits = col->as_mutable_ptr();
    chunk.append_column(bits, column_id, true);

    RETURN_IF_ERROR(iter->second.evaluate(&chunk, selection, from, to));

    size_t output_count = SIMD::count_nonzero(&selection[from], to - from);
    _opts.stats->rf_cond_input_rows += input_count;
    _opts.stats->rf_cond_output_rows += output_count;
    return Status::OK();
}

StatusOr<uint16_t> SegmentIterator::_filter_by_compound_and_predicates(
        Chunk* chunk, vector<rowid_t>* rowid, uint16_t from, uint16_t to, ColumnId column_id,
        const std::vector<const ColumnPredicate*>& and_predicates, std::vector<Column*>& current_cols,
        bool apply_runtime_filter) {
    if (from == to) {
        return to;
    }

    // Check if we need to process this column at all
    bool has_compound_predicates = and_predicates.size() > 0;
    bool has_runtime_filters = apply_runtime_filter && _opts.enable_join_runtime_filter_pushdown &&
                               _column_to_runtime_filters_map.contains(column_id);

    if (!has_compound_predicates && !has_runtime_filters) {
        return to;
    }

    SCOPED_RAW_TIMER(&_opts.stats->vec_cond_ns);
    Column* col = chunk->get_column_raw_ptr_by_id(column_id);
    // Evaluate compound predicates
    if (has_compound_predicates) {
        SCOPED_RAW_TIMER(&_opts.stats->vec_cond_evaluate_ns);
        if (col->empty()) {
            DCHECK(from == to);
            return to;
        }
        RETURN_IF_ERROR(compound_and_predicates_evaluate(and_predicates, col, _selection.data(), _selected_idx.data(),
                                                         from, to));
    } else {
        // If no compound predicates but have runtime filters, initialize selection to all true
        std::fill(&_selection[from], &_selection[to], 1);
    }

    // Evaluate runtime filters for this column
    if (has_runtime_filters) {
        RETURN_IF_ERROR(_evaluate_col_runtime_filters(column_id, col, _selection.data(), from, to));
    }

    SCOPED_RAW_TIMER(&_opts.stats->vec_cond_chunk_copy_ns);
    uint16_t chunk_size = _filter_columns_by_selection(current_cols, rowid, from, to);
    _opts.stats->rows_vec_cond_filtered += (to - chunk_size);
    return chunk_size;
}

StatusOr<uint16_t> SegmentIterator::_filter_by_non_expr_predicates(Chunk* chunk, vector<rowid_t>* rowid, uint16_t from,
                                                                   uint16_t to) {
    // There must be one predicate, either vectorized or branchless.
    DCHECK(!_non_expr_pred_tree.empty() || _del_vec);

    SCOPED_RAW_TIMER(&_opts.stats->vec_cond_ns);

    {
        SCOPED_RAW_TIMER(&_opts.stats->vec_cond_evaluate_ns);
        RETURN_IF_ERROR(_non_expr_pred_tree.evaluate(chunk, _selection.data(), from, to));
    }

    auto hit_count = SIMD::count_nonzero(&_selection[from], to - from);
    if (_opts.enable_join_runtime_filter_pushdown && !_runtime_filter_preds.empty()) {
        SCOPED_RAW_TIMER(&_opts.stats->rf_cond_evaluate_ns);
        size_t input_count = hit_count;
        RETURN_IF_ERROR(_runtime_filter_preds.evaluate(chunk, _selection.data(), from, to));
        hit_count = SIMD::count_nonzero(&_selection[from], to - from);
        _opts.stats->rf_cond_input_rows += input_count;
        _opts.stats->rf_cond_output_rows += hit_count;
    }

    SCOPED_RAW_TIMER(&_opts.stats->vec_cond_chunk_copy_ns);
    uint16_t chunk_size = _filter_chunk_by_selection(chunk, rowid, from, to);
    _opts.stats->rows_vec_cond_filtered += (to - chunk_size);
    return chunk_size;
}

StatusOr<uint16_t> SegmentIterator::_filter_by_expr_predicates(Chunk* chunk, vector<rowid_t>* rowid) {
    size_t chunk_size = chunk->num_rows();
    if (chunk_size > 0 && !_expr_pred_tree.empty()) {
        SCOPED_RAW_TIMER(&_opts.stats->expr_cond_evaluate_ns);
        RETURN_IF_ERROR(_expr_pred_tree.evaluate(chunk, _selection.data(), 0, chunk_size));

        size_t new_size = _filter_chunk_by_selection(chunk, rowid, 0, chunk_size);
        _opts.stats->rows_vec_cond_filtered += (chunk_size - new_size);
        chunk_size = new_size;
    }
    return chunk_size;
}

StatusOr<uint16_t> SegmentIterator::_filter_by_record_predicate(Chunk* chunk, vector<rowid_t>* rowid) {
    size_t chunk_size = chunk->num_rows();
    if (chunk_size > 0 && _opts.record_predicate != nullptr) {
        SCOPED_RAW_TIMER(&_opts.stats->record_predicate_evaluate_ns);
        RETURN_IF_ERROR(_opts.record_predicate->evaluate(chunk, _selection.data()));

        size_t new_size = _filter_chunk_by_selection(chunk, rowid, 0, chunk_size);
        _opts.stats->rows_record_predicate_filtered += (chunk_size - new_size);
        chunk_size = new_size;
    }
    return chunk_size;
}

uint16_t SegmentIterator::_filter_chunk_by_selection(Chunk* chunk, vector<rowid_t>* rowid, uint16_t from, uint16_t to) {
    auto hit_count = SIMD::count_nonzero(&_selection[from], to - from);
    uint16_t chunk_size = to;
    if (hit_count == 0) {
        chunk_size = from;
        chunk->set_num_rows(chunk_size);
        if (rowid != nullptr) {
            rowid->resize(chunk_size);
        }
    } else if (hit_count != to - from) {
        chunk_size = chunk->filter_range(_selection, from, to);
        if (rowid != nullptr) {
            auto size = ColumnHelper::filter_range<uint32_t>(_selection, rowid->data(), from, to);
            rowid->resize(size);
        }
    }
    return chunk_size;
}

uint16_t SegmentIterator::_filter_columns_by_selection(std::vector<Column*>& columns, vector<rowid_t>* rowid,
                                                       uint16_t from, uint16_t to) {
    auto hit_count = SIMD::count_nonzero(&_selection[from], to - from);
    uint16_t chunk_size = to;
    if (hit_count == 0) {
        chunk_size = from;
        for (auto& column : columns) {
            column->resize(chunk_size);
        }
        if (rowid != nullptr) {
            rowid->resize(chunk_size);
        }
    } else if (hit_count != to - from) {
        for (auto& column : columns) {
            column->filter_range(_selection, from, to);
        }
        chunk_size = columns[0]->size();
        if (rowid != nullptr) {
            auto size = ColumnHelper::filter_range<uint32_t>(_selection, rowid->data(), from, to);
            rowid->resize(size);
        }
    }

    return chunk_size;
}

inline bool SegmentIterator::_can_using_dict_code(const FieldPtr& field) const {
    if (field->type()->type() == TYPE_ARRAY) {
        return false;
    }
    if (_opts.pred_tree.contains_column(field->id())) {
        return _predicate_need_rewrite[field->id()];
    } else {
        return (_bitmap_index_evaluator.has_bitmap_index() || !_opts.pred_tree.empty()) &&
               _column_iterators[field->id()]->all_page_dict_encoded();
    }
}

bool SegmentIterator::_can_using_global_dict(const FieldPtr& field) const {
    auto cid = field->id();
    return _opts.global_dictmaps->find(cid) != _opts.global_dictmaps->end() &&
           !_column_decoders[cid].need_force_encode_to_global_id();
}

template <bool late_materialization>
Status SegmentIterator::_build_context(ScanContext* ctx) {
    const size_t predicate_count = _predicate_columns;
    const size_t num_fields = _schema.num_fields();

    const size_t ctx_fields = late_materialization ? predicate_count + 1 : num_fields;
    const size_t early_materialize_fields = late_materialization ? predicate_count : num_fields;

    ctx->stats = _opts.stats;
    ctx->_read_schema.reserve(ctx_fields);
    ctx->_dict_decode_schema.reserve(ctx_fields);
    ctx->_subfield_columns.reserve(ctx_fields);
    ctx->_is_dict_column.reserve(ctx_fields);
    ctx->_column_iterators.reserve(ctx_fields);
    ctx->_skip_dict_decode_indexes.reserve(ctx_fields);

    ctx->_prune_column_after_index_filter = _opts.prune_column_after_index_filter;

    // init skip dict_decode_code column indexes
    std::set<ColumnId> delete_pred_columns;
    _opts.delete_predicates.get_column_ids(&delete_pred_columns);
    std::set<ColumnId> output_columns;
    for (const auto& field : output_schema().fields()) {
        output_columns.insert(field->id());
    }
    std::set<ColumnId> record_predicate_cols;
    if (_opts.record_predicate != nullptr) {
        RETURN_IF_ERROR(
                RecordPredicateHelper::get_column_ids(*_opts.record_predicate, _schema, &record_predicate_cols));
    }

    for (size_t i = 0; i < early_materialize_fields; i++) {
        const FieldPtr& f = _schema.field(i);
        const ColumnId cid = f->id();
        bool use_global_dict_code = _can_using_global_dict(f);
        bool use_dict_code = _can_using_dict_code(f);

        if (delete_pred_columns.count(f->id()) || output_columns.count(f->id()) ||
            record_predicate_cols.count(f->id()) /* need decode to compute the record predicate */) {
            ctx->_skip_dict_decode_indexes.push_back(false);
        } else {
            ctx->_skip_dict_decode_indexes.push_back(true);
            if (_inverted_index_ctx && _inverted_index_ctx->prune_cols_candidate_by_inverted_index.count(f->id())) {
                // The column is pruneable if and only if:
                // 1. column in _prune_cols_candidate_by_inverted_index
                // 2. column not in output schema
                // 3. column is not one of the delete predicate columns
                // 4. column must not be dict decoded when the read is finished
                // 5. column not in record predicate
                ctx->_prune_cols.insert(cid);
            }
        }

        if (use_dict_code || use_global_dict_code) {
            // create FixedLengthColumn<int64_t> for saving dict codewords.
            FieldPtr f2;
            if (f->type()->type() == TYPE_ARRAY && f->sub_field(0).type()->type() == TYPE_VARCHAR) {
                auto child = f->sub_field(0);
                TypeInfoPtr typeInfo = get_array_type_info(get_type_info(kDictCodeType, -1, -1));
                f2 = std::make_shared<Field>(cid, f->name(), typeInfo, STORAGE_AGGREGATE_NONE, 0, false,
                                             f->is_nullable());
                f2->add_sub_field(Field(child.id(), child.name(), kDictCodeType, -1, -1, child.is_nullable()));
            } else {
                f2 = std::make_shared<Field>(cid, f->name(), kDictCodeType, -1, -1, f->is_nullable());
            }
            ColumnIterator* iter = nullptr;
            if (use_global_dict_code) {
                iter = new GlobalDictCodeColumnIterator(cid, _column_iterators[cid].get(),
                                                        _column_decoders[cid].code_convert_data(),
                                                        _column_decoders[cid].dict_convert_size());
            } else {
                iter = new DictCodeColumnIterator(cid, _column_iterators[cid].get());
            }

            _obj_pool.add(iter);
            ctx->_read_schema.append(f2);
            ctx->_column_iterators.emplace_back(iter);
            ctx->_is_dict_column.emplace_back(true);
            ctx->_has_dict_column = true;

            if (ctx->_skip_dict_decode_indexes[i]) {
                ctx->_dict_decode_schema.append(f2);
                ctx->_column_ids_to_column_iterators.emplace(cid, ctx->_column_iterators.back());
                ctx->_column_ids_to_index.emplace(cid, i);
                continue;
            }

            // When we use the global dictionary,
            // iterator return type is also int type
            if (use_global_dict_code) {
                ctx->_dict_decode_schema.append(f2);
            } else {
                ctx->_dict_decode_schema.append(f);
            }
        } else if (late_materialization && need_early_materialize_subfield(f)) {
            auto path = _predicate_column_access_paths[cid];
            ColumnIterator* iter = new FillSubfieldIterator(cid, path, _column_iterators[cid].get());
            _obj_pool.add(iter);
            ctx->_read_schema.append(f);
            ctx->_column_iterators.emplace_back(iter);
            ctx->_is_dict_column.emplace_back(false);
            ctx->_dict_decode_schema.append(f);
            if (output_columns.count(f->id()) != 0) {
                ctx->_subfield_columns.emplace_back(i);
                ctx->_subfield_iterators.emplace_back(iter);
            }
        } else {
            ctx->_read_schema.append(f);
            ctx->_column_iterators.emplace_back(_column_iterators[cid].get());
            ctx->_is_dict_column.emplace_back(false);
            ctx->_dict_decode_schema.append(f);
        }
        ctx->_column_ids_to_column_iterators.emplace(cid, ctx->_column_iterators.back());
        ctx->_column_ids_to_index.emplace(cid, i);
    }

    size_t build_read_index_size = ctx->_read_schema.num_fields();
    if (late_materialization && (predicate_count < _schema.num_fields() || !ctx->_subfield_columns.empty())) {
        // ordinal column
        ColumnId cid = -1;
        if (predicate_count < _schema.num_fields()) {
            cid = _schema.field(predicate_count)->id();
        }

        static_assert(std::is_same_v<rowid_t, TypeTraits<TYPE_UNSIGNED_INT>::CppType>);
        auto f = std::make_shared<Field>(cid, "ordinal", TYPE_UNSIGNED_INT, -1, -1, false);
        auto* iter = new RowIdColumnIterator();
        _obj_pool.add(iter);
        ctx->_read_schema.append(f);
        ctx->_dict_decode_schema.append(f);
        ctx->_column_iterators.emplace_back(iter);
        ctx->_is_dict_column.emplace_back(false);
        ctx->_late_materialize = true;
        ctx->_skip_dict_decode_indexes.push_back(false);
        ctx->_row_id_column_id = cid;
    }

    for (size_t i = 0; i < num_fields; ++i) {
        const FieldPtr& f = _schema.field(i);
        const ColumnId cid = f->id();
        ctx->_has_force_dict_encode |= _column_decoders[cid].need_force_encode_to_global_id();
    }

    // build index map
    DCHECK_LE(output_schema().num_fields(), _schema.num_fields());
    DCHECK(!(output_schema().num_fields() < _schema.num_fields()) || _opts.delete_predicates.empty())
            << "delete condition couldn't work with filter_unused_columns";

    // skip dict_decode column in _read_schema would not be mapping
    std::unordered_map<ColumnId, size_t> read_indexes;   // fid -> read schema index
    std::unordered_map<ColumnId, size_t> output_indexes; // fid -> output schema index
    for (size_t i = 0; i < build_read_index_size; i++) {
        if (!ctx->_skip_dict_decode_indexes[i]) {
            read_indexes[ctx->_read_schema.field(i)->id()] = i;
        }
    }

    // map output_schema[cid, index] to read_schema[cid index]
    ctx->_read_index_map.resize(read_indexes.size());
    for (size_t i = 0; i < read_indexes.size(); i++) {
        ctx->_read_index_map[i] = read_indexes[output_schema().field(i)->id()];
        output_indexes[output_schema().field(i)->id()] = i;
    }

    // convert the read schema index to output scheam index for subfield
    for (size_t i = 0; i < ctx->_subfield_columns.size(); i++) {
        auto read_index = ctx->_subfield_columns[i];
        auto fid = ctx->_read_schema.field(read_index)->id();
        ctx->_subfield_columns[i] = output_indexes[fid];
    }

    VLOG(2) << "SegmentIterator::_build_context late_materialization=" << late_materialization << " "
            << ctx->to_string();

    _build_context_for_predicate(ctx);

    return Status::OK();
}

Status SegmentIterator::_init_context() {
    _late_materialization_ratio = config::late_materialization_ratio;
    RETURN_IF_ERROR(_init_global_dict_decoder());

    if (_predicate_columns == 0 || _opts.pred_tree.empty() ||
        (_predicate_columns >= _schema.num_fields() && _predicate_column_access_paths.empty() &&
         !_enable_predicate_col_late_materialize)) {
        // non or all field has predicate, disable late materialization.
        RETURN_IF_ERROR(_build_context<false>(&_context_list[0]));
    } else {
        // metric column default enable late materialization
        for (const auto& field : _schema.fields()) {
            if (is_complex_metric_type(field->type()->type())) {
                _late_materialization_ratio = config::metric_late_materialization_ratio;
                break;
            }
        }

        if (_opts.lake_io_opts.cache_file_only) {
            // CACHE SELECT disable late materialization
            _late_materialization_ratio = 0;
        }
        if (_late_materialization_ratio <= 0) {
            // late materialization been disabled.
            RETURN_IF_ERROR(_build_context<false>(&_context_list[0]));
        } else if (_late_materialization_ratio < 1000) {
            // late materialization based on condition filter ratio.
            RETURN_IF_ERROR(_build_context<true>(&_context_list[0]));
            RETURN_IF_ERROR(_build_context<false>(&_context_list[1]));
            _context_list[0]._next = &_context_list[1];
            _context_list[1]._next = &_context_list[0];
        } else {
            // always use late materialization strategy.
            RETURN_IF_ERROR(_build_context<true>(&_context_list[0]));
        }
    }
    return _switch_context(&_context_list[0]);
}

void SegmentIterator::_build_context_for_predicate(ScanContext* ctx) {
    bool has_or_predicates = _non_expr_pred_tree.has_or_predicate() || _expr_pred_tree.has_or_predicate();
    // `_context` is not initialized while building ScanContext instances (e.g. during `_init_context`).
    // Using the argument `ctx` ensures the enablement decision is evaluated for the context under construction
    // instead of dereferencing the yet-to-be-initialized `_context`, which caused crashes on empty tablets.
    ctx->_enable_predicate_col_late_materialize =
            !has_or_predicates && ctx->_late_materialize && _opts.enable_predicate_col_late_materialize;

    const ColumnPredicateMap& column_predicate_map = _opts.pred_tree.get_immediate_column_predicate_map();
    if (column_predicate_map.empty()) {
        ctx->_only_output_one_predicate_col_with_filter_push_down = false;
        ctx->_enable_predicate_col_late_materialize = false;
        return;
    }

    // For case like : select col from table where col like "%jerry%"
    // we only need output one predicate column, so context's _late_materialize is always false
    // but we still can push down predicate into page level
    ctx->_only_output_one_predicate_col_with_filter_push_down =
            !has_or_predicates && (_predicate_columns == 1 && _schema.num_fields() == 1) &&
            ctx->_column_iterators[0]->support_push_down_predicate(column_predicate_map.begin()->second) &&
            _opts.enable_predicate_col_late_materialize;

    if (!ctx->_enable_predicate_col_late_materialize && !ctx->_only_output_one_predicate_col_with_filter_push_down) {
        return;
    }

    ctx->_column_predicate_map.reserve(column_predicate_map.size());
    ctx->_predicate_order.reserve(column_predicate_map.size());

    for (const auto& pair : column_predicate_map) {
        ctx->_predicate_order.emplace_back(pair.first);
        ctx->_column_predicate_map.emplace(pair.first, pair.second);
    }

    // if column predicate is always true for string column or already used by bitmap index
    // it will be removed from predicate tree
    // but we still need to read it
    // so we add the columnId into column_predicate_map with empty ColumnPredicates, so it will only read without filter
    if (ctx->_predicate_order.size() + 1 < ctx->_column_iterators.size()) {
        DCHECK(ctx->_column_ids_to_column_iterators.size() + 1 == ctx->_column_iterators.size());
        for (auto pair : ctx->_column_ids_to_column_iterators) {
            if (!ctx->_column_predicate_map.contains(pair.first)) {
                ctx->_predicate_order.emplace_back(pair.first);
                ctx->_column_predicate_map.emplace(pair.first, std::vector<const ColumnPredicate*>());
            }
        }
    }

    _build_column_oriented_rf(ctx);

    // all predicate columns + rowId column == _column_iterators size
    DCHECK(ctx->_predicate_order.size() + 1 == ctx->_column_iterators.size() ||
           ctx->_only_output_one_predicate_col_with_filter_push_down);

    DCHECK(!ctx->_predicate_order.empty());

    const ColumnId first_column_id = ctx->_predicate_order.front();
    ctx->_column_iterators_for_predicate_late_materialize.clear();

    ctx->_column_iterators_for_predicate_late_materialize.emplace_back(
            ctx->_column_ids_to_column_iterators[first_column_id]);
    ctx->_column_id_for_predicate_late_materialize.emplace_back(first_column_id);

    // if only one predicate, and only need read this column, we do not need to use rowid Column
    if (ctx->_only_output_one_predicate_col_with_filter_push_down) {
        return;
    }
    // add row id iterator
    ctx->_column_iterators_for_predicate_late_materialize.emplace_back(ctx->_column_iterators.back());
    ctx->_column_id_for_predicate_late_materialize.emplace_back(ctx->_row_id_column_id);
}

void SegmentIterator::_build_column_oriented_rf(ScanContext* ctx) {
    if (!_runtime_filter_preds.empty() && _opts.enable_join_runtime_filter_pushdown &&
        _column_to_runtime_filters_map.empty()) {
        // First, collect predicates by column id
        std::unordered_map<ColumnId, std::vector<RuntimeFilterPredicate*>> column_rf_map;
        for (auto* rf_pred : _runtime_filter_preds.rf_predicates()) {
            ColumnId cid = rf_pred->get_column_id();
            column_rf_map[cid].push_back(rf_pred);
        }

        // Create a RuntimeFilterPredicates object for each column
        for (const auto& [cid, rf_pred_list] : column_rf_map) {
            RuntimeFilterPredicates rf_preds(_runtime_filter_preds.driver_sequence());
            for (auto* rf_pred : rf_pred_list) {
                rf_preds.add_predicate(rf_pred);
            }
            _column_to_runtime_filters_map.emplace(cid, std::move(rf_preds));
        }
    }

    // Ensure columns with only runtime filters are also added to predicate_order and _column_predicate_map
    // These columns may not have regular predicates but have runtime filters
    if (_opts.enable_join_runtime_filter_pushdown) {
        for (const auto& [cid, rf_preds] : _column_to_runtime_filters_map) {
            // If this column is not in column_predicate_map, add it with empty predicates
            if (!ctx->_column_predicate_map.contains(cid)) {
                // Check if the column has a column iterator
                DCHECK(ctx->_column_ids_to_column_iterators.contains(cid));
                if (ctx->_column_ids_to_column_iterators.contains(cid)) {
                    ctx->_predicate_order.emplace_back(cid);
                    ctx->_column_predicate_map.emplace(cid, std::vector<const ColumnPredicate*>());
                }
            }
        }
    }

    // Expose runtime filter presence to ScanContext so PredicateLateMaterializationScanStrategy can decide
    // whether it's safe to push down predicates to the page level for the first predicate column.
    ctx->runtime_filters_by_column =
            (_opts.enable_join_runtime_filter_pushdown && !_column_to_runtime_filters_map.empty())
                    ? &_column_to_runtime_filters_map
                    : nullptr;
}

Status SegmentIterator::_init_global_dict_decoder() {
    // init decoder for all columns
    // in some case _build_context<false> won't be called
    for (int i = 0; i < _schema.num_fields(); ++i) {
        const FieldPtr& f = _schema.field(i);
        const ColumnId cid = f->id();
        if (_can_using_global_dict(f)) {
            auto iter = new GlobalDictCodeColumnIterator(cid, _column_iterators[cid].get(),
                                                         _column_decoders[cid].code_convert_data(),
                                                         _column_decoders[cid].dict_convert_size());
            _obj_pool.add(iter);
            _column_decoders[cid].set_iterator(iter);
        }
    }
    return Status::OK();
}

Status SegmentIterator::_rewrite_predicates() {
    {
        // in normal case it can always rewrite the predicate,
        // but for JSON extended column, it might be a JsonExtractColumnIterator, so we need to fallback to the orignal predicate
        ColumnPredicateRewriter rewriter(_column_iterators, _schema, _predicate_need_rewrite, _predicate_columns,
                                         _scan_range);
        auto st = (rewriter.rewrite_predicate(&_obj_pool, _opts.pred_tree));
        if (st.is_not_supported()) {
            VLOG(2) << "not supported predicate rewrite: " << _opts.pred_tree.root().debug_string() << " " << st;
        } else if (!st.ok()) {
            return st;
        } else {
            VLOG(2) << "rewrite_predicates: " << _opts.pred_tree.root().debug_string();
        }
    }
    if (_opts.enable_join_runtime_filter_pushdown) {
        RETURN_IF_ERROR(RuntimeFilterPredicatesRewriter::rewrite(&_obj_pool, _opts.runtime_filter_preds,
                                                                 _column_iterators, _schema));
    }

    // for each delete predicate,
    // If the global dictionary optimization is enabled for the column,
    // then the output column is of type INT, and we need to rewrite the delete condition
    // so that the input is of type INT (the original input is of type String)
    std::vector<uint8_t> disable_dict_rewrites(_column_decoders.size());
    for (size_t i = 0; i < _schema.num_fields(); i++) {
        const FieldPtr& field = _schema.field(i);
        ColumnId cid = field->id();
        disable_dict_rewrites[cid] = _column_decoders[cid].need_force_encode_to_global_id();
    }

    for (auto& conjunct_predicate : _opts.delete_predicates.predicate_list()) {
        GlobalDictPredicatesRewriter crewriter(*_opts.global_dictmaps, &disable_dict_rewrites);
        RETURN_IF_ERROR(crewriter.rewrite_predicate(&_obj_pool, conjunct_predicate));
    }

    return Status::OK();
}

Status SegmentIterator::_decode_dict_codes(ScanContext* ctx) {
    SCOPED_RAW_TIMER(&_opts.stats->decode_dict_ns);
    DCHECK_NE(ctx->_read_chunk, ctx->_dict_chunk);
    if (ctx->_read_chunk->num_rows() == 0) {
        ctx->_dict_chunk->set_num_rows(0);
        return Status::OK();
    }

    const Schema& decode_schema = ctx->_dict_decode_schema;
    const size_t n = decode_schema.num_fields();
    bool may_has_del_row = ctx->_read_chunk->delete_state() != DEL_NOT_SATISFIED;
    size_t decode_count = 0;
    for (size_t i = 0; i < n; i++) {
        const FieldPtr& f = decode_schema.field(i);
        const ColumnId cid = f->id();
        if (!ctx->_is_dict_column[i] || ctx->_skip_dict_decode_indexes[i]) {
            ctx->_dict_chunk->get_column_by_index(i).swap(ctx->_read_chunk->get_column_by_index(i));
        } else {
            auto dict_codes = ctx->_read_chunk->get_column_by_index(i);
            auto* dict_values = ctx->_dict_chunk->get_column_raw_ptr_by_index(i);
            dict_values->resize(0);

            RETURN_IF_ERROR(_column_decoders[cid].decode_dict_codes(*dict_codes, dict_values));
            decode_count += dict_codes->size();

            DCHECK_EQ(dict_codes->size(), dict_values->size());
            may_has_del_row |= (dict_values->delete_state() != DEL_NOT_SATISFIED);
            if (f->is_nullable()) {
                auto* nullable_codes = down_cast<NullableColumn*>(dict_codes->as_mutable_raw_ptr());
                auto* nullable_values = down_cast<NullableColumn*>(dict_values->as_mutable_raw_ptr());
                nullable_values->null_column_data().swap(nullable_codes->null_column_data());
                nullable_values->set_has_null(nullable_codes->has_null());
            }
        }
    }
    ctx->_dict_chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    ctx->_dict_chunk->check_or_die();
    _opts.stats->decode_dict_count += decode_count;
    return Status::OK();
}

Status SegmentIterator::_check_low_cardinality_optimization() {
    _predicate_need_rewrite.resize(1 + ChunkHelper::max_column_id(_schema), false);
    const size_t n = _opts.pred_tree.num_columns();
    for (size_t i = 0; i < n; i++) {
        const FieldPtr& field = _schema.field(i);
        const LogicalType type = field->type()->type();
        if (type != TYPE_CHAR && type != TYPE_VARCHAR) {
            continue;
        }
        ColumnId cid = field->id();
        if (_opts.pred_tree.contains_column(cid)) {
            // for string column of low cardinality, we can always rewrite predicates.
            _predicate_need_rewrite[cid] = true;
        }
    }
    return Status::OK();
}

Status SegmentIterator::_finish_late_materialization(ScanContext* ctx) {
    const size_t m = ctx->_read_schema.num_fields();

    bool may_has_del_row = ctx->_dict_chunk->delete_state() != DEL_NOT_SATISFIED;

    // last column of |_dict_chunk| is a fake column: it's filled by `RowIdColumnIterator`.
    ColumnPtr rowid_column = ctx->_dict_chunk->get_column_by_index(m - 1);
    const auto* ordinals = down_cast<const FixedLengthColumn<rowid_t>*>(rowid_column.get());

    if (_predicate_columns < _schema.num_fields()) {
        const size_t n = _schema.num_fields();
        const size_t start_pos = ctx->_read_index_map.size();
        for (size_t i = m - 1, j = start_pos; i < n; i++, j++) {
            const FieldPtr& f = _schema.field(i);
            const ColumnId cid = f->id();
            auto* col = ctx->_final_chunk->get_column_raw_ptr_by_index(j);
            col->reserve(ordinals->size());
            col->resize(0);

            RETURN_IF_ERROR(_column_decoders[cid].decode_values_by_rowid(*ordinals, col));
            DCHECK_EQ(ordinals->size(), col->size());
            may_has_del_row |= (col->delete_state() != DEL_NOT_SATISFIED);
        }
    }

    // fill subfield of early materialization columns
    for (size_t i = 0; i < ctx->_subfield_columns.size(); i++) {
        auto output_index = ctx->_subfield_columns[i];
        auto* col = ctx->_final_chunk->get_column_raw_ptr_by_index(output_index);
        // FillSubfieldIterator
        RETURN_IF_ERROR(ctx->_subfield_iterators[i]->fetch_values_by_rowid(*ordinals, col));
        DCHECK_EQ(ordinals->size(), col->size());
    }

    ctx->_final_chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    ctx->_final_chunk->check_or_die();

    return Status::OK();
}

void SegmentIterator::_build_final_chunk(ScanContext* ctx) {
    // trim all use less columns
    Columns& input_columns = ctx->_dict_chunk->columns();
    for (size_t i = 0; i < ctx->_read_index_map.size(); i++) {
        ctx->_final_chunk->get_column_by_index(i).swap(input_columns[ctx->_read_index_map[i]]);
    }
    bool may_has_del_row = ctx->_dict_chunk->delete_state() != DEL_NOT_SATISFIED;
    ctx->_final_chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
}

Status SegmentIterator::_encode_to_global_id(ScanContext* ctx) {
    int num_columns = ctx->_final_chunk->num_columns();
    auto final_chunk = ctx->_final_chunk;

    for (size_t i = 0; i < num_columns; i++) {
        const FieldPtr& f = output_schema().field(i);
        const ColumnId cid = f->id();
        auto* col = ctx->_final_chunk->get_column_raw_ptr_by_index(i);
        auto* dst = ctx->_adapt_global_dict_chunk->get_column_raw_ptr_by_index(i);
        if (_column_decoders[cid].need_force_encode_to_global_id()) {
            RETURN_IF_ERROR(_column_decoders[cid].encode_to_global_id(col, dst));
        } else {
            col->swap_column(*dst);
        }
    }
    return Status::OK();
}

static void erase_column_pred_from_pred_tree(PredicateTree& pred_tree,
                                             const std::unordered_set<const ColumnPredicate*>& erased_preds) {
    PredicateAndNode new_root;
    PredicateAndNode useless_root;
    pred_tree.release_root().partition_move(
            [&](const auto& node_var) {
                return node_var.visit(overloaded{
                        [&](const PredicateColumnNode& node) { return !erased_preds.contains(node.col_pred()); },
                        [&](const auto&) { return true; },
                });
            },
            &new_root, &useless_root);
    pred_tree = PredicateTree::create(std::move(new_root));
}

StatusOr<RowIdSparseRange> SegmentIterator::_sample_by_block() {
    size_t rows_per_block = _segment->num_rows_per_block();
    size_t total_rows = _segment->num_rows();
    int64_t probability_percent = _opts.sample_options.probability_percent;
    int64_t random_seed = _opts.sample_options.random_seed;

    auto sampler = DataSample::make_block_sample(probability_percent, random_seed, rows_per_block, total_rows);
    return sampler->sample(_opts.stats);
}

StatusOr<RowIdSparseRange> SegmentIterator::_sample_by_page() {
    RETURN_IF(_schema.num_fields() > 1, Status::InvalidArgument("page sample can only support at most 1 column"));

    ColumnId cid = _schema.field(0)->id();
    auto& column_iterator = _column_iterators[cid];
    ColumnReader* column_reader = column_iterator->get_column_reader();
    RETURN_IF(column_reader == nullptr, Status::InvalidArgument("Not support page smaple: no column_reader"));
    int32_t num_data_pages = column_reader->num_data_pages();
    PageIndexer page_indexer = [&](size_t page_index) { return column_reader->get_page_range(page_index); };

    int64_t probability_percent = _opts.sample_options.probability_percent;
    int64_t random_seed = _opts.sample_options.random_seed;
    auto sampler = DataSample::make_page_sample(probability_percent, random_seed, num_data_pages, page_indexer);

    if (column_reader->has_zone_map() && SortableZoneMap::is_support_data_type(column_reader->column_type())) {
        IndexReadOptions opts = _index_read_options(cid);
        ASSIGN_OR_RETURN(auto zonemap, column_reader->get_raw_zone_map(opts));
        auto sorted = std::make_shared<SortableZoneMap>(column_reader->column_type(), std::move(zonemap));
        sampler->with_zonemap(sorted);
    }

    return sampler->sample(_opts.stats);
}

Status SegmentIterator::_apply_data_sampling() {
    RETURN_IF(!_opts.sample_options.enable_sampling, Status::OK());
    RETURN_IF(_scan_range.empty(), Status::OK());
    RETURN_IF_ERROR(_segment->load_index(_opts.lake_io_opts));
    RETURN_IF(_opts.sample_options.probability_percent <= 0, Status::InvalidArgument("probability_percent must > 0"));

    DCHECK(_opts.sample_options.__isset.probability_percent);
    DCHECK(_opts.sample_options.__isset.random_seed);
    DCHECK(_opts.sample_options.__isset.sample_method);

    SCOPED_RAW_TIMER(&_opts.stats->sample_time_ns);

    SampleMethod::type sample_method = _opts.sample_options.sample_method;
    RowIdSparseRange sampled_ranges;
    switch (sample_method) {
    case SampleMethod::type::BY_BLOCK: {
        ASSIGN_OR_RETURN(sampled_ranges, _sample_by_block());
        break;
    }
    case SampleMethod::type::BY_PAGE: {
        ASSIGN_OR_RETURN(sampled_ranges, _sample_by_page());
        break;
    }
    default:
        return Status::InvalidArgument(fmt::format("unsupported sample_method: {}", sample_method));
    }
    _scan_range = _scan_range.intersection(sampled_ranges);

    return {};
}

IndexReadOptions SegmentIterator::_index_read_options(ColumnId cid) const {
    IndexReadOptions opts;
    opts.use_page_cache = !_opts.temporary_data && _opts.use_page_cache && !config::disable_storage_page_cache;
    opts.lake_io_opts = _opts.lake_io_opts;
    opts.read_file = _column_files.at(cid).get();
    opts.stats = _opts.stats;
    return opts;
}

// filter rows by evaluating column predicates using bitmap indexes.
// upon return, predicates that have been evaluated by bitmap indexes will be removed.
Status SegmentIterator::_apply_bitmap_index() {
    RETURN_IF(!config::enable_index_bitmap_filter, Status::OK());
    RETURN_IF(_scan_range.empty(), Status::OK());
    DCHECK_EQ(_predicate_columns, _opts.pred_tree.num_columns());

    {
        SCOPED_RAW_TIMER(&_opts.stats->bitmap_index_iterator_init_ns);

        std::unordered_map<ColumnId, ColumnUID> cid_2_ucid;
        for (auto& field : _schema.fields()) {
            cid_2_ucid[field->id()] = field->uid();
        }

        RETURN_IF_ERROR(_bitmap_index_evaluator.init([&cid_2_ucid,
                                                      this](ColumnId cid) -> StatusOr<BitmapIndexIterator*> {
            const ColumnUID ucid = cid_2_ucid[cid];
            // the column's index in this segment file
            ASSIGN_OR_RETURN(std::shared_ptr<Segment> segment_ptr, _get_dcg_segment(ucid));
            if (segment_ptr == nullptr) {
                // find segment from delta column group failed, using main segment
                segment_ptr = _segment;
            }

            IndexReadOptions opts;
            opts.use_page_cache = !_opts.temporary_data && _opts.use_page_cache &&
                                  !config::disable_storage_page_cache && config::enable_bitmap_index_memory_page_cache;
            opts.lake_io_opts = _opts.lake_io_opts;
            opts.read_file = _column_files[cid].get();
            opts.stats = _opts.stats;

            BitmapIndexIterator* bitmap_iter = nullptr;
            RETURN_IF_ERROR(segment_ptr->new_bitmap_index_iterator(ucid, opts, &bitmap_iter));
            return bitmap_iter;
        }));

        RETURN_IF(!_bitmap_index_evaluator.has_bitmap_index(), Status::OK());
    }

    {
        SCOPED_RAW_TIMER(&_opts.stats->bitmap_index_filter_timer);
        const auto input_rows = _scan_range.span_size();
        RETURN_IF_ERROR(_bitmap_index_evaluator.evaluate(_scan_range, _opts.pred_tree));
        _opts.stats->rows_bitmap_index_filtered += input_rows - _scan_range.span_size();
    }

    return Status::OK();
}

Status SegmentIterator::_apply_del_vector() {
    RETURN_IF(_scan_range.empty(), Status::OK());
    if (_opts.is_primary_keys && _opts.version > 0 && _del_vec && !_del_vec->empty()) {
        Roaring row_bitmap = range2roaring(_scan_range);
        size_t input_rows = row_bitmap.cardinality();
        row_bitmap -= *(_del_vec->roaring());
        _scan_range = roaring2range(row_bitmap);
        size_t filtered_rows = row_bitmap.cardinality();
        _opts.stats->rows_del_vec_filtered += input_rows - filtered_rows;
    }
    return Status::OK();
}

Status SegmentIterator::_init_inverted_index_iterators() {
    if (!_inverted_index_ctx) {
        _inverted_index_ctx = std::make_unique<InvertedIndexContext>();
    }

    _inverted_index_ctx->inverted_index_iterators.resize(ChunkHelper::max_column_id(_schema) + 1, nullptr);
    std::unordered_map<ColumnId, ColumnUID> cid_2_ucid;

    for (auto& field : _schema.fields()) {
        cid_2_ucid[field->id()] = field->uid();
    }
    for (const auto& pair : _opts.pred_tree.get_immediate_column_predicate_map()) {
        ColumnId cid = pair.first;
        ColumnUID ucid = cid_2_ucid[cid];

        IndexReadOptions index_opts;
        index_opts.use_page_cache =
                !_opts.temporary_data && _opts.use_page_cache && !config::disable_storage_page_cache;
        index_opts.lake_io_opts = _opts.lake_io_opts;
        index_opts.read_file = _column_files[cid].get();
        index_opts.stats = _opts.stats;
        index_opts.segment_rows = num_rows();

        if (_inverted_index_ctx->inverted_index_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_inverted_index_iterator(
                    ucid, &_inverted_index_ctx->inverted_index_iterators[cid], _opts, index_opts));
            _inverted_index_ctx->has_inverted_index |= (_inverted_index_ctx->inverted_index_iterators[cid] != nullptr);
        }
    }
    return Status::OK();
}

Status SegmentIterator::_apply_inverted_index() {
    RETURN_IF(_scan_range.empty(), Status::OK());
    RETURN_IF(!_opts.enable_gin_filter, Status::OK());

    RETURN_IF_ERROR(_init_inverted_index_iterators());
    if (!_inverted_index_ctx || !_inverted_index_ctx->has_inverted_index) {
        return Status::OK();
    }
    SCOPED_RAW_TIMER(&_opts.stats->gin_index_filter_ns);

    roaring::Roaring row_bitmap = range2roaring(_scan_range);
    size_t input_rows = row_bitmap.cardinality();
    std::unordered_set<const ColumnPredicate*> erased_preds;
    std::unordered_set<ColumnId> erased_pred_col_ids;

    std::unordered_map<ColumnId, ColumnId> cid_2_fid;
    for (int i = 0; i < _schema.num_fields(); i++) {
        cid_2_fid.emplace(_schema.field(i)->id(), i);
    }

    for (const auto& [cid, pred_list] : _opts.pred_tree.get_immediate_column_predicate_map()) {
        InvertedIndexIterator* inverted_iter = _inverted_index_ctx->inverted_index_iterators[cid];
        if (inverted_iter == nullptr) {
            continue;
        }
        const auto& it = cid_2_fid.find(cid);
        RETURN_IF(it == cid_2_fid.end(),
                  Status::InternalError(strings::Substitute("No fid can be mapped by cid $0", cid)));
        std::string column_name(_schema.field(it->second)->name());
        for (const ColumnPredicate* pred : pred_list) {
            if (_inverted_index_ctx->inverted_index_iterators[cid]->is_untokenized() ||
                pred->type() == PredicateType::kExpr) {
                Status res = pred->seek_inverted_index(column_name, _inverted_index_ctx->inverted_index_iterators[cid],
                                                       &row_bitmap);
                if (res.ok()) {
                    erased_preds.emplace(pred);
                    erased_pred_col_ids.emplace(cid);
                } else {
                    LOG(WARNING) << "Failed to seek inverted index for column " << column_name
                                 << ", reason: " << res.detailed_message();
                }
            }
        }
    }
    DCHECK_LE(row_bitmap.cardinality(), _scan_range.span_size());
    _scan_range = roaring2range(row_bitmap);

    // ---------------------------------------------------------
    // Erase predicates that hit inverted index.
    // ---------------------------------------------------------
    if (!erased_preds.empty()) {
        erase_column_pred_from_pred_tree(_opts.pred_tree, erased_preds);
        const auto& new_cid_to_predicates = _opts.pred_tree.get_immediate_column_predicate_map();

        for (const auto& cid : erased_pred_col_ids) {
            if (!new_cid_to_predicates.contains(cid)) {
                // predicate for pred->column_id() has been total erased by
                // inverted index filtering.These columns may can be pruned.
                _inverted_index_ctx->prune_cols_candidate_by_inverted_index.insert(cid);
            }
        }
    }

    for (auto* iter : _inverted_index_ctx->inverted_index_iterators) {
        if (iter != nullptr) {
            RETURN_IF_ERROR(iter->close());
        }
    }

    _opts.stats->rows_gin_filtered += input_rows - _scan_range.span_size();
    return Status::OK();
}

struct BloomFilterSupportChecker {
    bool operator()(const PredicateColumnNode& node) const {
        const auto* col_pred = node.col_pred();
        const auto cid = col_pred->column_id();
        const bool support =
                (column_iterators[cid]->has_original_bloom_filter_index() &&
                 col_pred->support_original_bloom_filter()) ||
                (column_iterators[cid]->has_ngram_bloom_filter_index() && col_pred->support_ngram_bloom_filter());
        if (support) {
            used_nodes.emplace(&node);
        }
        return support;
    }

    bool operator()(const PredicateAndNode& node) {
        bool support = false;
        for (const auto& child : node.children()) {
            // Use | not || to add all the used nodes to `used_nodes`.
            support |= child.visit(*this);
        }
        if (support) {
            used_nodes.emplace(&node);
        }
        return support;
    }

    bool operator()(const PredicateOrNode& node) {
        const bool support = std::all_of(node.children().begin(), node.children().end(),
                                         [&](const auto& child) { return child.visit(*this); });
        if (support) {
            used_nodes.emplace(&node);
        }
        return support;
    }

    std::vector<std::unique_ptr<ColumnIterator>>& column_iterators;
    std::unordered_set<const PredicateBaseNode*>& used_nodes;
};

struct BloomFilterEvaluator {
    Status operator()(const PredicateAndNode& node, SparseRange<>& dest_ranges) {
        if (!used_nodes.contains(&node)) {
            return Status::OK();
        }

        const auto& ctx = pred_tree.compound_node_context(node.id());
        const auto& cid_to_col_preds = ctx.cid_to_col_preds(node);
        for (const auto& [cid, col_preds] : cid_to_col_preds) {
            RETURN_IF_ERROR(column_iterators[cid]->get_row_ranges_by_bloom_filter(col_preds, &dest_ranges));
        }

        for (const auto& child : node.compound_children()) {
            RETURN_IF_ERROR(child.visit(*this, dest_ranges));
        }
        return Status::OK();
    }

    Status operator()(const PredicateOrNode& node, SparseRange<>& dest_ranges) {
        if (node.empty() || !used_nodes.contains(&node)) {
            return Status::OK();
        }

        SparseRange<> cur_dest_ranges;
        const auto& ctx = pred_tree.compound_node_context(node.id());
        const auto& cid_to_col_preds = ctx.cid_to_col_preds(node);
        for (const auto& [cid, col_preds] : cid_to_col_preds) {
            auto child_ranges = dest_ranges;
            RETURN_IF_ERROR(column_iterators[cid]->get_row_ranges_by_bloom_filter(col_preds, &child_ranges));
            cur_dest_ranges |= child_ranges;
        }

        for (const auto& child : node.compound_children()) {
            auto child_ranges = dest_ranges;
            RETURN_IF_ERROR(child.visit(*this, child_ranges));
            cur_dest_ranges |= child_ranges;
        }

        dest_ranges &= cur_dest_ranges;

        return Status::OK();
    }

    const PredicateTree& pred_tree;
    std::vector<std::unique_ptr<ColumnIterator>>& column_iterators;
    std::unordered_set<const PredicateBaseNode*>& used_nodes;
};

Status SegmentIterator::_get_row_ranges_by_bloom_filter() {
    RETURN_IF(!config::enable_index_bloom_filter, Status::OK());
    RETURN_IF(_scan_range.empty(), Status::OK());
    RETURN_IF(_opts.pred_tree.empty(), Status::OK());

    SCOPED_RAW_TIMER(&_opts.stats->bf_filter_ns);

    std::unordered_set<const PredicateBaseNode*> used_nodes;
    const bool support = _opts.pred_tree.visit(BloomFilterSupportChecker{_column_iterators, used_nodes});
    RETURN_IF(!support, Status::OK());

    const size_t prev_size = _scan_range.span_size();
    RETURN_IF_ERROR(
            _opts.pred_tree.visit(BloomFilterEvaluator{_opts.pred_tree, _column_iterators, used_nodes}, _scan_range));
    _opts.stats->rows_bf_filtered += prev_size - _scan_range.span_size();

    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_by_rowid_range() {
    DCHECK_EQ(0, _scan_range.span_size());

    _scan_range.add(Range<>(0, num_rows()));

    if (_opts.rowid_range_option != nullptr) {
        _scan_range &= (*_opts.rowid_range_option);

        // The rowid range is already applied key ranges at the short key block level.
        // For example, as for the following N-rows segment,
        // - after applying the rowid ranges, it contains n1+n2=b2+b3+b4=N-b1-b5 rows and filters out N-n1-n2 rows.
        // - after applying the short key range index, it contains n1.2+n2.1=n1+n2-b2.1-b4.2 rows and filters out
        //   n1+n2-n1.2-n2.1 rows.
        // Therefore, here rowid range index accounts rows_key_range_filtered=N-n1-...-nk, where N denotes the
        // number of rows of the segment, and ni denotes the number of rows of i-th split after rowid range index.
        //
        //                              N rows
        // ┌──────────────────────────────────────────────────────────────┐
        // │                                                              │
        // │                  b2                    b4                    │
        // │   ┌──────────┬────┬─────┬──────────┬────┬─────┬──────────┐   │
        // │   │    b1    │b2.1│ b2.2│    b3    │b4.1│ b4.2│    b5    │   │ short key blocks
        // │   └──────────┴────▲─────┴──────────┴────▲─────┴──────────┘   │
        // │                   │                     │                    │
        // │              ┌────┴──────────┬──────────┴─────┐              │
        // │              │     n1        │      n2        │              │ rowid ranges
        // │              └────▲──────────┴──────────▲─────┘              │
        // │               n1.1│                     │ n2.2               │
        // │                   ├──────────┬──────────┤                    │
        // │                   │   n1.2   │   n2.1   │                    │ key ranges
        // │                   └──────────┴──────────┘                    │
        // │                                                              │
        // └──────────────────────────────────────────────────────────────┘
        _opts.stats->rows_key_range_filtered += -static_cast<int64_t>(_scan_range.span_size());
        if (_opts.is_first_split_of_segment) {
            _opts.stats->rows_key_range_filtered += num_rows();
        }
    }

    return Status::OK();
}

// Currently, update stats is only used for lake tablet, and numeric statistics is nullptr for local tablet.
void SegmentIterator::_update_stats(io::SeekableInputStream* rfile) {
    auto stats_or = rfile->get_numeric_statistics();
    if (!stats_or.ok()) {
        LOG(WARNING) << "failed to get statistics: " << stats_or.status();
        return;
    }

    std::unique_ptr<io::NumericStatistics> stats = std::move(stats_or).value();
    if (stats == nullptr || stats->size() == 0) {
        return;
    }

    for (int64_t i = 0, sz = stats->size(); i < sz; ++i) {
        auto&& name = stats->name(i);
        auto&& value = stats->value(i);
        if (name == kBytesReadLocalDisk) {
            _opts.stats->compressed_bytes_read_local_disk += value;
            _opts.stats->compressed_bytes_read += value;
        } else if (name == kBytesWriteLocalDisk) {
            _opts.stats->compressed_bytes_write_local_disk += value;
        } else if (name == kBytesReadRemote) {
            _opts.stats->compressed_bytes_read_remote += value;
            _opts.stats->compressed_bytes_read += value;
        } else if (name == kIOCountLocalDisk) {
            _opts.stats->io_count_local_disk += value;
            _opts.stats->io_count += value;
        } else if (name == kIOCountRemote) {
            _opts.stats->io_count_remote += value;
            _opts.stats->io_count += value;
        } else if (name == kIONsReadLocalDisk) {
            _opts.stats->io_ns_read_local_disk += value;
        } else if (name == kIONsWriteLocalDisk) {
            _opts.stats->io_ns_write_local_disk += value;
        } else if (name == kIONsReadRemote) {
            _opts.stats->io_ns_remote += value;
        } else if (name == kPrefetchHitCount) {
            _opts.stats->prefetch_hit_count += value;
        } else if (name == kPrefetchWaitFinishNs) {
            _opts.stats->prefetch_wait_finish_ns += value;
        } else if (name == kPrefetchPendingNs) {
            _opts.stats->prefetch_pending_ns += value;
        }
    }
}

void SegmentIterator::close() {
    if (_del_vec) {
        _del_vec.reset();
    }
    _dcgs.clear();
    _context_list[0].close();
    _context_list[1].close();
    _column_iterators.resize(0);
    _obj_pool.clear();
    _segment.reset();
    _dcg_segments.clear();
    _column_decoders.clear();

    for (auto& [cid, rfile] : _column_files) {
        // update statistics before reset column file
        _update_stats(rfile.get());
        rfile.reset();
    }

    STLClearObject(&_selection);
    STLClearObject(&_selected_idx);

    _bitmap_index_evaluator.close();

    if (_inverted_index_ctx) {
        _inverted_index_ctx->cleanup();
    }
}

// put the field that has predicated on it ahead of those without one, for handle late
// materialization easier.
inline Schema reorder_schema(const Schema& input, const PredicateTree& pred_tree) {
    const std::vector<FieldPtr>& fields = input.fields();
    Schema output;
    output.reserve(fields.size());
    for (const auto& field : fields) {
        if (pred_tree.contains_column(field->id())) {
            output.append(field);
        }
    }
    for (const auto& field : fields) {
        if (!pred_tree.contains_column(field->id())) {
            output.append(field);
        }
    }
    return output;
}

ChunkIteratorPtr new_segment_iterator(const std::shared_ptr<Segment>& segment, const Schema& schema,
                                      const SegmentReadOptions& options) {
    if (options.pred_tree.empty() || options.pred_tree.num_columns() >= schema.num_fields()) {
        return std::make_shared<SegmentIterator>(segment, schema, options);
    } else {
        // _check_low_cardinality_optimization() implementation counts on this schema reorder
        Schema ordered_schema = reorder_schema(schema, options.pred_tree);
        auto seg_iter = std::make_shared<SegmentIterator>(segment, ordered_schema, options);
        return new_projection_iterator(schema, seg_iter);
    }
}

} // namespace starrocks
