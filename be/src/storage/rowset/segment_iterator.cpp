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
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/column_expr_predicate.h"
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
    SegmentIterator(std::shared_ptr<Segment> segment, Schema _schema, SegmentReadOptions options);

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
        ScanContext() = default;

        ~ScanContext() = default;

        void close() {
            _read_chunk.reset();
            _dict_chunk.reset();
            _final_chunk.reset();
            _adapt_global_dict_chunk.reset();
        }

        Status seek_columns(ordinal_t pos) {
            for (auto iter : _column_iterators) {
                RETURN_IF_ERROR(iter->seek_to_ordinal(pos));
            }
            return Status::OK();
        }

        Status read_columns(Chunk* chunk, const SparseRange<>& range) {
            bool may_has_del_row = chunk->delete_state() != DEL_NOT_SATISFIED;
            for (size_t i = 0; i < _column_iterators.size(); i++) {
                const ColumnPtr& col = chunk->get_column_by_index(i);
                if (_prune_column_after_index_filter && _prune_cols.count(i)) {
                    // make sure each pruned column has the same size as the unpruneable one.
                    col->resize(range.span_size());
                    continue;
                }
                RETURN_IF_ERROR(_column_iterators[i]->next_batch(range, col.get()));
                may_has_del_row |= (col->delete_state() != DEL_NOT_SATISFIED);
            }
            chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
            return Status::OK();
        }

        int64_t memory_usage() const {
            int64_t usage = 0;
            usage += (_read_chunk != nullptr) ? _read_chunk->memory_usage() : 0;
            usage += (_dict_chunk.get() != _read_chunk.get()) ? _dict_chunk->memory_usage() : 0;
            usage += (_final_chunk.get() != _dict_chunk.get()) ? _final_chunk->memory_usage() : 0;
            usage += (_adapt_global_dict_chunk.get() != _final_chunk.get()) ? _adapt_global_dict_chunk->memory_usage()
                                                                            : 0;
            return usage;
        }

        size_t column_size() { return _column_iterators.size(); }

        Schema _read_schema;
        Schema _dict_decode_schema;
        std::vector<bool> _is_dict_column;
        std::vector<ColumnIterator*> _column_iterators;
        std::vector<ColumnId> _subfield_columns;
        std::vector<ColumnIterator*> _subfield_iterators;
        ScanContext* _next{nullptr};

        // index the column which only be used for filter
        // thus its not need do dict_decode_code
        std::vector<size_t> _skip_dict_decode_indexes;
        // index: output schema index, values: read schema index
        std::vector<size_t> _read_index_map;

        std::shared_ptr<Chunk> _read_chunk;
        std::shared_ptr<Chunk> _dict_chunk;
        std::shared_ptr<Chunk> _final_chunk;
        std::shared_ptr<Chunk> _adapt_global_dict_chunk;

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
        // for inverted index.
        std::unordered_set<size_t> _prune_cols;
        bool _prune_column_after_index_filter = false;
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

    uint32_t segment_id() const { return _segment->id(); }
    uint32_t num_rows() const { return _segment->num_rows(); }

    Status _lookup_ordinal(const SeekTuple& key, bool lower, rowid_t end, rowid_t* rowid);
    Status _lookup_ordinal(const Slice& index_key, const Schema& short_key_schema, bool lower, rowid_t end,
                           rowid_t* rowid);
    Status _seek_columns(const Schema& schema, rowid_t pos);
    Status _read_columns(const Schema& schema, Chunk* chunk, size_t nrows);

    StatusOr<uint16_t> _filter_by_non_expr_predicates(Chunk* chunk, vector<rowid_t>* rowid, uint16_t from, uint16_t to);
    StatusOr<uint16_t> _filter_by_expr_predicates(Chunk* chunk, vector<rowid_t>* rowid);

    void _init_column_predicates();

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

    Status _read(Chunk* chunk, vector<rowid_t>* rowid, size_t n);

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

    void _update_stats(io::SeekableInputStream* rfile);

    //  This function will search and build the segment from delta column group.
    StatusOr<std::shared_ptr<Segment>> _get_dcg_segment(uint32_t ucid);

    bool need_early_materialize_subfield(const FieldPtr& field);

    Status _init_ann_reader();

    IndexReadOptions _index_read_options(ColumnId cid) const;

private:
    using RawColumnIterators = std::vector<std::unique_ptr<ColumnIterator>>;
    using ColumnDecoders = std::vector<ColumnDecoder>;
    std::shared_ptr<Segment> _segment;
    std::unordered_map<std::string, std::shared_ptr<Segment>> _dcg_segments;
    SegmentReadOptions _opts;
    RawColumnIterators _column_iterators;
    std::vector<int> _io_coalesce_column_index;
    ColumnDecoders _column_decoders;
    std::shared_ptr<VectorIndexReader> _ann_reader;
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
    bool _has_inverted_index = false;

    std::vector<InvertedIndexIterator*> _inverted_index_iterators;

    std::unordered_map<ColumnId, ColumnAccessPath*> _column_access_paths;
    std::unordered_map<ColumnId, ColumnAccessPath*> _predicate_column_access_paths;

    std::unordered_set<ColumnId> _prune_cols_candidate_by_inverted_index;

    // vector index params
    int64_t _k;
#ifdef WITH_TENANN
    tenann::PrimitiveSeqView _query_view;
    std::shared_ptr<tenann::IndexMeta> _index_meta;
#endif

    bool _always_build_rowid() const { return _use_vector_index && !_use_ivfpq; }

    bool _use_vector_index;
    std::string _vector_distance_column_name;
    int _vector_column_id;
    SlotId _vector_slot_id;
    std::unordered_map<rowid_t, float> _id2distance_map;
    std::map<std::string, std::string> _query_params;
    double _vector_range;
    int _result_order;
    bool _use_ivfpq;

    Status _init_reader_from_file(const std::string& index_path, const std::shared_ptr<TabletIndex>& tablet_index_meta,
                                  const std::map<std::string, std::string>& query_params);
};

SegmentIterator::SegmentIterator(std::shared_ptr<Segment> segment, Schema schema, SegmentReadOptions options)
        : ChunkIterator(std::move(schema), options.chunk_size),
          _segment(std::move(segment)),
          _opts(std::move(options)),
          _bitmap_index_evaluator(_schema, _opts.pred_tree),
          _predicate_columns(_opts.pred_tree.num_columns()),
          _use_vector_index(_opts.use_vector_index) {
    if (_use_vector_index) {
        // The K in front of Fe is long, which can be changed to uint32. This can be a problem,
        // but this k is wasted memory allocation, so it should not exceed the accuracy of uint32
        // options.query_vector is a string, passed to tenann as a float string, see if you need to use the stof function to convert
        // and consider precision loss
        _vector_distance_column_name = _opts.vector_search_option->vector_distance_column_name;
        _vector_column_id = _opts.vector_search_option->vector_column_id;
        _vector_slot_id = _opts.vector_search_option->vector_slot_id;
        _vector_range = _opts.vector_search_option->vector_range;
        _result_order = _opts.vector_search_option->result_order;
        _use_ivfpq = _opts.vector_search_option->use_ivfpq;
        _query_params = _opts.vector_search_option->query_params;
        if (_vector_range >= 0 && _use_ivfpq) {
            _k = _opts.vector_search_option->k * _opts.vector_search_option->pq_refine_factor *
                 _opts.vector_search_option->k_factor;
        } else {
            _k = _opts.vector_search_option->k * _opts.vector_search_option->k_factor;
        }
#ifdef WITH_TENANN
        _query_view = tenann::PrimitiveSeqView{
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
    RETURN_IF_ERROR(_init_context());
    _init_column_predicates();

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
    ASSIGN_OR_RETURN(auto meta, get_vector_meta(tablet_index_meta, query_params))
    _index_meta = std::make_shared<tenann::IndexMeta>(std::move(meta));
    RETURN_IF_ERROR(VectorIndexReaderFactory::create_from_file(index_path, _index_meta, &_ann_reader));
    auto status = _ann_reader->init_searcher(*_index_meta.get(), index_path);
    // means empty ann reader
    if (status.is_not_supported()) {
        _use_vector_index = false;
        return Status::OK();
    }
    return status;
#else
    return Status::OK();
#endif
}

Status SegmentIterator::_init_ann_reader() {
#ifdef WITH_TENANN
    RETURN_IF(!_use_vector_index, Status::OK());
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

    return _init_reader_from_file(index_path, tablet_index_meta, _query_params);
#else
    return Status::OK();
#endif
}

Status SegmentIterator::_get_row_ranges_by_vector_index() {
#ifdef WITH_TENANN
    RETURN_IF(!_use_vector_index, Status::OK());
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
        if (_vector_range >= 0) {
            st = _ann_reader->range_search(_query_view, _k, &result_ids, &result_distances, &del_id_filter,
                                           static_cast<float>(_vector_range), _result_order);
        } else {
            result_ids.resize(_k);
            result_distances.resize(_k);
            st = _ann_reader->search(_query_view, _k, (result_ids.data()),
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

    _id2distance_map.reserve(filtered_result_ids.size());
    for (size_t i = 0; i < filtered_result_ids.size(); i++) {
        _id2distance_map[static_cast<rowid_t>(filtered_result_ids[i])] = id2distance_map[filtered_result_ids[i]];
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
                size_t prev_size = _scan_range.span_size();
                SparseRange<> res;
                res.set_sorted(_scan_range.is_sorted());
                _range_iter = _range_iter.intersection(r, &res);
                std::swap(res, _scan_range);
                _range_iter.set_range(&_scan_range);
                _opts.stats->runtime_stats_filtered += (prev_size - _scan_range.span_size());
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

        if (path->is_from_predicate()) {
            _predicate_column_access_paths[path->index()] = path;
        } else {
            _column_access_paths[path->index()] = path;
        }
    }
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

    ColumnAccessPath* access_path = nullptr;
    if (_column_access_paths.find(cid) != _column_access_paths.end()) {
        access_path = _column_access_paths[cid];
    }

    std::string dcg_filename;
    FileEncryptionInfo dcg_encryption_info;
    if (ucid < 0) {
        LOG(ERROR) << "invalid unique columnid in segment iterator, ucid: " << ucid
                   << ", segment: " << _segment->file_name();
    }
    auto tablet_schema = _opts.tablet_schema ? _opts.tablet_schema : _segment->tablet_schema_share_ptr();
    const auto& col = tablet_schema->column(cid);
    ASSIGN_OR_RETURN(auto col_iter, _new_dcg_column_iterator(col, &dcg_filename, &dcg_encryption_info, access_path));
    if (col_iter == nullptr) {
        // not found in delta column group, create normal column iterator
        ASSIGN_OR_RETURN(_column_iterators[cid], _segment->new_column_iterator_or_default(col, access_path));
        const auto encryption_info = _segment->encryption_info();
        if (encryption_info) {
            opts.encryption_info = *encryption_info;
        }
        ASSIGN_OR_RETURN(auto rfile, _opts.fs->new_random_access_file(opts, _segment->file_info()));
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

StatusOr<SparseRange<>> SegmentIterator::_get_row_ranges_by_key_ranges() {
    DCHECK(_opts.short_key_ranges.empty());

    SparseRange<> res;

    if (_opts.ranges.empty()) {
        res.add(Range<>(0, num_rows()));
        return res;
    }

    RETURN_IF_ERROR(_segment->load_index(_opts.lake_io_opts));
    for (const SeekRange& range : _opts.ranges) {
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
            res.add(Range{lower_rowid, upper_rowid});
        }
    }

    return res;
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
            RETURN_IF_ERROR(
                    column_iterators[cid]->get_row_ranges_by_zone_map(col_preds, del_pred, &cur_row_ranges, Type));
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
                    RETURN_IF_ERROR(
                            column_iterators[cid]->get_row_ranges_by_zone_map({}, del_pred, &cur_row_ranges, Type));
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

    ASSIGN_OR_RETURN(auto hit_row_ranges,
                     _opts.pred_tree_for_zone_map.visit(ZoneMapFilterEvaluator{
                             _opts.pred_tree_for_zone_map, _column_iterators, _del_predicates, del_columns}));
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
        ColumnPtr& column = chunk->get_column_by_index(i);
        size_t nread = nrows;
        RETURN_IF_ERROR(_column_iterators[cid]->next_batch(&nread, column.get()));
        may_has_del_row = may_has_del_row | (column->delete_state() != DEL_NOT_SATISFIED);
        DCHECK_EQ(nrows, nread);
    }
    chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    return Status::OK();
}

inline Status SegmentIterator::_read(Chunk* chunk, vector<rowid_t>* rowids, size_t n) {
    size_t read_num = 0;
    SparseRange<> range;

    if (_cur_rowid != _range_iter.begin() || _cur_rowid == 0) {
        _cur_rowid = _range_iter.begin();
        _opts.stats->block_seek_num += 1;
        SCOPED_RAW_TIMER(&_opts.stats->block_seek_ns);
        RETURN_IF_ERROR(_context->seek_columns(_cur_rowid));
    }

    _range_iter.next_range(n, &range);
    read_num += range.span_size();

    {
        _opts.stats->blocks_load += 1;
        SCOPED_RAW_TIMER(&_opts.stats->block_fetch_ns);
        RETURN_IF_ERROR(_context->read_columns(chunk, range));
        chunk->check_or_die();
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
    chunk->check_or_die();
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
    std::vector<uint32_t>* p_rowids = _always_build_rowid() ? &rowids : nullptr;
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
        for (auto& [cid, stream] : _column_files) {
            ASSIGN_OR_RETURN(auto vec, _column_iterators[cid]->get_io_range_vec(_scan_range));
            for (auto e : vec) {
                // if buf_size is 1MB, offset is 123, and size is 2MB
                // after calculation, offset will be 0, and size will be 2MB+123
                size_t offset = (e.first / buf_size) * buf_size;
                size_t size = e.second + (e.first % buf_size);
                while (size > 0) {
                    size_t cur_size = std::min(buf_size, size);
                    RETURN_IF_ERROR(stream->touch_cache(offset, cur_size));
                    offset += cur_size;
                    size -= cur_size;
                }
            }
        }

        _opts.stats->block_load_ns += sw.elapsed_time();

        return Status::EndOfFile("no more data in segment");
    }
#endif // USE_STAROS

    const uint32_t chunk_capacity = _reserve_chunk_size;
    const uint32_t return_chunk_threshold = std::max<uint32_t>(chunk_capacity - chunk_capacity / 4, 1);
    const bool has_non_expr_predicate = !_non_expr_pred_tree.empty();
    const bool scan_range_normalized = _scan_range.is_sorted();
    const int64_t prev_raw_rows_read = _opts.stats->raw_rows_read;

    _context->_read_chunk->reset();
    _context->_dict_chunk->reset();
    _context->_final_chunk->reset();
    _context->_adapt_global_dict_chunk->reset();

    Chunk* chunk = _context->_read_chunk.get();
    uint16_t chunk_start = chunk->num_rows();

    while ((chunk_start < return_chunk_threshold) & _range_iter.has_more()) {
        RETURN_IF_ERROR(_read(chunk, rowid, chunk_capacity - chunk_start));
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

    ASSIGN_OR_RETURN(size_t chunk_size, _filter_by_expr_predicates(chunk, rowid));

    _opts.stats->block_load_ns += sw.elapsed_time();

    int64_t total_read = _opts.stats->raw_rows_read - prev_raw_rows_read;

    if (UNLIKELY(raw_chunk_size == 0)) {
        // Return directly if chunk_start is zero, i.e, chunk is empty.
        // Otherwise, chunk will be swapped with result, which is incorrect
        // because the chunk is a pointer to _read_chunk instead of _final_chunk.
        return Status::EndOfFile("no more data in segment");
    }

    if (_context->_has_dict_column) {
        chunk = _context->_dict_chunk.get();
        SCOPED_RAW_TIMER(&_opts.stats->decode_dict_ns);
        RETURN_IF_ERROR(_decode_dict_codes(_context));
    }

    _build_final_chunk(_context);
    chunk = _context->_final_chunk.get();

    bool need_switch_context = false;
    if (_context->_late_materialize) {
        chunk = _context->_final_chunk.get();
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

    if (_context->_has_force_dict_encode) {
        RETURN_IF_ERROR(_encode_to_global_id(_context));
        chunk = _context->_adapt_global_dict_chunk.get();
    }

    if (_use_vector_index && !_use_ivfpq) {
        DCHECK(rowid != nullptr);
        std::shared_ptr<FloatColumn> distance_column = FloatColumn::create();
        vector<rowid_t> rowids;
        for (const auto& rid : *rowid) {
            auto it = _id2distance_map.find(rid);
            if (LIKELY(it != _id2distance_map.end())) {
                rowids.emplace_back(it->first);
            } else {
                DCHECK(false) << "not found row id:" << rid << " in distance map";
                return Status::InternalError(fmt::format("not found row id:{} in distance map", rid));
            }
        }
        for (const auto& vrid : rowids) {
            distance_column->append(_id2distance_map[vrid]);
        }

        // TODO: plan vector column in FE Planner
        chunk->append_vector_column(distance_column, _make_field(_vector_column_id), _vector_slot_id);
    }

    result->swap_chunk(*chunk);

    if (need_switch_context) {
        RETURN_IF_ERROR(_switch_context(_context->_next));
    }

    return Status::OK();
}

FieldPtr SegmentIterator::_make_field(size_t i) {
    return std::make_shared<Field>(i, _vector_distance_column_name, get_type_info(TYPE_FLOAT), false);
}

Status SegmentIterator::_switch_context(ScanContext* to) {
    if (_context != nullptr) {
        const ordinal_t ordinal = _context->_column_iterators[0]->get_current_ordinal();
        for (ColumnIterator* iter : to->_column_iterators) {
            RETURN_IF_ERROR(iter->seek_to_ordinal(ordinal));
        }
        _context->close();
    }

    if (to->_read_chunk == nullptr) {
        to->_read_chunk = ChunkHelper::new_chunk(to->_read_schema, _reserve_chunk_size);
    }

    if (to->_has_dict_column) {
        if (to->_dict_chunk == nullptr) {
            to->_dict_chunk = ChunkHelper::new_chunk(to->_dict_decode_schema, _reserve_chunk_size);
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

        to->_final_chunk = ChunkHelper::new_chunk(final_chunk_schema, _reserve_chunk_size);
    } else {
        to->_final_chunk = ChunkHelper::new_chunk(this->output_schema(), _reserve_chunk_size);
    }

    to->_adapt_global_dict_chunk = to->_has_force_dict_encode
                                           ? ChunkHelper::new_chunk(this->output_schema(), _reserve_chunk_size)
                                           : to->_final_chunk;

    _context = to;
    return Status::OK();
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

    uint16_t chunk_size = to;
    SCOPED_RAW_TIMER(&_opts.stats->vec_cond_chunk_copy_ns);
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
    _opts.stats->rows_vec_cond_filtered += (to - chunk_size);
    return chunk_size;
}

StatusOr<uint16_t> SegmentIterator::_filter_by_expr_predicates(Chunk* chunk, vector<rowid_t>* rowid) {
    size_t chunk_size = chunk->num_rows();
    if (chunk_size > 0 && !_expr_pred_tree.empty()) {
        SCOPED_RAW_TIMER(&_opts.stats->expr_cond_evaluate_ns);
        RETURN_IF_ERROR(_expr_pred_tree.evaluate(chunk, _selection.data(), 0, chunk_size));

        size_t hit_count = SIMD::count_nonzero(_selection.data(), chunk_size);
        size_t new_size = chunk_size;
        if (hit_count == 0) {
            chunk->set_num_rows(0);
            new_size = 0;
            if (rowid != nullptr) {
                rowid->resize(0);
            }
        } else if (hit_count != chunk_size) {
            new_size = chunk->filter_range(_selection, 0, chunk_size);
            if (rowid != nullptr) {
                auto size = ColumnHelper::filter_range<uint32_t>(_selection, rowid->data(), 0, chunk_size);
                rowid->resize(size);
            }
        }
        _opts.stats->rows_vec_cond_filtered += (chunk_size - new_size);
        chunk_size = new_size;
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

    for (size_t i = 0; i < early_materialize_fields; i++) {
        const FieldPtr& f = _schema.field(i);
        const ColumnId cid = f->id();
        bool use_global_dict_code = _can_using_global_dict(f);
        bool use_dict_code = _can_using_dict_code(f);

        if (delete_pred_columns.count(f->id()) || output_columns.count(f->id())) {
            ctx->_skip_dict_decode_indexes.push_back(false);
        } else {
            ctx->_skip_dict_decode_indexes.push_back(true);
            if (_prune_cols_candidate_by_inverted_index.count(f->id())) {
                // The column is pruneable if and only if:
                // 1. column in _prune_cols_candidate_by_inverted_index
                // 2. column not in output schema
                // 3. column is not one of the delete predicate columns
                // 4. column must not be dict decoded when the read is finished
                ctx->_prune_cols.insert(i);
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
                                                        _column_decoders[cid].code_convert_data());
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
            ctx->_subfield_columns.emplace_back(i);
            ctx->_subfield_iterators.emplace_back(iter);
        } else {
            ctx->_read_schema.append(f);
            ctx->_column_iterators.emplace_back(_column_iterators[cid].get());
            ctx->_is_dict_column.emplace_back(false);
            ctx->_dict_decode_schema.append(f);
        }
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

    return Status::OK();
}

Status SegmentIterator::_init_context() {
    _late_materialization_ratio = config::late_materialization_ratio;
    RETURN_IF_ERROR(_init_global_dict_decoder());

    if (_predicate_columns == 0 || _opts.pred_tree.empty() ||
        (_predicate_columns >= _schema.num_fields() && _predicate_column_access_paths.empty())) {
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

Status SegmentIterator::_init_global_dict_decoder() {
    // init decoder for all columns
    // in some case _build_context<false> won't be called
    for (int i = 0; i < _schema.num_fields(); ++i) {
        const FieldPtr& f = _schema.field(i);
        const ColumnId cid = f->id();
        if (_can_using_global_dict(f)) {
            auto iter = new GlobalDictCodeColumnIterator(cid, _column_iterators[cid].get(),
                                                         _column_decoders[cid].code_convert_data());
            _obj_pool.add(iter);
            _column_decoders[cid].set_iterator(iter);
        }
    }
    return Status::OK();
}

Status SegmentIterator::_rewrite_predicates() {
    {
        ColumnPredicateRewriter rewriter(_column_iterators, _schema, _predicate_need_rewrite, _predicate_columns,
                                         _scan_range);
        RETURN_IF_ERROR(rewriter.rewrite_predicate(&_obj_pool, _opts.pred_tree));
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
    DCHECK_NE(ctx->_read_chunk, ctx->_dict_chunk);
    if (ctx->_read_chunk->num_rows() == 0) {
        ctx->_dict_chunk->set_num_rows(0);
        return Status::OK();
    }

    const Schema& decode_schema = ctx->_dict_decode_schema;
    const size_t n = decode_schema.num_fields();
    bool may_has_del_row = ctx->_read_chunk->delete_state() != DEL_NOT_SATISFIED;
    for (size_t i = 0; i < n; i++) {
        const FieldPtr& f = decode_schema.field(i);
        const ColumnId cid = f->id();
        if (!ctx->_is_dict_column[i] || ctx->_skip_dict_decode_indexes[i]) {
            ctx->_dict_chunk->get_column_by_index(i).swap(ctx->_read_chunk->get_column_by_index(i));
        } else {
            ColumnPtr& dict_codes = ctx->_read_chunk->get_column_by_index(i);
            ColumnPtr& dict_values = ctx->_dict_chunk->get_column_by_index(i);
            dict_values->resize(0);

            RETURN_IF_ERROR(_column_decoders[cid].decode_dict_codes(*dict_codes, dict_values.get()));

            DCHECK_EQ(dict_codes->size(), dict_values->size());
            may_has_del_row |= (dict_values->delete_state() != DEL_NOT_SATISFIED);
            if (f->is_nullable()) {
                auto* nullable_codes = down_cast<NullableColumn*>(dict_codes.get());
                auto* nullable_values = down_cast<NullableColumn*>(dict_values.get());
                nullable_values->null_column_data().swap(nullable_codes->null_column_data());
                nullable_values->set_has_null(nullable_codes->has_null());
            }
        }
    }
    ctx->_dict_chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    ctx->_dict_chunk->check_or_die();
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
    const auto* ordinals = down_cast<FixedLengthColumn<rowid_t>*>(rowid_column.get());

    if (_predicate_columns < _schema.num_fields()) {
        const size_t n = _schema.num_fields();
        const size_t start_pos = ctx->_read_index_map.size();
        for (size_t i = m - 1, j = start_pos; i < n; i++, j++) {
            const FieldPtr& f = _schema.field(i);
            const ColumnId cid = f->id();
            ColumnPtr& col = ctx->_final_chunk->get_column_by_index(j);
            col->reserve(ordinals->size());
            col->resize(0);

            RETURN_IF_ERROR(_column_decoders[cid].decode_values_by_rowid(*ordinals, col.get()));
            DCHECK_EQ(ordinals->size(), col->size());
            may_has_del_row |= (col->delete_state() != DEL_NOT_SATISFIED);
        }
    }

    // fill subfield of early materialization columns
    for (size_t i = 0; i < ctx->_subfield_columns.size(); i++) {
        auto output_index = ctx->_subfield_columns[i];
        ColumnPtr& col = ctx->_final_chunk->get_column_by_index(output_index);
        // FillSubfieldIterator
        RETURN_IF_ERROR(ctx->_subfield_iterators[i]->fetch_values_by_rowid(*ordinals, col.get()));
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
        ColumnPtr& col = ctx->_final_chunk->get_column_by_index(i);
        ColumnPtr& dst = ctx->_adapt_global_dict_chunk->get_column_by_index(i);
        if (_column_decoders[cid].need_force_encode_to_global_id()) {
            RETURN_IF_ERROR(_column_decoders[cid].encode_to_global_id(col.get(), dst.get()));
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
    opts.kept_in_memory = false;
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

        RETURN_IF_ERROR(
                _bitmap_index_evaluator.init([&cid_2_ucid, this](ColumnId cid) -> StatusOr<BitmapIndexIterator*> {
                    const ColumnUID ucid = cid_2_ucid[cid];
                    // the column's index in this segment file
                    ASSIGN_OR_RETURN(std::shared_ptr<Segment> segment_ptr, _get_dcg_segment(ucid));
                    if (segment_ptr == nullptr) {
                        // find segment from delta column group failed, using main segment
                        segment_ptr = _segment;
                    }

                    IndexReadOptions opts;
                    opts.use_page_cache =
                            !_opts.temporary_data && _opts.use_page_cache &&
                            (config::enable_bitmap_index_memory_page_cache || !config::disable_storage_page_cache);
                    opts.kept_in_memory = !_opts.temporary_data && config::enable_bitmap_index_memory_page_cache;
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
    _inverted_index_iterators.resize(ChunkHelper::max_column_id(_schema) + 1, nullptr);
    std::unordered_map<ColumnId, ColumnUID> cid_2_ucid;

    for (auto& field : _schema.fields()) {
        cid_2_ucid[field->id()] = field->uid();
    }
    for (const auto& pair : _opts.pred_tree.get_immediate_column_predicate_map()) {
        ColumnId cid = pair.first;
        ColumnUID ucid = cid_2_ucid[cid];

        if (_inverted_index_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_inverted_index_iterator(ucid, &_inverted_index_iterators[cid], _opts));
            _has_inverted_index |= (_inverted_index_iterators[cid] != nullptr);
        }
    }
    return Status::OK();
}

Status SegmentIterator::_apply_inverted_index() {
    RETURN_IF(_scan_range.empty(), Status::OK());
    RETURN_IF(!_opts.enable_gin_filter, Status::OK());

    RETURN_IF_ERROR(_init_inverted_index_iterators());
    RETURN_IF(!_has_inverted_index, Status::OK());
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
        InvertedIndexIterator* inverted_iter = _inverted_index_iterators[cid];
        if (inverted_iter == nullptr) {
            continue;
        }
        const auto& it = cid_2_fid.find(cid);
        RETURN_IF(it == cid_2_fid.end(),
                  Status::InternalError(strings::Substitute("No fid can be mapped by cid $0", cid)));
        std::string column_name(_schema.field(it->second)->name());
        for (const ColumnPredicate* pred : pred_list) {
            if (_inverted_index_iterators[cid]->is_untokenized() || pred->type() == PredicateType::kExpr) {
                Status res = pred->seek_inverted_index(column_name, _inverted_index_iterators[cid], &row_bitmap);
                if (res.ok()) {
                    erased_preds.emplace(pred);
                    erased_pred_col_ids.emplace(cid);
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
                _prune_cols_candidate_by_inverted_index.insert(cid);
            }
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
        } else if (name == kIONsRemote) {
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

    for (auto* iter : _inverted_index_iterators) {
        if (iter != nullptr) {
            delete iter;
        }
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
        Schema ordered_schema = reorder_schema(schema, options.pred_tree);
        auto seg_iter = std::make_shared<SegmentIterator>(segment, ordered_schema, options);
        return new_projection_iterator(schema, seg_iter);
    }
}

} // namespace starrocks