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
#include <stack>
#include <unordered_map>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "common/config.h"
#include "common/status.h"
#include "fs/fs.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "gutil/stl_util.h"
#include "segment_options.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/column_expr_predicate.h"
#include "storage/column_or_predicate.h"
#include "storage/column_predicate.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/del_vector.h"
#include "storage/lake/update_manager.h"
#include "storage/olap_runtime_range_pruner.hpp"
#include "storage/projection_iterator.h"
#include "storage/range.h"
#include "storage/roaring2range.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/column_decoder.h"
#include "storage/rowset/common.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/dictcode_column_iterator.h"
#include "storage/rowset/rowid_column_iterator.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/storage_engine.h"
#include "storage/types.h"
#include "storage/update_manager.h"
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
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override { return do_get_next(chunk); }

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
        ScanContext* _next{nullptr};

        // index the column which only be used for filter
        // thus its not need do dict_decode_code
        std::vector<size_t> _skip_dict_decode_indexes;
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
    };

    Status _init();
    Status _try_to_update_ranges_by_runtime_filter();
    Status _do_get_next(Chunk* result, vector<rowid_t>* rowid);

    template <bool check_global_dict>
    Status _init_column_iterators(const Schema& schema);
    Status _get_row_ranges_by_keys();
    Status _get_row_ranges_by_key_ranges();
    Status _get_row_ranges_by_short_key_ranges();
    Status _get_row_ranges_by_zone_map();
    Status _get_row_ranges_by_bloom_filter();
    Status _get_row_ranges_by_rowid_range();

    uint32_t segment_id() const { return _segment->id(); }
    uint32_t num_rows() const { return _segment->num_rows(); }

    Status _lookup_ordinal(const SeekTuple& key, bool lower, rowid_t end, rowid_t* rowid);
    Status _lookup_ordinal(const Slice& index_key, const Schema& short_key_schema, bool lower, rowid_t end,
                           rowid_t* rowid);
    Status _seek_columns(const Schema& schema, rowid_t pos);
    Status _read_columns(const Schema& schema, Chunk* chunk, size_t nrows);

    StatusOr<uint16_t> _filter(Chunk* chunk, vector<rowid_t>* rowid, uint16_t from, uint16_t to);
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

    Status _build_final_chunk(ScanContext* ctx);

    Status _encode_to_global_id(ScanContext* ctx);

    void _switch_context(ScanContext* to);

    // `_check_low_cardinality_optimization` and `_init_column_iterators` must have been called
    // before you calling this method, otherwise the result is incorrect.
    bool _can_using_dict_code(const FieldPtr& field) const;

    // check field use low_cardinality global dict optimization
    bool _can_using_global_dict(const FieldPtr& field) const;

    Status _init_bitmap_index_iterators();

    Status _apply_bitmap_index();

    Status _apply_del_vector();

    Status _read(Chunk* chunk, vector<rowid_t>* rowid, size_t n);

    bool _skip_fill_data_cache() const { return !_opts.fill_data_cache; }

    void _init_column_access_paths();

    // search delta column group by column uniqueid, if this column exist in delta column group,
    // then return column iterator and delta column's fillname.
    // Or just return null
    StatusOr<std::unique_ptr<ColumnIterator>> _new_dcg_column_iterator(uint32_t ucid, std::string* filename,
                                                                       ColumnAccessPath* path);

    // This function is a unified entry for creating column iterators.
    // `ucid` means unique column id, use it for searching delta column group.
    Status _init_column_iterator_by_cid(const ColumnId cid, const ColumnUID ucid, bool check_dict_enc);

    void _update_stats(RandomAccessFile* rfile);

    //  This function will search and build the segment from delta column group,
    // and also return the columns's index in this segment if need.
    StatusOr<std::shared_ptr<Segment>> _get_dcg_segment(uint32_t ucid, int32_t* col_index);

private:
    using RawColumnIterators = std::vector<std::unique_ptr<ColumnIterator>>;
    using ColumnDecoders = std::vector<ColumnDecoder>;
    std::shared_ptr<Segment> _segment;
    std::unordered_map<std::string, std::shared_ptr<Segment>> _dcg_segments;
    SegmentReadOptions _opts;
    RawColumnIterators _column_iterators;
    ColumnDecoders _column_decoders;
    std::vector<BitmapIndexIterator*> _bitmap_index_iterators;
    // delete predicates
    std::map<ColumnId, ColumnOrPredicate> _del_predicates;

    Status _get_del_vec_st;
    Status _get_dcg_st;
    DelVectorPtr _del_vec;
    DeltaColumnGroupList _dcgs;
    roaring::api::roaring_uint32_iterator_t _roaring_iter;

    std::unordered_map<ColumnId, std::unique_ptr<RandomAccessFile>> _column_files;

    SparseRange<> _scan_range;
    SparseRangeIterator<> _range_iter;

    std::vector<const ColumnPredicate*> _vectorized_preds;
    std::vector<const ColumnPredicate*> _branchless_preds;
    std::vector<const ColumnPredicate*> _expr_ctx_preds; // predicates using ExprContext*
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

    // initial size of |_opts.predicates|.
    int _predicate_columns = 0;

    // the next rowid to read
    rowid_t _cur_rowid = 0;

    int _late_materialization_ratio = 0;

    int _reserve_chunk_size = 0;

    bool _inited = false;
    bool _has_bitmap_index = false;

    std::unordered_map<ColumnId, ColumnAccessPath*> _column_access_paths;
    std::unordered_map<ColumnId, ColumnAccessPath*> _predicate_column_access_paths;
};

SegmentIterator::SegmentIterator(std::shared_ptr<Segment> segment, Schema schema, SegmentReadOptions options)
        : ChunkIterator(std::move(schema), options.chunk_size),
          _segment(std::move(segment)),
          _opts(std::move(options)),
          _predicate_columns(_opts.predicates.size()) {
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
        if (_opts.dcg_loader != nullptr) {
            SCOPED_RAW_TIMER(&_opts.stats->get_delta_column_group_ns);
            _get_dcg_st = _opts.dcg_loader->load(tsid, _opts.version, &_dcgs);
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
            VLOG(1) << "seg_iter init delvec tablet:" << _opts.tablet_id << " rowset:" << _opts.rowset_id
                    << " seg:" << segment_id() << " version req:" << _opts.version << " actual:" << _del_vec->version()
                    << " " << _del_vec->cardinality() << "/" << _segment->num_rows();
            roaring_init_iterator(&_del_vec->roaring()->roaring, &_roaring_iter);
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
    // filter by index stage
    // Use indexes and predicates to filter some data page
    RETURN_IF_ERROR(_init_bitmap_index_iterators());
    RETURN_IF_ERROR(_get_row_ranges_by_keys());
    RETURN_IF_ERROR(_get_row_ranges_by_rowid_range());
    RETURN_IF_ERROR(_apply_del_vector());
    RETURN_IF_ERROR(_apply_bitmap_index());
    RETURN_IF_ERROR(_get_row_ranges_by_zone_map());
    RETURN_IF_ERROR(_get_row_ranges_by_bloom_filter());
    // rewrite stage
    // Rewriting predicates using segment dictionary codes
    RETURN_IF_ERROR(_rewrite_predicates());
    RETURN_IF_ERROR(_init_context());
    _init_column_predicates();
    _range_iter = _scan_range.new_iterator();

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
                RETURN_IF_ERROR(_column_iterators[cid]->get_row_ranges_by_zone_map(predicates, del_pred, &r));
                size_t prev_size = _scan_range.span_size();
                SparseRange<> res;
                _range_iter = _range_iter.intersection(r, &res);
                std::swap(res, _scan_range);
                _range_iter.set_range(&_scan_range);
                _opts.stats->runtime_stats_filtered += (prev_size - _scan_range.span_size());
                return Status::OK();
            },
            _opts.stats->raw_rows_read);
}

StatusOr<std::shared_ptr<Segment>> SegmentIterator::_get_dcg_segment(uint32_t ucid, int32_t* col_index) {
    // iterate dcg from new ver to old ver
    for (const auto& dcg : _dcgs) {
        int32_t idx = dcg->get_column_idx(ucid);
        if (idx >= 0) {
            std::string column_file = dcg->column_file(parent_name(_segment->file_name()));
            if (_dcg_segments.count(column_file) == 0) {
                ASSIGN_OR_RETURN(auto dcg_segment, _segment->new_dcg_segment(*dcg));
                _dcg_segments[column_file] = dcg_segment;
            }
            if (col_index != nullptr) {
                *col_index = idx;
            }
            return _dcg_segments[column_file];
        }
    }
    // the column not exist in delta column group
    return nullptr;
}

StatusOr<std::unique_ptr<ColumnIterator>> SegmentIterator::_new_dcg_column_iterator(uint32_t ucid,
                                                                                    std::string* filename,
                                                                                    ColumnAccessPath* path) {
    // build column iter from delta column group
    int32_t col_index = 0;
    ASSIGN_OR_RETURN(auto dcg_segment, _get_dcg_segment(ucid, &col_index));
    if (dcg_segment != nullptr) {
        if (filename != nullptr) {
            *filename = dcg_segment->file_name();
        }
        return dcg_segment->new_column_iterator(col_index, path);
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
    iter_opts.check_dict_encoding = check_dict_enc;
    iter_opts.reader_type = _opts.reader_type;
    iter_opts.fill_data_cache = _opts.fill_data_cache;

    RandomAccessFileOptions opts{.skip_fill_local_cache = _skip_fill_data_cache()};

    ColumnAccessPath* access_path = nullptr;
    if (_column_access_paths.find(cid) != _column_access_paths.end()) {
        access_path = _column_access_paths[cid];
    }

    std::string dcg_filename;
    if (ucid < 0) {
        LOG(ERROR) << "invalid unique columnid in segment iterator, ucid: " << ucid
                   << ", segment: " << _segment->file_name();
    }
    ASSIGN_OR_RETURN(auto col_iter, _new_dcg_column_iterator((uint32_t)ucid, &dcg_filename, access_path));
    if (col_iter == nullptr) {
        // not found in delta column group, create normal column iterator
        ASSIGN_OR_RETURN(_column_iterators[cid], _segment->new_column_iterator(cid, access_path));
        ASSIGN_OR_RETURN(auto rfile, _opts.fs->new_random_access_file(opts, _segment->file_name()));
        iter_opts.read_file = rfile.get();
        _column_files[cid] = std::move(rfile);
    } else {
        // create delta column iterator
        _column_iterators[cid] = std::move(col_iter);
        ASSIGN_OR_RETURN(auto dcg_file, _opts.fs->new_random_access_file(opts, dcg_filename));
        iter_opts.read_file = dcg_file.get();
        _column_files[cid] = std::move(dcg_file);
    }
    RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts));
    return Status::OK();
}

template <bool check_global_dict>
Status SegmentIterator::_init_column_iterators(const Schema& schema) {
    DCHECK_EQ(_predicate_columns, _opts.predicates.size());

    const size_t n = std::max<size_t>(1 + ChunkHelper::max_column_id(schema), _column_iterators.size());
    _column_iterators.resize(n);
    if constexpr (check_global_dict) {
        _column_decoders.resize(n);
    }

    bool has_predicate = !_opts.predicates.empty();
    _predicate_need_rewrite.resize(n, false);
    for (const FieldPtr& f : schema.fields()) {
        const ColumnId cid = f->id();
        if (_column_iterators[cid] == nullptr) {
            bool check_dict_enc;
            if (_opts.global_dictmaps->count(cid)) {
                // if cid has global dict encode
                // we will force the use of dictionary codes
                check_dict_enc = true;
            } else if (_opts.predicates.count(cid)) {
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

void SegmentIterator::_init_column_predicates() {
    DCHECK_EQ(_predicate_columns, _opts.predicates.size());
    for (const auto& pair : _opts.predicates) {
        for (const ColumnPredicate* pred : pair.second) {
            // If this predicate is generated by join runtime filter,
            // We only use it to compute segment row range.
            if (pred->is_index_filter_only()) {
                continue;
            }
            if (pred->is_expr_predicate()) {
                _expr_ctx_preds.emplace_back(pred);
            } else if (pred->can_vectorized()) {
                _vectorized_preds.emplace_back(pred);
            } else {
                _branchless_preds.emplace_back(pred);
            }
        }
    }
    if (_vectorized_preds.empty() && _branchless_preds.empty()) {
        _opts.predicates.clear();
    }
}

Status SegmentIterator::_get_row_ranges_by_keys() {
    StarRocksMetrics::instance()->segment_row_total.increment(num_rows());

    if (!_opts.short_key_ranges.empty()) {
        RETURN_IF_ERROR(_get_row_ranges_by_short_key_ranges());
    } else {
        RETURN_IF_ERROR(_get_row_ranges_by_key_ranges());
    }

    _opts.stats->rows_key_range_filtered += num_rows() - _scan_range.span_size();
    StarRocksMetrics::instance()->segment_rows_by_short_key.increment(_scan_range.span_size());
    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_by_key_ranges() {
    DCHECK(_opts.short_key_ranges.empty());
    DCHECK_EQ(0, _scan_range.span_size());

    if (_opts.ranges.empty()) {
        _scan_range.add(Range<>(0, num_rows()));
        return Status::OK();
    }

    RETURN_IF_ERROR(_segment->load_index(_skip_fill_data_cache()));
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
            _scan_range.add(Range{lower_rowid, upper_rowid});
        }
    }

    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_by_short_key_ranges() {
    DCHECK(!_opts.short_key_ranges.empty());
    DCHECK_EQ(0, _scan_range.span_size());

    if (_opts.short_key_ranges.size() == 1 && _opts.short_key_ranges[0]->lower->is_infinite() &&
        _opts.short_key_ranges[0]->upper->is_infinite()) {
        _scan_range.add(Range<>(0, num_rows()));
        return Status::OK();
    }

    RETURN_IF_ERROR(_segment->load_index(_skip_fill_data_cache()));
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
            _scan_range.add(Range{lower_rowid, upper_rowid});
        }
    }

    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_by_zone_map() {
    SparseRange<> zm_range(0, num_rows());

    // -------------------------------------------------------------
    // group delete predicates by column id.
    // -------------------------------------------------------------
    // e.g, if there are two (disjunctive) delete predicates:
    // `c1=1 and c2=100` and `c1=100 and c2=200`, the group result
    // will be a mapping of `c1` to predicate `c1=1 or c1=100` and a
    // mapping of `c2` to predicate `c2=100 or c2=200`.
    std::set<ColumnId> columns;
    _opts.delete_predicates.get_column_ids(&columns);
    for (ColumnId cid : columns) {
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
    for (const auto& pair : _opts.predicates) {
        columns.insert(pair.first);
    }

    std::vector<const ColumnPredicate*> query_preds;
    for (ColumnId cid : columns) {
        auto iter1 = _opts.predicates_for_zone_map.find(cid);
        if (iter1 != _opts.predicates_for_zone_map.end()) {
            query_preds = iter1->second;
        } else {
            query_preds.clear();
        }

        const ColumnPredicate* del_pred;
        auto iter = _del_predicates.find(cid);
        del_pred = iter != _del_predicates.end() ? &(iter->second) : nullptr;
        SparseRange<> r;
        RETURN_IF_ERROR(_column_iterators[cid]->get_row_ranges_by_zone_map(query_preds, del_pred, &r));
        zm_range = zm_range.intersection(r);
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
    do {
        st = _do_get_next(chunk, nullptr);
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

Status SegmentIterator::_do_get_next(Chunk* result, vector<rowid_t>* rowid) {
    MonotonicStopWatch sw;
    sw.start();

    const uint32_t chunk_capacity = _reserve_chunk_size;
    const uint32_t return_chunk_threshold = std::max<uint32_t>(chunk_capacity - chunk_capacity / 4, 1);
    const bool has_predicate = !_opts.predicates.empty();
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

        if (has_predicate) {
            ASSIGN_OR_RETURN(next_start, _filter(chunk, rowid, chunk_start, next_start));
            chunk->check_or_die();
        }
        chunk_start = next_start;
        DCHECK_EQ(chunk_start, chunk->num_rows());
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

    result->swap_chunk(*chunk);

    if (need_switch_context) {
        _switch_context(_context->_next);
    }

    return Status::OK();
}

void SegmentIterator::_switch_context(ScanContext* to) {
    if (_context != nullptr) {
        const ordinal_t ordinal = _context->_column_iterators[0]->get_current_ordinal();
        for (ColumnIterator* iter : to->_column_iterators) {
            iter->seek_to_ordinal(ordinal);
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
        // rebuild encoded schema
        _encoded_schema.clear();
        for (const auto& field : schema().fields()) {
            if (_can_using_global_dict(field)) {
                _encoded_schema.append(Field::convert_to_dict_field(*field));
            } else {
                _encoded_schema.append(field);
            }
        }
        to->_final_chunk = ChunkHelper::new_chunk(this->_encoded_schema, _reserve_chunk_size);
    } else {
        to->_final_chunk = ChunkHelper::new_chunk(this->output_schema(), _reserve_chunk_size);
    }

    to->_adapt_global_dict_chunk = to->_has_force_dict_encode
                                           ? ChunkHelper::new_chunk(this->output_schema(), _reserve_chunk_size)
                                           : to->_final_chunk;

    _context = to;
}

StatusOr<uint16_t> SegmentIterator::_filter(Chunk* chunk, vector<rowid_t>* rowid, uint16_t from, uint16_t to) {
    // There must be one predicate, either vectorized or branchless.
    DCHECK(_vectorized_preds.size() + _branchless_preds.size() > 0 || _del_vec);

    SCOPED_RAW_TIMER(&_opts.stats->vec_cond_ns);

    // first evaluate
    if (!_vectorized_preds.empty()) {
        SCOPED_RAW_TIMER(&_opts.stats->vec_cond_evaluate_ns);
        const ColumnPredicate* pred = _vectorized_preds[0];
        Column* c = chunk->get_column_by_id(pred->column_id()).get();
        pred->evaluate(c, _selection.data(), from, to);
        for (int i = 1; i < _vectorized_preds.size(); ++i) {
            pred = _vectorized_preds[i];
            c = chunk->get_column_by_id(pred->column_id()).get();
            pred->evaluate_and(c, _selection.data(), from, to);
        }
    }

    // evaluate brachless
    if (!_branchless_preds.empty()) {
        SCOPED_RAW_TIMER(&_opts.stats->branchless_cond_evaluate_ns);

        uint16_t selected_size = 0;
        if (!_vectorized_preds.empty()) {
            for (uint16_t i = from; i < to; ++i) {
                _selected_idx[selected_size] = i;
                selected_size += _selection[i];
            }
        } else {
            // when there is no vectorized predicates, should initialize _selected_idx
            // in a vectorized way
            selected_size = to - from;
            for (uint16_t i = from, j = 0; i < to; ++i, ++j) {
                _selected_idx[j] = i;
            }
        }

        for (size_t i = 0; selected_size > 0 && i < _branchless_preds.size(); ++i) {
            const ColumnPredicate* pred = _branchless_preds[i];
            ColumnPtr& c = chunk->get_column_by_id(pred->column_id());
            ASSIGN_OR_RETURN(selected_size, pred->evaluate_branchless(c.get(), _selected_idx.data(), selected_size));
        }

        memset(&_selection[from], 0, to - from);
        for (uint16_t i = 0; i < selected_size; ++i) {
            _selection[_selected_idx[i]] = 1;
        }
    }

    auto hit_count = SIMD::count_nonzero(&_selection[from], to - from);
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
    if (_expr_ctx_preds.size() != 0 && chunk_size > 0) {
        SCOPED_RAW_TIMER(&_opts.stats->expr_cond_evaluate_ns);
        const auto* pred = _expr_ctx_preds[0];
        Column* c = chunk->get_column_by_id(pred->column_id()).get();
        pred->evaluate(c, _selection.data(), 0, chunk_size);

        for (int i = 1; i < _expr_ctx_preds.size(); ++i) {
            pred = _expr_ctx_preds[i];
            c = chunk->get_column_by_id(pred->column_id()).get();
            pred->evaluate_and(c, _selection.data(), 0, chunk_size);
        }

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
    if (_opts.predicates.find(field->id()) != _opts.predicates.end()) {
        return _predicate_need_rewrite[field->id()];
    } else {
        return (_has_bitmap_index || !_opts.predicates.empty()) &&
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
    ctx->_is_dict_column.reserve(ctx_fields);
    ctx->_column_iterators.reserve(ctx_fields);
    ctx->_skip_dict_decode_indexes.reserve(ctx_fields);

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
        }

        if (use_dict_code || use_global_dict_code) {
            // create FixedLengthColumn<int64_t> for saving dict codewords.
            auto f2 = std::make_shared<Field>(cid, f->name(), kDictCodeType, -1, -1, f->is_nullable());
            ColumnIterator* iter = nullptr;
            if (use_global_dict_code) {
                iter = new GlobalDictCodeColumnIterator(cid, _column_iterators[cid].get(),
                                                        _column_decoders[cid].code_convert_data(),
                                                        _opts.global_dictmaps->at(cid));
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
        } else {
            ctx->_read_schema.append(f);
            ctx->_column_iterators.emplace_back(_column_iterators[cid].get());
            ctx->_is_dict_column.emplace_back(false);
            ctx->_dict_decode_schema.append(f);
        }
    }

    size_t build_read_index_size = ctx->_read_schema.num_fields();
    if (late_materialization && predicate_count < _schema.num_fields()) {
        // ordinal column
        ColumnId cid = _schema.field(predicate_count)->id();
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
    // map _read_schema[cid, index] to output_schema[cid index]
    // skip dict_decode column in _read_schema would not be mapping
    std::unordered_map<ColumnId, size_t> read_indexes;
    for (size_t i = 0; i < build_read_index_size; i++) {
        if (!ctx->_skip_dict_decode_indexes[i]) {
            read_indexes[ctx->_read_schema.field(i)->id()] = i;
        }
    }

    ctx->_read_index_map.resize(read_indexes.size());
    for (size_t i = 0; i < read_indexes.size(); i++) {
        ctx->_read_index_map[i] = read_indexes[output_schema().field(i)->id()];
    }

    return Status::OK();
}

Status SegmentIterator::_init_context() {
    DCHECK_EQ(_predicate_columns, _opts.predicates.size());
    _late_materialization_ratio = config::late_materialization_ratio;

    RETURN_IF_ERROR(_init_global_dict_decoder());

    if (_predicate_columns == 0 || _predicate_columns >= _schema.num_fields()) {
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
    _switch_context(&_context_list[0]);
    return Status::OK();
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
                                                         _opts.global_dictmaps->at(cid));
            _obj_pool.add(iter);
            _column_decoders[cid].set_iterator(iter);
        }
    }
    return Status::OK();
}

Status SegmentIterator::_rewrite_predicates() {
    //
    ColumnPredicateRewriter rewriter(_column_iterators, _opts.predicates, _schema, _predicate_need_rewrite,
                                     _predicate_columns, _scan_range);
    RETURN_IF_ERROR(rewriter.rewrite_predicate(&_obj_pool));

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
        GlobalDictPredicatesRewriter crewriter(conjunct_predicate, *_opts.global_dictmaps, &disable_dict_rewrites);
        RETURN_IF_ERROR(crewriter.rewrite_predicate(&_obj_pool));
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
            ctx->_dict_chunk->get_column_by_index(i)->swap_column(*ctx->_read_chunk->get_column_by_index(i));
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
    DCHECK_EQ(_predicate_columns, _opts.predicates.size());
    _predicate_need_rewrite.resize(1 + ChunkHelper::max_column_id(_schema), false);
    const size_t n = _opts.predicates.size();
    for (size_t i = 0; i < n; i++) {
        const FieldPtr& field = _schema.field(i);
        const LogicalType type = field->type()->type();
        if (type != TYPE_CHAR && type != TYPE_VARCHAR) {
            continue;
        }
        ColumnId cid = field->id();
        auto iter = _opts.predicates.find(cid);
        DCHECK(iter != _opts.predicates.end());
        const PredicateList& preds = iter->second;
        if (preds.size() > 0) {
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
    ctx->_final_chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    ctx->_final_chunk->check_or_die();

    return Status::OK();
}

Status SegmentIterator::_build_final_chunk(ScanContext* ctx) {
    // trim all use less columns
    Columns& input_columns = ctx->_dict_chunk->columns();
    for (size_t i = 0; i < ctx->_read_index_map.size(); i++) {
        ctx->_final_chunk->get_column_by_index(i).swap(input_columns[ctx->_read_index_map[i]]);
    }
    bool may_has_del_row = ctx->_dict_chunk->delete_state() != DEL_NOT_SATISFIED;
    ctx->_final_chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    return Status::OK();
}

Status SegmentIterator::_encode_to_global_id(ScanContext* ctx) {
    int num_columns = ctx->_final_chunk->num_columns();
    auto final_chunk = ctx->_final_chunk;

    for (size_t i = 0; i < num_columns; i++) {
        const FieldPtr& f = _schema.field(i);
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

Status SegmentIterator::_init_bitmap_index_iterators() {
    DCHECK_EQ(_predicate_columns, _opts.predicates.size());
    _bitmap_index_iterators.resize(ChunkHelper::max_column_id(_schema) + 1, nullptr);
    std::unordered_map<ColumnId, ColumnUID> cid_2_ucid;
    for (auto& field : _schema.fields()) {
        cid_2_ucid[field->id()] = field->uid();
    }
    for (const auto& pair : _opts.predicates) {
        ColumnId cid = pair.first;
        if (_bitmap_index_iterators[cid] == nullptr) {
            ColumnUID ucid = cid_2_ucid[cid];
            // the column's index in this segment file
            int32_t col_index = 0;
            ASSIGN_OR_RETURN(std::shared_ptr<Segment> segment_ptr, _get_dcg_segment(ucid, &col_index));
            if (segment_ptr == nullptr) {
                // find segment from delta column group failed, using main segment
                segment_ptr = _segment;
                col_index = cid;
            }

            IndexReadOptions opts;
            opts.use_page_cache = config::enable_bitmap_index_memory_page_cache || !config::disable_storage_page_cache;
            opts.kept_in_memory = config::enable_bitmap_index_memory_page_cache;
            opts.skip_fill_data_cache = _skip_fill_data_cache();
            opts.read_file = _column_files[cid].get();
            opts.stats = _opts.stats;

            RETURN_IF_ERROR(segment_ptr->new_bitmap_index_iterator(col_index, opts, &_bitmap_index_iterators[cid]));
            _has_bitmap_index |= (_bitmap_index_iterators[cid] != nullptr);
        }
    }
    return Status::OK();
}

// filter rows by evaluating column predicates using bitmap indexes.
// upon return, predicates that have been evaluated by bitmap indexes will be removed.
Status SegmentIterator::_apply_bitmap_index() {
    DCHECK_EQ(_predicate_columns, _opts.predicates.size());
    RETURN_IF(!_has_bitmap_index, Status::OK());
    SCOPED_RAW_TIMER(&_opts.stats->bitmap_index_filter_timer);

    // ---------------------------------------------------------
    // Seek bitmap index.
    //  - Seek to the position of predicate's operand within
    //    bitmap index dictionary.
    // ---------------------------------------------------------
    std::vector<ColumnId> bitmap_columns;
    std::vector<SparseRange<>> bitmap_ranges;
    std::vector<bool> has_is_null_predicate;
    std::vector<const ColumnPredicate*> erased_preds;

    size_t mul_selected = 1;
    size_t mul_cardinality = 1;
    for (auto& [cid, pred_list] : _opts.predicates) {
        BitmapIndexIterator* bitmap_iter = _bitmap_index_iterators[cid];
        if (bitmap_iter == nullptr) {
            continue;
        }
        size_t cardinality = bitmap_iter->bitmap_nums();
        SparseRange<> selected(0, cardinality);
        bool has_is_null = false;
        for (const ColumnPredicate* pred : pred_list) {
            SparseRange<> r;
            Status st = pred->seek_bitmap_dictionary(bitmap_iter, &r);
            if (st.ok()) {
                selected &= r;
                erased_preds.emplace_back(pred);
                has_is_null |= (pred->type() == PredicateType::kIsNull);
            } else if (!st.is_cancelled()) {
                return st;
            }
        }
        if (selected.empty()) {
            _opts.stats->rows_bitmap_index_filtered += _scan_range.span_size();
            _scan_range.clear();
            return Status::OK();
        }
        if (selected.span_size() < cardinality) {
            bitmap_columns.emplace_back(cid);
            bitmap_ranges.emplace_back(selected);
            has_is_null_predicate.emplace_back(has_is_null);
            mul_selected *= selected.span_size();
            mul_cardinality *= cardinality;
        }
    }

    // ---------------------------------------------------------
    // Estimate the selectivity of the bitmap index.
    // ---------------------------------------------------------
    if (bitmap_columns.empty() || (mul_selected * 1000 > mul_cardinality * config::bitmap_max_filter_ratio)) {
        return Status::OK();
    }

    // ---------------------------------------------------------
    // Retrieve the bitmap of each field.
    // ---------------------------------------------------------
    Roaring row_bitmap = range2roaring(_scan_range);
    size_t input_rows = row_bitmap.cardinality();
    DCHECK_EQ(input_rows, _scan_range.span_size());

    for (size_t i = 0; i < bitmap_columns.size(); i++) {
        Roaring roaring;
        BitmapIndexIterator* bitmap_iter = _bitmap_index_iterators[bitmap_columns[i]];
        if (bitmap_iter->has_null_bitmap() && !has_is_null_predicate[i]) {
            Roaring null_bitmap;
            RETURN_IF_ERROR(bitmap_iter->read_null_bitmap(&null_bitmap));
            row_bitmap -= null_bitmap;
        }
        RETURN_IF_ERROR(bitmap_iter->read_union_bitmap(bitmap_ranges[i], &roaring));
        row_bitmap &= roaring;
    }

    DCHECK_LE(row_bitmap.cardinality(), _scan_range.span_size());
    if (row_bitmap.cardinality() < _scan_range.span_size()) {
        _scan_range = roaring2range(row_bitmap);
    }

    // ---------------------------------------------------------
    // Erase predicates that hit bitmap index.
    // ---------------------------------------------------------
    for (const ColumnPredicate* pred : erased_preds) {
        PredicateList& pred_list = _opts.predicates[pred->column_id()];
        pred_list.erase(std::find(pred_list.begin(), pred_list.end(), pred));
    }

    _opts.stats->rows_bitmap_index_filtered += (input_rows - _scan_range.span_size());
    return Status::OK();
}

Status SegmentIterator::_apply_del_vector() {
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

Status SegmentIterator::_get_row_ranges_by_bloom_filter() {
    RETURN_IF(_opts.predicates.empty(), Status::OK());
    size_t prev_size = _scan_range.span_size();
    for (const auto& [cid, preds] : _opts.predicates) {
        ColumnIterator* column_iter = _column_iterators[cid].get();
        RETURN_IF_ERROR(column_iter->get_row_ranges_by_bloom_filter(preds, &_scan_range));
    }
    _opts.stats->rows_bf_filtered += prev_size - _scan_range.span_size();
    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_by_rowid_range() {
    if (_opts.rowid_range_option == nullptr) {
        return Status::OK();
    }

    _scan_range = _scan_range.intersection(*_opts.rowid_range_option);
    return Status::OK();
}

// Currently, update stats is only used for lake tablet, and numeric statistics is nullptr for local tablet.
void SegmentIterator::_update_stats(RandomAccessFile* rfile) {
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
        } else if (name == kBytesReadRemote) {
            _opts.stats->compressed_bytes_read_remote += value;
            _opts.stats->compressed_bytes_read += value;
        } else if (name == kIOCountLocalDisk) {
            _opts.stats->io_count_local_disk += value;
            _opts.stats->io_count += value;
        } else if (name == kIOCountRemote) {
            _opts.stats->io_count_remote += value;
            _opts.stats->io_count += value;
        } else if (name == kIONsLocalDisk) {
            _opts.stats->io_ns_local_disk += value;
        } else if (name == kIONsRemote) {
            _opts.stats->io_ns_remote += value;
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

    for (auto* iter : _bitmap_index_iterators) {
        delete iter;
    }
}

// put the field that has predicated on it ahead of those without one, for handle late
// materialization easier.
inline Schema reorder_schema(const Schema& input, const std::unordered_map<ColumnId, PredicateList>& predicates) {
    const std::vector<FieldPtr>& fields = input.fields();

    Schema output;
    output.reserve(fields.size());
    for (const auto& field : fields) {
        if (predicates.count(field->id())) {
            output.append(field);
        }
    }
    for (const auto& field : fields) {
        if (!predicates.count(field->id())) {
            output.append(field);
        }
    }
    return output;
}

ChunkIteratorPtr new_segment_iterator(const std::shared_ptr<Segment>& segment, const Schema& schema,
                                      const SegmentReadOptions& options) {
    if (options.predicates.empty() || options.predicates.size() >= schema.num_fields()) {
        return std::make_shared<SegmentIterator>(segment, schema, options);
    } else {
        Schema ordered_schema = reorder_schema(schema, options.predicates);
        auto seg_iter = std::make_shared<SegmentIterator>(segment, ordered_schema, options);
        return new_projection_iterator(schema, seg_iter);
    }
}

} // namespace starrocks
