// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/rowset/vectorized/segment_iterator.h"

#include <memory>

#include "butil/containers/flat_map.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/config.h"
#include "common/status.h"
#include "gutil/stl_util.h"
#include "runtime/current_mem_tracker.h"
#include "simd/simd.h"
#include "storage/column_predicate.h"
#include "storage/del_vector.h"
#include "storage/fs/fs_util.h"
#include "storage/row_block2.h"
#include "storage/rowset/segment_v2/bitmap_index_reader.h"
#include "storage/rowset/segment_v2/column_reader.h"
#include "storage/rowset/segment_v2/common.h"
#include "storage/rowset/segment_v2/row_ranges.h"
#include "storage/rowset/segment_v2/segment.h"
#include "storage/rowset/vectorized/rowid_column_iterator.h"
#include "storage/rowset/vectorized/segment_options.h"
#include "storage/storage_engine.h"
#include "storage/types.h"
#include "storage/update_manager.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/chunk_iterator.h"
#include "storage/vectorized/column_or_predicate.h"
#include "storage/vectorized/column_predicate.h"
#include "storage/vectorized/projection_iterator.h"
#include "storage/vectorized/range.h"
#include "storage/vectorized/roaring2range.h"
#include "util/starrocks_metrics.h"

namespace starrocks::vectorized {

using segment_v2::BitmapIndexIterator;
using segment_v2::ColumnIterator;
using segment_v2::ColumnIteratorOptions;
using segment_v2::rowid_t;
using segment_v2::RowRanges;
using segment_v2::Segment;

constexpr static const FieldType kDictCodeType = OLAP_FIELD_TYPE_INT;

// compare |tuple| with the first row of |chunk|.
// NULL will be treated as a minimal value.
static int compare(const SeekTuple& tuple, const Chunk& chunk) {
    DCHECK_LE(tuple.columns(), chunk.num_columns());
    const auto& schema = tuple.schema();
    const size_t n = tuple.columns();
    for (size_t i = 0; i < n; i++) {
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

// DictCodeColumnIterator is a wrapper/proxy on another column iterator that will
// transform the invoking of `next_batch(size_t*, Column*)` to the invoking of
// `next_dict_codes(size_t*, Column*)`.
class DictCodeColumnIterator final : public ColumnIterator {
public:
    // does not take the ownership of |iter|.
    DictCodeColumnIterator(ColumnId cid, ColumnIterator* iter) : _cid(cid), _col_iter(iter) {}

    ~DictCodeColumnIterator() override = default;

    ColumnId column_id() const { return _cid; }

    ColumnIterator* column_iterator() const { return _col_iter; }

    Status next_batch(size_t* n, Column* dst) override { return _col_iter->next_dict_codes(n, dst); }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) override {
        return _col_iter->fetch_values_by_rowid(rowids, size, values);
    }

    Status seek_to_first() override { return _col_iter->seek_to_first(); }

    Status seek_to_ordinal(ordinal_t ord) override { return _col_iter->seek_to_ordinal(ord); }

    Status next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) override {
        return _col_iter->next_batch(n, dst, has_null);
    }

    ordinal_t get_current_ordinal() const override { return _col_iter->get_current_ordinal(); }

    bool all_page_dict_encoded() const override { return _col_iter->all_page_dict_encoded(); }

    int dict_lookup(const Slice& word) override { return _col_iter->dict_lookup(word); }

    Status next_dict_codes(size_t* n, vectorized::Column* dst) override { return _col_iter->next_dict_codes(n, dst); }

    Status decode_dict_codes(const int32_t* codes, size_t size, vectorized::Column* words) override {
        return _col_iter->decode_dict_codes(codes, size, words);
    }

    Status get_row_ranges_by_zone_map(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                      const ColumnPredicate* del_predicate,
                                      vectorized::SparseRange* row_ranges) override {
        return _col_iter->get_row_ranges_by_zone_map(predicates, del_predicate, row_ranges);
    }

private:
    ColumnId _cid;
    ColumnIterator* _col_iter;
};

/// SegmentIterator
// TODO(zhuming): Refine the implementation of this class to reduce the intellectual overhead.
// Too many policies encapsulated in this class, should split this class into many small classes.
class SegmentIterator final : public ChunkIterator {
public:
    SegmentIterator(std::shared_ptr<Segment> segment, vectorized::Schema _schema,
                    vectorized::SegmentReadOptions options);

    ~SegmentIterator() override = default;

    void close() override;

protected:
    Status do_get_next(Chunk* chunk) override;
    Status do_get_next(Chunk* chunk, vector<uint32_t>* rowid) override;

private:
    struct ScanContext {
        ScanContext() : _next(nullptr), _has_dict_column(false), _late_materialize(false) {}

        ~ScanContext() = default;

        void close() {
            CurrentMemTracker::release(memory_usage());
            _read_chunk.reset();
            _dict_chunk.reset();
            _final_chunk.reset();
        }

        Status seek_columns(ordinal_t pos) {
            for (auto iter : _column_iterators) {
                RETURN_IF_ERROR(iter->seek_to_ordinal(pos));
            }
            return Status::OK();
        }

        Status read_columns(Chunk* chunk, size_t n) {
            bool may_has_del_row = chunk->delete_state() != DEL_NOT_SATISFIED;
            for (size_t i = 0; i < _column_iterators.size(); i++) {
                const ColumnPtr& col = chunk->get_column_by_index(i);
                RETURN_IF_ERROR(_column_iterators[i]->next_batch(&n, col.get()));
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
            return usage;
        }

        Schema _read_schema;
        Schema _dict_decode_schema;
        std::vector<bool> _is_dict_column;
        std::vector<ColumnIterator*> _column_iterators;
        ScanContext* _next;

        std::shared_ptr<Chunk> _read_chunk;
        std::shared_ptr<Chunk> _dict_chunk;
        std::shared_ptr<Chunk> _final_chunk;

        // true iff |_is_dict_column| contains at least one `true`, i.e,
        // |_column_iterators| contains at least one `DictCodeColumnIterator`.
        bool _has_dict_column;

        // if true, the last item of |_column_iterators| is a `RowIdColumnIterator` and
        // the last item of |_read_schema| and |_dict_decode_schema| is a row id field.
        bool _late_materialize;
    };

    Status _init();
    Status _do_get_next(Chunk* result, vector<rowid_t>* rowid);

    Status _init_column_iterators(const Schema& schema);
    Status _get_row_ranges_by_keys();
    Status _get_row_ranges_by_zone_map();
    Status _get_row_ranges_by_bloom_filter();

    uint32_t segment_id() const { return _segment->id(); }
    uint32_t num_rows() const { return _segment->num_rows(); }

    Status _lookup_ordinal(const SeekTuple& key, bool lower, rowid_t end, rowid_t* rowid);
    Status _seek_columns(const Schema& schema, rowid_t pos);
    Status _read_columns(const Schema& schema, Chunk* chunk, size_t nrows);

    uint16_t _filter(Chunk* chunk, vector<rowid_t>* rowid, uint16_t from, uint16_t to);

    void _init_column_predicates();

    void _init_context();

    template <bool late_materialization>
    void _build_context(ScanContext* ctx);

    void _rewrite_predicates();

    bool _rewrite_predicate(const FieldPtr& field);

    Status _decode_dict_codes(ScanContext* ctx);

    void _check_low_cardinality_optimization();

    Status _finish_late_materialization(ScanContext* ctx);

    void _switch_context(ScanContext* to);

    // `_check_low_cardinality_optimization` and `_init_column_iterators` must have been called
    // before you calling this method, otherwise the result is incorrect.
    bool _can_using_dict_code(const FieldPtr& field) const;

    Status _init_bitmap_index_iterators();

    Status _apply_bitmap_index();

    Status _read(Chunk* chunk, vector<rowid_t>* rowid, size_t n);

private:
    std::shared_ptr<Segment> _segment;
    vectorized::SegmentReadOptions _opts;
    std::vector<ColumnIterator*> _column_iterators;
    std::vector<BitmapIndexIterator*> _bitmap_index_iterators;

    DelVectorPtr _del_vec;
    rowid_t _chunk_rowid_start = 0;
    roaring_uint32_iterator_t _roaring_iter;

    // block for file to read
    std::unique_ptr<fs::ReadableBlock> _rblock;

    SparseRange _scan_range;
    SparseRangeIterator _range_iter;

    std::vector<const ColumnPredicate*> _vectorized_preds;
    std::vector<const ColumnPredicate*> _branchless_preds;
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

    bool _inited = false;
    bool _has_bitmap_index = false;
};

SegmentIterator::SegmentIterator(std::shared_ptr<Segment> segment, vectorized::Schema schema,
                                 vectorized::SegmentReadOptions options)
        : ChunkIterator(std::move(schema), options.chunk_size),
          _segment(std::move(segment)),
          _opts(std::move(options)),
          _scan_range(),
          _predicate_columns(_opts.predicates.size()) {}

Status SegmentIterator::_init() {
    SCOPED_RAW_TIMER(&_opts.stats->segment_init_ns);
    if (_opts.is_primary_keys && _opts.version > 0) {
        TabletSegmentId tsid;
        tsid.tablet_id = _opts.tablet_id;
        tsid.segment_id = _opts.rowset_id + segment_id();
        RETURN_IF_ERROR(
                StorageEngine::instance()->update_manager()->get_del_vec(_opts.meta, tsid, _opts.version, &_del_vec));
        if (_del_vec && _del_vec->empty()) {
            _del_vec.reset();
        }
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

    _selection.resize(_opts.chunk_size);
    _selected_idx.resize(_opts.chunk_size);
    CurrentMemTracker::consume(_selection.capacity() * sizeof(_selection[0]));
    CurrentMemTracker::consume(_selected_idx.capacity() * sizeof(_selected_idx[0]));
    StarRocksMetrics::instance()->segment_read_total.increment(1);
    // get file handle from file descriptor of segment
    RETURN_IF_ERROR(_opts.block_mgr->open_block(_segment->file_name(), &_rblock));

    /// the calling order matters, do not change unless you know why.

    _check_low_cardinality_optimization();
    RETURN_IF_ERROR(_init_column_iterators(_schema));
    RETURN_IF_ERROR(_init_bitmap_index_iterators());
    RETURN_IF_ERROR(_get_row_ranges_by_keys());
    RETURN_IF_ERROR(_apply_bitmap_index());
    RETURN_IF_ERROR(_get_row_ranges_by_zone_map());
    RETURN_IF_ERROR(_get_row_ranges_by_bloom_filter());
    _rewrite_predicates();
    _init_context();
    _init_column_predicates();
    _range_iter = _scan_range.new_iterator();

    return Status::OK();
}

Status SegmentIterator::_init_column_iterators(const Schema& schema) {
    DCHECK_EQ(_predicate_columns, _opts.predicates.size());
    const size_t n = 1 + ChunkHelper::max_column_id(schema);
    _column_iterators.resize(n, nullptr);
    bool has_predicate = !_opts.predicates.empty();
    _predicate_need_rewrite.resize(n, false);
    for (const FieldPtr& f : schema.fields()) {
        const ColumnId cid = f->id();
        if (_column_iterators[cid] == nullptr) {
            bool check_dict_enc;
            if (_opts.predicates.count(cid)) {
                check_dict_enc = _predicate_need_rewrite[cid];
            } else {
                check_dict_enc = has_predicate;
            }

            RETURN_IF_ERROR(_segment->new_column_iterator(cid, &_column_iterators[cid]));

            _obj_pool.add(_column_iterators[cid]);
            ColumnIteratorOptions iter_opts;
            iter_opts.stats = _opts.stats;
            iter_opts.use_page_cache = _opts.use_page_cache;
            iter_opts.rblock = _rblock.get();
            iter_opts.check_dict_encoding = check_dict_enc;
            RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts));
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
            if (pred->can_vectorized()) {
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

    if (_opts.ranges.empty()) {
        _scan_range.add(Range(0, num_rows()));
        return Status::OK();
    }
    DCHECK_EQ(0, _scan_range.span_size());
    RETURN_IF_ERROR(_segment->_load_index());
    for (const SeekRange& range : _opts.ranges) {
        rowid_t lower_rowid = 0;
        rowid_t upper_rowid = num_rows();

        if (!range.upper().empty()) {
            _init_column_iterators(range.lower().schema());
            RETURN_IF_ERROR(_lookup_ordinal(range.upper(), !range.inclusive_upper(), num_rows(), &upper_rowid));
        }
        if (!range.lower().empty() && upper_rowid > 0) {
            _init_column_iterators(range.lower().schema());
            RETURN_IF_ERROR(_lookup_ordinal(range.lower(), range.inclusive_lower(), upper_rowid, &lower_rowid));
        }
        if (lower_rowid <= upper_rowid) {
            _scan_range.add(Range{lower_rowid, upper_rowid});
        }
    }
    _opts.stats->rows_key_range_filtered += num_rows() - _scan_range.span_size();
    StarRocksMetrics::instance()->segment_rows_by_short_key.increment(_scan_range.span_size());
    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_by_zone_map() {
    SparseRange zm_range(0, num_rows());

    // -------------------------------------------------------------
    // group delete predicates by column id.
    // -------------------------------------------------------------
    // e.g, if there are two (disjunctive) delete predicates:
    // `c1=1 and c2=100` and `c1=100 and c2=200`, the group result
    // will be a mapping of `c1` to predicate `c1=1 or c1=100` and a
    // mapping of `c2` to predicate `c2=100 or c2=200`.
    std::set<ColumnId> columns;
    std::map<ColumnId, ColumnOrPredicate> del_predicates;
    _opts.delete_predicates.get_column_ids(&columns);
    for (ColumnId cid : columns) {
        std::vector<const ColumnPredicate*> preds;
        for (size_t i = 0; i < _opts.delete_predicates.size(); i++) {
            _opts.delete_predicates[i].predicates_of_column(cid, &preds);
        }
        DCHECK(!preds.empty());
        del_predicates.insert({cid, ColumnOrPredicate(get_type_info(preds[0]->type_info()), cid, preds)});
    }

    // -------------------------------------------------------------
    // prune data pages by zone map index.
    // -------------------------------------------------------------
    for (const auto& pair : _opts.predicates) {
        columns.insert(pair.first);
    }

    std::vector<const ColumnPredicate*> query_preds;
    for (ColumnId cid : columns) {
        auto iter1 = _opts.predicates.find(cid);
        if (iter1 != _opts.predicates.end()) {
            query_preds = iter1->second;
        } else {
            query_preds.clear();
        }

        const ColumnPredicate* del_pred;
        auto iter = del_predicates.find(cid);
        del_pred = iter != del_predicates.end() ? &(iter->second) : nullptr;
        SparseRange r;
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

inline Status SegmentIterator::_read(Chunk* chunk, vector<rowid_t>* rowid, size_t n) {
    Range r = _range_iter.next(n);
    size_t nread = r.span_size();
    if (_cur_rowid != r.begin() || _cur_rowid == 0) {
        _cur_rowid = r.begin();
        _opts.stats->block_seek_num += 1;
        SCOPED_RAW_TIMER(&_opts.stats->block_seek_ns);
        RETURN_IF_ERROR(_context->seek_columns(_cur_rowid));
    }
    {
        _opts.stats->blocks_load += 1;
        SCOPED_RAW_TIMER(&_opts.stats->block_fetch_ns);
        RETURN_IF_ERROR(_context->read_columns(chunk, nread));
    }
    _chunk_rowid_start = _cur_rowid;
    if (rowid != nullptr) {
        for (uint32_t i = _cur_rowid; i < _cur_rowid + nread; i++) {
            rowid->push_back(i);
        }
    }
    _cur_rowid += nread;
    _opts.stats->raw_rows_read += nread;
    chunk->check_or_die();
    return Status::OK();
}

Status SegmentIterator::do_get_next(Chunk* chunk) {
    if (!_inited) {
        RETURN_IF_ERROR(_init());
        _inited = true;
    }

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

    const uint32_t chunk_capacity = _opts.chunk_size;
    const int64_t prev_raw_read = _opts.stats->raw_rows_read;
    const bool has_predicate = !_opts.predicates.empty();
    uint16_t chunk_start = 0;
    bool need_switch_context = false;

    CurrentMemTracker::release(_context->memory_usage());
    _context->_read_chunk->reset();
    _context->_dict_chunk->reset();
    _context->_final_chunk->reset();

    int64_t old_mem_usage = _context->memory_usage();
    int64_t curr_mem_usage = old_mem_usage; // Will be updated later.
    CurrentMemTracker::consume(old_mem_usage);

    Chunk* chunk = _context->_read_chunk.get();

    while ((chunk_start < chunk_capacity) & _range_iter.has_more()) {
        RETURN_IF_ERROR(_read(chunk, rowid, chunk_capacity - chunk_start));
        chunk->check_or_die();
        size_t next_start = chunk->num_rows();

        if (has_predicate || _del_vec) {
            next_start = _filter(chunk, rowid, chunk_start, next_start);
            chunk->check_or_die();
        }
        chunk_start = next_start;
        DCHECK_EQ(chunk_start, chunk->num_rows());
    }

    _opts.stats->block_load_ns += sw.elapsed_time();

    int64_t total_read = _opts.stats->raw_rows_read - prev_raw_read;

    DCHECK_EQ(chunk_start, chunk->num_rows());
    if (UNLIKELY(chunk_start == 0)) {
        // Return directly if chunk_start is zero, i.e, chunk is empty.
        // Otherwise, chunk will be swapped with result, which is incorrect
        // because the chunk is a pointer to _read_chunk instead of _final_chunk.
        curr_mem_usage = _context->memory_usage();
        CurrentMemTracker::consume(curr_mem_usage - old_mem_usage);
        return Status::EndOfFile("no more data in segment");
    }

    if (_context->_has_dict_column & (chunk_start > 0)) {
        chunk = _context->_dict_chunk.get();
        SCOPED_RAW_TIMER(&_opts.stats->decode_dict_ns);
        RETURN_IF_ERROR(_decode_dict_codes(_context));
    }
    if (_context->_late_materialize) {
        chunk = _context->_final_chunk.get();
        SCOPED_RAW_TIMER(&_opts.stats->late_materialize_ns);
        RETURN_IF_ERROR(_finish_late_materialization(_context));
        if (_context->_next != nullptr && (chunk_start * 1000 > total_read * _late_materialization_ratio)) {
            need_switch_context = true;
        }
    } else if (_context->_next != nullptr && _context_switch_count < 3 &&
               chunk_start * 1000 <= total_read * _late_materialization_ratio) {
        need_switch_context = true;
        _context_switch_count++;
    }

    // remove (logical) deleted rows.
    if (chunk->num_rows() > 0 && chunk->delete_state() != DEL_NOT_SATISFIED && !_opts.delete_predicates.empty()) {
        SCOPED_RAW_TIMER(&_opts.stats->del_filter_ns);
        size_t old_sz = chunk->num_rows();
        _opts.delete_predicates.evaluate(chunk, _selection.data());
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

    curr_mem_usage = _context->memory_usage();
    CurrentMemTracker::consume(curr_mem_usage - old_mem_usage);

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
        to->_read_chunk = ChunkHelper::new_chunk(to->_read_schema, _opts.chunk_size);
        CurrentMemTracker::consume(to->_read_chunk->memory_usage());
    }

    if (to->_has_dict_column) {
        if (to->_dict_chunk == nullptr) {
            to->_dict_chunk = ChunkHelper::new_chunk(to->_dict_decode_schema, _opts.chunk_size);
            CurrentMemTracker::consume(to->_dict_chunk->memory_usage());
        }
    } else {
        to->_dict_chunk = to->_read_chunk;
    }

    if (to->_late_materialize) {
        if (to->_final_chunk == nullptr) {
            to->_final_chunk = ChunkHelper::new_chunk(this->_schema, _opts.chunk_size);
            CurrentMemTracker::consume(to->_final_chunk->memory_usage());
        }
    } else {
        to->_final_chunk = to->_dict_chunk;
    }

    _context = to;
}

uint16_t SegmentIterator::_filter(Chunk* chunk, vector<rowid_t>* rowid, uint16_t from, uint16_t to) {
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
        SCOPED_RAW_TIMER(&_opts.stats->vec_cond_evaluate_ns);

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
            selected_size = pred->evaluate_branchless(c.get(), _selected_idx.data(), selected_size);
        }

        memset(&_selection[from], 0, to - from);
        for (uint16_t i = 0; i < selected_size; ++i) {
            _selection[_selected_idx[i]] = 1;
        }
    }

    int64_t del_vec_filtered = 0;
    if (_del_vec) {
        if (_vectorized_preds.empty() && _branchless_preds.empty()) {
            // setup selection vector
            memset(_selection.data() + from, 1, to - from);
        }
        // TODO: filter del_vec early if most of the rows are deleted
        uint32_t& cur_value = _roaring_iter.current_value;
        while (_roaring_iter.has_value && cur_value < _cur_rowid) {
            if (cur_value >= _chunk_rowid_start) {
                // valid delete
                auto& del = _selection[cur_value - _chunk_rowid_start + from];
                del_vec_filtered += del;
                del = 0;
            }
            roaring_advance_uint32_iterator(&_roaring_iter);
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
    _opts.stats->rows_del_vec_filtered += del_vec_filtered;
    _opts.stats->rows_vec_cond_filtered += (to - chunk_size) - del_vec_filtered;
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

template <bool late_materialization>
void SegmentIterator::_build_context(ScanContext* ctx) {
    const size_t predicate_count = _predicate_columns;
    const size_t num_fields = _schema.num_fields();

    const size_t ctx_fields = late_materialization ? predicate_count + 1 : num_fields;
    const size_t early_materialize_fields = late_materialization ? predicate_count : num_fields;

    ctx->_read_schema.reserve(ctx_fields);
    ctx->_dict_decode_schema.reserve(ctx_fields);
    ctx->_is_dict_column.reserve(ctx_fields);
    ctx->_column_iterators.reserve(ctx_fields);

    for (size_t i = 0; i < early_materialize_fields; i++) {
        const FieldPtr& f = _schema.field(i);
        const ColumnId cid = f->id();
        if (_can_using_dict_code(f)) {
            // create FixedLengthColumn<int64_t> for saving dict codewords.
            const std::string& name = f->name();
            auto f2 = std::make_shared<Field>(cid, name, kDictCodeType, -1, -1, f->is_nullable());
            auto* iter = new DictCodeColumnIterator(cid, _column_iterators[cid]);
            _obj_pool.add(iter);
            ctx->_read_schema.append(f2);
            ctx->_column_iterators.emplace_back(iter);
            ctx->_is_dict_column.emplace_back(true);
            ctx->_has_dict_column = true;
        } else {
            ctx->_read_schema.append(f);
            ctx->_column_iterators.emplace_back(_column_iterators[cid]);
            ctx->_is_dict_column.emplace_back(false);
        }
        ctx->_dict_decode_schema.append(f);
    }
    if (late_materialization && predicate_count < _schema.num_fields()) {
        // ordinal column
        ColumnId cid = _schema.field(predicate_count)->id();
        static_assert(std::is_same_v<rowid_t, TypeTraits<OLAP_FIELD_TYPE_UNSIGNED_INT>::CppType>);
        auto f = std::make_shared<Field>(cid, "ordinal", OLAP_FIELD_TYPE_UNSIGNED_INT, -1, -1, false);
        RowIdColumnIterator* iter = new RowIdColumnIterator();
        _obj_pool.add(iter);
        ctx->_read_schema.append(f);
        ctx->_dict_decode_schema.append(f);
        ctx->_column_iterators.emplace_back(iter);
        ctx->_is_dict_column.emplace_back(false);
        ctx->_late_materialize = true;
    }
}

void SegmentIterator::_init_context() {
    DCHECK_EQ(_predicate_columns, _opts.predicates.size());
    _late_materialization_ratio = config::late_materialization_ratio;

    if (_predicate_columns == 0 || _predicate_columns >= _schema.num_fields()) {
        // non or all field has predicate, disable late materialization.
        _build_context<false>(&_context_list[0]);
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
            _build_context<false>(&_context_list[0]);
        } else if (_late_materialization_ratio < 1000) {
            // late materialization based on condition filter ratio.
            _build_context<true>(&_context_list[0]);
            _build_context<false>(&_context_list[1]);
            _context_list[0]._next = &_context_list[1];
            _context_list[1]._next = &_context_list[0];
        } else {
            // always use late materialization strategy.
            _build_context<true>(&_context_list[0]);
        }
    }
    _switch_context(&_context_list[0]);
}

void SegmentIterator::_rewrite_predicates() {
    for (size_t i = 0; i < _predicate_columns; i++) {
        const FieldPtr& field = _schema.field(i);
        ColumnId cid = field->id();
        _predicate_need_rewrite[cid] = _predicate_need_rewrite[cid] && _rewrite_predicate(field);
    }
}

bool SegmentIterator::_rewrite_predicate(const FieldPtr& field) {
    ColumnId cid = field->id();
    DCHECK(_column_iterators[cid]->all_page_dict_encoded());

    auto iter = _opts.predicates.find(cid);
    RETURN_IF(iter == _opts.predicates.end(), false);

    PredicateList& preds = iter->second;
    // the predicate has been erased, because of bitmap index filter.
    RETURN_IF(preds.empty(), false);
    const ColumnPredicate* pred = preds[0];
    if (PredicateType::kEQ == pred->type()) {
        Datum value = pred->value();
        int code = _column_iterators[cid]->dict_lookup(value.get_slice());
        if (code < 0) {
            // predicate always false, clear scan range, this will make `get_next` return EOF directly.
            _scan_range = _scan_range.intersection(SparseRange());
            return false;
        }
        auto ptr = new_column_eq_predicate(get_type_info(kDictCodeType), cid, std::to_string(code));
        preds[0] = _obj_pool.add(ptr);
        return true;
    }
    if (PredicateType::kNE == pred->type()) {
        Datum value = pred->value();
        int code = _column_iterators[cid]->dict_lookup(value.get_slice());
        if (code < 0) {
            if (!field->is_nullable()) {
                // predicate always true, clear this predicate.
                preds.clear();
                return false;
            } else {
                // convert this predicate to `not null` predicate.
                auto ptr = new_column_null_predicate(get_type_info(OLAP_FIELD_TYPE_VARCHAR), cid, false);
                preds[0] = _obj_pool.add(ptr);
                return false; // disable low cardinality optimization.
            }
        }
        auto ptr = new_column_ne_predicate(get_type_info(kDictCodeType), cid, std::to_string(code));
        preds[0] = _obj_pool.add(ptr);
        return true;
    }
    if (PredicateType::kInList == pred->type()) {
        std::vector<Datum> values = pred->values();
        std::vector<int> codewords;
        for (const auto& value : values) {
            if (int code = _column_iterators[cid]->dict_lookup(value.get_slice()); code >= 0) {
                codewords.emplace_back(code);
            }
        }
        if (codewords.empty()) {
            // predicate always false, clear scan range.
            _scan_range = _scan_range.intersection(SparseRange());
            return false;
        }
        std::vector<std::string> str_codewords;
        str_codewords.reserve(codewords.size());
        for (int code : codewords) {
            str_codewords.emplace_back(std::to_string(code));
        }
        auto ptr = new_column_in_predicate(get_type_info(kDictCodeType), cid, str_codewords);
        preds[0] = _obj_pool.add(ptr);
        return true;
    }
    if (PredicateType::kNotInList == pred->type()) {
        std::vector<Datum> values = pred->values();
        std::vector<int> codewords;
        for (const auto& value : values) {
            if (int code = _column_iterators[cid]->dict_lookup(value.get_slice()); code >= 0) {
                codewords.emplace_back(code);
            }
        }
        if (codewords.empty()) {
            if (!field->is_nullable()) {
                // predicate always true, clear this predicate.
                preds.clear();
                return false;
            } else {
                // convert this predicate to `not null` predicate.
                auto ptr = new_column_null_predicate(get_type_info(OLAP_FIELD_TYPE_VARCHAR), cid, false);
                preds[0] = _obj_pool.add(ptr);
                return false; // disable low cardinality optimization.
            }
        }
        std::vector<std::string> str_codewords;
        str_codewords.reserve(codewords.size());
        for (int code : codewords) {
            str_codewords.emplace_back(std::to_string(code));
        }
        auto ptr = new_column_not_in_predicate(get_type_info(kDictCodeType), cid, str_codewords);
        preds[0] = _obj_pool.add(ptr);
        return true;
    }
    return false;
}

Status SegmentIterator::_decode_dict_codes(ScanContext* ctx) {
    DCHECK_NE(ctx->_read_chunk, ctx->_dict_chunk);
    const Schema& decode_schema = ctx->_dict_decode_schema;
    const size_t n = decode_schema.num_fields();
    bool may_has_del_row = ctx->_read_chunk->delete_state() != DEL_NOT_SATISFIED;
    for (size_t i = 0; i < n; i++) {
        const FieldPtr& f = decode_schema.field(i);
        const ColumnId cid = f->id();
        if (!ctx->_is_dict_column[i]) {
            ctx->_dict_chunk->get_column_by_index(i)->swap_column(*ctx->_read_chunk->get_column_by_index(i));
        } else {
            ColumnPtr& dict_codes = ctx->_read_chunk->get_column_by_index(i);
            ColumnPtr& dict_values = ctx->_dict_chunk->get_column_by_index(i);
            dict_values->resize(0);

            RETURN_IF_ERROR(_column_iterators[cid]->decode_dict_codes(*dict_codes, dict_values.get()));
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
    return Status::OK();
}

void SegmentIterator::_check_low_cardinality_optimization() {
    DCHECK_EQ(_predicate_columns, _opts.predicates.size());
    _predicate_need_rewrite.resize(1 + ChunkHelper::max_column_id(_schema), false);
    const size_t n = _opts.predicates.size();
    for (size_t i = 0; i < n; i++) {
        const FieldPtr& field = _schema.field(i);
        const FieldType type = field->type()->type();
        if (type != OLAP_FIELD_TYPE_CHAR && type != OLAP_FIELD_TYPE_VARCHAR) {
            continue;
        }
        ColumnId cid = field->id();
        auto iter = _opts.predicates.find(cid);
        DCHECK(iter != _opts.predicates.end());
        const PredicateList& preds = iter->second;
        if (preds.size() != 1) {
            continue;
        }
        _predicate_need_rewrite[cid] =
                preds[0]->type() == PredicateType::kEQ || preds[0]->type() == PredicateType::kInList ||
                preds[0]->type() == PredicateType::kNE || preds[0]->type() == PredicateType::kNotInList;
    }
}

Status SegmentIterator::_finish_late_materialization(ScanContext* ctx) {
    const size_t m = ctx->_read_schema.num_fields();

    bool may_has_del_row = ctx->_dict_chunk->delete_state() != DEL_NOT_SATISFIED;

    // last column of |_dict_chunk| is a fake column: it's filled by `RowIdColumnIterator`.
    ColumnPtr rowid_column = ctx->_dict_chunk->get_column_by_index(m - 1);
    const auto* ordinals = down_cast<FixedLengthColumn<rowid_t>*>(rowid_column.get());

    for (size_t i = 0; i < m - 1; i++) {
        ctx->_final_chunk->get_column_by_index(i)->swap_column(*ctx->_dict_chunk->get_column_by_index(i));
    }

    const size_t n = _schema.num_fields();
    for (size_t i = m - 1; i < n; i++) {
        const FieldPtr& f = _schema.field(i);
        const ColumnId cid = f->id();
        ColumnPtr& col = ctx->_final_chunk->get_column_by_index(i);
        col->reserve(ordinals->size());
        col->resize(0);

        RETURN_IF_ERROR(_column_iterators[cid]->fetch_values_by_rowid(*ordinals, col.get()));
        DCHECK_EQ(ordinals->size(), col->size());
        may_has_del_row |= (col->delete_state() != DEL_NOT_SATISFIED);
    }
    ctx->_final_chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    ctx->_final_chunk->check_or_die();
    return Status::OK();
}

Status SegmentIterator::_init_bitmap_index_iterators() {
    DCHECK_EQ(_predicate_columns, _opts.predicates.size());
    _bitmap_index_iterators.resize(ChunkHelper::max_column_id(_schema) + 1, nullptr);
    for (const auto& pair : _opts.predicates) {
        ColumnId cid = pair.first;
        if (_bitmap_index_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_bitmap_index_iterator(cid, &_bitmap_index_iterators[cid]));
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
    std::vector<SparseRange> bitmap_ranges;
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
        SparseRange selected(0, cardinality);
        bool has_is_null = false;
        for (const ColumnPredicate* pred : pred_list) {
            SparseRange r;
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

Status SegmentIterator::_get_row_ranges_by_bloom_filter() {
    RETURN_IF(_opts.predicates.empty(), Status::OK());
    size_t prev_size = _scan_range.span_size();
    for (const auto& [cid, preds] : _opts.predicates) {
        ColumnIterator* column_iter = _column_iterators[cid];
        RETURN_IF_ERROR(column_iter->get_row_ranges_by_bloom_filter(preds, &_scan_range));
    }
    _opts.stats->rows_bf_filtered += (prev_size - _scan_range.span_size());
    return Status::OK();
}

void SegmentIterator::close() {
    _context_list[0].close();
    _context_list[1].close();
    _obj_pool.clear();
    _rblock.reset();
    _segment.reset();

    CurrentMemTracker::release(_selection.capacity() * sizeof(_selection[0]));
    CurrentMemTracker::release(_selected_idx.capacity() * sizeof(_selected_idx[0]));
    STLClearObject(&_selection);
    STLClearObject(&_selected_idx);

    for (auto* iter : _bitmap_index_iterators) {
        delete iter;
    }
}

// put the field that has predicate on it ahead of those without one, for handle late
// materialization easier.
inline Schema reorder_schema(const Schema& input, const std::unordered_map<ColumnId, PredicateList>& predicates) {
    const std::vector<FieldPtr>& fields = input.fields();

    Schema output;
    output.reserve(fields.size());
    for (size_t i = 0; i < fields.size(); i++) {
        if (predicates.count(fields[i]->id())) {
            output.append(fields[i]);
        }
    }
    for (size_t i = 0; i < fields.size(); i++) {
        if (!predicates.count(fields[i]->id())) {
            output.append(fields[i]);
        }
    }
    return output;
}

ChunkIteratorPtr new_segment_iterator(const std::shared_ptr<segment_v2::Segment>& segment,
                                      const vectorized::Schema& schema, const vectorized::SegmentReadOptions& options) {
    if (options.predicates.empty() || options.predicates.size() >= schema.num_fields()) {
        return std::make_shared<SegmentIterator>(segment, schema, options);
    } else {
        Schema ordered_schema = reorder_schema(schema, options.predicates);
        auto seg_iter = std::make_shared<SegmentIterator>(segment, ordered_schema, options);
        return new_projection_iterator(schema, seg_iter);
    }
}

} // namespace starrocks::vectorized
