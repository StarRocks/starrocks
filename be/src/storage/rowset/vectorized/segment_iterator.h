// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>
#include <vector>

#include "storage/del_vector.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/vectorized/segment_options.h"
#include "storage/vectorized/chunk_iterator.h"

namespace starrocks {
class ColumnDecoder;
}

namespace starrocks::vectorized {

/// SegmentIterator
// TODO(zhuming): Refine the implementation of this class to reduce the intellectual overhead.
// Too many policies encapsulated in this class, should split this class into many small classes.
class SegmentIterator final : public ChunkIterator {
public:
    SegmentIterator(std::shared_ptr<Segment> segment, vectorized::Schema _schema,
                    vectorized::SegmentReadOptions options);

    ~SegmentIterator() override = default;

    void close() override;

    ColumnIterator* get_column_iterator(std::size_t idx);

protected:
    Status do_get_next(Chunk* chunk) override;
    Status do_get_next(Chunk* chunk, vector<uint32_t>* rowid) override;

private:
    struct ScanContext {
        ScanContext() {}

        ~ScanContext() = default;

        void close() {
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

        Status read_columns(Chunk* chunk, const vectorized::SparseRange& range) {
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

        // true iff |_is_dict_column| contains at least one `true`, i.e,
        // |_column_iterators| contains at least one `DictCodeColumnIterator`.
        bool _has_dict_column{false};

        // if true, the last item of |_column_iterators| is a `RowIdColumnIterator` and
        // the last item of |_read_schema| and |_dict_decode_schema| is a row id field.
        bool _late_materialize{false};
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
    uint16_t _filter_by_expr_predicates(Chunk* chunk, vector<rowid_t>* rowid);

    void _init_column_predicates();

    Status _init_context();

    template <bool late_materialization>
    Status _build_context(ScanContext* ctx);

    Status _init_global_dict_decoder();

    void _rewrite_predicates();

    Status _decode_dict_codes(ScanContext* ctx);

    Status _check_low_cardinality_optimization();

    Status _finish_late_materialization(ScanContext* ctx);

    Status _build_final_chunk(ScanContext* ctx);

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

    Status _read_by_column(size_t n, Chunk* result, vector<rowid_t>* rowids);

private:
    std::shared_ptr<Segment> _segment;
    vectorized::SegmentReadOptions _opts;
    std::vector<ColumnIterator*> _column_iterators;
    std::vector<ColumnDecoder> _column_decoders;
    std::vector<BitmapIndexIterator*> _bitmap_index_iterators;

    DelVectorPtr _del_vec;
    roaring_uint32_iterator_t _roaring_iter;

    // block for file to read
    std::unique_ptr<fs::ReadableBlock> _rblock;

    SparseRange _scan_range;
    SparseRangeIterator _range_iter;

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

    bool _inited = false;
    bool _has_bitmap_index = false;
};

ChunkIteratorPtr new_segment_iterator(const std::shared_ptr<Segment>& segment, const vectorized::Schema& schema,
                                      const SegmentReadOptions& options);

} // namespace starrocks::vectorized
