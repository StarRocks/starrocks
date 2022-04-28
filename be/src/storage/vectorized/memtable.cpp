// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/vectorized/memtable.h"

#include <memory>

#include "column/json_column.h"
#include "column/type_traits.h"
#include "common/logging.h"
#include "runtime/current_thread.h"
#include "runtime/primitive_type_infra.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/schema.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/orlp/pdqsort.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"

namespace starrocks::vectorized {

// TODO(cbl): move to common space latter
static const string LOAD_OP_COLUMN = "__op";
static const size_t kPrimaryKeyLimitSize = 128;

MemTable::MemTable(int64_t tablet_id, const TabletSchema* tablet_schema, const std::vector<SlotDescriptor*>* slot_descs,
                   RowsetWriter* rowset_writer, MemTracker* mem_tracker)
        : _tablet_id(tablet_id),
          _tablet_schema(tablet_schema),
          _slot_descs(slot_descs),
          _keys_type(tablet_schema->keys_type()),
          _rowset_writer(rowset_writer),
          _aggregator(nullptr),
          _mem_tracker(mem_tracker) {
    _vectorized_schema = std::move(ChunkHelper::convert_schema_to_format_v2(*tablet_schema));
    if (_keys_type == KeysType::PRIMARY_KEYS && _slot_descs->back()->col_name() == LOAD_OP_COLUMN) {
        // load slots have __op field, so add to _vectorized_schema
        auto op_column = std::make_shared<starrocks::vectorized::Field>((ColumnId)-1, LOAD_OP_COLUMN,
                                                                        FieldType::OLAP_FIELD_TYPE_TINYINT, false);
        op_column->set_aggregate_method(OLAP_FIELD_AGGREGATION_REPLACE);
        _vectorized_schema.append(op_column);
        _has_op_slot = true;
    }

    if (_keys_type != KeysType::DUP_KEYS) {
        // The ChunkAggregator used by MemTable may be used to aggregate into a large Chunk,
        // which is not suitable for obtaining Chunk from ColumnPool,
        // otherwise it will take up a lot of memory and may not be released.
        _aggregator = std::make_unique<ChunkAggregator>(&_vectorized_schema, 0, INT_MAX, 0);
    }
}

MemTable::MemTable(int64_t tablet_id, const Schema& schema, RowsetWriter* rowset_writer, int64_t max_buffer_size,
                   MemTracker* mem_tracker)
        : _tablet_id(tablet_id),
          _vectorized_schema(std::move(schema)),
          _tablet_schema(nullptr),
          _slot_descs(nullptr),
          _keys_type(schema.keys_type()),
          _rowset_writer(rowset_writer),
          _aggregator(nullptr),
          _use_slot_desc(false),
          _max_buffer_size(max_buffer_size),
          _mem_tracker(mem_tracker) {
    if (_keys_type != KeysType::DUP_KEYS) {
        // The ChunkAggregator used by MemTable may be used to aggregate into a large Chunk,
        // which is not suitable for obtaining Chunk from ColumnPool,
        // otherwise it will take up a lot of memory and may not be released.
        _aggregator = std::make_unique<ChunkAggregator>(&_vectorized_schema, 0, INT_MAX, 0);
    }
}

MemTable::~MemTable() = default;

size_t MemTable::memory_usage() const {
    size_t size = 0;

    // used for sort
    size += sizeof(PermutationItem) * _permutations.size();
    size += sizeof(uint32_t) * _selective_values.size();

    // _result_chunk is the final result before flush
    if (_result_chunk != nullptr && _result_chunk->num_rows() > 0) {
        size += _result_chunk->memory_usage();
    }

    // _aggregator_memory_usage is 0 if keys type is DUP_KEYS
    return size + _chunk_memory_usage + _aggregator_memory_usage;
}

size_t MemTable::write_buffer_size() const {
    if (_chunk == nullptr) {
        return 0;
    }

    // _aggregator_bytes_usage is 0 if keys type is DUP_KEYS
    return _chunk_bytes_usage + _aggregator_bytes_usage;
}

bool MemTable::is_full() const {
    return write_buffer_size() >= _max_buffer_size;
}

bool MemTable::insert(const Chunk& chunk, const uint32_t* indexes, uint32_t from, uint32_t size) {
    if (_chunk == nullptr) {
        _chunk = ChunkHelper::new_chunk(_vectorized_schema, 0);
    }

    size_t cur_row_count = _chunk->num_rows();
    if (_use_slot_desc) {
        // For schema change, FE will construct a shadow column.
        // The shadow column is not exist in _vectorized_schema
        // So the chunk can only be accessed by the subscript
        // instead of the column name.
        for (int i = 0; i < _slot_descs->size(); ++i) {
            const ColumnPtr& src = chunk.get_column_by_slot_id((*_slot_descs)[i]->id());
            ColumnPtr& dest = _chunk->get_column_by_index(i);
            dest->append_selective(*src, indexes, from, size);
        }
    } else {
        for (int i = 0; i < _vectorized_schema.num_fields(); i++) {
            const ColumnPtr& src = chunk.get_column_by_index(i);
            ColumnPtr& dest = _chunk->get_column_by_index(i);
            dest->append_selective(*src, indexes, from, size);
        }
    }

    if (chunk.has_rows()) {
        _chunk_memory_usage += chunk.memory_usage() * size / chunk.num_rows();
        _chunk_bytes_usage += _chunk->bytes_usage(cur_row_count, size);
    }

    // if memtable is full, push it to the flush executor,
    // and create a new memtable for incoming data
    bool suggest_flush = false;
    if (is_full()) {
        size_t orig_bytes = write_buffer_size();
        _merge();
        size_t new_bytes = write_buffer_size();
        if (new_bytes > orig_bytes * 2 / 3 && _merge_count <= 1) {
            // this means aggregate doesn't remove enough duplicate rows,
            // keep inserting into the buffer will cause additional sort&merge,
            // the cost of extra sort&merge is greater than extra flush IO,
            // so flush is suggested even buffer is not full
            suggest_flush = true;
        }
    }
    if (is_full()) {
        suggest_flush = true;
    }

    return suggest_flush;
}

Status MemTable::finalize() {
    if (_chunk == nullptr) {
        return Status::OK();
    }

    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);

        if (_keys_type != DUP_KEYS) {
            if (_chunk->num_rows() > 0) {
                // merge last undo merge
                _merge();
            }

            if (_merge_count > 1) {
                _chunk = _aggregator->aggregate_result();
                _aggregator->aggregate_reset();

                int64_t t1 = MonotonicMicros();
                _sort(true);
                int64_t t2 = MonotonicMicros();
                _aggregate(true);
                int64_t t3 = MonotonicMicros();
                VLOG(1) << Substitute("memtable final sort:$0 agg:$1 total:$2", t2 - t1, t3 - t2, t3 - t1);
            } else {
                // if there is only one data chunk and merge once,
                // no need to perform an additional merge.
                _chunk.reset();
                _result_chunk.reset();
            }
            _chunk_memory_usage = 0;
            _chunk_bytes_usage = 0;

            _result_chunk = _aggregator->aggregate_result();
            if (_keys_type == PRIMARY_KEYS &&
                PrimaryKeyEncoder::encode_exceed_limit(_vectorized_schema, *_result_chunk.get(), 0,
                                                       _result_chunk->num_rows(), kPrimaryKeyLimitSize)) {
                _aggregator.reset();
                _aggregator_memory_usage = 0;
                _aggregator_bytes_usage = 0;
                return Status::Cancelled("primary key size exceed the limit.");
            }
            if (_has_op_slot) {
                // TODO(cbl): mem_tracker
                ChunkPtr upserts;
                RETURN_IF_ERROR(_split_upserts_deletes(_result_chunk, &upserts, &_deletes));
                if (_result_chunk != upserts) {
                    _result_chunk = upserts;
                }
            }
            _aggregator.reset();
            _aggregator_memory_usage = 0;
            _aggregator_bytes_usage = 0;
        } else {
            _sort(true);
        }
    }

    StarRocksMetrics::instance()->memtable_flush_duration_us.increment(duration_ns / 1000);
    return Status::OK();
}

Status MemTable::flush() {
    if (UNLIKELY(_result_chunk == nullptr)) {
        return Status::OK();
    }
    std::string msg;
    if (_result_chunk->reach_capacity_limit(&msg)) {
        return Status::InternalError(
                fmt::format("memtable of tablet {} reache the capacity limit, detail msg: {}", _tablet_id, msg));
    }
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        if (_deletes) {
            RETURN_IF_ERROR(_rowset_writer->flush_chunk_with_deletes(*_result_chunk, *_deletes));
        } else {
            RETURN_IF_ERROR(_rowset_writer->flush_chunk(*_result_chunk));
        }
    }
    StarRocksMetrics::instance()->memtable_flush_total.increment(1);
    StarRocksMetrics::instance()->memtable_flush_duration_us.increment(duration_ns / 1000);
    VLOG(1) << "memtable flush: " << duration_ns / 1000 << "us";
    return Status::OK();
}

void MemTable::_merge() {
    if (_chunk == nullptr || _keys_type == KeysType::DUP_KEYS) {
        return;
    }

    int64_t t1 = MonotonicMicros();
    _sort(false);
    int64_t t2 = MonotonicMicros();
    _aggregate(false);
    int64_t t3 = MonotonicMicros();
    VLOG(1) << Substitute("memtable sort:$0 agg:$1 total:$2", t2 - t1, t3 - t2, t3 - t1);
    ++_merge_count;
}

void MemTable::_aggregate(bool is_final) {
    if (_result_chunk == nullptr || _result_chunk->num_rows() <= 0) {
        return;
    }

    DCHECK(_result_chunk->num_rows() < INT_MAX);
    DCHECK(_aggregator->source_exhausted());

    _aggregator->update_source(_result_chunk);

    DCHECK(_aggregator->is_do_aggregate());

    _aggregator->aggregate();
    _aggregator_memory_usage = _aggregator->memory_usage();
    _aggregator_bytes_usage = _aggregator->bytes_usage();

    // impossible finish
    DCHECK(!_aggregator->is_finish());
    DCHECK(_aggregator->source_exhausted());

    if (is_final) {
        _result_chunk.reset();
    } else {
        _result_chunk->reset();
    }
}

void MemTable::_sort(bool is_final) {
    _permutations.resize(_chunk->num_rows());
    for (uint32_t i = 0; i < _chunk->num_rows(); ++i) {
        _permutations[i] = {i, i};
    }
    if (_vectorized_schema.num_key_fields() <= 3 && _use_slot_desc) {
        _sort_chunk_by_columns();
    } else {
        _sort_chunk_by_rows();
    }
    if (is_final) {
        // No need to reserve, it will be reserve in IColumn::append_selective(),
        // Otherwise it will use more peak memory
        _result_chunk = _chunk->clone_empty_with_schema(0);
        _append_to_sorted_chunk<true>(_chunk.get(), _result_chunk.get());
        _chunk.reset();
    } else {
        _result_chunk = _chunk->clone_empty_with_schema();
        _append_to_sorted_chunk<false>(_chunk.get(), _result_chunk.get());
        _chunk->reset();
    }
    _chunk_memory_usage = 0;
    _chunk_bytes_usage = 0;
}

template <bool is_final>
void MemTable::_append_to_sorted_chunk(Chunk* src, Chunk* dest) {
    _selective_values.clear();
    _selective_values.reserve(src->num_rows());
    for (size_t i = 0; i < src->num_rows(); ++i) {
        _selective_values.push_back(_permutations[i].index_in_chunk);
    }
    if constexpr (is_final) {
        dest->rolling_append_selective(*src, _selective_values.data(), 0, src->num_rows());
    } else {
        dest->append_selective(*src, _selective_values.data(), 0, src->num_rows());
    }
}

Status MemTable::_split_upserts_deletes(ChunkPtr& src, ChunkPtr* upserts, std::unique_ptr<Column>* deletes) {
    size_t op_column_id = src->num_columns() - 1;
    auto op_column = src->get_column_by_index(op_column_id);
    src->remove_column_by_index(op_column_id);
    size_t nrows = src->num_rows();
    auto* ops = reinterpret_cast<const uint8_t*>(op_column->raw_data());
    size_t ndel = 0;
    for (size_t i = 0; i < nrows; i++) {
        ndel += (ops[i] == TOpType::DELETE);
    }
    size_t nupsert = nrows - ndel;
    if (ndel == 0) {
        // no deletes, short path
        *upserts = src;
        return Status::OK();
    }
    vector<uint32_t> indexes[2];
    indexes[TOpType::UPSERT].reserve(nupsert);
    indexes[TOpType::DELETE].reserve(ndel);
    for (uint32_t i = 0; i < nrows; i++) {
        // ops == 0: upsert  otherwise: delete
        indexes[ops[i] == TOpType::UPSERT ? TOpType::UPSERT : TOpType::DELETE].push_back(i);
    }
    *upserts = src->clone_empty_with_schema(nupsert);
    (*upserts)->append_selective(*src, indexes[TOpType::UPSERT].data(), 0, nupsert);
    if (!*deletes) {
        auto st = PrimaryKeyEncoder::create_column(_vectorized_schema, deletes);
        if (!st.ok()) {
            LOG(ERROR) << "create column for primary key encoder failed, schema:" << _vectorized_schema
                       << ", status:" << st.to_string();
            return st;
        }
    }
    (*deletes)->reset_column();
    auto& delidx = indexes[TOpType::DELETE];
    PrimaryKeyEncoder::encode_selective(_vectorized_schema, *src, delidx.data(), delidx.size(), deletes->get());
    return Status::OK();
}

// SortHelper functions only work for full sort.
class SortHelper {
public:
    // Sort on type-known column, and the column has no NULL value in sorting range.
    template <typename ColumnTypeName, typename CppTypeName>
    static void sort_on_not_null_column(Column* column, MemTable::Permutation* perm) {
        sort_on_not_null_column_within_range<ColumnTypeName, CppTypeName>(column, perm, 0, perm->size());
    }

    // Sort on type-known column, and the column may have NULL values in the sorting range.
    template <typename ColumnTypeName, typename CppTypeName>
    static void sort_on_nullable_column(Column* column, MemTable::Permutation* perm) {
        auto* nullable_col = down_cast<NullableColumn*>(column);

        auto null_first_fn = [&nullable_col](const MemTable::PermutationItem& item) -> bool {
            return nullable_col->is_null(item.index_in_chunk);
        };

        size_t data_offset = 0, data_count = 0;
        // separate null and non-null values
        // put all NULLs at the begin of the permutation.
        auto begin_of_not_null = std::stable_partition(perm->begin(), perm->end(), null_first_fn);
        data_offset = begin_of_not_null - perm->begin();
        if (data_offset < perm->size()) {
            data_count = perm->size() - data_offset;
        } else {
            return;
        }
        // sort non-null values
        sort_on_not_null_column_within_range<ColumnTypeName, CppTypeName>(nullable_col->mutable_data_column(), perm,
                                                                          data_offset, data_count);
    }

private:
    // Sort on type-known column, and the column has no NULL value in sorting range.
    template <typename ColumnTypeName, typename CppTypeName>
    static void sort_on_not_null_column_within_range(Column* column, MemTable::Permutation* perm, size_t offset,
                                                     size_t count = 0) {
        // for numeric column: integers, floats, date and datetime
        if constexpr (std::is_arithmetic_v<CppTypeName> || IsDate<CppTypeName> || IsDateTime<CppTypeName>) {
            sort_on_not_null_numeric_column_within_range<CppTypeName>(column, perm, offset, count);
            return;
        }
        // for binary column
        if constexpr (std::is_same_v<CppTypeName, RunTimeTypeTraits<TYPE_VARCHAR>::CppType>) {
            auto* col = down_cast<RunTimeTypeTraits<TYPE_VARCHAR>::ColumnType*>(column);
            sort_on_not_null_binary_column_within_range(col, perm, offset, count);
            return;
        }
        // for decimal column
        if constexpr (std::is_same_v<CppTypeName, RunTimeTypeTraits<TYPE_DECIMALV2>::CppType>) {
            auto* col = down_cast<RunTimeTypeTraits<TYPE_DECIMALV2>::ColumnType*>(column);
            sort_on_not_null_decimal_column_within_range(col, perm, offset, count);
            return;
        }

        // for other columns
        const ColumnTypeName* col = down_cast<ColumnTypeName*>(column);
        auto less_fn = [&col](const MemTable::PermutationItem& l, const MemTable::PermutationItem& r) -> bool {
            int c = col->compare_at(l.index_in_chunk, r.index_in_chunk, *col, 1);
            if (c == 0) {
                return l.permutation_index < r.permutation_index;
            } else {
                return c < 0;
            }
        };
        size_t end_pos = (count == 0 ? perm->size() : offset + count);
        if (end_pos > perm->size()) {
            end_pos = perm->size();
        }
        pdqsort(false, perm->begin() + offset, perm->begin() + end_pos, less_fn);
    }

    template <typename CppTypeName>
    struct SortItem {
        CppTypeName value;
        uint32_t index_in_chunk;
        uint32_t permutation_index; // sequence index for keeping sort stable.
    };

    // Sort on some numeric column which has no NULL value in sorting range.
    // Only supports: integers, floats. Not Slice, DecimalV2Value.
    template <typename CppTypeName>
    static void sort_on_not_null_numeric_column_within_range(Column* column, MemTable::Permutation* perm, size_t offset,
                                                             size_t count = 0) {
        // column->size() == perm.size()
        const size_t row_num = (count == 0 || offset + count > perm->size()) ? (perm->size() - offset) : count;
        const CppTypeName* data = static_cast<CppTypeName*>((void*)column->mutable_raw_data());
        std::vector<SortItem<CppTypeName>> sort_items(row_num);
        for (uint32_t i = 0; i < row_num; ++i) {
            sort_items[i] = {data[(*perm)[i + offset].index_in_chunk], (*perm)[i + offset].index_in_chunk, i};
        }

        auto less_fn = [](const SortItem<CppTypeName>& l, const SortItem<CppTypeName>& r) -> bool {
            if (l.value == r.value) {
                return l.permutation_index < r.permutation_index; // for stable sort
            } else {
                return l.value < r.value;
            }
        };

        pdqsort(false, sort_items.begin(), sort_items.end(), less_fn);

        // output permutation
        for (size_t i = 0; i < row_num; ++i) {
            (*perm)[i + offset].index_in_chunk = sort_items[i].index_in_chunk;
        }
    }

    static void sort_on_not_null_binary_column_within_range(Column* column, MemTable::Permutation* perm, size_t offset,
                                                            size_t count = 0) {
        const size_t row_num = (count == 0 || offset + count > perm->size()) ? (perm->size() - offset) : count;
        auto* binary_column = reinterpret_cast<BinaryColumn*>(column);
        auto& data = binary_column->get_data();
        std::vector<SortItem<Slice>> sort_items(row_num);
        for (uint32_t i = 0; i < row_num; ++i) {
            sort_items[i] = {data[(*perm)[i + offset].index_in_chunk], (*perm)[i + offset].index_in_chunk, i};
        }
        auto less_fn = [](const SortItem<Slice>& l, const SortItem<Slice>& r) -> bool {
            int res = l.value.compare(r.value);
            if (res == 0) {
                return l.permutation_index < r.permutation_index;
            } else {
                return res < 0;
            }
        };

        pdqsort(false, sort_items.begin(), sort_items.end(), less_fn);

        for (size_t i = 0; i < row_num; ++i) {
            (*perm)[i + offset].index_in_chunk = sort_items[i].index_in_chunk;
        }
    }

    static void sort_on_not_null_decimal_column_within_range(RunTimeTypeTraits<TYPE_DECIMALV2>::ColumnType* column,
                                                             MemTable::Permutation* perm, size_t offset,
                                                             size_t count = 0) {
        // avoid to call FixedLengthColumn::compare_at
        const auto& container = column->get_data();
        auto less_fn = [&container](const MemTable::PermutationItem& l, const MemTable::PermutationItem& r) -> bool {
            const auto& lv = container[l.index_in_chunk];
            const auto& rv = container[r.index_in_chunk];
            if (lv == rv) {
                return l.permutation_index < r.permutation_index;
            } else {
                return lv < rv;
            }
        };
        size_t end_pos = (count == 0 ? perm->size() : offset + count);
        if (end_pos > perm->size()) {
            end_pos = perm->size();
        }
        pdqsort(false, perm->begin() + offset, perm->begin() + end_pos, less_fn);
    }
};

void MemTable::_sort_chunk_by_columns() {
    for (int i = _vectorized_schema.num_key_fields() - 1; i >= 0; --i) {
        Column* column = _chunk->get_column_by_index(i).get();
        PrimitiveType slot_type = (*_slot_descs)[i]->type().type;
        if (column->is_nullable()) {
            switch (slot_type) {
#define M(ptype)                                                                                                      \
    case ptype: {                                                                                                     \
        SortHelper::sort_on_nullable_column<RunTimeColumnType<ptype>, RunTimeCppType<ptype>>(column, &_permutations); \
        break;                                                                                                        \
    }
                APPLY_FOR_ALL_SCALAR_TYPE(M)
#undef M

            default: {
                CHECK(false) << "This type couldn't be key column";
                break;
            }
            }
        } else {
            switch (slot_type) {
#define M(ptype)                                                                                                      \
    case ptype: {                                                                                                     \
        SortHelper::sort_on_not_null_column<RunTimeColumnType<ptype>, RunTimeCppType<ptype>>(column, &_permutations); \
        break;                                                                                                        \
    }
                APPLY_FOR_ALL_SCALAR_TYPE(M)
#undef M
            default: {
                CHECK(false) << "This type couldn't be key column";
                break;
            }
            }
        }
        // reset permutation_index
        const size_t size = _permutations.size();
        for (size_t j = 0; j < size; ++j) {
            _permutations[j].permutation_index = j;
        }
    }
}

void MemTable::_sort_chunk_by_rows() {
    pdqsort(false, _permutations.begin(), _permutations.end(),
            [this](const MemTable::PermutationItem& l, const MemTable::PermutationItem& r) {
                size_t col_number = _vectorized_schema.num_key_fields();
                int compare_result = 0;
                for (size_t col_index = 0; col_index < col_number; ++col_index) {
                    const auto& left_col = _chunk->get_column_by_index(col_index);
                    compare_result = left_col->compare_at(l.index_in_chunk, r.index_in_chunk, *left_col, -1);
                    if (compare_result != 0) {
                        return compare_result < 0;
                    }
                }
                return l.permutation_index < r.permutation_index;
            });
}

} // namespace starrocks::vectorized
