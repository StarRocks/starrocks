// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/chunk_aggregator.h"

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/config.h"
#include "gutil/casts.h"
#include "storage/vectorized/column_aggregate_func.h"

namespace starrocks::vectorized {

template <typename ColumnType, typename CppType>
inline void compare_row(const Column* col, uint8_t* flags) {
    const CppType* values = down_cast<const ColumnType*>(col)->get_data().data();
    const int size = col->size();

    for (int i = 1; i < size; ++i) {
        flags[i] = flags[i] & (values[i] == values[i - 1]);
    }
}

template <>
inline void compare_row<BinaryColumn, Slice>(const Column* col, uint8_t* flags) {
    const BinaryColumn* values = down_cast<const BinaryColumn*>(col);
    const int size = col->size();

    for (int i = 1; i < size; ++i) {
        flags[i] = flags[i] && (values->get_slice(i) == values->get_slice(i - 1));
    }
}

template <typename ColumnType, typename CppType>
inline void compare_row_with_null(const Column* col, uint8_t* flags) {
    const auto* nullable = down_cast<const NullableColumn*>(col);

    if (nullable->has_null()) {
        const uint8_t* nulls = nullable->null_column()->get_data().data();
        const CppType* values = down_cast<const ColumnType*>(nullable->data_column().get())->get_data().data();

        const int size = nullable->size();
        DCHECK(nulls[0] == 0 || nulls[0] == 1);
        for (int i = 1; i < size; ++i) {
            // null == null, flag & 1
            // not null = null, flag & 0
            // not null = not null, use value flag
            if (nulls[i] != nulls[i - 1]) {
                flags[i] = 0;
            } else if ((nulls[i] == 0) & (nulls[i - 1] == 0)) {
                flags[i] = flags[i] & (values[i] == values[i - 1]);
            }
        }
    } else {
        compare_row<ColumnType, CppType>(nullable->data_column().get(), flags);
    }
}

template <>
inline void compare_row_with_null<BinaryColumn, Slice>(const Column* col, uint8_t* flags) {
    const auto* nullable = down_cast<const NullableColumn*>(col);

    if (nullable->has_null()) {
        const uint8_t* nulls = nullable->null_column()->get_data().data();
        const BinaryColumn* values = down_cast<const BinaryColumn*>(nullable->data_column().get());

        const int size = nullable->size();
        DCHECK(nulls[0] == 0 || nulls[0] == 1);
        for (int i = 1; i < size; ++i) {
            // null == null, flag & 1
            // not null = null, flag & 0
            // not null = not null, use value flag
            if (nulls[i] != nulls[i - 1]) {
                flags[i] = 0;
            } else if ((nulls[i] == 0) & (nulls[i - 1] == 0)) {
                flags[i] = flags[i] && (values->get_slice(i) == values->get_slice(i - 1));
            }
        }
    } else {
        compare_row<BinaryColumn, Slice>(nullable->data_column().get(), flags);
    }
}

template <typename ColumnType, typename CppType>
inline CompareFN get_comparator(bool null_type) {
    if (null_type) {
        return &compare_row_with_null<ColumnType, CppType>;
    } else {
        return &compare_row<ColumnType, CppType>;
    }
}

ChunkAggregator::ChunkAggregator(const starrocks::vectorized::Schema* schema, uint32_t reserve_rows,
                                 uint32_t aggregate_rows, double factor)
        : _schema(schema),
          _reserve_rows(reserve_rows),
          _aggregate_rows(aggregate_rows),
          _factor(factor),
          _has_aggregate(false) {
#ifndef NDEBUG
    // ensure that the key fields are sorted by id and placed before others.
    for (size_t i = 0; i < _schema->num_key_fields(); i++) {
        CHECK(_schema->field(i)->is_key());
    }
    for (size_t i = 0; i + 1 < _schema->num_key_fields(); i++) {
        CHECK_LT(_schema->field(i)->id(), _schema->field(i + 1)->id());
    }
#endif
    _key_fields = _schema->num_key_fields();
    _num_fields = _schema->num_fields();

    _source_row = 0;
    _source_size = 0;
    _do_aggregate = true;

    _is_eq.reserve(_reserve_rows);
    _selective_index.reserve(_reserve_rows);
    _aggregate_loops.reserve(_reserve_rows);

    _column_aggregator.reserve(_num_fields);

    // comparator
    for (int i = 0; i < _key_fields; ++i) {
        _comparator.emplace_back(_choose_comparator(_schema->field(i)));
    }

    // column aggregator
    for (int i = 0; i < _key_fields; ++i) {
        _column_aggregator.emplace_back(ColumnAggregatorFactory::create_key_column_aggregator(_schema->field(i)));
    }
    for (int i = _key_fields; i < _num_fields; ++i) {
        _column_aggregator.emplace_back(ColumnAggregatorFactory::create_value_column_aggregator(_schema->field(i)));
    }

    aggregate_reset();
}

ChunkAggregator::ChunkAggregator(const starrocks::vectorized::Schema* schema, uint32_t aggregate_rows, double factor)
        : ChunkAggregator(schema, aggregate_rows, aggregate_rows, factor) {}

CompareFN ChunkAggregator::_choose_comparator(const FieldPtr& field) {
    switch (field->type()->type()) {
    case OLAP_FIELD_TYPE_TINYINT:
        return get_comparator<Int8Column, int8_t>(field->is_nullable());
    case OLAP_FIELD_TYPE_SMALLINT:
        return get_comparator<Int16Column, int16_t>(field->is_nullable());
    case OLAP_FIELD_TYPE_INT:
        return get_comparator<Int32Column, int32_t>(field->is_nullable());
    case OLAP_FIELD_TYPE_BIGINT:
        return get_comparator<Int64Column, int64_t>(field->is_nullable());
    case OLAP_FIELD_TYPE_LARGEINT:
        return get_comparator<Int128Column, int128_t>(field->is_nullable());
    case OLAP_FIELD_TYPE_BOOL:
        return get_comparator<BooleanColumn, uint8_t>(field->is_nullable());
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
        return get_comparator<BinaryColumn, Slice>(field->is_nullable());
    case OLAP_FIELD_TYPE_DECIMAL_V2:
        return get_comparator<DecimalColumn, DecimalV2Value>(field->is_nullable());
    case OLAP_FIELD_TYPE_DATE_V2:
        return get_comparator<DateColumn, DateValue>(field->is_nullable());
    case OLAP_FIELD_TYPE_TIMESTAMP:
        return get_comparator<TimestampColumn, TimestampValue>(field->is_nullable());
    case OLAP_FIELD_TYPE_DECIMAL32:
        return get_comparator<Decimal32Column, int32_t>(field->is_nullable());
    case OLAP_FIELD_TYPE_DECIMAL64:
        return get_comparator<Decimal64Column, int64_t>(field->is_nullable());
    case OLAP_FIELD_TYPE_DECIMAL128:
        return get_comparator<Decimal128Column, int128_t>(field->is_nullable());
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
    case OLAP_FIELD_TYPE_FLOAT:
    case OLAP_FIELD_TYPE_DOUBLE:
    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
    case OLAP_FIELD_TYPE_DATE:
    case OLAP_FIELD_TYPE_DATETIME:
    case OLAP_FIELD_TYPE_DECIMAL:
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_ARRAY:
    case OLAP_FIELD_TYPE_MAP:
    case OLAP_FIELD_TYPE_UNKNOWN:
    case OLAP_FIELD_TYPE_NONE:
    case OLAP_FIELD_TYPE_HLL:
    case OLAP_FIELD_TYPE_OBJECT:
    case OLAP_FIELD_TYPE_PERCENTILE:
    case OLAP_FIELD_TYPE_MAX_VALUE:
        CHECK(false) << "unhandled key column type: " << field->type()->type();
        return nullptr;
    }
    return nullptr;
}

bool ChunkAggregator::_row_equal(const Chunk* lhs, size_t m, const Chunk* rhs, size_t n) const {
    for (uint16_t i = 0; i < _key_fields; i++) {
        if (lhs->get_column_by_index(i)->compare_at(m, n, *rhs->get_column_by_index(i), -1) != 0) {
            return false;
        }
    }
    return true;
}

void ChunkAggregator::update_source(ChunkPtr& chunk) {
    _is_eq.assign(chunk->num_rows(), 1);
    _source_row = 0;
    _source_size = 0;
    _do_aggregate = true;

    size_t factor = chunk->num_rows() * _factor;
    for (int i = _key_fields - 1; i >= 0; --i) {
        _comparator[i](chunk->get_column_by_index(i).get(), _is_eq.data());

        if (_factor > 0 && SIMD::count_nonzero(_is_eq) < factor) {
            _do_aggregate = false;
            return;
        }
    }

    // update source column
    for (int j = 0; j < _num_fields; ++j) {
        _column_aggregator[j]->update_source(chunk->get_column_by_index(j));
    }
    _source_size = chunk->num_rows();

    if (_aggregate_chunk->num_rows() > 0 && _key_fields > 0) {
        _is_eq[0] = _row_equal(_aggregate_chunk.get(), _aggregate_chunk->num_rows() - 1, chunk.get(), 0);
    } else {
        _is_eq[0] = 0;
    }
    _merged_rows += SIMD::count_nonzero(_is_eq);
}

void ChunkAggregator::aggregate() {
    if (source_exhausted()) {
        return;
    }

    DCHECK(_source_row < _source_size) << "It's impossible";

    // maybe haven't new rows
    uint32_t row = _aggregate_chunk->num_rows();

    _selective_index.clear();
    _aggregate_loops.clear();

    // first key is not equal with last row in previous chunk
    bool previous_neq = !_is_eq[_source_row] && (_aggregate_chunk->num_rows() != 0);

    // same with last row
    if (_is_eq[_source_row] == 1) {
        _aggregate_loops.emplace_back(0);
    }

    // 1. Calculate the key rows selective arrays
    // 2. Calculate the value rows that can be aggregated for each key row
    uint32_t aggregate_rows = _source_row;
    for (; aggregate_rows < _source_size; ++aggregate_rows) {
        if (_is_eq[aggregate_rows] == 0) {
            if (row >= _aggregate_rows) {
                break;
            }
            ++row;
            _selective_index.emplace_back(aggregate_rows);
            _aggregate_loops.emplace_back(1);
        } else {
            _aggregate_loops[_aggregate_loops.size() - 1] += 1;
        }
    }

    // 3. Copy the selected key rows
    // 4. Aggregate the value rows
    for (int i = 0; i < _key_fields; ++i) {
        _column_aggregator[i]->aggregate_keys(_source_row, _selective_index.size(), _selective_index.data());
    }

    for (int i = _key_fields; i < _num_fields; ++i) {
        _column_aggregator[i]->aggregate_values(_source_row, _aggregate_loops.size(), _aggregate_loops.data(),
                                                previous_neq);
    }

    _source_row = aggregate_rows;
    _has_aggregate = true;
}

bool ChunkAggregator::is_finish() {
    return (_aggregate_chunk == nullptr || _aggregate_chunk->num_rows() >= _aggregate_rows);
}

void ChunkAggregator::aggregate_reset() {
    _aggregate_chunk = ChunkHelper::new_chunk(*_schema, _reserve_rows);

    for (int i = 0; i < _num_fields; ++i) {
        auto p = _aggregate_chunk->get_column_by_index(i).get();
        _column_aggregator[i]->update_aggregate(p);
    }
    _has_aggregate = false;

    _element_memory_usage = 0;
    _element_memory_usage_num_rows = 0;
    _bytes_usage = 0;
    _bytes_usage_num_rows = 0;
}

ChunkPtr ChunkAggregator::aggregate_result() {
    for (int i = 0; i < _num_fields; ++i) {
        _column_aggregator[i]->finalize();
    }
    _has_aggregate = false;
    return _aggregate_chunk;
}

size_t ChunkAggregator::memory_usage() {
    if (_aggregate_chunk == nullptr) {
        return 0;
    }

    size_t container_memory_usage = _aggregate_chunk->container_memory_usage();

    size_t num_rows = _aggregate_chunk->num_rows();
    // last column value is in aggregator before finalize,
    // the size of key columns is 1 greater that value columns,
    // so we use num_rows - 1 as chunk num rows.
    if (num_rows <= 1) {
        return container_memory_usage;
    }
    --num_rows;

    if (_element_memory_usage_num_rows == num_rows) {
        return container_memory_usage + _element_memory_usage;
    } else if (_element_memory_usage_num_rows > num_rows) {
        _element_memory_usage_num_rows = 0;
        _element_memory_usage = 0;
    }
    _element_memory_usage += _aggregate_chunk->element_memory_usage(_element_memory_usage_num_rows,
                                                                    num_rows - _element_memory_usage_num_rows);
    _element_memory_usage_num_rows = num_rows;
    return container_memory_usage + _element_memory_usage;
}

size_t ChunkAggregator::bytes_usage() {
    if (_aggregate_chunk == nullptr) {
        return 0;
    }

    size_t num_rows = _aggregate_chunk->num_rows();
    // last column value is in aggregator before finalize,
    // the size of key columns is 1 greater that value columns,
    // so we use num_rows - 1 as chunk num rows.
    if (num_rows <= 1) {
        return 0;
    }
    --num_rows;

    if (_bytes_usage_num_rows == num_rows) {
        return _bytes_usage;
    } else if (_bytes_usage_num_rows > num_rows) {
        _bytes_usage_num_rows = 0;
        _bytes_usage = 0;
    }
    _bytes_usage += _aggregate_chunk->bytes_usage(_bytes_usage_num_rows, num_rows - _bytes_usage_num_rows);
    _bytes_usage_num_rows = num_rows;
    return _bytes_usage;
}

void ChunkAggregator::close() {}

} // namespace starrocks::vectorized
