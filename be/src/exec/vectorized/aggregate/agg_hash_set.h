// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column_hash.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/type_traits.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "util/fixed_hash_map.h"
#include "util/hash_util.hpp"
#include "util/phmap/phmap.h"

namespace starrocks::vectorized {

// =====================
// one level agg hash set
template <PhmapSeed seed>
using Int8AggHashSet = SmallFixedSizeHashSet<int8_t>;
template <PhmapSeed seed>
using Int16AggHashSet = phmap::flat_hash_set<int16_t, StdHashWithSeed<int16_t, seed>>;
template <PhmapSeed seed>
using Int32AggHashSet = phmap::flat_hash_set<int32_t, StdHashWithSeed<int32_t, seed>>;
template <PhmapSeed seed>
using Int64AggHashSet = phmap::flat_hash_set<int64_t, StdHashWithSeed<int64_t, seed>>;
template <PhmapSeed seed>
using Int128AggHashSet = phmap::flat_hash_set<int128_t, Hash128WithSeed<seed>>;
template <PhmapSeed seed>
using DateAggHashSet = phmap::flat_hash_set<DateValue, StdHashWithSeed<DateValue, seed>>;
template <PhmapSeed seed>
using TimeStampAggHashSet = phmap::flat_hash_set<TimestampValue, StdHashWithSeed<TimestampValue, seed>>;
template <PhmapSeed seed>
using SliceAggHashSet =
        phmap::flat_hash_set<TSliceWithHash<seed>, THashOnSliceWithHash<seed>, TEqualOnSliceWithHash<seed>>;

// ==================
// one level fixed size slice hash set
template <PhmapSeed seed>
using FixedSize4SliceAggHashSet = phmap::flat_hash_set<SliceKey4, FixedSizeSliceKeyHash<SliceKey4, seed>>;
template <PhmapSeed seed>
using FixedSize8SliceAggHashSet = phmap::flat_hash_set<SliceKey8, FixedSizeSliceKeyHash<SliceKey8, seed>>;
template <PhmapSeed seed>
using FixedSize16SliceAggHashSet = phmap::flat_hash_set<SliceKey16, FixedSizeSliceKeyHash<SliceKey16, seed>>;

// =====================
// two level agg hash set
template <PhmapSeed seed>
using Int32AggTwoLevelHashSet = phmap::parallel_flat_hash_set<int32_t, StdHashWithSeed<int32_t, seed>>;

template <PhmapSeed seed>
using SliceAggTwoLevelHashSet =
        phmap::parallel_flat_hash_set<TSliceWithHash<seed>, THashOnSliceWithHash<seed>, TEqualOnSliceWithHash<seed>,
                                      phmap::priv::Allocator<Slice>, 4>;

// ==============================================================
// handle one number hash key
template <PrimitiveType primitive_type, typename HashSet>
struct AggHashSetOfOneNumberKey {
    using KeyType = typename HashSet::key_type;
    using Iterator = typename HashSet::iterator;
    using ColumnType = RunTimeColumnType<primitive_type>;
    using ResultVector = typename ColumnType::Container;
    using FieldType = RunTimeCppType<primitive_type>;
    HashSet hash_set;
    static_assert(sizeof(FieldType) <= sizeof(KeyType), "hash set key size needs to be larger than the actual element");

    AggHashSetOfOneNumberKey(int32_t chunk_size) {}
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool) {
        DCHECK(!key_columns[0]->is_nullable());
        auto* column = down_cast<ColumnType*>(key_columns[0].get());
        const size_t row_num = column->size();
        for (size_t i = 0; i < row_num; ++i) {
            hash_set.emplace(column->get_data()[i]);
        }
    }

    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    void build_set(size_t chunk_size, const Columns& key_columns, std::vector<uint8_t>* not_founds) {
        DCHECK(!key_columns[0]->is_nullable());
        auto* column = ColumnHelper::as_raw_column<ColumnType>(key_columns[0]);
        auto& keys = column->get_data();
        not_founds->resize(chunk_size);

        for (size_t i = 0; i < chunk_size; ++i) {
            (*not_founds)[i] = !hash_set.contains(keys[i]);
        }
    }

    void insert_keys_to_columns(const ResultVector& keys, const Columns& key_columns, size_t chunk_size) {
        auto* column = down_cast<ColumnType*>(key_columns[0].get());
        column->get_data().insert(column->get_data().end(), keys.begin(), keys.begin() + chunk_size);
    }

    static constexpr bool has_single_null_key = false;
    ResultVector results;
};

template <PrimitiveType primitive_type, typename HashSet>
struct AggHashSetOfOneNullableNumberKey {
    using KeyType = typename HashSet::key_type;
    using Iterator = typename HashSet::iterator;
    using ColumnType = RunTimeColumnType<primitive_type>;
    using ResultVector = typename ColumnType::Container;
    using FieldType = RunTimeCppType<primitive_type>;
    HashSet hash_set;

    static_assert(sizeof(FieldType) <= sizeof(KeyType), "hash set key size needs to be larger than the actual element");

    AggHashSetOfOneNullableNumberKey(int32_t chunk_size) {}
    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool) {
        if (key_columns[0]->only_null()) {
            has_null_key = true;
        } else {
            DCHECK(key_columns[0]->is_nullable());
            auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
            auto* data_column = down_cast<ColumnType*>(nullable_column->data_column().get());
            const auto& null_data = nullable_column->null_column_data();

            size_t row_num = nullable_column->size();
            if (nullable_column->has_null()) {
                for (size_t i = 0; i < row_num; ++i) {
                    if (null_data[i]) {
                        has_null_key = true;
                    } else {
                        hash_set.emplace(data_column->get_data()[i]);
                    }
                }
            } else {
                for (size_t i = 0; i < row_num; ++i) {
                    hash_set.emplace(data_column->get_data()[i]);
                }
            }
        }
    }

    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    void build_set(size_t chunk_size, const Columns& key_columns, std::vector<uint8_t>* not_founds) {
        not_founds->assign(chunk_size, 0);
        if (key_columns[0]->only_null()) {
            has_null_key = true;
        } else {
            DCHECK(key_columns[0]->is_nullable());
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(key_columns[0]);
            auto* data_column = ColumnHelper::as_raw_column<ColumnType>(nullable_column->data_column());
            const auto& null_data = nullable_column->null_column_data();
            auto& keys = data_column->get_data();

            if (nullable_column->has_null()) {
                for (size_t i = 0; i < chunk_size; ++i) {
                    if (null_data[i]) {
                        has_null_key = true;
                    } else {
                        (*not_founds)[i] = !hash_set.contains(keys[i]);
                    }
                }
            } else {
                for (size_t i = 0; i < chunk_size; ++i) {
                    (*not_founds)[i] = !hash_set.contains(keys[i]);
                }
            }
        }
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, size_t chunk_size) {
        auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
        auto* column = down_cast<ColumnType*>(nullable_column->mutable_data_column());
        column->get_data().insert(column->get_data().end(), keys.begin(), keys.begin() + chunk_size);
        nullable_column->null_column_data().resize(chunk_size);
    }

    static constexpr bool has_single_null_key = true;
    bool has_null_key = false;
    ResultVector results;
};

template <typename HashSet>
struct AggHashSetOfOneStringKey {
    using Iterator = typename HashSet::iterator;
    using KeyType = typename HashSet::key_type;
    using ResultVector = typename std::vector<Slice>;
    HashSet hash_set;

    AggHashSetOfOneStringKey(int32_t chunk_size) {}

    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool) {
        DCHECK(key_columns[0]->is_binary());
        auto* column = down_cast<BinaryColumn*>(key_columns[0].get());

        size_t row_num = column->size();
        for (size_t i = 0; i < row_num; ++i) {
            auto tmp = column->get_slice(i);
            KeyType key(tmp);

            hash_set.lazy_emplace(key, [&](const auto& ctor) {
                // we must persist the slice before insert
                uint8_t* pos = pool->allocate(key.size);
                memcpy(pos, key.data, key.size);
                ctor(pos, key.size, key.hash);
            });
        }
    }

    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    void build_set(size_t chunk_size, const Columns& key_columns, std::vector<uint8_t>* not_founds) {
        DCHECK(key_columns[0]->is_binary());
        auto* column = ColumnHelper::as_raw_column<BinaryColumn>(key_columns[0]);
        not_founds->assign(chunk_size, 0);

        for (size_t i = 0; i < chunk_size; ++i) {
            auto key = column->get_slice(i);
            (*not_founds)[i] = !hash_set.contains(key);
        }
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, size_t chunk_size) {
        auto* column = down_cast<BinaryColumn*>(key_columns[0].get());
        keys.resize(chunk_size);
        column->append_strings(keys);
    }

    static constexpr bool has_single_null_key = false;
    ResultVector results;
};

template <typename HashSet>
struct AggHashSetOfOneNullableStringKey {
    using Iterator = typename HashSet::iterator;
    using KeyType = typename HashSet::key_type;
    using ResultVector = typename std::vector<Slice>;
    HashSet hash_set;

    AggHashSetOfOneNullableStringKey(int32_t chunk_size) {}

    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool) {
        if (key_columns[0]->only_null()) {
            has_null_key = true;
        } else if (key_columns[0]->is_nullable()) {
            auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
            auto* data_column = down_cast<BinaryColumn*>(nullable_column->data_column().get());
            const auto& null_data = nullable_column->null_column_data();

            size_t row_num = nullable_column->size();
            if (nullable_column->has_null()) {
                for (size_t i = 0; i < row_num; ++i) {
                    if (null_data[i]) {
                        has_null_key = true;
                    } else {
                        _handle_data_key_column(data_column, i, pool);
                    }
                }
            } else {
                for (size_t i = 0; i < row_num; ++i) {
                    _handle_data_key_column(data_column, i, pool);
                }
            }
        }
    }

    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    void build_set(size_t chunk_size, const Columns& key_columns, std::vector<uint8_t>* not_founds) {
        not_founds->assign(chunk_size, 0);
        if (key_columns[0]->only_null()) {
            has_null_key = true;
        } else {
            DCHECK(key_columns[0]->is_nullable());
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(key_columns[0]);
            auto* data_column = ColumnHelper::as_raw_column<BinaryColumn>(nullable_column->data_column());
            const auto& null_data = nullable_column->null_column_data();

            if (nullable_column->has_null()) {
                for (size_t i = 0; i < chunk_size; ++i) {
                    if (null_data[i]) {
                        has_null_key = true;
                    } else {
                        _handle_data_key_column(data_column, i, not_founds);
                    }
                }
            } else {
                for (size_t i = 0; i < chunk_size; ++i) {
                    _handle_data_key_column(data_column, i, not_founds);
                }
            }
        }
    }

    void _handle_data_key_column(BinaryColumn* data_column, size_t row, MemPool* pool) {
        auto tmp = data_column->get_slice(row);
        KeyType key(tmp);

        hash_set.lazy_emplace(key, [&](const auto& ctor) {
            uint8_t* pos = pool->allocate(key.size);
            memcpy(pos, key.data, key.size);
            ctor(pos, key.size, key.hash);
        });
    }

    void _handle_data_key_column(BinaryColumn* data_column, size_t row, std::vector<uint8_t>* not_founds) {
        auto key = data_column->get_slice(row);
        (*not_founds)[row] = !hash_set.contains(key);
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, size_t chunk_size) {
        DCHECK(key_columns[0]->is_nullable());
        auto* nullable_column = down_cast<NullableColumn*>(key_columns[0].get());
        auto* column = down_cast<BinaryColumn*>(nullable_column->mutable_data_column());
        keys.resize(chunk_size);
        column->append_strings(keys);
        nullable_column->null_column_data().resize(chunk_size);
    }

    static constexpr bool has_single_null_key = true;
    bool has_null_key = false;
    ResultVector results;
};

template <typename HashSet>
struct AggHashSetOfSerializedKey {
    using Iterator = typename HashSet::iterator;
    using ResultVector = typename std::vector<Slice>;
    using KeyType = typename HashSet::key_type;
    HashSet hash_set;

    AggHashSetOfSerializedKey(int32_t chunk_size)
            : _mem_pool(std::make_unique<MemPool>()),
              _buffer(_mem_pool->allocate(max_one_row_size * chunk_size)),
              _chunk_size(chunk_size) {}

    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool) {
        slice_sizes.assign(_chunk_size, 0);

        size_t cur_max_one_row_size = get_max_serialize_size(key_columns);
        if (UNLIKELY(cur_max_one_row_size > max_one_row_size)) {
            max_one_row_size = cur_max_one_row_size;
            _mem_pool->clear();
            // reserved extra SLICE_MEMEQUAL_OVERFLOW_PADDING bytes to prevent SIMD instructions
            // from accessing out-of-bound memory.
            _buffer = _mem_pool->allocate(max_one_row_size * _chunk_size + SLICE_MEMEQUAL_OVERFLOW_PADDING);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(_buffer, slice_sizes, chunk_size, max_one_row_size);
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            Slice tmp = {_buffer + i * max_one_row_size, slice_sizes[i]};
            KeyType key(tmp);

            hash_set.lazy_emplace(key, [&](const auto& ctor) {
                // we must persist the slice before insert
                uint8_t* pos = pool->allocate(key.size);
                memcpy(pos, key.data, key.size);
                ctor(pos, key.size, key.hash);
            });
        }
    }

    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    void build_set(size_t chunk_size, const Columns& key_columns, std::vector<uint8_t>* not_founds) {
        slice_sizes.assign(_chunk_size, 0);

        size_t cur_max_one_row_size = get_max_serialize_size(key_columns);
        if (UNLIKELY(cur_max_one_row_size > max_one_row_size)) {
            max_one_row_size = cur_max_one_row_size;
            _mem_pool->clear();
            _buffer = _mem_pool->allocate(max_one_row_size * _chunk_size);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(_buffer, slice_sizes, chunk_size, max_one_row_size);
        }

        not_founds->assign(chunk_size, 0);
        for (size_t i = 0; i < chunk_size; ++i) {
            Slice key = {_buffer + i * max_one_row_size, slice_sizes[i]};
            (*not_founds)[i] = !hash_set.contains(key);
        }
    }

    size_t get_max_serialize_size(const Columns& key_columns) {
        size_t max_size = 0;
        for (const auto& key_column : key_columns) {
            max_size += key_column->max_one_element_serialize_size();
        }
        return max_size;
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, int32_t chunk_size) {
        // When GroupBy has multiple columns, the memory is serialized by row.
        // If the length of a row is relatively long and there are multiple columns,
        // deserialization by column will cause the memory locality to deteriorate,
        // resulting in poor performance
        if (keys.size() > 0 && keys[0].size > 64) {
            // deserialize by row
            for (size_t i = 0; i < chunk_size; i++) {
                for (const auto& key_column : key_columns) {
                    keys[i].data =
                            (char*)(key_column->deserialize_and_append(reinterpret_cast<const uint8_t*>(keys[i].data)));
                }
            }
        } else {
            // deserialize by column
            for (const auto& key_column : key_columns) {
                key_column->deserialize_and_append_batch(keys, chunk_size);
            }
        }
    }

    static constexpr bool has_single_null_key = false;

    Buffer<uint32_t> slice_sizes;
    size_t max_one_row_size = 8;

    std::unique_ptr<MemPool> _mem_pool;
    uint8_t* _buffer;
    ResultVector results;

    int32_t _chunk_size;
};

template <typename HashSet>
struct AggHashSetOfSerializedKeyFixedSize {
    using Iterator = typename HashSet::iterator;
    using KeyType = typename HashSet::key_type;
    using FixedSizeSliceKey = typename HashSet::key_type;
    using ResultVector = typename std::vector<FixedSizeSliceKey>;
    HashSet hash_set;

    bool has_null_column = false;
    int fixed_byte_size = -1; // unset state
    static constexpr size_t max_fixed_size = sizeof(FixedSizeSliceKey);

    AggHashSetOfSerializedKeyFixedSize(int32_t chunk_size)
            : _mem_pool(std::make_unique<MemPool>()),
              buffer(_mem_pool->allocate(max_fixed_size * chunk_size)),
              _chunk_size(chunk_size) {
        memset(buffer, 0x0, max_fixed_size * _chunk_size);
    }

    void build_set(size_t chunk_size, const Columns& key_columns, MemPool* pool) {
        DCHECK(fixed_byte_size != -1);
        slice_sizes.assign(chunk_size, 0);

        if (has_null_column) {
            memset(buffer, 0x0, max_fixed_size * chunk_size);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, max_fixed_size);
        }

        FixedSizeSliceKey* key = reinterpret_cast<FixedSizeSliceKey*>(buffer);

        if (has_null_column) {
            for (size_t i = 0; i < chunk_size; ++i) {
                key[i].u.size = slice_sizes[i];
            }
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            hash_set.insert(key[i]);
        }
    }

    // Elements queried in HashSet will be added to HashSet
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    void build_set(size_t chunk_size, const Columns& key_columns, std::vector<uint8_t>* not_founds) {
        DCHECK(fixed_byte_size != -1);
        slice_sizes.assign(chunk_size, 0);

        if (has_null_column) {
            memset(buffer, 0x0, max_fixed_size * chunk_size);
        }

        for (const auto& key_column : key_columns) {
            key_column->serialize_batch(buffer, slice_sizes, chunk_size, max_fixed_size);
        }

        not_founds->assign(chunk_size, 0);

        FixedSizeSliceKey key;

        if (!has_null_column) {
            for (size_t i = 0; i < chunk_size; ++i) {
                memcpy(key.u.data, buffer + i * max_fixed_size, max_fixed_size);
                (*not_founds)[i] = !hash_set.contains(key);
            }
        } else {
            for (size_t i = 0; i < chunk_size; ++i) {
                memcpy(key.u.data, buffer + i * max_fixed_size, max_fixed_size);
                key.u.size = slice_sizes[i];
                (*not_founds)[i] = !hash_set.contains(key);
            }
        }
    }

    void insert_keys_to_columns(ResultVector& keys, const Columns& key_columns, int32_t chunk_size) {
        DCHECK(fixed_byte_size != -1);
        tmp_slices.reserve(chunk_size);

        if (!has_null_column) {
            for (int i = 0; i < chunk_size; i++) {
                FixedSizeSliceKey& key = keys[i];
                tmp_slices[i].data = key.u.data;
                tmp_slices[i].size = fixed_byte_size;
            }
        } else {
            for (int i = 0; i < chunk_size; i++) {
                FixedSizeSliceKey& key = keys[i];
                tmp_slices[i].data = key.u.data;
                tmp_slices[i].size = key.u.size;
            }
        }

        // deserialize by column
        for (const auto& key_column : key_columns) {
            key_column->deserialize_and_append_batch(tmp_slices, chunk_size);
        }
    }

    static constexpr bool has_single_null_key = false;

    Buffer<uint32_t> slice_sizes;
    std::unique_ptr<MemPool> _mem_pool;
    uint8_t* buffer;
    ResultVector results;
    std::vector<Slice> tmp_slices;

    int32_t _chunk_size;
};

} // namespace starrocks::vectorized
