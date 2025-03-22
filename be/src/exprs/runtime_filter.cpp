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

#include "exprs/runtime_filter.h"

#include "types/logical_type_infra.h"
#include "util/compression/stream_compression.h"

namespace starrocks {

// ------------------------------------------------------------------------------------
// SimdBlockFilter
// ------------------------------------------------------------------------------------

void SimdBlockFilter::init(size_t nums) {
    nums = std::max(MINIMUM_ELEMENT_NUM, nums);
    int log_heap_space = std::ceil(std::log2(nums));
    _log_num_buckets = std::max(1, log_heap_space - LOG_BUCKET_BYTE_SIZE);
    _directory_mask = (1ull << std::min(63, _log_num_buckets)) - 1;
    const size_t alloc_size = get_alloc_size();
    const int malloc_failed = posix_memalign(reinterpret_cast<void**>(&_directory), 64, alloc_size);
    if (malloc_failed) throw ::std::bad_alloc();
    memset(_directory, 0, alloc_size);
}

SimdBlockFilter::SimdBlockFilter(SimdBlockFilter&& bf) noexcept {
    _log_num_buckets = bf._log_num_buckets;
    _directory_mask = bf._directory_mask;
    _directory = bf._directory;
    bf._directory = nullptr;
}

size_t SimdBlockFilter::max_serialized_size() const {
    const size_t alloc_size = _directory == nullptr ? 0 : get_alloc_size();
    return sizeof(_log_num_buckets) + sizeof(_directory_mask) + // data size + max data size
           sizeof(int32_t) + alloc_size;
}

size_t SimdBlockFilter::serialize(uint8_t* data) const {
    size_t offset = 0;
#define SIMD_BF_COPY_FIELD(field)                 \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);
    SIMD_BF_COPY_FIELD(_log_num_buckets);
    SIMD_BF_COPY_FIELD(_directory_mask);

    const size_t alloc_size = get_alloc_size();
    int32_t data_size = alloc_size;
    SIMD_BF_COPY_FIELD(data_size);
    if (LIKELY(data_size > 0)) {
        memcpy(data + offset, _directory, data_size);
        offset += data_size;
    }
    return offset;
#undef SIMD_BF_COPY_FIELD
}

size_t SimdBlockFilter::deserialize(const uint8_t* data) {
    size_t offset = 0;
    int32_t data_size = 0;

#define SIMD_BF_COPY_FIELD(field)                 \
    memcpy(&field, data + offset, sizeof(field)); \
    offset += sizeof(field);
    SIMD_BF_COPY_FIELD(_log_num_buckets);
    SIMD_BF_COPY_FIELD(_directory_mask);
    SIMD_BF_COPY_FIELD(data_size);
#undef SIMD_BF_COPY_FIELD
    const size_t alloc_size = get_alloc_size();
    DCHECK(data_size == alloc_size);
    if (LIKELY(data_size > 0)) {
        const int malloc_failed = posix_memalign(reinterpret_cast<void**>(&(_directory)), 64, alloc_size);
        if (malloc_failed) throw ::std::bad_alloc();
        memcpy(_directory, data + offset, data_size);
        offset += data_size;
    }
    return offset;
}

void SimdBlockFilter::merge(const SimdBlockFilter& bf) {
    if (_directory == nullptr || bf._directory == nullptr) {
        return;
    }
    DCHECK(_log_num_buckets == bf._log_num_buckets);
    for (int i = 0; i < (1 << _log_num_buckets); i++) {
#ifdef __AVX2__
        auto* const dst = reinterpret_cast<__m256i*>(_directory[i]);
        auto* const src = reinterpret_cast<__m256i*>(bf._directory[i]);
        const __m256i a = _mm256_load_si256(src);
        const __m256i b = _mm256_load_si256(dst);
        const __m256i c = _mm256_or_si256(a, b);
        _mm256_store_si256(dst, c);
#else
        for (int j = 0; j < BITS_SET_PER_BLOCK; j++) {
            _directory[i][j] |= bf._directory[i][j];
        }
#endif
    }
}

// For scalar version:

void SimdBlockFilter::make_mask(uint32_t key, uint32_t* masks) const {
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        // add some salt to key
        masks[i] = key * SALT[i];
        // masks[i] mod 32
        masks[i] = masks[i] >> 27;
        // set the masks[i]-th bit
        masks[i] = 0x1 << masks[i];
    }
}

bool SimdBlockFilter::check_equal(const SimdBlockFilter& bf) const {
    const size_t alloc_size = get_alloc_size();
    return _log_num_buckets == bf._log_num_buckets && _directory_mask == bf._directory_mask &&
           memcmp(_directory, bf._directory, alloc_size) == 0;
}

void SimdBlockFilter::clear() {
    if (_directory) {
        free(_directory);
        _directory = nullptr;
        _log_num_buckets = 0;
        _directory_mask = 0;
    }
}

// ------------------------------------------------------------------------------------
// RuntimeBloomFilter
// ------------------------------------------------------------------------------------

template <LogicalType Type>
size_t TRuntimeBloomFilter<Type>::max_serialized_size() const {
    // todo(yan): noted that it's not serialize compatible with 32-bit and 64-bit.
    auto num_partitions = _hash_partition_bf.size();
    size_t size = sizeof(_has_null) + sizeof(_size) + sizeof(num_partitions) + sizeof(_join_mode);
    if (num_partitions == 0) {
        size += _bf.max_serialized_size();
    } else {
        for (const auto& bf : _hash_partition_bf) {
            size += bf.max_serialized_size();
        }
    }
    return size;
}
template <LogicalType Type>
size_t TRuntimeBloomFilter<Type>::serialize(int serialize_version, uint8_t* data) const {
    size_t offset = 0;
    auto num_partitions = _hash_partition_bf.size();

#define JRF_COPY_FIELD(field)                     \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);

    JRF_COPY_FIELD(_has_null);
    JRF_COPY_FIELD(_size);
    JRF_COPY_FIELD(num_partitions);
    JRF_COPY_FIELD(_join_mode);
#undef JRF_COPY_FIELD

    if (num_partitions == 0) {
        offset += _bf.serialize(data + offset);
    } else {
        for (const auto& bf : _hash_partition_bf) {
            offset += bf.serialize(data + offset);
        }
    }
    return offset;
}
template <LogicalType Type>
size_t TRuntimeBloomFilter<Type>::deserialize(int serialize_version, const uint8_t* data) {
    size_t offset = 0;
    size_t num_partitions = 0;

#define JRF_COPY_FIELD(field)                     \
    memcpy(&field, data + offset, sizeof(field)); \
    offset += sizeof(field);

    JRF_COPY_FIELD(_has_null);
    JRF_COPY_FIELD(_size);
    JRF_COPY_FIELD(num_partitions);
    JRF_COPY_FIELD(_join_mode);
#undef JRF_COPY_FIELD

    if (num_partitions == 0) {
        offset += _bf.deserialize(data + offset);
    } else {
        for (size_t i = 0; i < num_partitions; i++) {
            SimdBlockFilter bf;
            offset += bf.deserialize(data + offset);
            _hash_partition_bf.emplace_back(std::move(bf));
        }
    }

    return offset;
}

template <LogicalType Type>
void TRuntimeBloomFilter<Type>::clear_bf() {
    if (_hash_partition_bf.empty()) {
        _bf.clear();
    } else {
        for (size_t i = 0; i < _hash_partition_bf.size(); i++) {
            _hash_partition_bf[i].clear();
        }
    }
    _size = 0;
}

// ------------------------------------------------------------------------------------
// RuntimeEmptyFilter
// ------------------------------------------------------------------------------------

template <LogicalType LT>
std::string RuntimeEmptyFilter<LT>::debug_string() const {
    std::stringstream ss;
    ss << "RuntimeEmptyFilter("
       << "has_null=" << _has_null     //
       << ", join_mode=" << _join_mode //
       << ", num_elements=" << _size   //
       << ")";
    return ss.str();
}

template <LogicalType LT>
size_t RuntimeEmptyFilter<LT>::max_serialized_size() const {
    return sizeof(_has_null) + // 1. has_null
           sizeof(_size) +     // 2. num_elements
           sizeof(_join_mode); // 3. join_mode
}

template <LogicalType LT>
size_t RuntimeEmptyFilter<LT>::serialize(int serialize_version, uint8_t* data) const {
    DCHECK(serialize_version != RF_VERSION);

    size_t offset = 0;

#define JRF_COPY_FIELD(field)                     \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);

    JRF_COPY_FIELD(_has_null);  // 1. has_null
    JRF_COPY_FIELD(_size);      // 2. num_elements
    JRF_COPY_FIELD(_join_mode); // 3. join_mode

#undef JRF_COPY_FIELD

    return offset;
}

template <LogicalType LT>
size_t RuntimeEmptyFilter<LT>::deserialize(int serialize_version, const uint8_t* data) {
    DCHECK(serialize_version != RF_VERSION);

    size_t offset = 0;

#define JRF_COPY_FIELD(field)                     \
    memcpy(&field, data + offset, sizeof(field)); \
    offset += sizeof(field);

    JRF_COPY_FIELD(_has_null);  // 1. has_null
    JRF_COPY_FIELD(_size);      // 2. num_elements
    JRF_COPY_FIELD(_join_mode); // 3. join_mode

#undef JRF_COPY_FIELD

    return offset;
}

// ------------------------------------------------------------------------------------
// Bitset
// ------------------------------------------------------------------------------------

template <LogicalType LT>
void Bitset<LT>::insert(const CppType& value) {
    const uint64_t bucket = _value_interval(value);
    const uint64_t group = bucket / 8;
    const uint64_t index_in_group = bucket % 8;
    _bitset[group] |= (1 << index_in_group);
}

template <LogicalType LT>
bool Bitset<LT>::contains_range(const CppType& min_value, const CppType& max_value) const {
    if (min_value > _max_value || max_value < _min_value || min_value > max_value) {
        return false;
    }

    // `min_value < _min_value <= max_value` always contains _min_value.
    // `min_value <= _max_value < max_value` always contains _max_value.
    if (min_value < _min_value || max_value > _max_value) {
        return true;
    }

    // _min_value <= min_value <= max_value <= _max_value
    //
    // 1. Check [min_index_in_group, 7] in min_group.
    // 2. Check [0, max_index_in_group] in max_group.
    // 3. Check [min_group + 1, max_group - 1] group.
    //
    // min_group  max_group
    // |              |
    // ┌──┬──┬──┬──┬──┐
    // └┬─┴──┴──┴──┴─┬┘
    //  └────────────┘
    //  |            |
    // min_index  max_index
    // _in_group  _in_group

    const size_t min_bucket = _value_interval(min_value);
    const uint64_t min_group = min_bucket / 8;
    const uint64_t min_index_in_group = min_bucket % 8;
    // Clear low `min_index_in_group` bits.
    const uint8_t min_group_mask = _bitset[min_group] & (0xFFull << min_index_in_group);

    const size_t max_bucket = _value_interval(max_value);
    const uint64_t max_group = max_bucket / 8;
    const uint64_t max_index_in_group = max_bucket % 8;
    // Clear high `max_index_in_group` bits.
    const uint8_t max_group_mask = _bitset[max_group] & (0xFFull >> (7 - max_index_in_group));

    if (min_group == max_group) {
        if (min_group_mask & max_group_mask) {
            return true;
        }
    } else {
        if (min_group_mask | max_group_mask) {
            return true;
        }
    }

    if (min_group + 1 <= max_group - 1) {
        return SIMD::count_nonzero(_bitset.data() + min_group + 1, max_group - min_group - 1);
    }
    return false;
}

template <LogicalType LT>
void Bitset<LT>::init() {
    const size_t key_interval = value_interval() + 1;
    const size_t num_buckets = (key_interval + 7) / 8;
    _bitset.resize(num_buckets);
}

template <LogicalType LT>
template <bool CheckRange>
void Bitset<LT>::contains_batch(uint8_t* __restrict selection, const CppType* __restrict values, size_t from,
                                size_t to) const {
    for (size_t i = from; i < to; i++) {
        selection[i] = contains<CheckRange>(values[i], selection[i]);
    }
}

template <LogicalType LT>
template <bool CheckRange, bool NullIsTrue>
void Bitset<LT>::contains_batch(uint8_t* __restrict selection, const CppType* __restrict values,
                                const uint8_t* __restrict is_nulls, size_t from, size_t to) const {
    // `contains<false/*CheckRange*/>` considers the values[i] with true `selection[i]` is in the range, which is set
    // by the min-max-filter, and doesn't check range again.
    // However, min-max-filter will also set `selection[i]` to true, if NullIsTrue and `is_nulls[i]` is true regardless
    // of the value of `values[i]`.
    // Therefore, we need to temporarily set `selection[i]` to false and re-consider `is_nulls[i]` later, if NullIsTrue
    // and `is_nulls[i]` is true.
    for (int i = from; i < to; i++) {
        selection[i] &= is_nulls[i] == 0;
    }
    for (int i = from; i < to; i++) {
        selection[i] = contains<CheckRange>(values[i], selection[i]);
    }

    if constexpr (NullIsTrue) {
        for (int i = from; i < to; i++) {
            selection[i] |= is_nulls[i] != 0;
        }
    }
}

// ------------------------------------------------------------------------------------
// RuntimeBitsetFilter
// ------------------------------------------------------------------------------------

template <LogicalType LT>
void RuntimeBitsetFilter<LT>::init(size_t num_elements) {
    RuntimeMembershipFilter::init(num_elements);
    _bitset.init();
}

template <LogicalType LT>
void RuntimeBitsetFilter<LT>::evaluate(const Column* input_column, RunningContext* ctx) const {
    const size_t num_rows = input_column->size();

    Filter& selection_filter = ctx->use_merged_selection ? ctx->merged_selection : ctx->selection;
    selection_filter.resize(num_rows);
    uint8_t* selection = selection_filter.data();

    if (_has_null) {
        _evaluate_vectorized<true>(input_column, selection, 0, num_rows);
    } else {
        _evaluate_vectorized<false>(input_column, selection, 0, num_rows);
    }
}

template <LogicalType LT>
void RuntimeBitsetFilter<LT>::evaluate(const Column* input_column, const std::vector<uint32_t>& hash_values,
                                       uint8_t* selection, uint16_t from, uint16_t to) const {
    if (_has_null) {
        _evaluate_vectorized<true>(input_column, selection, from, to);
    } else {
        _evaluate_vectorized<false>(input_column, selection, from, to);
    }
}

template <LogicalType LT>
uint16_t RuntimeBitsetFilter<LT>::evaluate(const Column* input_column, const std::vector<uint32_t>& hash_values,
                                           uint16_t* sel, uint16_t sel_size, uint16_t* dst_sel) const {
    if (_has_null) {
        return _evaluate_branchless<true>(input_column, hash_values, sel, sel_size, dst_sel);
    } else {
        return _evaluate_branchless<false>(input_column, hash_values, sel, sel_size, dst_sel);
    }
}

template <LogicalType LT>
template <bool null_is_true>
void RuntimeBitsetFilter<LT>::_evaluate_vectorized(const Column* __restrict input_column, uint8_t* __restrict selection,
                                                   size_t from, size_t to) const {
    if (input_column->is_constant()) {
        const auto* const_column = down_cast<const ConstColumn*>(input_column);
        if (const_column->only_null()) {
            memset(selection + from, _has_null, to - from);
        } else {
            const auto& values = GetContainer<LT>::get_data(const_column->data_column());
            const bool selected = _test_data(values[0], selection[0]);
            memset(selection + from, selected, to - from);
        }
    } else if (input_column->is_nullable()) {
        const auto* nullable_column = down_cast<const NullableColumn*>(input_column);
        const auto* values = GetContainer<LT>::get_data(nullable_column->data_column()).data();
        if (nullable_column->has_null()) {
            const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
            _bitset.template contains_batch<false /*CheckRange*/, null_is_true>(selection, values, null_data, from, to);
        } else {
            _bitset.template contains_batch<false /*CheckRange*/>(selection, values, from, to);
        }
    } else {
        const auto* values = GetContainer<LT>::get_data(input_column).data();
        _bitset.template contains_batch<false /*CheckRange*/>(selection, values, from, to);
    }
}

template <LogicalType LT>
template <bool null_is_true>
uint16_t RuntimeBitsetFilter<LT>::_evaluate_branchless(const Column* __restrict input_column,
                                                       const std::vector<uint32_t>& hash_values,
                                                       uint16_t* __restrict sel, uint16_t sel_size,
                                                       uint16_t* dst_sel) const {
    uint16_t num_dst_rows = 0;
    if (input_column->is_nullable()) {
        const auto* nullable_column = down_cast<const NullableColumn*>(input_column);
        const auto& values = GetContainer<LT>::get_data(nullable_column->data_column());
        if (nullable_column->has_null()) {
            const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
            for (int i = 0; i < sel_size; i++) {
                const auto idx = sel[i];
                dst_sel[num_dst_rows] = idx;
                if constexpr (!null_is_true) {
                    num_dst_rows += _test_data(values[idx], null_data[idx] == 0);
                } else {
                    num_dst_rows += (null_data[idx] != 0) | _test_data(values[idx], null_data[idx] == 0);
                }
            }
        } else {
            for (int i = 0; i < sel_size; i++) {
                const auto idx = sel[i];
                dst_sel[num_dst_rows] = idx;
                num_dst_rows += _test_data(values[idx], true);
            }
        }
    } else {
        const auto& values = GetContainer<LT>::get_data(input_column);
        for (int i = 0; i < sel_size; i++) {
            const auto idx = sel[i];
            dst_sel[num_dst_rows] = idx;
            num_dst_rows += _test_data(values[idx], true);
        }
    }

    return num_dst_rows;
}

template <LogicalType LT>
std::string RuntimeBitsetFilter<LT>::debug_string() const {
    std::stringstream ss;
    ss << "RuntimeBitsetFilter("
       << "has_null=" << _has_null                        //
       << ", join_mode=" << _join_mode                    //
       << ", num_elements=" << _size                      //
       << ", min_value=" << _bitset.min_value()           //
       << ", max_value=" << _bitset.max_value()           //
       << ", value_interval=" << _bitset.value_interval() //
       << ", bitset_bytes=" << _bitset.size()             //
       << ")";
    return ss.str();
}

template <LogicalType LT>
size_t RuntimeBitsetFilter<LT>::max_serialized_size() const {
    return sizeof(_has_null) +              // 1. has_null
           sizeof(_size) +                  // 2. num_elements
           sizeof(_join_mode) +             // 3. join_mode
           sizeof(_bitset.min_value()) +    // 4. min_value
           sizeof(_bitset.max_value()) +    // 5. max_value
           sizeof(size_t) + _bitset.size(); // 6. bitset
}

template <LogicalType LT>
size_t RuntimeBitsetFilter<LT>::serialize(int serialize_version, uint8_t* data) const {
    DCHECK(serialize_version >= RF_VERSION_V3);

    size_t offset = 0;

#define JRF_COPY_FIELD(field)                     \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);

    JRF_COPY_FIELD(_has_null);  // 1. has_null
    JRF_COPY_FIELD(_size);      // 2. num_elements
    JRF_COPY_FIELD(_join_mode); // 3. join_mode

    const auto min_value = _bitset.min_value();
    const auto max_value = _bitset.max_value();
    JRF_COPY_FIELD(min_value); // 4. min_value
    JRF_COPY_FIELD(max_value); // 5. max_value

    // 6. bitset
    const size_t bitset_size = _bitset.size();
    JRF_COPY_FIELD(bitset_size);
    memcpy(data + offset, _bitset.data(), bitset_size);
    offset += bitset_size;

#undef JRF_COPY_FIELD

    return offset;
}

template <LogicalType LT>
size_t RuntimeBitsetFilter<LT>::deserialize(int serialize_version, const uint8_t* data) {
    DCHECK(serialize_version >= RF_VERSION_V3);

    size_t offset = 0;

#define JRF_COPY_FIELD(field)                     \
    memcpy(&field, data + offset, sizeof(field)); \
    offset += sizeof(field);

    JRF_COPY_FIELD(_has_null);  // 1. has_null
    JRF_COPY_FIELD(_size);      // 2. num_elements
    JRF_COPY_FIELD(_join_mode); // 3. join_mode

    CppType min_value;
    CppType max_value;
    JRF_COPY_FIELD(min_value); // 4. min_value
    JRF_COPY_FIELD(max_value); // 5. max_value
    _bitset.set_min_max(min_value, max_value);
    _bitset.init();

    // 6. bitset
    size_t bitset_size = 0;
    JRF_COPY_FIELD(bitset_size);
    DCHECK_EQ(_bitset.size(), bitset_size);
    memcpy(_bitset.data(), data + offset, bitset_size);
    offset += bitset_size;

#undef JRF_COPY_FIELD

    return offset;
}

// ------------------------------------------------------------------------------------
// Instantiate runtime filters.
// ------------------------------------------------------------------------------------

#define InstantiateRuntimeFilter(LT)                                  \
    template class RuntimeEmptyFilter<LT>;                            \
    template class ComposedRuntimeFilter<LT, RuntimeEmptyFilter<LT>>; \
    template class TRuntimeBloomFilter<LT>;                           \
    template class ComposedRuntimeFilter<LT, TRuntimeBloomFilter<LT>>;

APPLY_FOR_ALL_SCALAR_TYPE(InstantiateRuntimeFilter)
#undef InstantiateRuntimeFilter

#define InstantiateRuntimeBitsetFilter(LT)  \
    template class Bitset<LT>;              \
    template class RuntimeBitsetFilter<LT>; \
    template class ComposedRuntimeFilter<LT, RuntimeBitsetFilter<LT>>;

APPLY_FOR_BITSET_FILTER_SUPPORTED_TYPE(InstantiateRuntimeBitsetFilter)
#undef InstantiateRuntimeBitsetFilter

} // namespace starrocks
