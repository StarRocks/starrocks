// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/runtime_filter.h"

#include "util/compression/stream_compression.h"
namespace starrocks::vectorized {

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

static constexpr uint32_t SALT[8] = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
                                     0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

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

size_t JoinRuntimeFilter::max_serialized_size() const {
    // todo(yan): noted that it's not serialize compatible with 32-bit and 64-bit.
    size_t size = sizeof(_has_null) + sizeof(_size) + sizeof(_num_hash_partitions) + sizeof(_join_mode);
    if (_num_hash_partitions == 0) {
        size += _bf.max_serialized_size();
    } else {
        for (const auto& bf : _hash_partition_bf) {
            size += bf.max_serialized_size();
        }
    }
    return size;
}

size_t JoinRuntimeFilter::serialize(uint8_t* data) const {
    size_t offset = 0;
#define JRF_COPY_FIELD(field)                     \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);
    JRF_COPY_FIELD(_has_null);
    JRF_COPY_FIELD(_size);
    JRF_COPY_FIELD(_num_hash_partitions);
    JRF_COPY_FIELD(_join_mode);
#undef JRF_COPY_FIELD

    if (_num_hash_partitions == 0) {
        offset += _bf.serialize(data + offset);

    } else {
        for (const auto& bf : _hash_partition_bf) {
            offset += bf.serialize(data + offset);
        }
    }
    return offset;
}

size_t JoinRuntimeFilter::deserialize(const uint8_t* data) {
    size_t offset = 0;
#define JRF_COPY_FIELD(field)                     \
    memcpy(&field, data + offset, sizeof(field)); \
    offset += sizeof(field);
    JRF_COPY_FIELD(_has_null);
    JRF_COPY_FIELD(_size);
    JRF_COPY_FIELD(_num_hash_partitions);
    JRF_COPY_FIELD(_join_mode);
#undef JRF_COPY_FIELD

    if (_num_hash_partitions == 0) {
        offset += _bf.deserialize(data + offset);
    } else {
        for (size_t i = 0; i < _num_hash_partitions; i++) {
            SimdBlockFilter bf;
            offset += bf.deserialize(data + offset);
            _hash_partition_bf.emplace_back(std::move(bf));
        }
    }

    return offset;
}

bool JoinRuntimeFilter::check_equal(const JoinRuntimeFilter& rf) const {
    bool first = (_has_null == rf._has_null && _size == rf._size && _num_hash_partitions == rf._num_hash_partitions &&
                  _join_mode == rf._join_mode);
    if (!first) return false;
    if (_num_hash_partitions == 0) {
        if (!_bf.check_equal(rf._bf)) return false;
    } else {
        for (size_t i = 0; i < _num_hash_partitions; i++) {
            if (!_hash_partition_bf[i].check_equal(rf._hash_partition_bf[i])) {
                return false;
            }
        }
    }
    return true;
}

<<<<<<< HEAD:be/src/exprs/vectorized/runtime_filter.cpp
} // namespace starrocks::vectorized
=======
void JoinRuntimeFilter::clear_bf() {
    if (_num_hash_partitions == 0) {
        _bf.clear();
    } else {
        for (size_t i = 0; i < _num_hash_partitions; i++) {
            _hash_partition_bf[i].clear();
        }
    }
    _size = 0;
}

} // namespace starrocks
>>>>>>> 4ba3e1b6f1 ([Enhancement] limit global runtime filter size (#32909)):be/src/exprs/runtime_filter.cpp
