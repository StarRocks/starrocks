// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/column_hash.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {
namespace vectorized {

// Modify from https://github.com/FastFilter/fastfilter_cpp/blob/master/src/bloom/simd-block.h
// This is avx2 simd implementation for paper <<Cache-, Hash- and Space-Efficient Bloom Filters>>
class SimdBlockFilter {
public:
    // The filter is divided up into Buckets:
    static constexpr int BITS_SET_PER_BLOCK = 8;
    using Bucket = uint32_t[BITS_SET_PER_BLOCK];

    SimdBlockFilter() = default;

    ~SimdBlockFilter() noexcept { free(_directory); }

    SimdBlockFilter(const SimdBlockFilter& bf) = delete;
    SimdBlockFilter(SimdBlockFilter&& bf) noexcept;

    void init(size_t nums);

    void insert_hash(const uint64_t hash) noexcept {
        const uint32_t bucket_idx = hash & _directory_mask;
#ifdef __AVX2__
        const __m256i mask = make_mask(hash >> _log_num_buckets);
        __m256i* const bucket = &reinterpret_cast<__m256i*>(_directory)[bucket_idx];
        _mm256_store_si256(bucket, _mm256_or_si256(*bucket, mask));
#else
        uint32_t masks[BITS_SET_PER_BLOCK];
        make_mask(hash >> _log_num_buckets, masks);
        for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
            _directory[bucket_idx][i] |= masks[i];
        }
#endif
    }

    bool test_hash(const uint64_t hash) const noexcept {
        const uint32_t bucket_idx = hash & _directory_mask;
#ifdef __AVX2__
        const __m256i mask = make_mask(hash >> _log_num_buckets);
        const __m256i bucket = reinterpret_cast<__m256i*>(_directory)[bucket_idx];
        // We should return true if 'bucket' has a one wherever 'mask' does. _mm256_testc_si256
        // takes the negation of its first argument and ands that with its second argument. In
        // our case, the result is zero everywhere iff there is a one in 'bucket' wherever
        // 'mask' is one. testc returns 1 if the result is 0 everywhere and returns 0 otherwise.
        return _mm256_testc_si256(bucket, mask);
#else
        uint32_t masks[BITS_SET_PER_BLOCK];
        make_mask(hash >> _log_num_buckets, masks);
        for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
            if ((_directory[bucket_idx][i] & masks[i]) == 0) {
                return false;
            }
        }
        return true;
#endif
    }

#ifdef __AVX2__
    void insert_hash_in_same_bucket(const uint64_t* hash_values, size_t n) {
        if (n == 0) return;
        const uint32_t bucket_idx = hash_values[0] & _directory_mask;
#ifdef __AVX2__
        __m256i* addr = reinterpret_cast<__m256i*>(_directory + bucket_idx);
        __m256i now = _mm256_load_si256(addr);
        for (size_t i = 0; i < n; i++) {
            const __m256i mask = make_mask(hash_values[i] >> _log_num_buckets);
            now = _mm256_or_si256(now, mask);
        }
        _mm256_store_si256(addr, now);
#else
        uint32_t masks[BITS_SET_PER_BLOCK];
        for (size_t i = 0; i < n; i++) {
            auto hash = hash_values[i];

            make_mask(hash >> _log_num_buckets, masks);
            for (int j = 0; j < BITS_SET_PER_BLOCK; ++j) {
                _directory[bucket_idx][j] |= masks[j];
            }
        }
#endif
    }
#endif

    size_t max_serialized_size() const;
    size_t serialize(uint8_t* data) const;
    size_t deserialize(const uint8_t* data);
    void merge(const SimdBlockFilter& bf);
    bool check_equal(const SimdBlockFilter& bf) const;
    uint32_t directory_mask() const { return _directory_mask; }

private:
    // The number of bits to set in a tiny Bloom filter block

    // For scalar version:
    void make_mask(uint32_t key, uint32_t* masks) const;

#ifdef __AVX2__
    // For simd version:
    __m256i make_mask(const uint32_t hash) const noexcept {
        // Load hash into a YMM register, repeated eight times
        __m256i hash_data = _mm256_set1_epi32(hash);
        // Multiply-shift hashing ala Dietzfelbinger et al.: multiply 'hash' by eight different
        // odd constants, then keep the 5 most significant bits from each product.
        const __m256i rehash = _mm256_setr_epi32(0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU, 0x705495c7U,
                                                 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U);
        hash_data = _mm256_mullo_epi32(rehash, hash_data);
        hash_data = _mm256_srli_epi32(hash_data, 27);
        const __m256i ones = _mm256_set1_epi32(1);
        // Use these 5 bits to shift a single bit to a location in each 32-bit lane
        return _mm256_sllv_epi32(ones, hash_data);
    }
#endif
    // log2(number of bytes in a bucket):
    static constexpr int LOG_BUCKET_BYTE_SIZE = 5;

    size_t get_alloc_size() const { return 1ull << (_log_num_buckets + LOG_BUCKET_BYTE_SIZE); }

    // Common:
    // log_num_buckets_ is the log (base 2) of the number of buckets in the directory:
    int _log_num_buckets;
    // directory_mask_ is (1 << log_num_buckets_) - 1
    uint32_t _directory_mask;
    Bucket* _directory = nullptr;
};

// If size is very small(< 1000), SmallHashSet is faster than SimdBlockFilter
// This fast bloom filter is inspired by parallel-hashmap row_hash_set
class SmallHashSet {
public:
    using ctrl_t = int8_t;

    ~SmallHashSet() { free(_ctrl); }

    size_t grow_to_lower_bound_capacity(size_t growth) {
        return growth + static_cast<size_t>((static_cast<int64_t>(growth) - 1) / 7);
    }

    size_t normalize_capacity(size_t n) { return n ? ~size_t{} >> __builtin_clzll(n) : 1; }

    static constexpr ctrl_t KEMPTY = -128;
    void init(size_t size) {
        _capacity = normalize_capacity(grow_to_lower_bound_capacity(size * 2));
        posix_memalign(reinterpret_cast<void**>(&_ctrl), 16, _capacity + 17);
        memset(_ctrl, -128, _capacity + 17);
    }

    void insert_hash(size_t hash) {
        size_t h1_hash = hash >> 7;
        size_t offset_ = h1_hash & _capacity;
        ctrl_t h2_hash = hash & 0x7F;
        while ((_ctrl[offset_] != KEMPTY) & (_ctrl[offset_] != h2_hash)) {
            offset_++;
        }
        _ctrl[offset_] = h2_hash;
    }

    bool test_hash(size_t hash) {
        size_t h1_hash = hash >> 7;
        size_t offset_ = h1_hash & _capacity;
        char h2_hash = hash & 0x7F;
#ifdef __SSE2__
        __m128i ctrl = _mm_loadu_si128(reinterpret_cast<__m128i*>(_ctrl + offset_));
        auto match = _mm_set1_epi8(h2_hash);
        return _mm_movemask_epi8(_mm_cmpeq_epi8(match, ctrl));
#else
        for (size_t i = 0; i < 16; ++i) {
            if (_ctrl[offset_] == h2_hash) {
                return true;
            }
        }
        return false;
#endif
    }

private:
    ctrl_t* _ctrl = nullptr;
    size_t _capacity = 0;
};

// The runtime filter generated by join right small table
class JoinRuntimeFilter {
public:
    virtual ~JoinRuntimeFilter() = default;

    virtual void init(size_t hash_table_size) = 0;

    class RunningContext {
    public:
        Column::Filter selection;
        std::vector<uint32_t> hash_values;
    };

    virtual size_t evaluate(Column* input_column, RunningContext* ctx) const = 0;

    size_t size() const { return _size; }

    bool has_null() const { return _has_null; }

    virtual std::string debug_string() const = 0;

    void set_join_mode(int8_t join_mode) { _join_mode = join_mode; }

    virtual size_t max_serialized_size() const;
    virtual size_t serialize(uint8_t* data) const;
    virtual size_t deserialize(const uint8_t* data);
    virtual void merge(const JoinRuntimeFilter* rf) {
        _has_null |= rf->_has_null;
        _bf.merge(rf->_bf);
    }

    virtual void concat(JoinRuntimeFilter* rf) {
        _has_null |= rf->_has_null;
        _hash_partition_bf.emplace_back(std::move(rf->_bf));
        _hash_partition_number = _hash_partition_bf.size();
        _join_mode = rf->_join_mode;
        _size += rf->_size;
    }
    virtual bool check_equal(const JoinRuntimeFilter& rf) const;
    virtual JoinRuntimeFilter* create_empty(ObjectPool* pool) = 0;

protected:
    bool _has_null = false;
    size_t _size = 0;
    int8_t _join_mode = 0;
    SimdBlockFilter _bf;
    size_t _hash_partition_number = 0;
    std::vector<SimdBlockFilter> _hash_partition_bf;
};

// The join runtime filter implement by bloom filter
template <PrimitiveType Type>
class RuntimeBloomFilter final : public JoinRuntimeFilter {
public:
    using CppType = RunTimeCppType<Type>;
    using ColumnType = RunTimeColumnType<Type>;

    RuntimeBloomFilter() = default;
    ~RuntimeBloomFilter() override = default;

    RuntimeBloomFilter* create_empty(ObjectPool* pool) override { return pool->add(new RuntimeBloomFilter()); };

    void init_min_max() {
        _has_min_max = false;

        if constexpr (IsSlice<CppType>) {
            _min = Slice::max_value();
            _max = Slice::min_value();
        } else if constexpr (std::is_integral_v<CppType>) {
            _min = std::numeric_limits<CppType>::max();
            _max = std::numeric_limits<CppType>::lowest();
        } else if constexpr (std::is_floating_point_v<CppType>) {
            _min = std::numeric_limits<CppType>::max();
            _max = std::numeric_limits<CppType>::lowest();
        } else if constexpr (IsDate<CppType>) {
            _min = DateValue::MAX_DATE_VALUE;
            _max = DateValue::MIN_DATE_VALUE;
        } else if constexpr (IsTimestamp<CppType>) {
            _min = TimestampValue::MAX_TIMESTAMP_VALUE;
            _max = TimestampValue::MIN_TIMESTAMP_VALUE;
        } else if constexpr (IsDecimal<CppType>) {
            _min = DecimalV2Value::get_max_decimal();
            _max = DecimalV2Value::get_min_decimal();
        }
    }
    void init_bloom_filter(size_t hash_table_size) {
        _size = hash_table_size;
        _bf.init(_size);
    }

    void init(size_t hash_table_size) override {
        init_bloom_filter(hash_table_size);
        init_min_max();
    }

    size_t compute_hash(CppType value) const {
        if constexpr (IsSlice<CppType>) {
            return SliceHash()(value);
        } else {
            return phmap_mix<sizeof(size_t)>()(std::hash<CppType>()(value));
        }
    }

    void insert(CppType* value) {
        if (value == nullptr) {
            _has_null = true;
            return;
        }

        size_t hash = compute_hash(*value);
        _bf.insert_hash(hash);

        _min = std::min(*value, _min);
        _max = std::max(*value, _max);
        _has_min_max = true;
    }

    CppType min_value() const { return _min; }

    CppType max_value() const { return _max; }

    bool has_min_max() const { return _has_min_max; }

    bool test_data(CppType value) const {
        if constexpr (!IsSlice<CppType>) {
            if (value < _min || value > _max) {
                return false;
            }
        }
        size_t hash = compute_hash(value);
        return _bf.test_hash(hash);
    }

    bool test_data_with_hash(CppType value, const uint32_t shuffle_hash) const {
        if constexpr (!IsSlice<CppType>) {
            if (value < _min || value > _max) {
                return false;
            }
        }
        // module has been done outside, so actually here is bucket idx.
        const uint32_t bucket_idx = shuffle_hash;
        size_t hash = compute_hash(value);
        return _hash_partition_bf[bucket_idx].test_hash(hash);
    }

    size_t evaluate(Column* input_column, RunningContext* ctx) const override {
        if (_hash_partition_number != 0) {
            return t_evaluate<true>(input_column, ctx);
        } else {
            return t_evaluate<false>(input_column, ctx);
        }
    }

    // `hash_parittion` parameters means if this runtime filter has multiple `simd-block-filter` underneath.
    // for local runtime filter, it only has once `sime-block-filter`, and `hash_partition` is false.
    // and for global runtime filter, since it concates multiple runtime filters from partitions
    // so it has multiple `sime-block-filter` and `hash_partition` is true.
    // For more information, you can refers to doc `shuffle-aware runtime filter`.
    template <bool hash_partition = false>
    size_t t_evaluate(Column* input_column, RunningContext* ctx) const {
        size_t size = input_column->size();
        Column::Filter& _selection = ctx->selection;
        std::vector<uint32_t>& _hash_values = ctx->hash_values;
        size_t true_count = 0;
        if constexpr (hash_partition) {
            DCHECK(_join_mode == TRuntimeFilterBuildJoinMode::BORADCAST ||
                   _join_mode == TRuntimeFilterBuildJoinMode::PARTITIONED ||
                   _join_mode == TRuntimeFilterBuildJoinMode::BUCKET_SHUFFLE)
                    << "unexpected join mode: " << _join_mode;
            // NOTE(yan): must be consistent with data stream sender hash function.
            // this is implementation of shuffle-aware global runtime filter.
            if (_join_mode == TRuntimeFilterBuildJoinMode::BORADCAST) {
                // since there is only one copy and one rf
                _hash_values.assign(size, 0);
            } else if (_join_mode == TRuntimeFilterBuildJoinMode::PARTITIONED) {
                _hash_values.assign(size, HashUtil::FNV_SEED);
                input_column->fnv_hash(_hash_values.data(), 0, size);
                for (size_t i = 0; i < size; i++) {
                    _hash_values[i] %= _hash_partition_number;
                }
            } else if (_join_mode == TRuntimeFilterBuildJoinMode::BUCKET_SHUFFLE) {
                _hash_values.assign(size, 0);
                input_column->crc32_hash(_hash_values.data(), 0, size);
                for (size_t i = 0; i < size; i++) {
                    _hash_values[i] %= _hash_partition_number;
                }
            }
        }

        if (input_column->is_constant()) {
            const auto* const_column = down_cast<const ConstColumn*>(input_column);
            bool sel = true;
            if (const_column->only_null()) {
                sel = _has_null;
            } else {
                auto* input_data = down_cast<const ColumnType*>(const_column->data_column().get())->get_data().data();
                if constexpr (hash_partition) {
                    sel = test_data_with_hash(input_data[0], _hash_values[0]);
                } else {
                    sel = test_data(input_data[0]);
                }
            }
            for (int i = 0; i < size; i++) {
                _selection[i] &= sel;
                true_count += sel;
            }
        } else if (input_column->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(input_column);
            auto* input_data = down_cast<const ColumnType*>(nullable_column->data_column().get())->get_data().data();
            if (nullable_column->has_null()) {
                const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
                for (int i = 0; i < size; ++i) {
                    if (null_data[i]) {
                        _selection[i] &= _has_null;
                        true_count += _has_null;
                    } else {
                        if constexpr (hash_partition) {
                            const auto hit = test_data_with_hash(input_data[i], _hash_values[i]);
                            _selection[i] &= hit;
                            true_count += hit;
                        } else {
                            const auto hit = test_data(input_data[i]);
                            _selection[i] &= hit;
                            true_count += hit;
                        }
                    }
                }
            } else {
                for (int i = 0; i < size; ++i) {
                    if constexpr (hash_partition) {
                        const auto hit = test_data_with_hash(input_data[i], _hash_values[i]);
                        _selection[i] &= hit;
                        true_count += hit;
                    } else {
                        const auto hit = test_data(input_data[i]);
                        _selection[i] &= hit;
                        true_count += hit;
                    }
                }
            }
        } else {
            auto* input_data = down_cast<const ColumnType*>(input_column)->get_data().data();
            for (int i = 0; i < size; ++i) {
                if constexpr (hash_partition) {
                    const auto hit = test_data_with_hash(input_data[i], _hash_values[i]);
                    _selection[i] &= hit;
                    true_count += hit;
                } else {
                    const auto hit = test_data(input_data[i]);
                    _selection[i] &= hit;
                    true_count += hit;
                }
            }
        }

        return true_count;
    }

    void merge(const JoinRuntimeFilter* rf) override {
        JoinRuntimeFilter::merge(rf);
        merge_min_max(down_cast<const RuntimeBloomFilter*>(rf));
    }

    void concat(JoinRuntimeFilter* rf) override {
        JoinRuntimeFilter::concat(rf);
        merge_min_max(down_cast<const RuntimeBloomFilter*>(rf));
    }

    void merge_min_max(const RuntimeBloomFilter* bf) {
        _min = std::min(_min, bf->_min);
        _max = std::max(_max, bf->_max);
        _has_min_max |= bf->_has_min_max;

        if constexpr (IsSlice<CppType>) {
            if (_has_min_max) {
                // maybe we are refering to another runtime filter instance
                // for security we have to copy that back to our instance.
                if (_min.size != 0 && _min.data != _slice_min.data()) {
                    _slice_min.resize(_min.size);
                    memcpy(_slice_min.data(), _min.data, _min.size);
                    _min.data = _slice_min.data();
                }
                if (_max.size != 0 && _max.data != _slice_max.data()) {
                    _slice_max.resize(_max.size);
                    memcpy(_slice_max.data(), _max.data, _max.size);
                    _max.data = _slice_max.data();
                }
            }
        }
    }

    std::string debug_string() const override {
        PrimitiveType ptype = Type;
        std::stringstream ss;
        ss << "RuntimeBF(type = " << ptype << ", bfsize = " << _size << ", has_null = " << _has_null;
        if constexpr (std::is_integral_v<CppType> || std::is_floating_point_v<CppType>) {
            if constexpr (!std::is_same_v<CppType, __int128>) {
                ss << ", _min = " << _min << ", _max = " << _max;
            } else {
                ss << ", _min/_max = int128";
            }
        } else if constexpr (IsSlice<CppType>) {
            ss << ", _min/_max = slice";
        } else if constexpr (IsDate<CppType> || IsTimestamp<CppType> || IsDecimal<CppType>) {
            ss << ", _min = " << _min.to_string() << ", _max = " << _max.to_string();
        }
        ss << ")";
        return ss.str();
    }

    size_t max_serialized_size() const override {
        size_t size = sizeof(Type) + JoinRuntimeFilter::max_serialized_size();
        // _has_min_max
        size += 1;

        if (_has_min_max) {
            if constexpr (!IsSlice<CppType>) {
                size += sizeof(_min) + sizeof(_max);
            } else {
                // slice format = | min_size | max_size | min_data | max_data |
                size += sizeof(_min.size) + _min.size;
                size += sizeof(_max.size) + _max.size;
            }
        }

        return size;
    }

    size_t serialize(uint8_t* data) const override {
        PrimitiveType ptype = Type;
        size_t offset = 0;
        memcpy(data + offset, &ptype, sizeof(ptype));
        offset += sizeof(ptype);
        offset += JoinRuntimeFilter::serialize(data + offset);
        memcpy(data + offset, &_has_min_max, sizeof(_has_min_max));
        offset += sizeof(_has_min_max);

        if (_has_min_max) {
            if constexpr (!IsSlice<CppType>) {
                memcpy(data + offset, &_min, sizeof(_min));
                offset += sizeof(_min);
                memcpy(data + offset, &_max, sizeof(_max));
                offset += sizeof(_max);
            } else {
                memcpy(data + offset, &_min.size, sizeof(_min.size));
                offset += sizeof(_min.size);
                memcpy(data + offset, &_max.size, sizeof(_max.size));
                offset += sizeof(_max.size);

                if (_min.size != 0) {
                    memcpy(data + offset, _min.data, _min.size);
                    offset += _min.size;
                }

                if (_max.size != 0) {
                    memcpy(data + offset, _max.data, _max.size);
                    offset += _max.size;
                }
            }
        }
        return offset;
    }

    size_t deserialize(const uint8_t* data) override {
        PrimitiveType ptype = Type;
        size_t offset = 0;
        memcpy(&ptype, data + offset, sizeof(ptype));
        offset += sizeof(ptype);
        offset += JoinRuntimeFilter::deserialize(data + offset);
        memcpy(&_has_min_max, data + offset, sizeof(_has_min_max));
        offset += sizeof(_has_min_max);

        if (_has_min_max) {
            if constexpr (!IsSlice<CppType>) {
                memcpy(&_min, data + offset, sizeof(_min));
                offset += sizeof(_min);
                memcpy(&_max, data + offset, sizeof(_max));
                offset += sizeof(_max);
            } else {
                _min.data = nullptr;
                _max.data = nullptr;
                memcpy(&_min.size, data + offset, sizeof(_min.size));
                offset += sizeof(_min.size);
                memcpy(&_max.size, data + offset, sizeof(_max.size));
                offset += sizeof(_max.size);

                if (_min.size != 0) {
                    _slice_min.resize(_min.size);
                    memcpy(_slice_min.data(), data + offset, _min.size);
                    offset += _min.size;
                    _min.data = _slice_min.data();
                }

                if (_max.size != 0) {
                    _slice_max.resize(_max.size);
                    memcpy(_slice_max.data(), data + offset, _max.size);
                    offset += _max.size;
                    _max.data = _slice_max.data();
                }
            }
        } else {
            init_min_max();
        }

        return offset;
    }

    bool check_equal(const JoinRuntimeFilter& base_rf) const override {
        if (!JoinRuntimeFilter::check_equal(base_rf)) return false;
        const auto& rf = static_cast<const RuntimeBloomFilter<Type>&>(base_rf);
        if constexpr (!IsSlice<CppType>) {
            bool eq = (memcmp(&_min, &rf._min, sizeof(_min)) == 0) && (memcmp(&_max, &rf._max, sizeof(_max)) == 0);
            if (!eq) return false;
        } else {
            bool eq = (_min == rf._min) && (_max == rf._max);
            if (!eq) return false;
        }
        return true;
    }

private:
    CppType _min;
    CppType _max;
    std::string _slice_min;
    std::string _slice_max;
    bool _has_min_max;
};

} // namespace vectorized
} // namespace starrocks
