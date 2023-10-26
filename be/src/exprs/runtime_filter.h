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

#pragma once

#include "column/chunk.h"
#include "column/column_hash.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "exec/pipeline/exchange/shuffler.h"
#include "exprs/runtime_filter_layout.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "types/logical_type.h"

namespace starrocks {
// 0x1. initial global runtime filter impl
// 0x2. change simd-block-filter hash function.
// 0x3. Fix serialize problem
inline const constexpr uint8_t RF_VERSION = 0x2;
inline const constexpr uint8_t RF_VERSION_V2 = 0x3;
static_assert(sizeof(RF_VERSION_V2) == sizeof(RF_VERSION));
inline const constexpr int32_t RF_VERSION_SZ = sizeof(RF_VERSION_V2);

// compatible code from 2.5 to 3.0
// TODO: remove it
class RuntimeFilterSerializeType {
public:
    enum PrimitiveType {
        INVALID_TYPE = 0,
        TYPE_NULL,     /* 1 */
        TYPE_BOOLEAN,  /* 2 */
        TYPE_TINYINT,  /* 3 */
        TYPE_SMALLINT, /* 4 */
        TYPE_INT,      /* 5 */
        TYPE_BIGINT,   /* 6 */
        TYPE_LARGEINT, /* 7 */
        TYPE_FLOAT,    /* 8 */
        TYPE_DOUBLE,   /* 9 */
        TYPE_VARCHAR,  /* 10 */
        TYPE_DATE,     /* 11 */
        TYPE_DATETIME, /* 12 */
        TYPE_BINARY,
        /* 13 */      // Not implemented
        TYPE_DECIMAL, /* 14 */
        TYPE_CHAR,    /* 15 */

        TYPE_STRUCT,    /* 16 */
        TYPE_ARRAY,     /* 17 */
        TYPE_MAP,       /* 18 */
        TYPE_HLL,       /* 19 */
        TYPE_DECIMALV2, /* 20 */

        TYPE_TIME,       /* 21 */
        TYPE_OBJECT,     /* 22 */
        TYPE_PERCENTILE, /* 23 */
        TYPE_DECIMAL32,  /* 24 */
        TYPE_DECIMAL64,  /* 25 */
        TYPE_DECIMAL128, /* 26 */

        TYPE_JSON,      /* 27 */
        TYPE_FUNCTION,  /* 28 */
        TYPE_VARBINARY, /* 28 */
    };

    static_assert(sizeof(PrimitiveType) == sizeof(int32_t));
    static_assert(sizeof(PrimitiveType) == sizeof(LogicalType));
    static_assert(sizeof(TPrimitiveType::type) == sizeof(LogicalType));

    static PrimitiveType to_serialize_type(LogicalType type);

    static LogicalType from_serialize_type(PrimitiveType type);
};

static constexpr uint32_t SALT[8] = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
                                     0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

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
#elif defined(__ARM_NEON)
        uint32x4_t masks[2];

        uint32x4_t directory_1 = vld1q_u32(&_directory[bucket_idx][0]);
        uint32x4_t directory_2 = vld1q_u32(&_directory[bucket_idx][4]);

        make_mask(hash >> _log_num_buckets, masks);
        uint32x4_t out_1 = vbicq_u32(masks[0], directory_1);
        uint32x4_t out_2 = vbicq_u32(masks[1], directory_2);
        out_1 = vorrq_u32(out_1, out_2);
        uint32x2_t low_1 = vget_low_u32(out_1);
        uint32x2_t high_1 = vget_high_u32(out_1);
        low_1 = vorr_u32(low_1, high_1);
        uint32_t res = vget_lane_u32(low_1, 0) | vget_lane_u32(low_1, 1);
        return !(res);
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

#ifdef __ARM_NEON
    // For Neon version:
    void make_mask(uint32_t key, uint32x4_t* masks) const noexcept {
        uint32x4_t hash_data_1 = vdupq_n_u32(key);
        uint32x4_t hash_data_2 = vdupq_n_u32(key);
        uint32x4_t rehash_1 = vld1q_u32(&SALT[0]);
        uint32x4_t rehash_2 = vld1q_u32(&SALT[4]);
        hash_data_1 = vmulq_u32(rehash_1, hash_data_1);
        hash_data_2 = vmulq_u32(rehash_2, hash_data_2);
        hash_data_1 = vshrq_n_u32(hash_data_1, 27);
        hash_data_2 = vshrq_n_u32(hash_data_2, 27);
        const uint32x4_t ones = vdupq_n_u32(1);
        masks[0] = vshlq_u32(ones, reinterpret_cast<int32x4_t>(hash_data_1));
        masks[1] = vshlq_u32(ones, reinterpret_cast<int32x4_t>(hash_data_2));
    }
#endif

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
class JoinRuntimeFilter;
using JoinRuntimeFilterPtr = std::shared_ptr<const JoinRuntimeFilter>;
using MutableJoinRuntimeFilterPtr = std::shared_ptr<JoinRuntimeFilter>;
class JoinRuntimeFilter {
public:
    virtual ~JoinRuntimeFilter() = default;

    virtual void init(size_t hash_table_size) = 0;

    class RunningContext {
    public:
        Filter selection;
        Filter merged_selection;
        bool use_merged_selection;
        std::vector<uint32_t> hash_values;
        bool compatibility = true;
    };

    virtual void compute_partition_index(const RuntimeFilterLayout& layout, const std::vector<Column*>& columns,
                                         RunningContext* ctx) const = 0;
    virtual void evaluate(Column* input_column, RunningContext* ctx) const = 0;

    size_t size() const { return _size; }
    bool always_true() const { return _always_true; }
    size_t num_hash_partitions() const { return _hash_partition_bf.size(); }

    bool has_null() const { return _has_null; }

    virtual std::string debug_string() const = 0;

    void set_join_mode(int8_t join_mode) { _join_mode = join_mode; }
<<<<<<< HEAD
=======

    void clear_bf();

    bool can_use_bf() const {
        if (_hash_partition_bf.empty()) {
            return _bf.can_use();
        }
        return _hash_partition_bf[0].can_use();
    }

>>>>>>> c17af30f5b ([Enhancement] Introduce pipeline-level multi-partitioned runtime filter (#33002))
    // RuntimeFilter version
    // if the RuntimeFilter is updated, the version will be updated as well,
    // (usually used for TopN Filter)
    size_t rf_version() const { return _rf_version; }

    virtual size_t max_serialized_size() const;
    virtual size_t serialize(int serialize_version, uint8_t* data) const;
    virtual size_t deserialize(int serialize_version, const uint8_t* data);

    virtual void intersect(const JoinRuntimeFilter* rf) = 0;

    virtual void merge(const JoinRuntimeFilter* rf) {
        _has_null |= rf->_has_null;
        _bf.merge(rf->_bf);
    }

    virtual void concat(JoinRuntimeFilter* rf) {
        _has_null |= rf->_has_null;
        if (rf->_hash_partition_bf.empty()) {
            _hash_partition_bf.emplace_back(std::move(rf->_bf));
        } else {
            for (auto&& bf : rf->_hash_partition_bf) {
                _hash_partition_bf.emplace_back(std::move(bf));
            }
        }
        _join_mode = rf->_join_mode;
        _size += rf->_size;
    }
    virtual bool check_equal(const JoinRuntimeFilter& rf) const;
    virtual JoinRuntimeFilter* create_empty(ObjectPool* pool) = 0;
    void set_global() { this->_global = true; }

protected:
    void _update_version() { _rf_version++; }

    bool _has_null = false;
    bool _global = false;
    size_t _size = 0;
    int8_t _join_mode = 0;
    SimdBlockFilter _bf;
    std::vector<SimdBlockFilter> _hash_partition_bf;
    bool _always_true = false;
    size_t _rf_version = 0;
};

template <typename ModuloFunc>
struct WithModuloArg {
    template <TRuntimeFilterLayoutMode::type M>
    struct HashValueCompute {
        void operator()(const RuntimeFilterLayout& layout, const std::vector<Column*>& columns, size_t num_rows,
                        size_t real_num_partitions, std::vector<uint32_t>& hash_values) const {
            if constexpr (layout_is_singleton<M>) {
                hash_values.assign(num_rows, 0);
                return;
            }

            typedef void (Column::*HashFuncType)(uint32_t*, uint32_t, uint32_t) const;
            auto compute_hash = [&columns, &num_rows, &hash_values](HashFuncType hash_func) {
                for (Column* input_column : columns) {
                    (input_column->*hash_func)(hash_values.data(), 0, num_rows);
                }
            };

            if constexpr (layout_is_shuffle<M>) {
                hash_values.assign(num_rows, HashUtil::FNV_SEED);
                compute_hash(&Column::fnv_hash);
                [[maybe_unused]] const auto num_instances = layout.num_instances();
                [[maybe_unused]] const auto num_drivers_per_instance = layout.num_drivers_per_instance();
                [[maybe_unused]] const auto num_partitions = num_instances * num_drivers_per_instance;
                for (auto i = 0; i < num_rows; ++i) {
                    auto& hash_value = hash_values[i];
                    if constexpr (layout_is_pipeline_shuffle<M>) {
                        hash_value = ModuloFunc()(HashUtil::xorshift32(hash_value), num_drivers_per_instance);
                    } else if constexpr (layout_is_global_shuffle_1l<M>) {
                        hash_value = ModuloFunc()(hash_value, real_num_partitions);
                    } else if constexpr (layout_is_global_shuffle_2l<M>) {
                        auto instance_id = ModuloFunc()(hash_value, num_instances);
                        auto driver_id = ModuloFunc()(HashUtil::xorshift32(hash_value), num_drivers_per_instance);
                        hash_value = instance_id * num_drivers_per_instance + driver_id;
                    }
                }
            } else if (layout_is_bucket<M>) {
                hash_values.assign(num_rows, 0);
                compute_hash(&Column::crc32_hash);
                [[maybe_unused]] const auto& bucketseq_to_instance = layout.bucketseq_to_instance();
                [[maybe_unused]] const auto& bucketseq_to_driverseq = layout.bucketseq_to_driverseq();
                [[maybe_unused]] const auto& bucketseq_to_partition = layout.bucketseq_to_partition();
                [[maybe_unused]] const auto num_drivers_per_instance = layout.num_drivers_per_instance();
                for (auto i = 0; i < num_rows; ++i) {
                    auto& hash_value = hash_values[i];
                    if constexpr (layout_is_pipeline_bucket<M>) {
                        hash_value = bucketseq_to_driverseq[ModuloFunc()(hash_value, bucketseq_to_driverseq.size())];
                    } else if constexpr (layout_is_pipeline_bucket_lx<M>) {
                        hash_value = ModuloFunc()(HashUtil::xorshift32(hash_value), num_drivers_per_instance);
                    } else if constexpr (layout_is_global_bucket_1l<M>) {
                        hash_value = bucketseq_to_instance[ModuloFunc()(hash_value, bucketseq_to_instance.size())];
                    } else if constexpr (layout_is_global_bucket_2l<M>) {
                        hash_value = bucketseq_to_partition[ModuloFunc()(hash_value, bucketseq_to_partition.size())];
                    } else if constexpr (layout_is_global_bucket_2l_lx<M>) {
                        const auto bucketseq = ModuloFunc()(hash_value, bucketseq_to_instance.size());
                        const auto instance = bucketseq_to_instance[bucketseq];
                        const auto driverseq = ModuloFunc()(HashUtil::xorshift32(hash_value), num_drivers_per_instance);
                        hash_value = (instance == BUCKET_ABSENT) ? BUCKET_ABSENT
                                                                 : instance * num_drivers_per_instance + driverseq;
                    }
                }
            }
        }
    };
};

// The join runtime filter implement by bloom filter
template <LogicalType Type>
class RuntimeBloomFilter final : public JoinRuntimeFilter {
public:
    using CppType = RunTimeCppType<Type>;
    using ColumnType = RunTimeColumnType<Type>;
<<<<<<< HEAD
=======
    using ContainerType = RunTimeProxyContainerType<Type>;
    using SelfType = RuntimeBloomFilter<Type>;
>>>>>>> c17af30f5b ([Enhancement] Introduce pipeline-level multi-partitioned runtime filter (#33002))

    RuntimeBloomFilter() { _init_min_max(); }
    ~RuntimeBloomFilter() override = default;

    RuntimeBloomFilter* create_empty(ObjectPool* pool) override {
        auto* p = pool->add(new RuntimeBloomFilter());
        return p;
    };

    // create a min/max LT/GT RuntimeFilter with val
    template <bool is_min>
    static RuntimeBloomFilter* create_with_range(ObjectPool* pool, CppType val, bool is_close_interval) {
        auto* p = pool->add(new RuntimeBloomFilter());
        p->_init_full_range();
        p->init(1);

        if constexpr (IsSlice<CppType>) {
            p->_slice_min = val.to_string();
            val = Slice(p->_slice_min.data(), val.get_size());
        }

        if constexpr (is_min) {
            p->_min = val;
            p->_left_close_interval = is_close_interval;
        } else {
            p->_max = val;
            p->_right_close_interval = is_close_interval;
        }

        p->_always_true = true;
        return p;
    }

    template <bool is_min>
    void update_min_max(CppType val) {
        // now slice have not support update min/max
        if constexpr (IsSlice<CppType>) {
            return;
        }

        if constexpr (is_min) {
            if (_min < val) {
                _min = val;
                _update_version();
            }
        } else {
            if (_max > val) {
                _max = val;
                _update_version();
            }
        }
    }

    void init(size_t hash_table_size) override {
        _size = hash_table_size;
        _bf.init(_size);
    }

    size_t compute_hash(CppType value) const {
        if constexpr (IsSlice<CppType>) {
            return SliceHash()(value);
        } else {
            return phmap_mix<sizeof(size_t)>()(std::hash<CppType>()(value));
        }
    }

    void insert(const CppType& value) {
        size_t hash = compute_hash(value);
        _bf.insert_hash(hash);

        _min = std::min(value, _min);
        _max = std::max(value, _max);
    }

    void insert_null() { _has_null = true; }

    CppType min_value() const { return _min; }

    CppType max_value() const { return _max; }

    bool left_close_interval() const { return _left_close_interval; }
    bool right_close_interval() const { return _right_close_interval; }

    void evaluate(Column* input_column, RunningContext* ctx) const override {
<<<<<<< HEAD
        if (_num_hash_partitions != 0) {
            return _t_evaluate<true>(input_column, ctx);
=======
        if (!_hash_partition_bf.empty()) {
            return _hash_partition_bf[0].can_use() ? _t_evaluate<true, true>(input_column, ctx)
                                                   : _t_evaluate<true, false>(input_column, ctx);
>>>>>>> c17af30f5b ([Enhancement] Introduce pipeline-level multi-partitioned runtime filter (#33002))
        } else {
            return _t_evaluate<false>(input_column, ctx);
        }
    }

    // this->max = std::max(other->max, this->max)
    void merge(const JoinRuntimeFilter* rf) override {
        JoinRuntimeFilter::merge(rf);
        _merge_min_max(down_cast<const RuntimeBloomFilter*>(rf));
    }

    // this->min = std::max(other->min, this->min)
    // this->max = std::min(other->max, this->max)
    void intersect(const JoinRuntimeFilter* rf) override {
        auto other = down_cast<const RuntimeBloomFilter*>(rf);

        update_min_max<true>(other->_min);
        update_min_max<false>(other->_max);
    }

    void concat(JoinRuntimeFilter* rf) override {
        JoinRuntimeFilter::concat(rf);
        _merge_min_max(down_cast<const RuntimeBloomFilter*>(rf));
    }

    std::string debug_string() const override {
        LogicalType ltype = Type;
        std::stringstream ss;
        ss << "RuntimeBF(type = " << ltype << ", bfsize = " << _size << ", has_null = " << _has_null;
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
        // _has_min_max. for backward compatibility.
        size += 1;

        if constexpr (!IsSlice<CppType>) {
            size += sizeof(_min) + sizeof(_max);
        } else {
            // slice format = | min_size | max_size | min_data | max_data |
            size += sizeof(_min.size) + _min.size;
            size += sizeof(_max.size) + _max.size;
        }

        return size;
    }

    size_t serialize(int serialize_version, uint8_t* data) const override {
        size_t offset = 0;
        if (serialize_version == RF_VERSION) {
            auto ltype = RuntimeFilterSerializeType::to_serialize_type(Type);
            memcpy(data + offset, &ltype, sizeof(ltype));
            offset += sizeof(ltype);
        } else {
            auto ltype = to_thrift(Type);
            memcpy(data + offset, &ltype, sizeof(ltype));
            offset += sizeof(ltype);
        }

        offset += JoinRuntimeFilter::serialize(serialize_version, data + offset);
        memcpy(data + offset, &_has_min_max, sizeof(_has_min_max));
        offset += sizeof(_has_min_max);

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
        return offset;
    }

    size_t deserialize(int serialize_version, const uint8_t* data) override {
        size_t offset = 0;
        if (serialize_version == RF_VERSION) {
            RuntimeFilterSerializeType::PrimitiveType ltype = RuntimeFilterSerializeType::to_serialize_type(Type);
            memcpy(&ltype, data + offset, sizeof(ltype));
            offset += sizeof(ltype);
        } else {
            auto ltype = to_thrift(Type);
            memcpy(&ltype, data + offset, sizeof(ltype));
            offset += sizeof(ltype);
        }

        offset += JoinRuntimeFilter::deserialize(serialize_version, data + offset);

        bool has_min_max = false;
        memcpy(&has_min_max, data + offset, sizeof(has_min_max));
        offset += sizeof(has_min_max);

        if (has_min_max) {
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

    // filter zonemap by evaluating
    // [min_value, max_value] overlapped with [min, max]
    bool filter_zonemap_with_min_max(const CppType* min_value, const CppType* max_value) const {
        if (min_value == nullptr || max_value == nullptr) return false;
        if (*max_value < _min) return true;
        if (*min_value > _max) return true;
        return false;
    }

    void compute_partition_index(const RuntimeFilterLayout& layout, const std::vector<Column*>& columns,
                                 RunningContext* ctx) const override {
        if (columns.empty() || _join_mode == TRuntimeFilterBuildJoinMode::NONE) return;
        size_t num_rows = columns[0]->size();

        // initialize hash_values.
        // reuse ctx's hash_values object.
        std::vector<uint32_t>& _hash_values = ctx->hash_values;
        // compute hash_values
        auto use_reduce = !ctx->compatibility && (_join_mode == TRuntimeFilterBuildJoinMode::PARTITIONED ||
                                                  _join_mode == TRuntimeFilterBuildJoinMode::SHUFFLE_HASH_BUCKET);
        if (use_reduce) {
            dispatch_layout<WithModuloArg<ReduceOp>::HashValueCompute>(_global, layout, columns, num_rows,
                                                                       _hash_partition_bf.size(), _hash_values);
        } else {
            dispatch_layout<WithModuloArg<ModuloOp>::HashValueCompute>(_global, layout, columns, num_rows,
                                                                       _hash_partition_bf.size(), _hash_values);
        }
    }

private:
    void _init_min_max() {
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
        } else if constexpr (Type != TYPE_JSON) {
            // for json vaue, cpp type is JsonValue*
            // but min/max value type is JsonValue
            // and JsonValue needs special serialization handling.
            _min = RunTimeTypeLimits<Type>::min_value();
            _max = RunTimeTypeLimits<Type>::max_value();
        }
    }

    void _init_full_range() {
        if constexpr (IsSlice<CppType>) {
            _max = Slice::max_value();
            _min = Slice::min_value();
        } else if constexpr (std::is_integral_v<CppType>) {
            _max = std::numeric_limits<CppType>::max();
            _min = std::numeric_limits<CppType>::lowest();
        } else if constexpr (std::is_floating_point_v<CppType>) {
            _max = std::numeric_limits<CppType>::max();
            _min = std::numeric_limits<CppType>::lowest();
        } else if constexpr (IsDate<CppType>) {
            _max = DateValue::MAX_DATE_VALUE;
            _min = DateValue::MIN_DATE_VALUE;
        } else if constexpr (IsTimestamp<CppType>) {
            _max = TimestampValue::MAX_TIMESTAMP_VALUE;
            _min = TimestampValue::MIN_TIMESTAMP_VALUE;
        } else if constexpr (IsDecimal<CppType>) {
            _max = DecimalV2Value::get_max_decimal();
            _min = DecimalV2Value::get_min_decimal();
        } else if constexpr (Type != TYPE_JSON) {
            // for json vaue, cpp type is JsonValue*
            // but min/max value type is JsonValue
            // and JsonValue needs special serialization handling.
            _max = RunTimeTypeLimits<Type>::min_value();
            _min = RunTimeTypeLimits<Type>::max_value();
        }
    }

    void _evaluate_min_max(const CppType* values, uint8_t* selection, size_t size) const {
        if constexpr (!IsSlice<CppType>) {
            for (size_t i = 0; i < size; i++) {
                selection[i] = (values[i] >= _min && values[i] <= _max);
            }
        } else {
            memset(selection, 0x1, size);
        }
    }

    void _merge_min_max(const RuntimeBloomFilter* bf) {
        if (bf->_has_min_max) {
            _min = std::min(_min, bf->_min);
            _max = std::max(_max, bf->_max);

            if constexpr (IsSlice<CppType>) {
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

    bool _test_data(CppType value) const {
        size_t hash = compute_hash(value);
        return _bf.test_hash(hash);
    }

    bool _test_data_with_hash(CppType value, const uint32_t shuffle_hash) const {
        if (shuffle_hash == BUCKET_ABSENT) {
            return false;
        }
        // module has been done outside, so actually here is bucket idx.
        const uint32_t bucket_idx = shuffle_hash;
        size_t hash = compute_hash(value);
        return _hash_partition_bf[bucket_idx].test_hash(hash);
    }

    using HashValues = std::vector<uint32_t>;
    template <bool hash_partition, class DataType>
    void _rf_test_data(uint8_t* selection, const DataType* input_data, const HashValues& hash_values, int idx) const {
        if (selection[idx]) {
            if constexpr (hash_partition) {
                selection[idx] = _test_data_with_hash(input_data[idx], hash_values[idx]);
            } else {
                selection[idx] = _test_data(input_data[idx]);
            }
        }
    }

    // `multi_partition` parameters means if this runtime filter has multiple `simd-block-filter` underneath.
    // for local runtime filter, it only has once `simd-block-filter`, and `multi_partition` is false.
    // and for global runtime filter, since it concates multiple runtime filters from partitions
    // so it has multiple `simd-block-filter` and `multi_partition` is true.
    // For more information, you can refers to doc `shuffle-aware runtime filter`.
<<<<<<< HEAD
    template <bool hash_partition = false>
=======
    template <bool multi_partition = false, bool can_use_bf = true>
>>>>>>> c17af30f5b ([Enhancement] Introduce pipeline-level multi-partitioned runtime filter (#33002))
    void _t_evaluate(Column* input_column, RunningContext* ctx) const {
        size_t size = input_column->size();
        Filter& _selection_filter = ctx->use_merged_selection ? ctx->merged_selection : ctx->selection;
        _selection_filter.resize(size);
        uint8_t* _selection = _selection_filter.data();

        // reuse ctx's hash_values object.
        HashValues& _hash_values = ctx->hash_values;
        if constexpr (multi_partition) {
            DCHECK_LE(size, _hash_values.size());
        }
        if (input_column->is_constant()) {
            const auto* const_column = down_cast<const ConstColumn*>(input_column);
            if (const_column->only_null()) {
                _selection[0] = _has_null;
            } else {
                auto* input_data = down_cast<const ColumnType*>(const_column->data_column().get())->get_data().data();
                _evaluate_min_max(input_data, _selection, 1);
<<<<<<< HEAD
                _rf_test_data<hash_partition>(_selection, input_data, _hash_values, 0);
=======
                if constexpr (can_use_bf) {
                    _rf_test_data<multi_partition>(_selection, input_data, _hash_values, 0);
                }
>>>>>>> c17af30f5b ([Enhancement] Introduce pipeline-level multi-partitioned runtime filter (#33002))
            }
            uint8_t sel = _selection[0];
            memset(_selection, sel, size);
        } else if (input_column->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(input_column);
            auto* input_data = down_cast<const ColumnType*>(nullable_column->data_column().get())->get_data().data();
            _evaluate_min_max(input_data, _selection, size);
            if (nullable_column->has_null()) {
                const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
                for (int i = 0; i < size; i++) {
                    if (null_data[i]) {
                        _selection[i] = _has_null;
                    } else {
<<<<<<< HEAD
                        _rf_test_data<hash_partition>(_selection, input_data, _hash_values, i);
=======
                        if constexpr (can_use_bf) {
                            _rf_test_data<multi_partition>(_selection, input_data, _hash_values, i);
                        }
                    }
                }
            } else {
                if constexpr (can_use_bf) {
                    for (int i = 0; i < size; ++i) {
                        _rf_test_data<multi_partition>(_selection, input_data, _hash_values, i);
>>>>>>> c17af30f5b ([Enhancement] Introduce pipeline-level multi-partitioned runtime filter (#33002))
                    }
                }
            } else {
                for (int i = 0; i < size; ++i) {
                    _rf_test_data<multi_partition>(_selection, input_data, _hash_values, i);
                }
            }
        } else {
            auto* input_data = down_cast<const ColumnType*>(input_column)->get_data().data();
            _evaluate_min_max(input_data, _selection, size);
            for (int i = 0; i < size; ++i) {
                _rf_test_data<hash_partition>(_selection, input_data, _hash_values, i);
            }
        }
    }

private:
    CppType _min;
    CppType _max;
    std::string _slice_min;
    std::string _slice_max;
    bool _has_min_max = true;
    bool _left_close_interval = true;
    bool _right_close_interval = true;
};

} // namespace starrocks
