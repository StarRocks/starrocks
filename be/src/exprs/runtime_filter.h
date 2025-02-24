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

#include <numeric>
#include <sstream>

#include "column/chunk.h"
#include "column/column_hash.h"
#include "column/column_helper.h"
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
inline const constexpr uint8_t RF_VERSION = 0x2; // deprecated
inline const constexpr uint8_t RF_VERSION_V2 = 0x3;
static_assert(sizeof(RF_VERSION_V2) == sizeof(RF_VERSION));
inline const constexpr int32_t RF_VERSION_SZ = sizeof(RF_VERSION_V2);

static constexpr uint32_t SALT[8] = {0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
                                     0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

// Modify from https://github.com/FastFilter/fastfilter_cpp/blob/master/src/bloom/simd-block.h
// This is avx2 simd implementation for paper <<Cache-, Hash- and Space-Efficient Bloom Filters>>
class SimdBlockFilter {
public:
    // The filter is divided up into Buckets:
    static constexpr int BITS_SET_PER_BLOCK = 8;
    using Bucket = uint32_t[BITS_SET_PER_BLOCK];
    static constexpr size_t MINIMUM_ELEMENT_NUM = 1UL;

    SimdBlockFilter() = default;

    ~SimdBlockFilter() noexcept {
        if (_directory) free(_directory);
    }

    SimdBlockFilter(const SimdBlockFilter& bf) = delete;
    SimdBlockFilter(SimdBlockFilter&& bf) noexcept;

    void init(size_t nums);

    void insert_hash(const uint64_t hash) noexcept {
        const uint32_t bucket_idx = hash & _directory_mask;
#ifdef __AVX2__
        const __m256i mask = make_mask(hash >> _log_num_buckets);
        __m256i* const bucket = &reinterpret_cast<__m256i*>(_directory)[bucket_idx];
        _mm256_store_si256(bucket, _mm256_or_si256(*bucket, mask));
#elif defined(__ARM_NEON)
        uint32x4_t masks[2];
        make_mask(hash >> _log_num_buckets, masks);
        uint32x4_t directory_1 = vld1q_u32(&_directory[bucket_idx][0]);
        uint32x4_t directory_2 = vld1q_u32(&_directory[bucket_idx][4]);
        directory_1 = vorrq_u32(directory_1, masks[0]);
        directory_2 = vorrq_u32(directory_2, masks[1]);
        vst1q_u32(&_directory[bucket_idx][0], directory_1);
        vst1q_u32(&_directory[bucket_idx][4], directory_2);
#else
        uint32_t masks[BITS_SET_PER_BLOCK];
        make_mask(hash >> _log_num_buckets, masks);
        for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
            _directory[bucket_idx][i] |= masks[i];
        }
#endif
    }

    bool test_hash(const uint64_t hash) const noexcept {
        if (UNLIKELY(_directory == nullptr)) {
            DCHECK(false) << "unexpected test_hash on cleared bf";
            return true;
        }
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

    void clear();
    // whether this bloom filter can be used
    // if the bloom filter's size of partial rf has exceed the size limit of global rf,
    // we still send this rf but ignore bloom filter and only keep min/max filter,
    // in this case, we will use clear() to release the memory of bloom filter,
    // we can use can_use() to check if this bloom filter can be used
    bool can_use() const { return _directory != nullptr; }

    size_t get_alloc_size() const {
        return _log_num_buckets == 0 ? 0 : (1ull << (_log_num_buckets + LOG_BUCKET_BYTE_SIZE));
    }

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

    // Common:
    // log_num_buckets_ is the log (base 2) of the number of buckets in the directory:
    int _log_num_buckets = 0;
    // directory_mask_ is (1 << log_num_buckets_) - 1
    uint32_t _directory_mask = 0;
    Bucket* _directory = nullptr;
};

// The runtime filter generated by join right small table
class RuntimeFilter;
class RuntimeBloomFilter;
using RuntimeFilterPtr = std::shared_ptr<const RuntimeFilter>;
using MutableRuntimeFilterPtr = std::shared_ptr<RuntimeFilter>;

class RuntimeFilter {
public:
    virtual ~RuntimeFilter() = default;

    class RunningContext {
    public:
        Filter selection;
        Filter merged_selection;
        bool use_merged_selection;
        std::vector<uint32_t> hash_values;
        bool compatibility = true;
    };

    virtual size_t num_hash_partitions() const { return 0; }

    virtual void compute_partition_index(const RuntimeFilterLayout& layout, const std::vector<Column*>& columns,
                                         RunningContext* ctx) const = 0;
    virtual void evaluate(Column* input_column, RunningContext* ctx) const = 0;

    bool always_true() const { return _always_true; }

    bool has_null() const { return _has_null; }

    virtual std::string debug_string() const = 0;

    // RuntimeFilter version
    // if the RuntimeFilter is updated, the version will be updated as well,
    // (usually used for TopN Filter)
    size_t rf_version() const { return _rf_version; }

    virtual size_t max_serialized_size() const {
        DCHECK(false) << "unreachable path";
        return 0;
    }
    virtual size_t serialize(int serialize_version, uint8_t* data) const {
        DCHECK(false) << "unreachable path";
        return 0;
    }
    virtual size_t deserialize(int serialize_version, const uint8_t* data) {
        DCHECK(false) << "unreachable path";
        return 0;
    }

    virtual void intersect(const RuntimeFilter* rf) = 0;

    virtual void merge(const RuntimeFilter* rf) { _has_null |= rf->_has_null; }

    virtual void concat(RuntimeFilter* rf) { _has_null |= rf->_has_null; }

    virtual RuntimeFilter* create_empty(ObjectPool* pool) = 0;

    // only used in local colocate filter
    bool is_group_colocate_filter() const { return !_group_colocate_filters.empty(); }
    std::vector<RuntimeFilter*>& group_colocate_filter() { return _group_colocate_filters; }
    const std::vector<RuntimeFilter*>& group_colocate_filter() const { return _group_colocate_filters; }

    virtual const RuntimeFilter* get_min_max_filter() const {
        DCHECK(false) << "unreachable path";
        return nullptr;
    }
    virtual RuntimeBloomFilter* get_bloom_filter() {
        DCHECK(false) << "unreachable path";
        return nullptr;
    }

protected:
    void _update_version() { _rf_version++; }

    bool _has_null = false;
    bool _always_true = false;
    size_t _rf_version = 0;
    // local colocate filters is local filter we don't have to serialize them
    std::vector<RuntimeFilter*> _group_colocate_filters;
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

template <LogicalType Type>
class MinMaxRuntimeFilter final : public RuntimeFilter {
public:
    using CppType = RunTimeCppType<Type>;
    using ColumnType = RunTimeColumnType<Type>;
    using ContainerType = RunTimeProxyContainerType<Type>;

    MinMaxRuntimeFilter() { _init_min_max(); }
    ~MinMaxRuntimeFilter() override = default;

    const RuntimeFilter* get_min_max_filter() const override { return this; }

    RuntimeFilter* create_empty(ObjectPool* pool) override {
        auto* p = pool->add(new MinMaxRuntimeFilter());
        return p;
    }

    static MinMaxRuntimeFilter* create_with_empty_range_without_null(ObjectPool* pool) {
        auto* rf = pool->add(new MinMaxRuntimeFilter());
        rf->_always_true = true;
        return rf;
    }

    static MinMaxRuntimeFilter* create_with_only_null_range(ObjectPool* pool) {
        auto* rf = pool->add(new MinMaxRuntimeFilter());
        rf->insert_null();
        rf->_always_true = true;
        return rf;
    }

    static MinMaxRuntimeFilter* create_with_full_range_without_null(ObjectPool* pool) {
        auto* rf = pool->add(new MinMaxRuntimeFilter());
        rf->_init_full_range();
        rf->_always_true = true;
        return rf;
    }

    // create a min/max LT/GT RuntimeFilter with val
    template <bool is_min>
    static MinMaxRuntimeFilter* create_with_range(ObjectPool* pool, CppType val, bool is_close_interval) {
        auto* p = pool->add(new MinMaxRuntimeFilter());
        p->_init_full_range();

        if constexpr (IsSlice<CppType>) {
            if constexpr (is_min) {
                p->_slice_min = val.to_string();
                val = Slice(p->_slice_min.data(), p->_slice_min.size());
            } else {
                p->_slice_max = val.to_string();
                val = Slice(p->_slice_max.data(), p->_slice_max.size());
            }
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
    static MinMaxRuntimeFilter* create_with_range(ObjectPool* pool, CppType val, bool is_close_internal,
                                                  bool need_null) {
        auto* rf = create_with_range<is_min>(pool, val, is_close_internal);
        if (need_null) {
            rf->insert_null();
        }
        return rf;
    }

    void set_left_close_interval(bool close_interval) { _left_close_interval = close_interval; }
    void set_right_close_interval(bool close_interval) { _right_close_interval = close_interval; }
    bool left_close_interval() const { return _left_close_interval; }
    bool right_close_interval() const { return _right_close_interval; }

    bool is_empty_range() const { return _min > _max; }

    bool is_full_range() const {
        if constexpr (IsSlice<CppType>) {
            return _min == Slice::min_value() && _max == Slice::max_value();
        } else if constexpr (std::is_integral_v<CppType> || std::is_floating_point_v<CppType>) {
            return _min == std::numeric_limits<CppType>::lowest() && _max == std::numeric_limits<CppType>::max();
        } else if constexpr (IsDate<CppType>) {
            return _min == DateValue::MIN_DATE_VALUE && _max == DateValue::MAX_DATE_VALUE;
        } else if constexpr (IsTimestamp<CppType>) {
            return _min = TimestampValue::MIN_TIMESTAMP_VALUE && _max == TimestampValue::MAX_TIMESTAMP_VALUE;
        } else if constexpr (IsDecimal<CppType>) {
            return _min == DecimalV2Value::get_min_decimal() && _max == DecimalV2Value::get_max_decimal();
        } else if constexpr (Type != TYPE_JSON) {
            return _min == RunTimeTypeLimits<Type>::min_value() && _max == RunTimeTypeLimits<Type>::max_value();
        } else {
            return false;
        }
    }

    void insert(const CppType& value) {
        _min = std::min(value, _min);
        _max = std::max(value, _max);
    }

    void insert_null() { _has_null = true; }

    // this->max = std::max(other->max, this->max)
    void merge(const RuntimeFilter* rf) override { _merge_min_max(down_cast<const MinMaxRuntimeFilter*>(rf)); }

    // this->min = std::max(other->min, this->min)
    // this->max = std::min(other->max, this->max)
    void intersect(const RuntimeFilter* rf) override {
        auto other = down_cast<const MinMaxRuntimeFilter*>(rf);
        update_min_max<true>(other->_min);
        update_min_max<false>(other->_max);
    }

    void concat(RuntimeFilter* rf) override { _merge_min_max(down_cast<const MinMaxRuntimeFilter*>(rf)); }

    template <bool is_min>
    void update_min_max(CppType val) {
        // now slice have not support update min/max
        if constexpr (IsSlice<CppType>) {
            std::lock_guard<std::mutex> lk(_slice_mutex);
            if constexpr (is_min) {
                if (_min < val) {
                    _slice_min = val.to_string();
                    _min.data = _slice_min.data();
                    _min.size = _slice_min.size();
                    _update_version();
                }
            } else {
                if (_max > val) {
                    _slice_max = val.to_string();
                    _max.data = _slice_max.data();
                    _max.size = _slice_max.size();
                    _update_version();
                }
            }
        } else {
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
    }

    void update_to_all_null() {
        DCHECK(_has_null);

        if (is_empty_range()) {
            return;
        }
        _init_min_max();
        _update_version();
    }

    void update_to_empty_and_not_null() {
        if (!is_empty_range() || _has_null) {
            _init_min_max();
            _has_null = false;
            _update_version();
        }
    }

    template <bool evaluate_null>
    void t_evaluate(Column* input_column, RunningContext* ctx) const {
        size_t size = input_column->size();
        Filter& selection_filter = ctx->use_merged_selection ? ctx->merged_selection : ctx->selection;
        selection_filter.resize(size);
        uint8_t* selection = selection_filter.data();
        if (input_column->is_constant()) {
            const auto* const_column = down_cast<const ConstColumn*>(input_column);
            if (const_column->only_null() && evaluate_null) {
                selection[0] = _has_null;
            } else {
                const auto& input_data = GetContainer<Type>::get_data(const_column->data_column());
                evaluate_min_max(input_data, selection, 1);
            }
            uint8_t sel = selection[0];
            memset(selection, sel, size);
        } else if (input_column->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(input_column);
            const auto& input_data = GetContainer<Type>::get_data(nullable_column->data_column());
            evaluate_min_max(input_data, selection, size);
            if (nullable_column->has_null() && evaluate_null) {
                const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
                for (int i = 0; i < size; i++) {
                    if (null_data[i]) {
                        selection[i] = _has_null;
                    }
                }
            }
        } else {
            const auto& input_data = GetContainer<Type>::get_data(input_column);
            evaluate_min_max(input_data, selection, size);
        }
    }

    void evaluate_min_max(const ContainerType& values, uint8_t* selection, size_t size) const {
        DCHECK(_has_min_max);
        if constexpr (!IsSlice<CppType>) {
            const auto* data = values.data();
            if (_left_close_interval) {
                if (_right_close_interval) {
                    for (size_t i = 0; i < size; i++) {
                        selection[i] = (data[i] >= _min && data[i] <= _max);
                    }
                } else {
                    for (size_t i = 0; i < size; i++) {
                        selection[i] = (data[i] >= _min && data[i] < _max);
                    }
                }
            } else {
                if (_right_close_interval) {
                    for (size_t i = 0; i < size; i++) {
                        selection[i] = (data[i] > _min && data[i] <= _max);
                    }
                } else {
                    for (size_t i = 0; i < size; i++) {
                        selection[i] = (data[i] > _min && data[i] < _max);
                    }
                }
            }
        } else {
            memset(selection, 0x1, size);
        }
    }

    CppType min_value(ObjectPool* pool) const {
        if constexpr (IsSlice<CppType>) {
            std::lock_guard<std::mutex> lk(_slice_mutex);
            auto* str = pool->template add<std::string>(new std::string(_min.get_data(), _min.get_size()));
            return Slice(*str);
        } else {
            return _min;
        }
    }

    CppType max_value(ObjectPool* pool) const {
        if constexpr (IsSlice<CppType>) {
            std::lock_guard<std::mutex> lk(_slice_mutex);
            auto* str = pool->template add<std::string>(new std::string(_max.get_data(), _max.get_size()));
            return Slice(*str);
        } else {
            return _max;
        }
    }

    const CppType& min() const { return _min; }

    const CppType& max() const { return _max; }

    // filter zonemap by evaluating
    // [min_value, max_value] overlapped with [min, max]
    bool filter_zonemap_with_min_max(const CppType* min_value, const CppType* max_value) const {
        if (min_value == nullptr || max_value == nullptr) return false;
        if (_left_close_interval) {
            if (*max_value < _min) return true;
        } else {
            if (*max_value <= _min) return true;
        }
        if (_right_close_interval) {
            if (*min_value > _max) return true;
        } else {
            if (*min_value >= _max) return true;
        }
        return false;
    }

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "RuntimeMinMax(";
        ss << "type = " << Type << " ";
        ss << "has_null = " << _has_null << " ";
        if constexpr (std::is_integral_v<CppType> || std::is_floating_point_v<CppType>) {
            if constexpr (!std::is_same_v<CppType, __int128>) {
                ss << "_min = " << _min << ", _max = " << _max;
            } else {
                ss << "_min/_max = int128";
            }
        } else if constexpr (IsSlice<CppType>) {
            ss << "_min/_max = slice";
        } else if constexpr (IsDate<CppType> || IsTimestamp<CppType> || IsDecimal<CppType>) {
            ss << "_min = " << _min.to_string() << ", _max = " << _max.to_string();
        }
        ss << " left_close_interval = " << _left_close_interval << ", right_close_interval = " << _right_close_interval
           << " ";
        ss << ")";
        return ss.str();
    }

    size_t min_max_serialized_size() const {
        size_t size = 0;
        if constexpr (!IsSlice<CppType>) {
            size += sizeof(_min) + sizeof(_max);
        } else {
            // slice format = | min_size | max_size | min_data | max_data |
            size += sizeof(_min.size) + _min.size;
            size += sizeof(_max.size) + _max.size;
        }
        return size;
    }

    size_t serialize_minmax(uint8_t* dst) const {
        uint8_t* begin = dst;
        memcpy(dst, &_has_min_max, sizeof(_has_min_max));
        dst += sizeof(_has_min_max);
        if constexpr (!IsSlice<CppType>) {
            memcpy(dst, &_min, sizeof(_min));
            dst += sizeof(_min);
            memcpy(dst, &_max, sizeof(_max));
            dst += sizeof(_max);
        } else {
            memcpy(dst, &_min.size, sizeof(_min.size));
            dst += sizeof(_min.size);
            memcpy(dst, &_max.size, sizeof(_max.size));
            dst += sizeof(_max.size);

            if (_min.size != 0) {
                memcpy(dst, _min.data, _min.size);
                dst += _min.size;
            }

            if (_max.size != 0) {
                memcpy(dst, _max.data, _max.size);
                dst += _max.size;
            }
        }
        return dst - begin;
    }

    size_t deserialize_minmax(const uint8_t* dst) {
        const uint8_t* begin = dst;
        if constexpr (!IsSlice<CppType>) {
            memcpy(&_min, dst, sizeof(_min));
            dst += sizeof(_min);
            memcpy(&_max, dst, sizeof(_max));
            dst += sizeof(_max);
        } else {
            _min.data = nullptr;
            _max.data = nullptr;
            memcpy(&_min.size, dst, sizeof(_min.size));
            dst += sizeof(_min.size);
            memcpy(&_max.size, dst, sizeof(_max.size));
            dst += sizeof(_max.size);

            if (_min.size != 0) {
                _slice_min.resize(_min.size);
                memcpy(_slice_min.data(), dst, _min.size);
                dst += _min.size;
                _min.data = _slice_min.data();
            }

            if (_max.size != 0) {
                _slice_max.resize(_max.size);
                memcpy(_slice_max.data(), dst, _max.size);
                dst += _max.size;
                _max.data = _slice_max.data();
            }
        }
        return dst - begin;
    }

    void compute_partition_index(const RuntimeFilterLayout& layout, const std::vector<Column*>& columns,
                                 RunningContext* ctx) const override {}
    void evaluate(Column* input_column, RunningContext* ctx) const override { t_evaluate<true>(input_column, ctx); }

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

    void _merge_min_max(const MinMaxRuntimeFilter* rf) {
        if (rf->_has_min_max) {
            _min = std::min(_min, rf->_min);
            _max = std::max(_max, rf->_max);

            if constexpr (IsSlice<CppType>) {
                std::lock_guard<std::mutex> lk(_slice_mutex);
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

private:
    CppType _min;
    CppType _max;
    std::string _slice_min;
    std::string _slice_max;
    mutable std::mutex _slice_mutex;
    bool _has_min_max = true;
    bool _left_close_interval = true;
    bool _right_close_interval = true;
};

class RuntimeBloomFilter : public RuntimeFilter {
public:
    void init(size_t hash_table_size) {
        _size = hash_table_size;
        _bf.init(_size);
    }

    size_t num_hash_partitions() const override { return _hash_partition_bf.size(); }

    bool can_use_bf() const {
        if (_hash_partition_bf.empty()) {
            return _bf.can_use();
        }
        return _hash_partition_bf[0].can_use();
    }

    size_t bf_alloc_size() const {
        if (_hash_partition_bf.empty()) {
            return _bf.get_alloc_size();
        }
        return std::accumulate(
                _hash_partition_bf.begin(), _hash_partition_bf.end(), 0ull,
                [](size_t total, const SimdBlockFilter& bf) -> size_t { return total + bf.get_alloc_size(); });
    }

    void clear_bf();

    void merge(const RuntimeFilter* rf) override {
        RuntimeFilter::merge(rf);
        auto* other = down_cast<const RuntimeBloomFilter*>(rf);
        _bf.merge(other->_bf);
    }

    void concat(RuntimeFilter* rf) override {
        RuntimeFilter::concat(rf);
        auto* other = down_cast<RuntimeBloomFilter*>(rf);
        _join_mode = other->_join_mode;
        _size += other->_size;

        if (other->_hash_partition_bf.empty()) {
            _hash_partition_bf.emplace_back(std::move(other->_bf));
        } else {
            for (auto&& bf : other->_hash_partition_bf) {
                _hash_partition_bf.emplace_back(std::move(bf));
            }
        }
    }

    RuntimeBloomFilter* get_bloom_filter() override { return this; }

    void set_global() { _global = true; }

    void set_join_mode(int8_t join_mode) { _join_mode = join_mode; }
    size_t size() const { return _size; }

    size_t max_serialized_size() const override;
    size_t serialize(int serialize_version, uint8_t* data) const override;
    size_t deserialize(int serialize_version, const uint8_t* data) override;

protected:
    bool _global = false;
    // bloom filter expected elements size
    size_t _size = 0;
    int8_t _join_mode = 0;
    SimdBlockFilter _bf;
    std::vector<SimdBlockFilter> _hash_partition_bf;
};

// The join runtime filter implement by bloom filter
template <LogicalType Type>
class TRuntimeBloomFilter final : public RuntimeBloomFilter {
public:
    using CppType = RunTimeCppType<Type>;
    using ColumnType = RunTimeColumnType<Type>;
    using ContainerType = RunTimeProxyContainerType<Type>;

    TRuntimeBloomFilter() = default;
    ~TRuntimeBloomFilter() override = default;

    TRuntimeBloomFilter* create_empty(ObjectPool* pool) override {
        auto* p = pool->add(new TRuntimeBloomFilter());
        return p;
    }

    size_t compute_hash(CppType value) const {
        if constexpr (IsSlice<CppType>) {
            return SliceHash()(value);
        } else {
            return phmap_mix<sizeof(size_t)>()(std::hash<CppType>()(value));
        }
    }

    void insert(const CppType& value) {
        if (LIKELY(_bf.can_use())) {
            size_t hash = compute_hash(value);
            _bf.insert_hash(hash);
        }
    }

    void insert_null() { _has_null = true; }

    void evaluate(Column* input_column, RunningContext* ctx) const override {
        if (!_hash_partition_bf.empty()) {
            return _hash_partition_bf[0].can_use() ? _t_evaluate<true, true>(input_column, ctx)
                                                   : _t_evaluate<true, false>(input_column, ctx);
        } else {
            return _bf.can_use() ? _t_evaluate<false, true>(input_column, ctx)
                                 : _t_evaluate<false, false>(input_column, ctx);
        }
    }

    void intersect(const RuntimeFilter* rf) override { DCHECK(false) << "unsupported"; }

    std::string debug_string() const override {
        LogicalType ltype = Type;
        std::stringstream ss;
        ss << "RuntimeBF(type = " << ltype << ", size = " << _size << ", has_null = " << _has_null
           << ", can_use_bf = " << can_use_bf();
        ss << ")";
        return ss.str();
    }

    size_t max_serialized_size() const override {
        size_t size = sizeof(Type) + RuntimeBloomFilter::max_serialized_size();
        return size;
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

    bool test_data(CppType value) const { return _test_data(value); }

private:
    bool _test_data(CppType value) const {
        DCHECK(_bf.can_use());
        size_t hash = compute_hash(value);
        return _bf.test_hash(hash);
    }

    bool _test_data_with_hash(CppType value, const uint32_t shuffle_hash) const {
        if (shuffle_hash == BUCKET_ABSENT) {
            return false;
        }
        // module has been done outside, so actually here is bucket idx.
        const uint32_t bucket_idx = shuffle_hash;
        DCHECK(_hash_partition_bf[bucket_idx].can_use());
        size_t hash = compute_hash(value);
        return _hash_partition_bf[bucket_idx].test_hash(hash);
    }

    using HashValues = std::vector<uint32_t>;
    template <bool hash_partition>
    void _rf_test_data(uint8_t* selection, const ContainerType& input_data, const HashValues& hash_values,
                       int idx) const {
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
    template <bool multi_partition = false, bool can_use_bf = true>
    void _t_evaluate(Column* input_column, RunningContext* ctx) const {
        size_t size = input_column->size();
        Filter& selection_filter = ctx->use_merged_selection ? ctx->merged_selection : ctx->selection;
        selection_filter.resize(size);
        uint8_t* selection = selection_filter.data();

        // reuse ctx's hash_values object.
        HashValues& hash_values = ctx->hash_values;
        if constexpr (multi_partition) {
            DCHECK_LE(size, hash_values.size());
        }
        if (input_column->is_constant()) {
            const auto* const_column = down_cast<const ConstColumn*>(input_column);
            if (const_column->only_null()) {
                selection[0] = _has_null;
            } else {
                const auto& input_data = GetContainer<Type>::get_data(const_column->data_column());
                if constexpr (can_use_bf) {
                    _rf_test_data<multi_partition>(selection, input_data, hash_values, 0);
                }
            }
            uint8_t sel = selection[0];
            memset(selection, sel, size);
        } else if (input_column->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(input_column);
            const auto& input_data = GetContainer<Type>::get_data(nullable_column->data_column());
            if (nullable_column->has_null()) {
                const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
                for (int i = 0; i < size; i++) {
                    if (null_data[i]) {
                        selection[i] = _has_null;
                    } else {
                        if constexpr (can_use_bf) {
                            _rf_test_data<multi_partition>(selection, input_data, hash_values, i);
                        }
                    }
                }
            } else {
                if constexpr (can_use_bf) {
                    for (int i = 0; i < size; ++i) {
                        _rf_test_data<multi_partition>(selection, input_data, hash_values, i);
                    }
                }
            }
        } else {
            const auto& input_data = GetContainer<Type>::get_data(input_column);
            if constexpr (can_use_bf) {
                for (int i = 0; i < size; ++i) {
                    _rf_test_data<multi_partition>(selection, input_data, hash_values, i);
                }
            }
        }
    }
};

template <LogicalType Type>
class ComposedRuntimeFilter final : public RuntimeFilter {
public:
    using CppType = RunTimeCppType<Type>;
    using ColumnType = RunTimeColumnType<Type>;
    using ContainerType = RunTimeProxyContainerType<Type>;

    ComposedRuntimeFilter() = default;
    ~ComposedRuntimeFilter() override = default;

    size_t num_hash_partitions() const override { return _bloom_filter.num_hash_partitions(); }

    const RuntimeFilter* get_min_max_filter() const override { return &_min_max_filter; }
    RuntimeBloomFilter* get_bloom_filter() override { return &_bloom_filter; }

    ComposedRuntimeFilter* create_empty(ObjectPool* pool) override {
        auto* p = pool->add(new ComposedRuntimeFilter());
        return p;
    }

    void insert(const CppType& value) {
        bloom_filter().insert(value);
        min_max_filter().insert(value);
    }

    void insert_null() {
        _has_null = true;
        _bloom_filter.insert_null();
        _min_max_filter.insert_null();
    }

    MinMaxRuntimeFilter<Type>& min_max_filter() { return _min_max_filter; }
    const MinMaxRuntimeFilter<Type>& min_max_filter() const { return _min_max_filter; }

    TRuntimeBloomFilter<Type>& bloom_filter() { return _bloom_filter; }
    const TRuntimeBloomFilter<Type>& bloom_filter() const { return _bloom_filter; }

    void evaluate(Column* input_column, RunningContext* ctx) const override {
        _min_max_filter.template t_evaluate<false>(input_column, ctx);
        bloom_filter().evaluate(input_column, ctx);
    }

    void merge(const RuntimeFilter* rf) override {
        RuntimeFilter::merge(rf);
        auto* other = down_cast<const ComposedRuntimeFilter*>(rf);
        bloom_filter().merge(&other->bloom_filter());
        min_max_filter().merge(&other->min_max_filter());
    }

    void intersect(const RuntimeFilter* rf) override { DCHECK(false) << "unsupported"; }

    void concat(RuntimeFilter* rf) override {
        RuntimeFilter::concat(rf);
        auto* other = down_cast<ComposedRuntimeFilter*>(rf);
        bloom_filter().concat(&other->bloom_filter());
        min_max_filter().merge(&other->min_max_filter());
    }

    std::string debug_string() const override {
        std::stringstream ss;
        ss << bloom_filter().debug_string();
        ss << min_max_filter().debug_string();
        return ss.str();
    }

    size_t max_serialized_size() const override {
        size_t size = bloom_filter().max_serialized_size();
        // _has_min_max. for backward compatibility.
        size += 1;
        size += min_max_filter().min_max_serialized_size();
        return size;
    }

    size_t serialize(int serialize_version, uint8_t* data) const override {
        size_t offset = 0;
        DCHECK(serialize_version != RF_VERSION);
        auto ltype = to_thrift(Type);
        memcpy(data + offset, &ltype, sizeof(ltype));
        offset += sizeof(ltype);
        offset += _bloom_filter.serialize(serialize_version, data + offset);
        offset += _min_max_filter.serialize_minmax(data + offset);
        return offset;
    }

    size_t deserialize(int serialize_version, const uint8_t* data) override {
        size_t offset = 0;
        DCHECK(serialize_version != RF_VERSION);
        auto ltype = to_thrift(Type);
        memcpy(&ltype, data + offset, sizeof(ltype));
        offset += sizeof(ltype);

        offset += _bloom_filter.deserialize(serialize_version, data + offset);

        bool has_min_max = false;
        memcpy(&has_min_max, data + offset, sizeof(has_min_max));
        offset += sizeof(has_min_max);

        if (has_min_max) {
            offset += min_max_filter().deserialize_minmax(data + offset);
        }

        _has_null = _bloom_filter.has_null();
        if (_has_null) {
            _min_max_filter.insert_null();
        }

        return offset;
    }

    void compute_partition_index(const RuntimeFilterLayout& layout, const std::vector<Column*>& columns,
                                 RunningContext* ctx) const override {
        bloom_filter().compute_partition_index(layout, columns, ctx);
    }

private:
    TRuntimeBloomFilter<Type> _bloom_filter;
    MinMaxRuntimeFilter<Type> _min_max_filter;
};

} // namespace starrocks
