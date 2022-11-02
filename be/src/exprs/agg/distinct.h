// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <algorithm>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/hash_set.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/sum.h"
#include "gen_cpp/Data_types.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "thrift/protocol/TJSONProtocol.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"
#include "util/phmap/phmap_dump.h"
#include "util/slice.h"

namespace starrocks::vectorized {

enum AggDistinctType { COUNT = 0, SUM = 1 };

static const size_t MIN_SIZE_OF_HASH_SET_SERIALIZED_DATA = 24;

template <PrimitiveType PT, PrimitiveType SumPT, typename = guard::Guard>
struct DistinctAggregateState {};

template <PrimitiveType PT, PrimitiveType SumPT>
struct DistinctAggregateState<PT, SumPT, FixedLengthPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    using SumType = RunTimeCppType<SumPT>;

    size_t update(T key) {
        auto pair = set.insert(key);
        return pair.second * phmap::item_serialize_size<HashSet<T>>::value;
    }

    size_t update_with_hash([[maybe_unused]] MemPool* mempool, T key, size_t hash) {
        auto pair = set.emplace_with_hash(hash, key);
        return pair.second * phmap::item_serialize_size<HashSet<T>>::value;
    }

    void prefetch(T key) { set.prefetch(key); }

    int64_t disctint_count() const { return set.size(); }

    size_t serialize_size() const {
        size_t size = set.dump_bound();
        DCHECK(size >= MIN_SIZE_OF_HASH_SET_SERIALIZED_DATA);
        return size;
    }

    void serialize(uint8_t* dst) const {
        phmap::InMemoryOutput output(reinterpret_cast<char*>(dst));
        set.dump(output);
        DCHECK(output.length() == set.dump_bound());
    }

    size_t deserialize_and_merge(const uint8_t* src, size_t len) {
        phmap::InMemoryInput input(reinterpret_cast<const char*>(src));
        auto old_size = set.size();
        if (old_size == 0) {
            set.load(input);
            return set.size() * phmap::item_serialize_size<HashSet<T>>::value;
        } else {
            HashSet<T> set_src;
            set_src.load(input);
            set.merge(set_src);
            return (set.size() - old_size) * phmap::item_serialize_size<HashSet<T>>::value;
        }
    }

    SumType sum_distinct() const {
        SumType sum{};
        // Sum distinct doesn't support timestamp and date type
        if constexpr (IsDateTime<SumType>) {
            return sum;
        }

        for (auto& key : set) {
            sum += key;
        }
        return sum;
    }

    HashSet<T> set;
};

template <PrimitiveType PT, PrimitiveType SumPT>
struct DistinctAggregateState<PT, SumPT, BinaryPTGuard<PT>> {
    DistinctAggregateState() = default;
    using KeyType = typename SliceHashSet::key_type;

    size_t update(MemPool* mem_pool, Slice raw_key) {
        size_t ret = 0;
        KeyType key(raw_key);
        set.template lazy_emplace(key, [&](const auto& ctor) {
            uint8_t* pos = mem_pool->allocate(key.size);
            assert(pos != nullptr);
            memcpy(pos, key.data, key.size);
            ctor(pos, key.size, key.hash);
            ret = phmap::item_serialize_size<SliceHashSet>::value;
        });
        return ret;
    }

    size_t update_with_hash(MemPool* mem_pool, Slice raw_key, size_t hash) {
        size_t ret = 0;
        KeyType key(reinterpret_cast<uint8_t*>(raw_key.data), raw_key.size, hash);
        set.template lazy_emplace_with_hash(key, hash, [&](const auto& ctor) {
            uint8_t* pos = mem_pool->allocate(key.size);
            assert(pos != nullptr);
            memcpy(pos, key.data, key.size);
            ctor(pos, key.size, key.hash);
            ret = phmap::item_serialize_size<SliceHashSet>::value;
        });
        return ret;
    }

    int64_t disctint_count() const { return set.size(); }

    size_t serialize_size() const {
        size_t size = 0;
        for (auto& key : set) {
            size += key.size + sizeof(uint32_t);
        }
        return size;
    }

    // TODO(kks): If we put all string key to one continue memory,
    // then we could only one memcpy.
    void serialize(uint8_t* dst) const {
        for (auto& key : set) {
            uint32_t size = (uint32_t)key.size;
            memcpy(dst, &size, sizeof(uint32_t));
            dst += sizeof(uint32_t);
            memcpy(dst, key.data, key.size);
            dst += key.size;
        }
    }

    size_t deserialize_and_merge(MemPool* mem_pool, const uint8_t* src, size_t len) {
        size_t mem_usage = 0;
        const uint8_t* end = src + len;
        while (src < end) {
            uint32_t size = 0;
            memcpy(&size, src, sizeof(uint32_t));
            src += sizeof(uint32_t);
            Slice raw_key(src, size);
            KeyType key(raw_key);
            // we only memcpy when the key is new
            set.template lazy_emplace(key, [&](const auto& ctor) {
                uint8_t* pos = mem_pool->allocate(key.size);
                assert(pos != nullptr);
                memcpy(pos, key.data, key.size);
                ctor(pos, key.size, key.hash);
                mem_usage += phmap::item_serialize_size<SliceHashSet>::value;
            });
            src += size;
        }
        DCHECK(src == end);
        return mem_usage;
    }

    SliceHashSet set;
};

// use a different way to do serialization to gain performance.
template <PrimitiveType PT, PrimitiveType SumPT, typename = guard::Guard>
struct DistinctAggregateStateV2 {};

template <PrimitiveType PT, PrimitiveType SumPT>
struct DistinctAggregateStateV2<PT, SumPT, FixedLengthPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    using SumType = RunTimeCppType<SumPT>;
    using MyHashSet = HashSet<T>;
    static constexpr size_t item_size = phmap::item_serialize_size<MyHashSet>::value;

    size_t update(T key) {
        auto pair = set.insert(key);
        return pair.second * item_size;
    }

    size_t update_with_hash([[maybe_unused]] MemPool* mempool, T key, size_t hash) {
        auto pair = set.emplace_with_hash(hash, key);
        return pair.second * item_size;
    }

    void prefetch(T key) { set.prefetch(key); }

    int64_t disctint_count() const { return set.size(); }

    size_t serialize_size() const {
        size_t size = set.size() * sizeof(T) + sizeof(size_t);
        size = std::max(size, MIN_SIZE_OF_HASH_SET_SERIALIZED_DATA);
        return size;
    }

    void serialize(uint8_t* dst) const {
        size_t size = set.size();
        memcpy(dst, &size, sizeof(size));
        dst += sizeof(size);
        for (auto& key : set) {
            memcpy(dst, &key, sizeof(key));
            dst += sizeof(T);
        }
    }

    size_t deserialize_and_merge(const uint8_t* src, size_t len) {
        size_t size = 0;
        memcpy(&size, src, sizeof(size));
        set.rehash(set.size() + size);

        size_t old_size = set.size();
        src += sizeof(size);
        for (size_t i = 0; i < size; i++) {
            T key;
            memcpy(&key, src, sizeof(T));
            set.insert(key);
            src += sizeof(T);
        }
        size_t new_size = set.size();
        return (new_size - old_size) * item_size;
    }

    SumType sum_distinct() const {
        SumType sum{};
        // Sum distinct doesn't support timestamp and date type
        if constexpr (IsDateTime<SumType>) {
            return sum;
        }

        for (auto& key : set) {
            sum += key;
        }
        return sum;
    }

    ~DistinctAggregateStateV2() {
#ifdef PHMAP_USE_CUSTOM_INFO_HANDLE
        const auto& info = set.infoz();
        VLOG_FILE << "HashSet Info: # of insert = " << info.insert_number
                  << ", insert probe length = " << info.insert_probe_length << ", # of rehash = " << info.rehash_number;
#endif
    }

    MyHashSet set;
};

template <PrimitiveType PT, PrimitiveType SumPT>
struct DistinctAggregateStateV2<PT, SumPT, BinaryPTGuard<PT>> : public DistinctAggregateState<PT, SumPT> {};

// Dear god this template class as template parameter kills me!
template <PrimitiveType PT, PrimitiveType SumPT,
          template <PrimitiveType X, PrimitiveType Y, typename = guard::Guard> class TDistinctAggState,
          AggDistinctType DistinctType, typename T = RunTimeCppType<PT>>
class TDistinctAggregateFunction : public AggregateFunctionBatchHelper<
                                           TDistinctAggState<PT, SumPT>,
                                           TDistinctAggregateFunction<PT, SumPT, TDistinctAggState, DistinctType, T>> {
public:
    using ColumnType = RunTimeColumnType<PT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        const ColumnType* column = down_cast<const ColumnType*>(columns[0]);
        size_t mem_usage;
        if constexpr (IsSlice<T>) {
            mem_usage = this->data(state).update(ctx->impl()->mem_pool(), column->get_slice(row_num));
        } else {
            mem_usage = this->data(state).update(column->get_data()[row_num]);
        }
        ctx->impl()->add_mem_usage(mem_usage);
    }

    // The following two functions are specialized because of performance issue.
    // We have found out that by precomputing and prefetching hash values, we can boost peformance of hash table by a lot.
    // And this is a quite useful pattern for phmap::flat_hash_table.
    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        const ColumnType* column = down_cast<const ColumnType*>(columns[0]);
        size_t mem_usage = 0;
        auto& agg_state = this->data(state);

        struct CacheEntry {
            size_t hash_value;
        };

        std::vector<CacheEntry> cache(chunk_size);
        const auto& container_data = column->get_data();
        for (size_t i = 0; i < chunk_size; ++i) {
            size_t hash_value = agg_state.set.hash_function()(container_data[i]);
            cache[i] = CacheEntry{hash_value};
        }
        // This is just an empirical value based on benchmark, and you can tweak it if more proper value is found.
        size_t prefetch_index = 16;

        MemPool* mem_pool = ctx->impl()->mem_pool();
        for (size_t i = 0; i < chunk_size; ++i) {
            if (prefetch_index < chunk_size) {
                agg_state.set.prefetch_hash(cache[prefetch_index].hash_value);
                prefetch_index++;
            }
            mem_usage += agg_state.update_with_hash(mem_pool, container_data[i], cache[i].hash_value);
        }
        ctx->impl()->add_mem_usage(mem_usage);
    }

    void update_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        const ColumnType* column = down_cast<const ColumnType*>(columns[0]);
        size_t mem_usage = 0;

        // We find that agg_states are scatterd in `states`, we can collect them together with hash value,
        // so there will be good cache locality. We can also collect column data into this `CacheEntry` to
        // exploit cache locality further, but I don't see much steady performance gain by doing that.
        struct CacheEntry {
            TDistinctAggState<PT, SumPT>* agg_state;
            size_t hash_value;
        };

        std::vector<CacheEntry> cache(chunk_size);
        const auto& container_data = column->get_data();
        for (size_t i = 0; i < chunk_size; ++i) {
            AggDataPtr state = states[i] + state_offset;
            auto& agg_state = this->data(state);
            size_t hash_value = agg_state.set.hash_function()(container_data[i]);
            cache[i] = CacheEntry{&agg_state, hash_value};
        }
        // This is just an empirical value based on benchmark, and you can tweak it if more proper value is found.
        size_t prefetch_index = 16;

        MemPool* mem_pool = ctx->impl()->mem_pool();
        for (size_t i = 0; i < chunk_size; ++i) {
            if (prefetch_index < chunk_size) {
                cache[prefetch_index].agg_state->set.prefetch_hash(cache[prefetch_index].hash_value);
                prefetch_index++;
            }
            mem_usage += cache[i].agg_state->update_with_hash(mem_pool, container_data[i], cache[i].hash_value);
        }
        ctx->impl()->add_mem_usage(mem_usage);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        const auto* input_column = down_cast<const BinaryColumn*>(column);
        Slice slice = input_column->get_slice(row_num);
        size_t mem_usage = 0;
        if constexpr (IsSlice<T>) {
            mem_usage += this->data(state).deserialize_and_merge(ctx->impl()->mem_pool(), (const uint8_t*)slice.data,
                                                                 slice.size);
        } else {
            // slice size larger than `MIN_SIZE_OF_HASH_SET_SERIALIZED_DATA`, means which is a hash set
            // that's said, size of hash set serialization data should be larger than `MIN_SIZE_OF_HASH_SET_SERIALIZED_DATA`
            // otherwise this slice could be reinterpreted as a single value going be to inserted into hashset.
            if (slice.size >= MIN_SIZE_OF_HASH_SET_SERIALIZED_DATA) {
                mem_usage += this->data(state).deserialize_and_merge((const uint8_t*)slice.data, slice.size);
            } else {
                T key;
                memcpy(&key, slice.data, sizeof(T));
                mem_usage += this->data(state).update(key);
            }
        }
        ctx->impl()->add_mem_usage(mem_usage);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* column = down_cast<BinaryColumn*>(to);
        size_t old_size = column->get_bytes().size();
        size_t new_size = old_size + this->data(state).serialize_size();
        column->get_bytes().resize(new_size);
        this->data(state).serialize(column->get_bytes().data() + old_size);
        column->get_offset().emplace_back(new_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK((*dst)->is_binary());
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = dst_column->get_bytes();

        const ColumnType* src_column = down_cast<const ColumnType*>(src[0].get());
        if constexpr (IsSlice<T>) {
            bytes.reserve(chunk_size * (sizeof(uint32_t) + src_column->get_slice(0).size));
        } else {
            bytes.reserve(chunk_size * sizeof(T));
        }
        dst_column->get_offset().resize(chunk_size + 1);

        size_t old_size = bytes.size();
        for (size_t i = 0; i < chunk_size; ++i) {
            if constexpr (IsSlice<T>) {
                Slice key = src_column->get_slice(i);
                size_t new_size = old_size + key.size + sizeof(uint32_t);
                bytes.resize(new_size);

                uint32_t size = (uint32_t)key.size;
                memcpy(bytes.data() + old_size, &size, sizeof(uint32_t));
                old_size += sizeof(uint32_t);
                memcpy(bytes.data() + old_size, key.data, key.size);
                old_size += key.size;
                dst_column->get_offset()[i + 1] = new_size;
            } else {
                T key = src_column->get_data()[i];

                size_t new_size = old_size + sizeof(T);
                bytes.resize(new_size);
                memcpy(bytes.data() + old_size, &key, sizeof(T));

                dst_column->get_offset()[i + 1] = new_size;
                old_size = new_size;
            }
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable());
        if constexpr (DistinctType == AggDistinctType::COUNT) {
            down_cast<Int64Column*>(to)->append(this->data(state).disctint_count());
        } else if constexpr (DistinctType == AggDistinctType::SUM && is_starrocks_arithmetic<T>::value) {
            to->append_datum(Datum(this->data(state).sum_distinct()));
        }
    }

    std::string get_name() const override {
        if constexpr (DistinctType == AggDistinctType::COUNT) {
            return "count-distinct";
        } else {
            return "sum-distinct";
        }
    }
};

template <PrimitiveType PT, AggDistinctType DistinctType, typename T = RunTimeCppType<PT>>
class DistinctAggregateFunction
        : public TDistinctAggregateFunction<PT, SumResultPT<PT>, DistinctAggregateState, DistinctType, T> {};

template <PrimitiveType PT, AggDistinctType DistinctType, typename T = RunTimeCppType<PT>>
class DistinctAggregateFunctionV2
        : public TDistinctAggregateFunction<PT, SumResultPT<PT>, DistinctAggregateStateV2, DistinctType, T> {};

template <PrimitiveType PT, AggDistinctType DistinctType, typename T = RunTimeCppType<PT>>
class DecimalDistinctAggregateFunction
        : public TDistinctAggregateFunction<PT, TYPE_DECIMAL128, DistinctAggregateStateV2, DistinctType, T> {};

// now we only support String
struct DictMergeState : DistinctAggregateStateV2<TYPE_VARCHAR, SumResultPT<TYPE_VARCHAR>> {
    DictMergeState() = default;

    bool over_limit = false;
};

class DictMergeAggregateFunction final
        : public AggregateFunctionBatchHelper<DictMergeState, DictMergeAggregateFunction> {
public:
    static constexpr int DICT_DECODE_MAX_SIZE = 256;
    static constexpr int FAKE_DICT_SIZE = DICT_DECODE_MAX_SIZE + 1;

    DictMergeState fake_dict_state(FunctionContext* ctx) const {
        DictMergeState fake_state;
        for (int i = 0; i < FAKE_DICT_SIZE; ++i) {
            fake_state.update(ctx->impl()->mem_pool(), std::to_string(i));
        }
        return fake_state;
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(false) << "this method shouldn't be called";
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        [[maybe_unused]] size_t mem_usage = 0;
        auto& agg_state = this->data(state);
        const auto* column = down_cast<const ArrayColumn*>(columns[0]);
        MemPool* mem_pool = ctx->impl()->mem_pool();

        // if dict size greater than DICT_DECODE_MAX_SIZE. we return a FAKE dictionary
        if (agg_state.over_limit) {
            return;
        }

        const auto& elements_column = column->elements();
        if (column->elements().is_nullable()) {
            const auto& null_column = down_cast<const NullableColumn&>(elements_column);
            const auto& null_data = null_column.immutable_null_column_data();
            const auto& binary_column = down_cast<const BinaryColumn&>(null_column.data_column_ref());

            for (size_t i = 0; i < binary_column.size(); ++i) {
                if (!null_data[i]) {
                    mem_usage += agg_state.update(mem_pool, binary_column.get_slice(i));
                }
            }
            agg_state.over_limit = agg_state.set.size() > DICT_DECODE_MAX_SIZE;
        } else {
            const auto& binary_column = down_cast<const BinaryColumn&>(elements_column);
            for (size_t i = 0; i < binary_column.size(); ++i) {
                mem_usage += agg_state.update(mem_pool, binary_column.get_slice(i));
            }
            agg_state.over_limit = agg_state.set.size() > DICT_DECODE_MAX_SIZE;
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        auto& agg_state = this->data(state);

        if (agg_state.over_limit) {
            return;
        }

        const auto* input_column = down_cast<const BinaryColumn*>(column);
        Slice slice = input_column->get_slice(row_num);

        size_t mem_usage = 0;
        mem_usage += this->data(state).deserialize_and_merge(ctx->impl()->mem_pool(), (const uint8_t*)slice.data,
                                                             slice.size);
        ctx->impl()->add_mem_usage(mem_usage);

        agg_state.over_limit = agg_state.set.size() > DICT_DECODE_MAX_SIZE;
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& agg_state = this->data(state);

        auto serialize = [=](const DictMergeState& dict_state) {
            auto* column = down_cast<BinaryColumn*>(to);
            size_t old_size = column->get_bytes().size();
            size_t new_size = old_size + dict_state.serialize_size();
            column->get_bytes().resize(new_size);
            dict_state.serialize(column->get_bytes().data() + old_size);
            column->get_offset().emplace_back(new_size);
        };

        if (agg_state.over_limit) {
            serialize(fake_dict_state(ctx));
        } else {
            serialize(this->data(state));
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK(false) << "this method shouldn't be called";
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& agg_state = this->data(state);

        auto finalize = [](const DictMergeState& agg_state, Column* to) {
            if (agg_state.set.size() == 0) {
                to->append_default();
                return;
            }
            std::vector<int32_t> dict_ids;
            dict_ids.resize(agg_state.set.size());

            auto* binary_column = down_cast<BinaryColumn*>(to);

            // set dict_ids as [1...n]
            for (int i = 0; i < dict_ids.size(); ++i) {
                dict_ids[i] = i + 1;
            }

            TGlobalDict tglobal_dict;
            tglobal_dict.__isset.ids = true;
            tglobal_dict.ids = std::move(dict_ids);
            tglobal_dict.__isset.strings = true;
            tglobal_dict.strings.reserve(dict_ids.size());

            for (const auto& v : agg_state.set) {
                tglobal_dict.strings.emplace_back(v.data, v.size);
            }

            // Since the id in global dictionary may be used for sorting,
            // we also need to ensure that the dictionary is ordered when we build it

            Slice::Comparator comparator;
            std::sort(tglobal_dict.strings.begin(), tglobal_dict.strings.end(), comparator);

            std::string result_value = apache::thrift::ThriftJSONString(tglobal_dict);

            size_t old_size = binary_column->get_bytes().size();
            size_t new_size = old_size + result_value.size();

            auto& data = binary_column->get_bytes();
            data.resize(old_size + new_size);

            memcpy(data.data() + old_size, result_value.data(), result_value.size());

            binary_column->get_offset().emplace_back(new_size);
        };

        if (agg_state.over_limit) {
            finalize(fake_dict_state(ctx), to);
        } else {
            finalize(agg_state, to);
        }
    }

    std::string get_name() const override { return "dict_merge"; }
};

} // namespace starrocks::vectorized
