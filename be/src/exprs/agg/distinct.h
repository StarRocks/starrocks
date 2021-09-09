// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/agg/distinct.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <limits>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/hash_set.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/sum.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "udf/udf_internal.h"
#include "util/phmap/phmap_dump.h"

namespace starrocks::vectorized {

enum AggDistinctType { COUNT = 0, SUM = 1 };

template <PrimitiveType PT, typename = guard::Guard>
struct DistinctAggregateState {};

template <PrimitiveType PT>
struct DistinctAggregateState<PT, FixedLengthPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    using SumType = RunTimeCppType<SumResultPT<PT>>;

    size_t update(T key) {
        auto pair = set.insert(key);
        return pair.second * phmap::item_serialize_size<HashSet<T>>::value;
    }

    int64_t disctint_count() const { return set.size(); }

    size_t serialize_size() const { return set.dump_bound(); }

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

template <PrimitiveType PT>
struct DistinctAggregateState<PT, BinaryPTGuard<PT>> {
    DistinctAggregateState() = default;
    using KeyType = typename SliceHashSet::key_type;

    size_t update(MemPool* mem_pool, Slice raw_key) {
        size_t ret = 0;
        KeyType key(raw_key);
        set.template lazy_emplace(key, [&](const auto& ctor) {
            uint8_t* pos = mem_pool->allocate(key.size);
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

template <PrimitiveType PT, AggDistinctType DistinctType, typename T = RunTimeCppType<PT>>
class DistinctAggregateFunction final
        : public AggregateFunctionBatchHelper<DistinctAggregateState<PT>,
                                              DistinctAggregateFunction<PT, DistinctType, T>> {
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

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row_num) const override {
        DCHECK(column->is_binary());
        const auto* input_column = down_cast<const BinaryColumn*>(column);
        Slice slice = input_column->get_slice(row_num);
        size_t mem_usage = 0;
        if constexpr (IsSlice<T>) {
            mem_usage += this->data(state).deserialize_and_merge(ctx->impl()->mem_pool(), (const uint8_t*)slice.data,
                                                                 slice.size);
        } else {
            // slice size larger than 16, means which is a hash set
            if (slice.size > 16) {
                mem_usage += this->data(state).deserialize_and_merge((const uint8_t*)slice.data, slice.size);
            } else {
                T key = *reinterpret_cast<T*>(slice.data);
                mem_usage += this->data(state).update(key);
            }
        }
        ctx->impl()->add_mem_usage(mem_usage);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        auto* column = down_cast<BinaryColumn*>(to);
        size_t old_size = column->get_bytes().size();
        size_t new_size = old_size + this->data(state).serialize_size();
        column->get_bytes().resize(new_size);
        this->data(state).serialize(column->get_bytes().data() + old_size);
        column->get_offset().emplace_back(new_size);
    }

    void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const override {
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

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
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

} // namespace starrocks::vectorized
