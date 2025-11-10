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

#include <memory>
#include <optional>
#include <stdexcept>

#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "common/object_pool.h"
#include "exprs/runtime_filter.h"
#include "gutil/port.h"
#include "runtime/mem_pool.h"
#include "util/slice.h"
#include "util/unaligned_access.h"

namespace starrocks {

namespace detail {
struct SliceHashSet : SliceNormalHashSet {
    using Base = SliceNormalHashSet;
    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::empty;
    using Base::size;

    void emplace(const Slice& slice) {
        this->lazy_emplace(slice, [&](const auto& ctor) {
            uint8_t* pos = pool->allocate_with_reserve(slice.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            memcpy(pos, slice.data, slice.size);
            ctor(pos, slice.size);
        });
    }
    std::unique_ptr<MemPool> pool = std::make_unique<MemPool>();
};

template <LogicalType Type, typename Enable = void>
struct LHashSet {
    using LType = HashSet<RunTimeCppType<Type>>;
};

template <LogicalType Type>
struct LHashSet<Type, std::enable_if_t<isSliceLT<Type>>> {
    using LType = SliceHashSet;
};

} // namespace detail

class AbstractInRuntimeFilter : public RuntimeFilter {
public:
    AbstractInRuntimeFilter() = default;
    virtual size_t size() const = 0;
    virtual void clear() = 0;
};

template <LogicalType Type>
class InRuntimeFilter final : public AbstractInRuntimeFilter {
public:
    using CppType = RunTimeCppType<Type>;
    using ColumnType = RunTimeColumnType<Type>;
    using ContainerType = RunTimeProxyContainerType<Type>;
    using HashSet = typename detail::LHashSet<Type>::LType;
    using ScopedPtr = typename butil::DoublyBufferedData<HashSet>::ScopedPtr;

    RuntimeFilterSerializeType type() const override { return RuntimeFilterSerializeType::IN_FILTER; }

    ~InRuntimeFilter() override = default;
    const RuntimeFilter* get_in_filter() const override { return this; }
    const RuntimeFilter* get_min_max_filter() const override { return nullptr; }

    InRuntimeFilter* create_empty(ObjectPool* pool) override { return InRuntimeFilter::create(pool); }

    static InRuntimeFilter* create(ObjectPool* pool) {
        auto rf = new InRuntimeFilter();
        rf->_always_true = true;
        if (pool != nullptr) {
            return pool->add(std::move(rf));
        }
        return rf;
    }

    void build(Column* column) {
        HashSet set;
        DCHECK(!column->is_constant());
        size_t num_rows = column->size();
        if (column->is_nullable()) {
            auto* nullable = down_cast<NullableColumn*>(column);
            const auto& null_data = nullable->null_column_data();
            const auto& data = GetContainer<Type>::get_data(nullable->data_column());
            for (size_t i = 0; i < num_rows; ++i) {
                if (null_data[i]) {
                    this->insert_null();
                } else {
                    set.emplace(data[i]);
                }
            }
        } else {
            const auto& data = GetContainer<Type>::get_data(column);
            for (size_t i = 0; i < num_rows; ++i) {
                set.emplace(data[i]);
            }
        }

        auto update = [&](auto& dst) {
            dst = std::move(set);
            return true;
        };
        _values.Modify(update);
    }

    std::set<CppType> get_set(ObjectPool* pool) const {
        std::set<CppType> set;
        ScopedPtr ptr;
        if (_values.Read(&ptr) != 0) {
            return set;
        }
        const HashSet& hash_set = *ptr;
        if constexpr (IsSlice<CppType>) {
            auto mem_pool = pool->add(new MemPool());
            for (auto slice : hash_set) {
                uint8_t* pos = mem_pool->allocate_with_reserve(slice.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
                memcpy(pos, slice.data, slice.size);
                set.insert(Slice(pos, slice.size));
            }
        } else {
            for (auto v : hash_set) {
                set.insert(v);
            }
        }

        return set;
    }

    void insert_null() { _has_null = true; }

    void merge(const RuntimeFilter* rf) override {
        {
            ScopedPtr ptr;
            if (_values.Read(&ptr) != 0) {
            }
            HashSet& set = const_cast<HashSet&>(*ptr);
            auto* other = down_cast<const InRuntimeFilter*>(rf);
            ScopedPtr other_ptr;
            if (other->_values.Read(&other_ptr) != 0) {
            }
            for (auto& v : *other_ptr) {
                set.emplace(v);
            }
        }
    }

    void intersect(const RuntimeFilter* rf) override { DCHECK(false) << "unsupported"; }

    void concat(RuntimeFilter* rf) override { merge(rf); }

    size_t size() const override {
        ScopedPtr ptr;
        if (_values.Read(&ptr) != 0) {
            // unreachable path
            CHECK(false) << "unreachable path";
        }
        return ptr->size();
    }

    void clear() override {
        auto update = [&](auto& dst) {
            dst = HashSet{};
            return true;
        };
        _values.Modify(update);
    }

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "InRuntimeFilter(";
        ss << "is_not_in = " << _is_not_in << " ";
        ss << "type = " << Type << " ";
        ss << "has_null = " << _has_null << " ";
        return ss.str();
    }

    size_t max_serialized_size() const override {
        size_t serialize_size = sizeof(Type);
        ScopedPtr ptr;
        CHECK(_values.Read(&ptr) == 0);
        if constexpr (IsSlice<CppType>) {
            serialize_size += sizeof(int32_t);
            for (auto slice : *ptr) {
                serialize_size += sizeof(int32_t);
                serialize_size += slice.get_size();
            }
            return serialize_size;
        } else {
            return serialize_size + sizeof(int32_t) + sizeof(CppType) * ptr->size();
        }
    }

    size_t serialize(int serialize_version, uint8_t* data) const override {
        size_t offset = 0;
        auto ltype = to_thrift(Type);
        memcpy(data + offset, &ltype, sizeof(ltype));
        offset += sizeof(ltype);

        ScopedPtr ptr;
        CHECK(_values.Read(&ptr) == 0);

        UNALIGNED_STORE32(data + offset, ptr->size());
        offset += sizeof(int32_t);

        if constexpr (IsSlice<CppType>) {
            for (Slice slice : *ptr) {
                size_t slice_size = slice.get_size();
                unaligned_store<int32_t>(data + offset, slice_size);
                offset += sizeof(int32_t);
                memcpy(data + offset, slice.get_data(), slice_size);
                offset += slice_size;
            }
        } else {
            for (auto element : *ptr) {
                unaligned_store<CppType>(data + offset, element);
                offset += sizeof(CppType);
            }
        }

        return offset;
    }

    size_t deserialize(int serialize_version, const uint8_t* data) override {
        size_t offset = 0;
        HashSet set;
        DCHECK(serialize_version != RF_VERSION);
        auto ltype = to_thrift(Type);
        memcpy(&ltype, data + offset, sizeof(ltype));
        offset += sizeof(ltype);
        int32_t element_size = UNALIGNED_LOAD32(data + offset);
        offset += sizeof(int32_t);
        for (size_t i = 0; i < element_size; ++i) {
            if constexpr (IsSlice<CppType>) {
                int32_t slice_size = unaligned_load<int32_t>(data + offset);
                offset += sizeof(int32_t);
                set.emplace(Slice(data + offset, slice_size));
                offset += slice_size;
            } else {
                CppType val = unaligned_load<CppType>(data + offset);
                offset += sizeof(CppType);
                set.emplace(val);
            }
        }
        auto update = [&](auto& dst) {
            dst = std::move(set);
            return true;
        };
        _values.Modify(update);

        return offset;
    }

    void compute_partition_index(const RuntimeFilterLayout& layout, const std::vector<const Column*>& columns,
                                 RunningContext* ctx) const override {
        throw std::runtime_error("not supported");
    }
    void compute_partition_index(const RuntimeFilterLayout& layout, const std::vector<const Column*>& columns,
                                 uint16_t* sel, uint16_t sel_size, std::vector<uint32_t>& hash_values) const override {
        throw std::runtime_error("not supported");
    }
    void compute_partition_index(const RuntimeFilterLayout& layout, const std::vector<const Column*>& columns,
                                 uint8_t* selection, uint16_t from, uint16_t to,
                                 std::vector<uint32_t>& hash_values) const override {
        throw std::runtime_error("not supported");
    }

    void evaluate(const Column* input_column, RunningContext* ctx) const override {
        throw std::runtime_error("not supported");
    }

    uint16_t evaluate(const Column* input_column, const std::vector<uint32_t>& hash_values, uint16_t* sel,
                      uint16_t sel_size, uint16_t* dst_sel) const override {
        return sel_size;
    }
    void evaluate(const Column* input_column, const std::vector<uint32_t>& hash_values, uint8_t* selection,
                  uint16_t from, uint16_t to) const override {}

private:
    InRuntimeFilter() = default;

    bool _is_not_in = false;
    mutable butil::DoublyBufferedData<HashSet> _values;
};

} // namespace starrocks
