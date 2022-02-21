// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/types.h

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

#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_map>

#include "column/datum.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "gen_cpp/segment.pb.h" // for ColumnMetaPB
#include "runtime/mem_pool.h"
#include "storage/collection.h"
#include "storage/olap_common.h"
#include "util/mem_util.hpp"
#include "util/unaligned_access.h"

namespace starrocks {

class TabletColumn;
class TypeInfo;
class ScalarTypeInfo;
using TypeInfoPtr = std::shared_ptr<TypeInfo>;

class TypeInfo {
public:
    using Datum = vectorized::Datum;

    virtual bool equal(const void* left, const void* right) const = 0;
    virtual int cmp(const void* left, const void* right) const = 0;

    virtual void shallow_copy(void* dest, const void* src) const = 0;

    virtual void deep_copy(void* dest, const void* src, MemPool* mem_pool) const = 0;

    // See copy_row_in_memtable() in olap/row.h, will be removed in future.
    // It is same with deep_copy() for all type except for HLL and OBJECT type
    virtual void copy_object(void* dest, const void* src, MemPool* mem_pool) const = 0;

    // The mem_pool is used to allocate memory for array type.
    // The scalar type can copy the value directly
    virtual void direct_copy(void* dest, const void* src, MemPool* mem_pool) const = 0;

    //convert and deep copy value from other type's source
    virtual Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type, MemPool* mem_pool) const = 0;

    virtual Status convert_from(Datum& dest, const Datum& src, const TypeInfoPtr& src_type) const {
        return Status::NotSupported("Not supported function");
    }

    virtual Status from_string(void* buf, const std::string& scan_key) const = 0;

    virtual std::string to_string(const void* src) const = 0;
    virtual void set_to_max(void* buf) const = 0;
    virtual void set_to_min(void* buf) const = 0;

    virtual uint32_t hash_code(const void* data, uint32_t seed) const = 0;
    virtual size_t size() const = 0;

    virtual FieldType type() const = 0;

    virtual int precision() const { return -1; }

    virtual int scale() const { return -1; }

    ////////// Datum-based methods

    Status from_string(vectorized::Datum* buf, const std::string& scan_key) const = delete;
    std::string to_string(const vectorized::Datum& datum) const = delete;

    int cmp(const Datum& left, const Datum& right) const {
        if (left.is_null() || right.is_null()) {
            return right.is_null() - left.is_null();
        }
        return _datum_cmp_impl(left, right);
    }

protected:
    virtual int _datum_cmp_impl(const Datum& left, const Datum& right) const = 0;
};

class ScalarTypeInfo final : public TypeInfo {
public:
    bool equal(const void* left, const void* right) const override { return _equal(left, right); }

    int cmp(const void* left, const void* right) const override { return _cmp(left, right); }

    void shallow_copy(void* dest, const void* src) const override { _shallow_copy(dest, src); }

    void deep_copy(void* dest, const void* src, MemPool* mem_pool) const override { _deep_copy(dest, src, mem_pool); }

    // See copy_row_in_memtable() in olap/row.h, will be removed in future.
    // It is same with deep_copy() for all type except for HLL and OBJECT type
    void copy_object(void* dest, const void* src, MemPool* mem_pool) const override {
        _copy_object(dest, src, mem_pool);
    }

    void direct_copy(void* dest, const void* src, MemPool* mem_pool) const override {
        _direct_copy(dest, src, mem_pool);
    }

    //convert and deep copy value from other type's source
    Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type, MemPool* mem_pool) const override {
        return _convert_from(dest, src, src_type, mem_pool);
    }

    Status from_string(void* buf, const std::string& scan_key) const override { return _from_string(buf, scan_key); }

    std::string to_string(const void* src) const override { return _to_string(src); }

    void set_to_max(void* buf) const override { _set_to_max(buf); }
    void set_to_min(void* buf) const override { _set_to_min(buf); }

    uint32_t hash_code(const void* data, uint32_t seed) const override { return _hash_code(data, seed); }
    size_t size() const override { return _size; }

    FieldType type() const override { return _field_type; }

protected:
    int _datum_cmp_impl(const Datum& left, const Datum& right) const override { return _datum_cmp(left, right); }

private:
    bool (*_equal)(const void* left, const void* right);
    int (*_cmp)(const void* left, const void* right);

    void (*_shallow_copy)(void* dest, const void* src);
    void (*_deep_copy)(void* dest, const void* src, MemPool* mem_pool);
    void (*_copy_object)(void* dest, const void* src, MemPool* mem_pool);
    void (*_direct_copy)(void* dest, const void* src, MemPool* mem_pool);
    Status (*_convert_from)(void* dest, const void* src, const TypeInfoPtr& src_type, MemPool* mem_pool);

    Status (*_from_string)(void* buf, const std::string& scan_key);
    std::string (*_to_string)(const void* src);

    void (*_set_to_max)(void* buf);
    void (*_set_to_min)(void* buf);

    uint32_t (*_hash_code)(const void* data, uint32_t seed);

    // Datum based methods.
    int (*_datum_cmp)(const Datum& left, const Datum& right);

    const size_t _size;
    const FieldType _field_type;

    friend class ScalarTypeInfoResolver;
    template <typename TypeTraitsClass>
    ScalarTypeInfo(TypeTraitsClass t);
};

class ArrayTypeInfo final : public TypeInfo {
public:
    explicit ArrayTypeInfo(const TypeInfoPtr& item_type_info)
            : _item_type_info(item_type_info), _item_size(item_type_info->size()) {}

    bool equal(const void* left, const void* right) const override {
        auto l_value = unaligned_load<Collection>(left);
        auto r_value = unaligned_load<Collection>(right);
        if (l_value.length != r_value.length) {
            return false;
        }
        size_t len = l_value.length;

        if (!l_value.has_null && !r_value.has_null) {
            for (size_t i = 0; i < len; ++i) {
                if (!_item_type_info->equal((uint8_t*)(l_value.data) + i * _item_size,
                                            (uint8_t*)(r_value.data) + i * _item_size)) {
                    return false;
                }
            }
        } else {
            for (size_t i = 0; i < len; ++i) {
                if (l_value.null_signs[i]) {
                    if (r_value.null_signs[i]) { // both are null
                        continue;
                    } else { // left is null & right is not null
                        return false;
                    }
                } else if (r_value.null_signs[i]) { // left is not null & right is null
                    return false;
                }
                if (!_item_type_info->equal((uint8_t*)(l_value.data) + i * _item_size,
                                            (uint8_t*)(r_value.data) + i * _item_size)) {
                    return false;
                }
            }
        }
        return true;
    }

    int cmp(const void* left, const void* right) const override {
        auto l_value = unaligned_load<Collection>(left);
        auto r_value = unaligned_load<Collection>(right);
        size_t l_length = l_value.length;
        size_t r_length = r_value.length;
        size_t cur = 0;

        if (!l_value.has_null && !r_value.has_null) {
            while (cur < l_length && cur < r_length) {
                int result = _item_type_info->cmp((uint8_t*)(l_value.data) + cur * _item_size,
                                                  (uint8_t*)(r_value.data) + cur * _item_size);
                if (result != 0) {
                    return result;
                }
                ++cur;
            }
        } else {
            while (cur < l_length && cur < r_length) {
                if (l_value.null_signs[cur]) {
                    if (!r_value.null_signs[cur]) { // left is null & right is not null
                        return -1;
                    }
                } else if (r_value.null_signs[cur]) { // left is not null & right is null
                    return 1;
                } else { // both are not null
                    int result = _item_type_info->cmp((uint8_t*)(l_value.data) + cur * _item_size,
                                                      (uint8_t*)(r_value.data) + cur * _item_size);
                    if (result != 0) {
                        return result;
                    }
                }
                ++cur;
            }
        }

        if (l_length < r_length) {
            return -1;
        } else if (l_length > r_length) {
            return 1;
        } else {
            return 0;
        }
    }

    void shallow_copy(void* dest, const void* src) const override {
        unaligned_store<Collection>(dest, unaligned_load<Collection>(src));
    }

    void deep_copy(void* dest, const void* src, MemPool* mem_pool) const override {
        Collection dest_value;
        Collection src_value = unaligned_load<Collection>(src);

        dest_value.length = src_value.length;

        size_t item_size = src_value.length * _item_size;
        size_t nulls_size = src_value.has_null ? src_value.length : 0;
        dest_value.data = mem_pool->allocate(item_size + nulls_size);
        assert(dest_value.data != nullptr);
        dest_value.has_null = src_value.has_null;
        dest_value.null_signs = src_value.has_null ? reinterpret_cast<uint8_t*>(dest_value.data) + item_size : nullptr;

        // copy null_signs
        if (src_value.has_null) {
            memory_copy(dest_value.null_signs, src_value.null_signs, sizeof(uint8_t) * src_value.length);
        }

        // copy item
        for (uint32_t i = 0; i < src_value.length; ++i) {
            if (dest_value.is_null_at(i)) {
                Collection* item = reinterpret_cast<Collection*>((uint8_t*)dest_value.data + i * _item_size);
                item->data = nullptr;
                item->length = 0;
                item->has_null = false;
                item->null_signs = nullptr;
            } else {
                _item_type_info->deep_copy((uint8_t*)(dest_value.data) + i * _item_size,
                                           (uint8_t*)(src_value.data) + i * _item_size, mem_pool);
            }
        }
        unaligned_store<Collection>(dest, dest_value);
    }

    void copy_object(void* dest, const void* src, MemPool* mem_pool) const override { deep_copy(dest, src, mem_pool); }

    void direct_copy(void* dest, const void* src, MemPool* mem_pool) const override { deep_copy(dest, src, mem_pool); }

    Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type, MemPool* mem_pool) const override {
        return Status::NotSupported("Not supported function");
    }

    Status from_string(void* buf, const std::string& scan_key) const override {
        return Status::NotSupported("Not supported function");
    }

    std::string to_string(const void* src) const override {
        auto src_value = unaligned_load<Collection>(src);
        std::string result = "[";

        for (size_t i = 0; i < src_value.length; ++i) {
            if (src_value.has_null && src_value.null_signs[i]) {
                result += "NULL";
            } else {
                result += _item_type_info->to_string((uint8_t*)(src_value.data) + i * _item_size);
            }
            if (i != src_value.length - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    }

    void set_to_max(void* buf) const override { DCHECK(false) << "set_to_max of list is not implemented."; }

    void set_to_min(void* buf) const override { DCHECK(false) << "set_to_min of list is not implemented."; }

    uint32_t hash_code(const void* data, uint32_t seed) const override {
        auto value = unaligned_load<Collection>(data);
        uint32_t result = HashUtil::hash(&(value.length), sizeof(size_t), seed);
        for (size_t i = 0; i < value.length; ++i) {
            if (value.null_signs[i]) {
                result = seed * result;
            } else {
                result = seed * result + _item_type_info->hash_code((uint8_t*)(value.data) + i * _item_size, seed);
            }
        }
        return result;
    }

    size_t size() const override { return sizeof(Collection); }

    FieldType type() const override { return OLAP_FIELD_TYPE_ARRAY; }

    const TypeInfoPtr& item_type_info() const { return _item_type_info; }

protected:
    int _datum_cmp_impl(const Datum& left, const Datum& right) const override {
        CHECK(false) << "not implemented";
        return -1;
    }

private:
    TypeInfoPtr _item_type_info;
    const size_t _item_size;
};

// TypeComparator
// static compare functions for performance-critical scenario
template <FieldType ftype>
struct TypeComparator {
    static int cmp(const void* lhs, const void* rhs);
};

bool is_scalar_field_type(FieldType field_type);

bool is_complex_metric_type(FieldType field_type);

const ScalarTypeInfo* get_scalar_type_info(FieldType t);

TypeInfoPtr get_type_info(FieldType field_type);

TypeInfoPtr get_type_info(const ColumnMetaPB& column_meta_pb);

TypeInfoPtr get_type_info(const TabletColumn& col);

TypeInfoPtr get_type_info(FieldType field_type, [[maybe_unused]] int precision, [[maybe_unused]] int scale);

TypeInfoPtr get_type_info(const TypeInfo* type_info);

// CppTypeTraits:
// Infer on-disk type(CppType) from FieldType
template <FieldType field_type>
struct CppTypeTraits {};

template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_BOOL> {
    using CppType = bool;
    using UnsignedCppType = bool;
};

template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_NONE> {
    using CppType = bool;
    using UnsignedCppType = bool;
};

template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_TINYINT> {
    using CppType = int8_t;
    using UnsignedCppType = uint8_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_UNSIGNED_TINYINT> {
    using CppType = uint8_t;
    using UnsignedCppType = uint8_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_SMALLINT> {
    using CppType = int16_t;
    using UnsignedCppType = uint16_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_UNSIGNED_SMALLINT> {
    using CppType = uint16_t;
    using UnsignedCppType = uint16_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_INT> {
    using CppType = int32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_UNSIGNED_INT> {
    using CppType = uint32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_BIGINT> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_UNSIGNED_BIGINT> {
    using CppType = uint64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_LARGEINT> {
    using CppType = int128_t;
    using UnsignedCppType = uint128_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_FLOAT> {
    using CppType = float;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DOUBLE> {
    using CppType = double;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL> {
    using CppType = decimal12_t;
    using UnsignedCppType = decimal12_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL_V2> {
    using CppType = DecimalV2Value;
    using UnsignedCppType = DecimalV2Value;
};

template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL32> {
    using CppType = int32_t;
    using UnsignedCppType = uint32_t;
};

template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL64> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};

template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL128> {
    using CppType = int128_t;
    using UnsignedCppType = uint128_t;
};

template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DATE> {
    using CppType = uint24_t;
    using UnsignedCppType = uint24_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DATE_V2> {
    using CppType = int32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_DATETIME> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_TIMESTAMP> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_CHAR> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_VARCHAR> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_HLL> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_OBJECT> {
    using CppType = Slice;
};

template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_PERCENTILE> {
    using CppType = Slice;
};

template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_JSON> {
    using CppType = Slice;
};

template <>
struct CppTypeTraits<OLAP_FIELD_TYPE_ARRAY> {
    using CppType = Collection;
};

// CppColumnTraits
// Infer ColumnType from FieldType
template <FieldType ftype>
struct CppColumnTraits {
    using CppType = typename CppTypeTraits<ftype>::CppType;
    using ColumnType = typename vectorized::ColumnTraits<CppType>::ColumnType;
};

template <>
struct CppColumnTraits<OLAP_FIELD_TYPE_BOOL> {
    using ColumnType = vectorized::UInt8Column;
};

// deprecated
template <>
struct CppColumnTraits<OLAP_FIELD_TYPE_DATE> {
    using ColumnType = vectorized::FixedLengthColumn<uint24_t>;
};

template <>
struct CppColumnTraits<OLAP_FIELD_TYPE_DATE_V2> {
    using ColumnType = vectorized::DateColumn;
};

template <>
struct CppColumnTraits<OLAP_FIELD_TYPE_TIMESTAMP> {
    using ColumnType = vectorized::TimestampColumn;
};

// deprecated
template <>
struct CppColumnTraits<OLAP_FIELD_TYPE_DECIMAL> {
    using ColumnType = vectorized::FixedLengthColumn<decimal12_t>;
};

template <>
struct CppColumnTraits<OLAP_FIELD_TYPE_HLL> {
    using ColumnType = vectorized::HyperLogLogColumn;
};

template <>
struct CppColumnTraits<OLAP_FIELD_TYPE_PERCENTILE> {
    using ColumnType = vectorized::PercentileColumn;
};

template <>
struct CppColumnTraits<OLAP_FIELD_TYPE_OBJECT> {
    using ColumnType = vectorized::BitmapColumn;
};

template <>
struct CppColumnTraits<OLAP_FIELD_TYPE_UNSIGNED_INT> {
    using ColumnType = vectorized::UInt32Column;
};

template <>
struct CppColumnTraits<OLAP_FIELD_TYPE_JSON> {
    using ColumnType = vectorized::JsonColumn;
};

// Instantiate this template to get static access to the type traits.
template <FieldType field_type>
struct TypeTraits {
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using ColumnType = typename CppColumnTraits<field_type>::ColumnType;

    static const FieldType type = field_type;
    static const int32_t size = sizeof(CppType);
};

} // namespace starrocks
