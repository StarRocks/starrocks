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

#ifndef STARROCKS_BE_SRC_OLAP_TYPES_H
#define STARROCKS_BE_SRC_OLAP_TYPES_H

#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_map>

#include "column/datum.h"
#include "gen_cpp/segment_v2.pb.h" // for ColumnMetaPB
#include "gutil/strings/numbers.h"
#include "runtime/date_value.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/mem_pool.h"
#include "runtime/vectorized/time_types.h"
#include "storage/collection.h"
#include "storage/decimal12.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/tablet_schema.h" // for TabletColumn
#include "storage/uint24.h"
#include "storage/vectorized/convert_helper.h"
#include "util/hash_util.hpp"
#include "util/mem_util.hpp"
#include "util/slice.h"
#include "util/string_parser.hpp"
#include "util/types.h"
#include "util/unaligned_access.h"

namespace starrocks {
class TabletColumn;

class TypeInfo;
class ScalarTypeInfo;
using TypeInfoPtr = std::shared_ptr<TypeInfo>;

class ScalarTypeInfoResolver {
    DECLARE_SINGLETON(ScalarTypeInfoResolver);

public:
    const TypeInfoPtr get_type_info(const FieldType t);

    const ScalarTypeInfo* get_scalar_type_info(const FieldType t);

private:
    template <FieldType field_type>
    void add_mapping();

    // item_type_info -> list_type_info
    std::unordered_map<FieldType, std::unique_ptr<ScalarTypeInfo>, std::hash<size_t>> _mapping;

    ScalarTypeInfoResolver(const ScalarTypeInfoResolver&) = delete;
    const ScalarTypeInfoResolver& operator=(const ScalarTypeInfoResolver&) = delete;
};

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
    virtual OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                                    MemPool* mem_pool) const = 0;

    virtual OLAPStatus convert_from(Datum& dest, const Datum& src, const TypeInfoPtr& src_type) const {
        return OLAPStatus::OLAP_ERR_FUNC_NOT_IMPLEMENTED;
    }

    virtual OLAPStatus from_string(void* buf, const std::string& scan_key) const = 0;

    virtual std::string to_string(const void* src) const = 0;
    virtual void set_to_max(void* buf) const = 0;
    virtual void set_to_min(void* buf) const = 0;

    virtual uint32_t hash_code(const void* data, uint32_t seed) const = 0;
    virtual size_t size() const = 0;

    virtual FieldType type() const = 0;

    virtual int precision() const { return -1; }

    virtual int scale() const { return -1; }

    ////////// Datum-based methods

    OLAPStatus from_string(vectorized::Datum* buf, const std::string& scan_key) const = delete;
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
    OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                            MemPool* mem_pool) const override {
        return _convert_from(dest, src, src_type, mem_pool);
    }

    OLAPStatus from_string(void* buf, const std::string& scan_key) const override {
        return _from_string(buf, scan_key);
    }

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
    OLAPStatus (*_convert_from)(void* dest, const void* src, const TypeInfoPtr& src_type, MemPool* mem_pool);

    OLAPStatus (*_from_string)(void* buf, const std::string& scan_key);
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

    OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                            MemPool* mem_pool) const override {
        return OLAPStatus::OLAP_ERR_FUNC_NOT_IMPLEMENTED;
    }

    OLAPStatus from_string(void* buf, const std::string& scan_key) const override {
        return OLAPStatus::OLAP_ERR_FUNC_NOT_IMPLEMENTED;
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

bool is_scalar_type(FieldType field_type);

bool is_complex_metric_type(FieldType field_type);

const ScalarTypeInfo* get_scalar_type_info(FieldType t);

TypeInfoPtr get_type_info(FieldType field_type);

TypeInfoPtr get_type_info(const segment_v2::ColumnMetaPB& column_meta_pb);

TypeInfoPtr get_type_info(const TabletColumn& col);

TypeInfoPtr get_type_info(FieldType field_type, [[maybe_unused]] int precision, [[maybe_unused]] int scale);
TypeInfoPtr get_type_info(const TypeInfo* type_info);
// NOLINTNEXTLINE
static const std::vector<std::string> DATE_FORMATS{
        "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d", "%y/%m/%d",
};

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
struct CppTypeTraits<OLAP_FIELD_TYPE_SMALLINT> {
    using CppType = int16_t;
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
struct CppTypeTraits<OLAP_FIELD_TYPE_ARRAY> {
    using CppType = Collection;
};

template <FieldType field_type>
struct BaseFieldtypeTraits : public CppTypeTraits<field_type> {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    static bool equal(const void* left, const void* right) {
        CppType l_value = unaligned_load<CppType>(left);
        CppType r_value = unaligned_load<CppType>(right);
        return l_value == r_value;
    }

    static int cmp(const void* left, const void* right) {
        CppType left_int = unaligned_load<CppType>(left);
        CppType right_int = unaligned_load<CppType>(right);
        if (left_int < right_int) {
            return -1;
        } else if (left_int > right_int) {
            return 1;
        } else {
            return 0;
        }
    }

    static void shallow_copy(void* dest, const void* src) {
        unaligned_store<CppType>(dest, unaligned_load<CppType>(src));
    }

    static void deep_copy(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        unaligned_store<CppType>(dest, unaligned_load<CppType>(src));
    }

    static void copy_object(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        unaligned_store<CppType>(dest, unaligned_load<CppType>(src));
    }

    static void direct_copy(void* dest, const void* src, MemPool* mem_pool) {
        unaligned_store<CppType>(dest, unaligned_load<CppType>(src));
    }

    static OLAPStatus convert_from(void* dest __attribute__((unused)), const void* src __attribute__((unused)),
                                   const TypeInfoPtr& src_type __attribute__((unused)),
                                   MemPool* mem_pool __attribute__((unused))) {
        return OLAPStatus::OLAP_ERR_FUNC_NOT_IMPLEMENTED;
    }

    static void set_to_max(void* buf) { unaligned_store<CppType>(buf, std::numeric_limits<CppType>::max()); }

    static void set_to_min(void* buf) { unaligned_store<CppType>(buf, std::numeric_limits<CppType>::lowest()); }

    static uint32_t hash_code(const void* data, uint32_t seed) { return HashUtil::hash(data, sizeof(CppType), seed); }

    static std::string to_string(const void* src) {
        std::stringstream stream;
        stream << unaligned_load<CppType>(src);
        return stream.str();
    }

    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType value = 0;
        if (scan_key.length() > 0) {
            value = static_cast<CppType>(strtol(scan_key.c_str(), nullptr, 10));
        }
        unaligned_store<CppType>(buf, value);
        return OLAP_SUCCESS;
    }

    static int datum_cmp(const vectorized::Datum& left, const vectorized::Datum& right) {
        CppType v1 = left.get<CppType>();
        CppType v2 = right.get<CppType>();
        return (v1 < v2) ? -1 : (v2 < v1) ? 1 : 0;
    }
};

template <typename T>
OLAPStatus convert_int_from_varchar(void* dest, const void* src) {
    using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_VARCHAR>::CppType;
    auto src_value = unaligned_load<SrcType>(src);
    StringParser::ParseResult parse_res;
    T result = StringParser::string_to_int<T>(src_value.get_data(), src_value.get_size(), &parse_res);
    if (UNLIKELY(parse_res != StringParser::PARSE_SUCCESS)) {
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
    memcpy(dest, &result, sizeof(T));
    return OLAPStatus::OLAP_SUCCESS;
}

template <typename T>
OLAPStatus convert_float_from_varchar(void* dest, const void* src) {
    using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_VARCHAR>::CppType;
    auto src_value = unaligned_load<SrcType>(src);
    StringParser::ParseResult parse_res;
    T result = StringParser::string_to_float<T>(src_value.get_data(), src_value.get_size(), &parse_res);
    if (UNLIKELY(parse_res != StringParser::PARSE_SUCCESS)) {
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
    unaligned_store<T>(dest, result);
    return OLAPStatus::OLAP_SUCCESS;
}

template <FieldType field_type>
struct FieldTypeTraits : public BaseFieldtypeTraits<field_type> {};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_BOOL> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_BOOL> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", *reinterpret_cast<const bool*>(src));
        return std::string(buf);
    }

    static void set_to_max(void* buf) { (*(bool*)buf) = true; }
    static void set_to_min(void* buf) { (*(bool*)buf) = false; }

    static int datum_cmp(const vectorized::Datum& left, const vectorized::Datum& right) {
        uint8_t v1 = left.get<uint8_t>();
        uint8_t v2 = right.get<uint8_t>();
        return (v1 < v2) ? -1 : (v2 < v1) ? 1 : 0;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_NONE> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_NONE> {};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_TINYINT> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_TINYINT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", *reinterpret_cast<const int8_t*>(src));
        return std::string(buf);
    }

    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                                   MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_int_from_varchar<CppType>(dest, src);
        }
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_SMALLINT> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_SMALLINT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", unaligned_load<int16_t>(src));
        return std::string(buf);
    }
    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                                   MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_int_from_varchar<CppType>(dest, src);
        }
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_INT> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_INT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", unaligned_load<int32_t>(src));
        return std::string(buf);
    }
    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                                   MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_int_from_varchar<CppType>(dest, src);
        }
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_BIGINT> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_BIGINT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%" PRId64, unaligned_load<int64_t>(src));
        return std::string(buf);
    }
    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                                   MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_int_from_varchar<CppType>(dest, src);
        }
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_LARGEINT> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_LARGEINT> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        int128_t value;

        const char* value_string = scan_key.c_str();
        char* end = nullptr;
        value = strtol(value_string, &end, 10);
        if (*end != 0) {
            value = 0;
        } else if (value > LONG_MIN && value < LONG_MAX) {
            // use strtol result directly
        } else {
            bool is_negative = false;
            if (*value_string == '-' || *value_string == '+') {
                if (*(value_string++) == '-') {
                    is_negative = true;
                }
            }

            uint128_t current = 0;
            uint128_t max_int128 = ~((int128_t)(1) << 127);
            while (*value_string != 0) {
                if (current > max_int128 / 10) {
                    break;
                }

                current = current * 10 + (*(value_string++) - '0');
            }
            if (*value_string != 0 || (!is_negative && current > max_int128) ||
                (is_negative && current > max_int128 + 1)) {
                current = 0;
            }

            value = is_negative ? -current : current;
        }
        unaligned_store<int128_t>(buf, value);
        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        char buf[1024];
        int128_t value = unaligned_load<int128_t>(src);
        if (value >= std::numeric_limits<int64_t>::lowest() && value <= std::numeric_limits<int64_t>::max()) {
            snprintf(buf, sizeof(buf), "%" PRId64, (int64_t)value);
        } else {
            char* current = buf;
            uint128_t abs_value = value;
            if (value < 0) {
                *(current++) = '-';
                abs_value = -value;
            }

            // the max value of uint64_t is 18446744073709551615UL,
            // so use Z19_UINT64 to divide uint128_t
            const static uint64_t Z19_UINT64 = 10000000000000000000ULL;
            uint64_t suffix = abs_value % Z19_UINT64;
            uint64_t middle = abs_value / Z19_UINT64 % Z19_UINT64;
            uint64_t prefix = abs_value / Z19_UINT64 / Z19_UINT64;

            char* end = buf + sizeof(buf);
            if (prefix > 0) {
                current += snprintf(current, end - current, "%" PRIu64, prefix);
                current += snprintf(current, end - current, "%.19" PRIu64, middle);
                current += snprintf(current, end - current, "%.19" PRIu64, suffix);
                (void)current; // avoid unused value warning.
            } else if (OLAP_LIKELY(middle > 0)) {
                current += snprintf(current, end - current, "%" PRIu64, middle);
                current += snprintf(current, end - current, "%.19" PRIu64, suffix);
                (void)current; // avoid unused value warning.
            } else {
                current += snprintf(current, end - current, "%" PRIu64, suffix);
                (void)current; // avoid unused value warning.
            }
        }

        return std::string(buf);
    }

    // GCC7.3 will generate movaps instruction, which will lead to SEGV when buf is
    // not aligned to 16 byte
    static void shallow_copy(void* dest, const void* src) {
        unaligned_store<int128_t>(dest, unaligned_load<int128_t>(src));
    }
    static void deep_copy(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        unaligned_store<int128_t>(dest, unaligned_load<int128_t>(src));
    }

    static void copy_object(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        unaligned_store<int128_t>(dest, unaligned_load<int128_t>(src));
    }
    static void direct_copy(void* dest, const void* src, MemPool* mem_pool) {
        unaligned_store<int128_t>(dest, unaligned_load<int128_t>(src));
    }
    static void set_to_max(void* buf) { unaligned_store<int128_t>(buf, ~((int128_t)(1) << 127)); }
    static void set_to_min(void* buf) { unaligned_store<int128_t>(buf, (int128_t)(1) << 127); }
    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                                   MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_int_from_varchar<CppType>(dest, src);
        }
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }

    static int datum_cmp(const vectorized::Datum& left, const vectorized::Datum& right) {
        const int128_t& v1 = left.get_int128();
        const int128_t& v2 = right.get_int128();
        return (v1 < v2) ? -1 : (v2 < v1) ? 1 : 0;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_FLOAT> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_FLOAT> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType value = 0.0f;
        if (scan_key.length() > 0) {
            value = static_cast<CppType>(atof(scan_key.c_str()));
        }
        unaligned_store<CppType>(buf, value);
        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        int length = FloatToBuffer(unaligned_load<CppType>(src), MAX_FLOAT_STR_LENGTH, buf);
        DCHECK(length >= 0) << "gcvt float failed, float value=" << unaligned_load<CppType>(src);
        return std::string(buf);
    }
    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                                   MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_float_from_varchar<CppType>(dest, src);
        }
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DOUBLE> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DOUBLE> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType value = 0.0;
        if (scan_key.length() > 0) {
            value = atof(scan_key.c_str());
        }
        unaligned_store<CppType>(buf, value);
        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        int length = DoubleToBuffer(unaligned_load<CppType>(src), MAX_DOUBLE_STR_LENGTH, buf);
        DCHECK(length >= 0) << "gcvt float failed, float value=" << unaligned_load<CppType>(src);
        return std::string(buf);
    }
    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                                   MemPool* mem_pool __attribute__((unused))) {
        //only support float now
        if (src_type->type() == OLAP_FIELD_TYPE_FLOAT) {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_FLOAT>::CppType;
            //http://www.softelectro.ru/ieee754_en.html
            //According to the definition of IEEE754, the effect of converting a float binary to a double binary
            //is the same as that of static_cast . Data precision cannot be guaranteed, but the progress of
            //decimal system can be guaranteed by converting a float to a char buffer and then to a double.
            //float v2 = static_cast<double>(v1),
            //float 0.3000000 is: 0 | 01111101 | 00110011001100110011010
            //double 0.300000011920929 is: 0 | 01111111101 | 0000000000000000000001000000000000000000000000000000
            //==float to char buffer to strtod==
            //float 0.3000000 is: 0 | 01111101 | 00110011001100110011010
            //double 0.300000000000000 is: 0 | 01111111101 | 0011001100110011001100110011001100110011001100110011
            char buf[64] = {0};
            snprintf(buf, 64, "%f", unaligned_load<SrcType>(src));
            char* tg;
            unaligned_store<CppType>(dest, strtod(buf, &tg));
            return OLAPStatus::OLAP_SUCCESS;
        }
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_float_from_varchar<CppType>(dest, src);
        }
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DECIMAL> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DECIMAL> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType t;
        auto r = t.from_string(scan_key);
        if (r != OLAP_SUCCESS) {
            return r;
        }
        unaligned_store<CppType>(buf, t);
        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        CppType t = unaligned_load<CppType>(src);
        return t.to_string();
    }
    static void set_to_max(void* buf) {
        CppType data;
        data.integer = 999999999999999999L;
        data.fraction = 999999999;
        unaligned_store<CppType>(buf, data);
    }
    static void set_to_min(void* buf) {
        CppType data;
        data.integer = -999999999999999999;
        data.fraction = -999999999;
        unaligned_store<CppType>(buf, data);
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DECIMAL_V2> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DECIMAL_V2> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        CppType val;
        if (val.parse_from_str(scan_key.c_str(), scan_key.size()) != E_DEC_OK) {
            return OLAP_ERR_INVALID_SCHEMA;
        }
        unaligned_store<CppType>(buf, val);
        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        CppType tmp = unaligned_load<CppType>(src);
        return tmp.to_string();
    }
    // GCC7.3 will generate movaps instruction, which will lead to SEGV when buf is
    // not aligned to 16 byte
    static void shallow_copy(void* dest, const void* src) { memcpy(dest, src, sizeof(CppType)); }
    static void deep_copy(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        memcpy(dest, src, sizeof(CppType));
    }

    static void copy_object(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        memcpy(dest, src, sizeof(CppType));
    }
    static void direct_copy(void* dest, const void* src, MemPool* mem_pool) { memcpy(dest, src, sizeof(CppType)); }

    static void set_to_max(void* buf) {
        CppType v;
        v.from_olap_decimal(999999999999999999L, 999999999);
        unaligned_store<CppType>(buf, v);
    }
    static void set_to_min(void* buf) {
        CppType v;
        v.from_olap_decimal(-999999999999999999L, -999999999);
        unaligned_store<CppType>(buf, v);
    }

    static int datum_cmp(const vectorized::Datum& left, const vectorized::Datum& right) {
        const DecimalV2Value& v1 = left.get_decimal();
        const DecimalV2Value& v2 = right.get_decimal();
        return (v1 < v2) ? -1 : (v1 > v2) ? 1 : 0;
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DATE> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DATE> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        tm time_tm;
        char* res = strptime(scan_key.c_str(), "%Y-%m-%d", &time_tm);

        if (nullptr != res) {
            int value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
            unaligned_store<CppType>(buf, value);
        } else {
            // 1400 - 01 - 01
            unaligned_store<CppType>(buf, 716833);
        }

        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        CppType v = unaligned_load<CppType>(src);
        return v.to_string();
    }

    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                                   MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_DATETIME) {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_DATETIME>::CppType;
            auto src_value = unaligned_load<SrcType>(src);
            //only need part one
            SrcType part1 = (src_value / 1000000L);
            CppType year = static_cast<CppType>((part1 / 10000L) % 10000);
            CppType mon = static_cast<CppType>((part1 / 100) % 100);
            CppType mday = static_cast<CppType>(part1 % 100);
            unaligned_store<CppType>(dest, (year << 9) + (mon << 5) + mday);
            return OLAPStatus::OLAP_SUCCESS;
        }

        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_TIMESTAMP) {
            int year, month, day, hour, minute, second, usec;
            auto src_value = unaligned_load<vectorized::TimestampValue>(src);
            src_value.to_timestamp(&year, &month, &day, &hour, &minute, &second, &usec);
            unaligned_store<CppType>(dest, (year << 9) + (month << 5) + day);
            return OLAPStatus::OLAP_SUCCESS;
        }

        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_INT) {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_INT>::CppType;
            SrcType src_value = unaligned_load<SrcType>(src);
            DateTimeValue dt;
            if (!dt.from_date_int64(src_value)) {
                return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
            }
            CppType year = static_cast<CppType>(src_value / 10000);
            CppType month = static_cast<CppType>((src_value % 10000) / 100);
            CppType day = static_cast<CppType>(src_value % 100);
            unaligned_store<CppType>(dest, (year << 9) + (month << 5) + day);
            return OLAPStatus::OLAP_SUCCESS;
        }

        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_VARCHAR>::CppType;
            auto src_value = unaligned_load<SrcType>(src);
            DateTimeValue dt;
            for (const auto& format : DATE_FORMATS) {
                if (dt.from_date_format_str(format.c_str(), format.length(), src_value.get_data(),
                                            src_value.get_size())) {
                    unaligned_store<CppType>(dest, (dt.year() << 9) + (dt.month() << 5) + dt.day());
                    return OLAPStatus::OLAP_SUCCESS;
                }
            }
            return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
        }

        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
    static void set_to_max(void* buf) {
        // max is 9999 * 16 * 32 + 12 * 32 + 31;
        unaligned_store<CppType>(buf, 5119903);
    }
    static void set_to_min(void* buf) {
        // min is 0 * 16 * 32 + 1 * 32 + 1;
        unaligned_store<CppType>(buf, 33);
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DATE_V2> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DATE_V2> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        vectorized::DateValue date;
        if (!date.from_string(scan_key.data(), scan_key.size())) {
            // Compatible with OLAP_FIELD_TYPE_DATE
            date.from_string("1400-01-01", sizeof("1400-01-01") - 1);
        }
        unaligned_store<vectorized::DateValue>(buf, date);
        return OLAP_SUCCESS;
    }

    static std::string to_string(const void* src) {
        auto src_val = unaligned_load<vectorized::DateValue>(src);
        return src_val.to_string();
    }

    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                                   MemPool* mem_pool __attribute__((unused))) {
        auto converter = vectorized::get_type_converter(src_type->type(), OLAP_FIELD_TYPE_DATE_V2);
        auto st = converter->convert(dest, src, mem_pool);
        if (st.ok()) {
            return OLAP_SUCCESS;
        }
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
    static void set_to_max(void* buf) {
        // max is 9999 * 16 * 32 + 12 * 32 + 31;
        unaligned_store<CppType>(buf, vectorized::date::MAX_DATE);
    }
    static void set_to_min(void* buf) {
        // min is 0 * 16 * 32 + 1 * 32 + 1;
        unaligned_store<CppType>(buf, vectorized::date::MIN_DATE);
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_DATETIME> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_DATETIME> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        tm time_tm;
        char* res = strptime(scan_key.c_str(), "%Y-%m-%d %H:%M:%S", &time_tm);

        if (nullptr != res) {
            CppType value =
                    ((time_tm.tm_year + 1900) * 10000L + (time_tm.tm_mon + 1) * 100L + time_tm.tm_mday) * 1000000L +
                    time_tm.tm_hour * 10000L + time_tm.tm_min * 100L + time_tm.tm_sec;
            unaligned_store<CppType>(buf, value);
        } else {
            // 1400 - 01 - 01
            unaligned_store<CppType>(buf, 14000101000000L);
        }

        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        tm time_tm;
        CppType tmp = unaligned_load<CppType>(src);
        CppType part1 = (tmp / 1000000L);
        CppType part2 = (tmp - part1 * 1000000L);

        time_tm.tm_year = static_cast<int>((part1 / 10000L) % 10000) - 1900;
        time_tm.tm_mon = static_cast<int>((part1 / 100) % 100) - 1;
        time_tm.tm_mday = static_cast<int>(part1 % 100);

        time_tm.tm_hour = static_cast<int>((part2 / 10000L) % 10000);
        time_tm.tm_min = static_cast<int>((part2 / 100) % 100);
        time_tm.tm_sec = static_cast<int>(part2 % 100);

        char buf[20] = {'\0'};
        strftime(buf, 20, "%Y-%m-%d %H:%M:%S", &time_tm);
        return std::string(buf);
    }

    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                                   MemPool* memPool __attribute__((unused))) {
        // when convert date to datetime, automatic padding zero
        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_DATE) {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_DATE>::CppType;
            auto value = unaligned_load<SrcType>(src);
            int day = static_cast<int>(value & 31);
            int mon = static_cast<int>(value >> 5 & 15);
            int year = static_cast<int>(value >> 9);
            unaligned_store<CppType>(dest, (year * 10000L + mon * 100L + day) * 1000000);
            return OLAPStatus::OLAP_SUCCESS;
        }

        // when convert date to datetime, automatic padding zero
        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_DATE_V2) {
            auto src_value = unaligned_load<vectorized::DateValue>(src);
            int year, month, day;
            src_value.to_date(&year, &month, &day);
            unaligned_store<CppType>(dest, (year * 10000L + month * 100L + day) * 1000000);
            return OLAPStatus::OLAP_SUCCESS;
        }

        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
    static void set_to_max(void* buf) { unaligned_store<CppType>(buf, 99991231235959L); }
    static void set_to_min(void* buf) { unaligned_store<CppType>(buf, 101000000); }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_TIMESTAMP> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_TIMESTAMP> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        auto timestamp = unaligned_load<vectorized::TimestampValue>(buf);
        if (!timestamp.from_string(scan_key.data(), scan_key.size())) {
            // Compatible with OLAP_FIELD_TYPE_DATETIME
            timestamp.from_string("1400-01-01 00:00:00", sizeof("1400-01-01 00:00:00") - 1);
        }
        unaligned_store<vectorized::TimestampValue>(buf, timestamp);
        return OLAP_SUCCESS;
    }

    static std::string to_string(const void* src) {
        auto timestamp = unaligned_load<vectorized::TimestampValue>(src);
        return timestamp.to_string();
    }

    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type, MemPool* mem_pool) {
        vectorized::TimestampValue value;
        auto converter = vectorized::get_type_converter(src_type->type(), OLAP_FIELD_TYPE_TIMESTAMP);
        auto st = converter->convert(&value, src, mem_pool);
        unaligned_store<vectorized::TimestampValue>(dest, value);
        if (st.ok()) {
            return OLAP_SUCCESS;
        }
        return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
    }
    static void set_to_max(void* buf) { unaligned_store<CppType>(buf, vectorized::timestamp::MAX_TIMESTAMP); }
    static void set_to_min(void* buf) { unaligned_store<CppType>(buf, vectorized::timestamp::MIN_TIMESTAMP); }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_CHAR> : public BaseFieldtypeTraits<OLAP_FIELD_TYPE_CHAR> {
    static bool equal(const void* left, const void* right) {
        auto l_slice = unaligned_load<Slice>(left);
        auto r_slice = unaligned_load<Slice>(right);
        return l_slice == r_slice;
    }
    static int cmp(const void* left, const void* right) {
        auto l_slice = unaligned_load<Slice>(left);
        auto r_slice = unaligned_load<Slice>(right);
        return l_slice.compare(r_slice);
    }
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > OLAP_STRING_MAX_LENGTH) {
            LOG(WARNING) << "the len of value string is too long, len=" << value_len
                         << ", max_len=" << OLAP_STRING_MAX_LENGTH;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        auto slice = unaligned_load<Slice>(buf);
        memory_copy(slice.data, scan_key.c_str(), value_len);
        if (slice.size < value_len) {
            /*
             * CHAR type is of fixed length. Size in slice can be modified
             * only if value_len is greater than the fixed length. ScanKey
             * inputed by user may be greater than fixed length.
             */
            slice.size = value_len;
            unaligned_store<Slice>(buf, slice);
        } else {
            // append \0 to the tail
            memset(slice.data + value_len, 0, slice.size - value_len);
        }
        return OLAP_SUCCESS;
    }
    static std::string to_string(const void* src) {
        auto slice = unaligned_load<Slice>(src);
        return slice.to_string();
    }
    static void deep_copy(void* dest, const void* src, MemPool* mem_pool) {
        Slice l_slice = unaligned_load<Slice>(dest);
        Slice r_slice = unaligned_load<Slice>(src);
        l_slice.data = reinterpret_cast<char*>(mem_pool->allocate(r_slice.size));
        memory_copy(l_slice.data, r_slice.data, r_slice.size);
        l_slice.size = r_slice.size;
        unaligned_store<Slice>(dest, l_slice);
    }

    static void copy_object(void* dest, const void* src, MemPool* mem_pool) { deep_copy(dest, src, mem_pool); }

    static void direct_copy(void* dest, const void* src, MemPool* mem_pool) {
        Slice l_slice = unaligned_load<Slice>(dest);
        Slice r_slice = unaligned_load<Slice>(src);
        memory_copy(l_slice.data, r_slice.data, r_slice.size);
        l_slice.size = r_slice.size;
        unaligned_store<Slice>(dest, l_slice);
    }

    // using field.set_to_max to set varchar/char,not here
    static void (*set_to_max)(void*);

    static void set_to_min(void* buf) {
        auto slice = unaligned_load<Slice>(buf);
        memset(slice.data, 0, slice.size);
    }
    static uint32_t hash_code(const void* data, uint32_t seed) {
        auto slice = unaligned_load<Slice>(data);
        return HashUtil::hash(slice.data, slice.size, seed);
    }

    static int datum_cmp(const vectorized::Datum& left, const vectorized::Datum& right) {
        const Slice& v1 = left.get_slice();
        const Slice& v2 = right.get_slice();
        return v1.compare(v2);
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_VARCHAR> : public FieldTypeTraits<OLAP_FIELD_TYPE_CHAR> {
    static OLAPStatus from_string(void* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > OLAP_STRING_MAX_LENGTH) {
            LOG(WARNING) << "the len of value string is too long, len=" << value_len
                         << ", max_len=" << OLAP_STRING_MAX_LENGTH;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        Slice slice = unaligned_load<Slice>(buf);
        memory_copy(slice.data, scan_key.c_str(), value_len);
        slice.size = value_len;
        unaligned_store<Slice>(buf, slice);
        return OLAP_SUCCESS;
    }

    static OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type, MemPool* mem_pool) {
        if (src_type->type() == OLAP_FIELD_TYPE_TINYINT || src_type->type() == OLAP_FIELD_TYPE_SMALLINT ||
            src_type->type() == OLAP_FIELD_TYPE_INT || src_type->type() == OLAP_FIELD_TYPE_BIGINT ||
            src_type->type() == OLAP_FIELD_TYPE_LARGEINT || src_type->type() == OLAP_FIELD_TYPE_FLOAT ||
            src_type->type() == OLAP_FIELD_TYPE_DOUBLE || src_type->type() == OLAP_FIELD_TYPE_DECIMAL ||
            src_type->type() == OLAP_FIELD_TYPE_DECIMAL_V2 || src_type->type() == OLAP_FIELD_TYPE_DECIMAL32 ||
            src_type->type() == OLAP_FIELD_TYPE_DECIMAL64 || src_type->type() == OLAP_FIELD_TYPE_DECIMAL128) {
            auto result = src_type->to_string(src);
            auto slice = reinterpret_cast<Slice*>(dest);
            slice->data = reinterpret_cast<char*>(mem_pool->allocate(result.size()));
            if (UNLIKELY(slice->data == nullptr)) {
                return OLAP_ERR_MALLOC_ERROR;
            }
            memcpy(slice->data, result.c_str(), result.size());
            slice->size = result.size();
            return OLAP_SUCCESS;
        }
        return OLAP_ERR_INVALID_SCHEMA;
    }

    static void set_to_min(void* buf) {
        auto slice = reinterpret_cast<Slice*>(buf);
        slice->size = 0;
    }

    static int datum_cmp(const vectorized::Datum& left, const vectorized::Datum& right) {
        const Slice& v1 = left.get_slice();
        const Slice& v2 = right.get_slice();
        return v1.compare(v2);
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_HLL> : public FieldTypeTraits<OLAP_FIELD_TYPE_VARCHAR> {
    /*
     * Hyperloglog type only used as value, so
     * cmp/from_string/set_to_max/set_to_min function
     * in this struct has no significance
     */

    // See copy_row_in_memtable() in olap/row.h, will be removed in future.
    static void copy_object(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        Slice dst_slice;
        Slice src_slice = unaligned_load<Slice>(src);
        DCHECK_EQ(src_slice.size, 0);
        dst_slice.data = src_slice.data;
        dst_slice.size = 0;
        unaligned_store<Slice>(dest, dst_slice);
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_OBJECT> : public FieldTypeTraits<OLAP_FIELD_TYPE_VARCHAR> {
    /*
     * Object type only used as value, so
     * cmp/from_string/set_to_max/set_to_min function
     * in this struct has no significance
     */

    // See copy_row_in_memtable() in olap/row.h, will be removed in future.
    static void copy_object(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        Slice dst_slice;
        Slice src_slice = unaligned_load<Slice>(src);
        DCHECK_EQ(src_slice.size, 0);
        dst_slice.data = src_slice.data;
        dst_slice.size = 0;
        unaligned_store<Slice>(dest, dst_slice);
    }
};

template <>
struct FieldTypeTraits<OLAP_FIELD_TYPE_PERCENTILE> : public FieldTypeTraits<OLAP_FIELD_TYPE_VARCHAR> {
    // See copy_row_in_memtable() in olap/row.h, will be removed in future.
    static void copy_object(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        auto dst_slice = reinterpret_cast<Slice*>(dest);
        auto src_slice = reinterpret_cast<const Slice*>(src);
        DCHECK_EQ(src_slice->size, 0);
        dst_slice->data = src_slice->data;
        dst_slice->size = 0;
    }
};

// Instantiate this template to get static access to the type traits.
template <FieldType field_type>
struct TypeTraits : public FieldTypeTraits<field_type> {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    static const FieldType type = field_type;
    static const int32_t size = sizeof(CppType);
};

} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_TYPES_H
