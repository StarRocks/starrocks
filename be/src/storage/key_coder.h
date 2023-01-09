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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/key_coder.h

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

#include <string>
#include <type_traits>

#include "column/datum.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem_pool.h"
#include "storage/type_traits.h"
#include "storage/types.h"
#include "util/bit_util.h"

namespace starrocks {

using strings::Substitute;

using FullEncodeAscendingFunc = void (*)(const void* value, std::string* buf);
using EncodeAscendingFunc = void (*)(const void* value, size_t index_size, std::string* buf);
using DecodeAscendingFunc = Status (*)(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr, MemPool* pool);
using FullEncodeAscendingFuncDatum = void (*)(const Datum& value, std::string* buf);
using EncodeAscendingFuncDatum = void (*)(const Datum& value, size_t index_size, std::string* buf);

// Order-preserving binary encoding for values of a particular type so that
// those values can be compared by memcpy their encoded bytes.
//
// To obtain instance of this class, use the `get_key_coder(LogicalType)` method.
class KeyCoder {
public:
    template <typename TraitsType>
    explicit KeyCoder(TraitsType traits);

    // encode the provided `value` into `buf`.
    void full_encode_ascending(const void* value, std::string* buf) const { _full_encode_ascending(value, buf); }

    void full_encode_ascending(const Datum& value, std::string* buf) const { _full_encode_ascending_datum(value, buf); }

    // similar to `full_encode_ascending`, but only encode part (the first `index_size` bytes) of the value.
    // only applicable to string type
    void encode_ascending(const void* value, size_t index_size, std::string* buf) const {
        _encode_ascending(value, index_size, buf);
    }

    void encode_ascending(const Datum& value, size_t index_size, std::string* buf) const {
        _encode_ascending_datum(value, index_size, buf);
    }

    Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr, MemPool* pool) const {
        return _decode_ascending(encoded_key, index_size, cell_ptr, pool);
    }

private:
    FullEncodeAscendingFunc _full_encode_ascending;
    EncodeAscendingFunc _encode_ascending;
    DecodeAscendingFunc _decode_ascending;
    FullEncodeAscendingFuncDatum _full_encode_ascending_datum;
    EncodeAscendingFuncDatum _encode_ascending_datum;
};

extern const KeyCoder* get_key_coder(LogicalType type);

template <LogicalType field_type, typename Enable = void>
class KeyCoderTraits {};

template <LogicalType field_type>
class KeyCoderTraits<field_type,
                     typename std::enable_if_t<std::is_integral_v<typename CppTypeTraits<field_type>::CppType>>> {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using UnsignedCppType = typename CppTypeTraits<field_type>::UnsignedCppType;

public:
    static void full_encode_ascending(const void* value, std::string* buf) {
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, value, sizeof(unsigned_val));
        // swap MSB to encode integer
        if (std::is_signed<CppType>::value) {
            unsigned_val ^= (static_cast<UnsignedCppType>(1) << (sizeof(UnsignedCppType) * CHAR_BIT - 1));
        }
        unsigned_val = BitUtil::big_endian(unsigned_val);
        buf->append((char*)&unsigned_val, sizeof(unsigned_val));
    }

    static void full_encode_ascending_datum(const Datum& value, std::string* buf) {
        static_assert(field_type != TYPE_DECIMAL && field_type != TYPE_DATE_V1);
        CppType raw = value.get<CppType>();
        full_encode_ascending(&raw, buf);
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        DCHECK_EQ(sizeof(CppType), index_size);
        full_encode_ascending(value, buf);
    }

    static void encode_ascending_datum(const Datum& value, size_t index_size, std::string* buf) {
        DCHECK_EQ(sizeof(CppType), index_size);
        full_encode_ascending_datum(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size __attribute__((unused)), uint8_t* cell_ptr,
                                   MemPool* pool __attribute__((unused))) {
        if (encoded_key->size < sizeof(UnsignedCppType)) {
            return Status::InvalidArgument(
                    Substitute("Key too short, need=$0 vs real=$1", sizeof(UnsignedCppType), encoded_key->size));
        }
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, encoded_key->data, sizeof(UnsignedCppType));
        unsigned_val = BitUtil::big_endian_to_host(unsigned_val);
        if (std::is_signed<CppType>::value) {
            unsigned_val ^= (static_cast<UnsignedCppType>(1) << (sizeof(UnsignedCppType) * CHAR_BIT - 1));
        }
        memcpy(cell_ptr, &unsigned_val, sizeof(UnsignedCppType));
        encoded_key->remove_prefix(sizeof(UnsignedCppType));
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<TYPE_BOOLEAN> {
public:
    using CppType = typename CppTypeTraits<TYPE_BOOLEAN>::CppType;

public:
    static void full_encode_ascending(const void* value, std::string* buf) {
        bool v = *reinterpret_cast<const bool*>(value);
        static_assert(!std::is_signed_v<bool>);
        buf->append((char*)&v, sizeof(v));
    }

    static void full_encode_ascending_datum(const Datum& value, std::string* buf) {
        bool b = static_cast<bool>(value.get_int8());
        full_encode_ascending(&b, buf);
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        DCHECK_EQ(sizeof(CppType), index_size);
        full_encode_ascending(value, buf);
    }

    static void encode_ascending_datum(const Datum& value, size_t index_size, std::string* buf) {
        DCHECK_EQ(sizeof(CppType), index_size);
        full_encode_ascending_datum(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size __attribute__((unused)), uint8_t* cell_ptr,
                                   MemPool* pool __attribute__((unused))) {
        if (encoded_key->size < sizeof(bool)) {
            return Status::InvalidArgument(
                    Substitute("Key too short, need=$0 vs real=$1", sizeof(bool), encoded_key->size));
        }
        bool v = reinterpret_cast<const bool*>(encoded_key->data);
        memcpy(cell_ptr, &v, sizeof(v));
        encoded_key->remove_prefix(sizeof(v));
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<TYPE_DATE_V1> {
public:
    using CppType = typename CppTypeTraits<TYPE_DATE_V1>::CppType;
    using UnsignedCppType = typename CppTypeTraits<TYPE_DATE_V1>::UnsignedCppType;

public:
    static void full_encode_ascending(const void* value, std::string* buf) {
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, value, sizeof(unsigned_val));
        unsigned_val = BigEndian::FromHost24(unsigned_val);
        buf->append((char*)&unsigned_val, sizeof(unsigned_val));
    }

    static void full_encode_ascending_datum(const Datum& datum, std::string* buf) {
        auto value = datum.get_uint24();
        full_encode_ascending(&value, buf);
    }

    static void encode_ascending(const void* value, size_t index_size __attribute__((unused)), std::string* buf) {
        full_encode_ascending(value, buf);
    }

    static void encode_ascending_datum(const Datum& value, size_t index_size, std::string* buf) {
        DCHECK_EQ(index_size, sizeof(CppType));
        full_encode_ascending_datum(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size __attribute__((unused)), uint8_t* cell_ptr,
                                   MemPool* pool __attribute__((unused))) {
        if (encoded_key->size < sizeof(UnsignedCppType)) {
            return Status::InvalidArgument(
                    Substitute("Key too short, need=$0 vs real=$1", sizeof(UnsignedCppType), encoded_key->size));
        }
        UnsignedCppType unsigned_val;
        memcpy(&unsigned_val, encoded_key->data, sizeof(UnsignedCppType));
        unsigned_val = BigEndian::ToHost24(unsigned_val);
        memcpy(cell_ptr, &unsigned_val, sizeof(UnsignedCppType));
        encoded_key->remove_prefix(sizeof(UnsignedCppType));
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<TYPE_DECIMAL> {
public:
    static void full_encode_ascending(const void* value, std::string* buf) {
        decimal12_t decimal_val;
        memcpy((void*)&decimal_val, value, sizeof(decimal12_t));
        KeyCoderTraits<TYPE_BIGINT>::full_encode_ascending(&decimal_val.integer, buf);
        KeyCoderTraits<TYPE_INT>::full_encode_ascending(&decimal_val.fraction, buf);
    }

    static void full_encode_ascending_datum(const Datum& datum, std::string* buf) {
        auto value = datum.get_decimal12();
        full_encode_ascending(&value, buf);
    }

    static void encode_ascending(const void* value, size_t index_size __attribute__((unused)), std::string* buf) {
        full_encode_ascending(value, buf);
    }

    static void encode_ascending_datum(const Datum& value, size_t index_size, std::string* buf) {
        DCHECK_EQ(sizeof(decimal12_t), index_size);
        full_encode_ascending_datum(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size __attribute__((unused)), uint8_t* cell_ptr,
                                   MemPool* pool) {
        decimal12_t decimal_val;
        RETURN_IF_ERROR(KeyCoderTraits<TYPE_BIGINT>::decode_ascending(encoded_key, sizeof(decimal_val.integer),
                                                                      (uint8_t*)&decimal_val.integer, pool));
        RETURN_IF_ERROR(KeyCoderTraits<TYPE_INT>::decode_ascending(encoded_key, sizeof(decimal_val.fraction),
                                                                   (uint8_t*)&decimal_val.fraction, pool));
        memcpy(cell_ptr, &decimal_val, sizeof(decimal12_t));
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<TYPE_DECIMALV2> {
public:
    static void full_encode_ascending(const void* value, std::string* buf) {
        KeyCoderTraits<TYPE_LARGEINT>::full_encode_ascending(value, buf);
    }

    static void full_encode_ascending_datum(const Datum& value, std::string* buf) {
        // NOTE: datum store `DECIMAL` as DecimalV2Value but the CppType is decimal12_t.
        const DecimalV2Value& v2 = value.get_decimal();
        KeyCoderTraits<TYPE_LARGEINT>::full_encode_ascending(&v2, buf);
    }

    static void encode_ascending(const void* value, size_t index_size __attribute__((unused)), std::string* buf) {
        full_encode_ascending(value, buf);
    }

    static void encode_ascending_datum(const Datum& value, size_t index_size, std::string* buf) {
        DCHECK_EQ(sizeof(DecimalV2Value), index_size);
        full_encode_ascending_datum(value, buf);
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size __attribute__((unused)), uint8_t* cell_ptr,
                                   MemPool* pool) {
        RETURN_IF_ERROR(KeyCoderTraits<TYPE_LARGEINT>::decode_ascending(encoded_key, index_size, cell_ptr, pool));
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<TYPE_DECIMAL32> : KeyCoderTraits<TYPE_INT> {};

template <>
class KeyCoderTraits<TYPE_DECIMAL64> : KeyCoderTraits<TYPE_BIGINT> {};

template <>
class KeyCoderTraits<TYPE_DECIMAL128> : KeyCoderTraits<TYPE_LARGEINT> {};

template <>
class KeyCoderTraits<TYPE_CHAR> {
public:
    static void full_encode_ascending(const void* value, std::string* buf) {
        auto slice = reinterpret_cast<const Slice*>(value);
        buf->append(slice->get_data(), slice->get_size());
    }

    static void full_encode_ascending_datum(const Datum& value, std::string* buf) {
        const Slice& slice = value.get_slice();
        buf->append(slice.get_data(), slice.get_size());
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        const auto* slice = (const Slice*)value;
        DCHECK_LE(index_size, slice->size);
        buf->append(slice->data, index_size);
    }

    static void encode_ascending_datum(const Datum& value, size_t index_size, std::string* buf) {
        const Slice& slice = value.get_slice();
        buf->append(slice.data, slice.size);
        size_t pad = index_size > slice.size ? index_size - slice.size : 0;
        buf->append(pad, '\0');
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr, MemPool* pool) {
        if (encoded_key->size < index_size) {
            return Status::InvalidArgument(
                    Substitute("Key too short, need=$0 vs real=$1", index_size, encoded_key->size));
        }
        auto* slice = (Slice*)cell_ptr;
        slice->data = (char*)pool->allocate(index_size);
        RETURN_IF_UNLIKELY_NULL(slice->data, Status::MemoryAllocFailed("alloc mem for key decoder failed"));
        slice->size = index_size;
        memcpy(slice->data, encoded_key->data, index_size);
        encoded_key->remove_prefix(index_size);
        return Status::OK();
    }
};

template <>
class KeyCoderTraits<TYPE_VARCHAR> {
public:
    static void full_encode_ascending(const void* value, std::string* buf) {
        auto slice = reinterpret_cast<const Slice*>(value);
        buf->append(slice->get_data(), slice->get_size());
    }

    static void full_encode_ascending_datum(const Datum& value, std::string* buf) {
        const Slice& slice = value.get_slice();
        buf->append(slice.get_data(), slice.get_size());
    }

    static void encode_ascending(const void* value, size_t index_size, std::string* buf) {
        const auto* slice = (const Slice*)value;
        size_t copy_size = std::min(index_size, slice->size);
        buf->append(slice->data, copy_size);
    }

    static void encode_ascending_datum(const Datum& value, size_t index_size, std::string* buf) {
        const Slice& slice = value.get_slice();
        buf->append(slice.data, std::min(index_size, slice.size));
    }

    static Status decode_ascending(Slice* encoded_key, size_t index_size, uint8_t* cell_ptr, MemPool* pool) {
        DCHECK(encoded_key->size <= index_size)
                << "encoded_key size is larger than index_size, key_size=" << encoded_key->size
                << ", index_size=" << index_size;
        auto copy_size = encoded_key->size;
        auto* slice = (Slice*)cell_ptr;
        slice->data = (char*)pool->allocate(copy_size);
        RETURN_IF_UNLIKELY_NULL(slice->data, Status::MemoryAllocFailed("alloc mem for key decoder failed"));
        slice->size = copy_size;
        memcpy(slice->data, encoded_key->data, copy_size);
        encoded_key->remove_prefix(copy_size);
        return Status::OK();
    }
};

} // namespace starrocks
