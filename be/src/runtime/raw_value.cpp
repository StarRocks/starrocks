// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/raw_value.cpp

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

#include "runtime/raw_value.h"

#include <sstream>

#include "runtime/datetime_value.h"
#include "runtime/decimal_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/large_int_value.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.h"
#include "runtime/string_value.hpp"
#include "storage/utils.h"
#include "util/hash_util.hpp"
#include "util/unaligned_access.h"

namespace starrocks {

const int RawValue::ASCII_PRECISION = 16; // print 16 digits for double/float

void RawValue::print_value_as_bytes(const void* value, const TypeDescriptor& type, std::stringstream* stream) {
    if (value == nullptr) {
        return;
    }

    const char* chars = reinterpret_cast<const char*>(value);
    const StringValue* string_val = nullptr;

    switch (type.type) {
    case TYPE_NULL:
        break;
    case TYPE_BOOLEAN:
        stream->write(chars, sizeof(bool));
        return;

    case TYPE_TINYINT:
        stream->write(chars, sizeof(int8_t));
        break;

    case TYPE_SMALLINT:
        stream->write(chars, sizeof(int16_t));
        break;

    case TYPE_INT:
        stream->write(chars, sizeof(int32_t));
        break;

    case TYPE_BIGINT:
        stream->write(chars, sizeof(int64_t));
        break;

    case TYPE_FLOAT:
        stream->write(chars, sizeof(float));
        break;

    case TYPE_DOUBLE:
        stream->write(chars, sizeof(double));
        break;

    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_CHAR:
        string_val = reinterpret_cast<const StringValue*>(value);
        stream->write(static_cast<char*>(string_val->ptr), string_val->len);
        return;

    case TYPE_DATE:
    case TYPE_DATETIME:
        stream->write(chars, sizeof(DateTimeValue));
        break;

    case TYPE_DECIMAL:
        stream->write(chars, sizeof(DecimalValue));
        break;

    case TYPE_DECIMALV2:
        stream->write(chars, sizeof(DecimalV2Value));
        break;

    case TYPE_LARGEINT:
        stream->write(chars, sizeof(__int128));
        break;

    default:
        DCHECK(false) << "bad RawValue::print_value() type: " << type;
    }
}

void RawValue::print_value(const void* value, const TypeDescriptor& type, int scale, std::stringstream* stream) {
    if (value == nullptr) {
        *stream << "NULL";
        return;
    }

    int old_precision = stream->precision();
    std::ios_base::fmtflags old_flags = stream->flags();

    if (scale > -1) {
        stream->precision(scale);
        // Setting 'fixed' causes precision to set the number of digits printed after the
        // decimal (by default it sets the maximum number of digits total).
        *stream << std::fixed;
    }

    std::string tmp;
    const StringValue* string_val = nullptr;

    switch (type.type) {
    case TYPE_BOOLEAN: {
        bool val = *reinterpret_cast<const bool*>(value);
        *stream << (val ? "true" : "false");
        return;
    }

    case TYPE_TINYINT:
        // Extra casting for chars since they should not be interpreted as ASCII.
        *stream << static_cast<int>(*reinterpret_cast<const int8_t*>(value));
        break;

    case TYPE_SMALLINT:
        *stream << *reinterpret_cast<const int16_t*>(value);
        break;

    case TYPE_INT:
        *stream << *reinterpret_cast<const int32_t*>(value);
        break;

    case TYPE_BIGINT:
        *stream << *reinterpret_cast<const int64_t*>(value);
        break;

    case TYPE_FLOAT:
        *stream << *reinterpret_cast<const float*>(value);
        break;

    case TYPE_DOUBLE:
        *stream << *reinterpret_cast<const double*>(value);
        break;
    case TYPE_HLL:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        string_val = reinterpret_cast<const StringValue*>(value);
        tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
        *stream << tmp;
        return;

    case TYPE_DATE:
    case TYPE_DATETIME:
        *stream << *reinterpret_cast<const DateTimeValue*>(value);
        break;

    case TYPE_DECIMAL:
        *stream << *reinterpret_cast<const DecimalValue*>(value);
        break;

    case TYPE_DECIMALV2:
        *stream << unaligned_load<int128_t>(value);
        break;

    case TYPE_LARGEINT:
        *stream << unaligned_load<int128_t>(value);
        break;

    default:
        DCHECK(false) << "bad RawValue::print_value() type: " << type;
    }

    stream->precision(old_precision);
    // Undo setting stream to fixed
    stream->flags(old_flags);
}

void RawValue::print_value(const void* value, const TypeDescriptor& type, int scale, std::string* str) {
    if (value == nullptr) {
        *str = "NULL";
        return;
    }

    std::stringstream out;
    out.precision(ASCII_PRECISION);
    const StringValue* string_val = nullptr;
    std::string tmp;
    bool val = false;

    // Special case types that we can print more efficiently without using a std::stringstream
    switch (type.type) {
    case TYPE_BOOLEAN:
        val = *reinterpret_cast<const bool*>(value);
        *str = (val ? "true" : "false");
        return;

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_OBJECT:
    case TYPE_PERCENTILE:
    case TYPE_HLL: {
        string_val = reinterpret_cast<const StringValue*>(value);
        std::stringstream ss;
        ss << "ptr:" << (void*)string_val->ptr << " len" << string_val->len;
        tmp = ss.str();
        if (string_val->len <= 1000) {
            tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
        }
        str->swap(tmp);
        return;
    }
    case TYPE_NULL: {
        *str = "NULL";
        return;
    }
    default:
        print_value(value, type, scale, &out);
    }

    *str = out.str();
}

void RawValue::write(const void* value, void* dst, const TypeDescriptor& type, MemPool* pool) {
    DCHECK(value != nullptr);

    switch (type.type) {
    case TYPE_NULL:
        break;
    case TYPE_BOOLEAN: {
        *reinterpret_cast<bool*>(dst) = *reinterpret_cast<const bool*>(value);
        break;
    }

    case TYPE_TINYINT: {
        *reinterpret_cast<int8_t*>(dst) = *reinterpret_cast<const int8_t*>(value);
        break;
    }

    case TYPE_SMALLINT: {
        *reinterpret_cast<int16_t*>(dst) = *reinterpret_cast<const int16_t*>(value);
        break;
    }

    case TYPE_INT: {
        *reinterpret_cast<int32_t*>(dst) = *reinterpret_cast<const int32_t*>(value);
        break;
    }

    case TYPE_BIGINT: {
        *reinterpret_cast<int64_t*>(dst) = *reinterpret_cast<const int64_t*>(value);
        break;
    }

    case TYPE_LARGEINT: {
        int128_t tmp = unaligned_load<int128_t>(value);
        unaligned_store<int128_t>(dst, tmp);
        break;
    }

    case TYPE_FLOAT: {
        *reinterpret_cast<float*>(dst) = *reinterpret_cast<const float*>(value);
        break;
    }

    case TYPE_TIME:
    case TYPE_DOUBLE: {
        *reinterpret_cast<double*>(dst) = *reinterpret_cast<const double*>(value);
        break;
    }

    case TYPE_DATE:
    case TYPE_DATETIME:
        *reinterpret_cast<DateTimeValue*>(dst) = *reinterpret_cast<const DateTimeValue*>(value);
        break;

    case TYPE_DECIMAL:
        *reinterpret_cast<DecimalValue*>(dst) = *reinterpret_cast<const DecimalValue*>(value);
        break;

    case TYPE_DECIMALV2: {
        int128_t tmp = unaligned_load<int128_t>(value);
        unaligned_store<int128_t>(dst, tmp);
        break;
    }

    case TYPE_OBJECT:
    case TYPE_HLL:
    case TYPE_PERCENTILE:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        const StringValue* src = reinterpret_cast<const StringValue*>(value);
        StringValue* dest = reinterpret_cast<StringValue*>(dst);
        dest->len = src->len;

        if (pool != nullptr) {
            dest->ptr = reinterpret_cast<char*>(pool->allocate(dest->len));
            assert(dest->ptr != nullptr);
            memcpy(dest->ptr, src->ptr, dest->len);
        } else {
            dest->ptr = src->ptr;
        }

        break;
    }

    default:
        DCHECK(false) << "RawValue::write(): bad type: " << type;
    }
}

// TODO: can we remove some of this code duplication? Templated allocator?
void RawValue::write(const void* value, const TypeDescriptor& type, void* dst, uint8_t** buf) {
    DCHECK(value != nullptr);
    switch (type.type) {
    case TYPE_BOOLEAN:
        *reinterpret_cast<bool*>(dst) = *reinterpret_cast<const bool*>(value);
        break;
    case TYPE_TINYINT:
        *reinterpret_cast<int8_t*>(dst) = *reinterpret_cast<const int8_t*>(value);
        break;
    case TYPE_SMALLINT:
        *reinterpret_cast<int16_t*>(dst) = *reinterpret_cast<const int16_t*>(value);
        break;
    case TYPE_INT:
        *reinterpret_cast<int32_t*>(dst) = *reinterpret_cast<const int32_t*>(value);
        break;
    case TYPE_BIGINT:
        *reinterpret_cast<int64_t*>(dst) = *reinterpret_cast<const int64_t*>(value);
        break;

    case TYPE_LARGEINT: {
        int128_t tmp = unaligned_load<int128_t>(value);
        unaligned_store<int128_t>(dst, tmp);
        break;
    }

    case TYPE_FLOAT:
        *reinterpret_cast<float*>(dst) = *reinterpret_cast<const float*>(value);
        break;
    case TYPE_DOUBLE:
        *reinterpret_cast<double*>(dst) = *reinterpret_cast<const double*>(value);
        break;
    case TYPE_DATE:
    case TYPE_DATETIME:
        *reinterpret_cast<DateTimeValue*>(dst) = *reinterpret_cast<const DateTimeValue*>(value);
        break;
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        DCHECK(buf != nullptr);
        const StringValue* src = reinterpret_cast<const StringValue*>(value);
        StringValue* dest = reinterpret_cast<StringValue*>(dst);
        dest->len = src->len;
        dest->ptr = reinterpret_cast<char*>(*buf);
        memcpy(dest->ptr, src->ptr, dest->len);
        *buf += dest->len;
        break;
    }
    case TYPE_DECIMAL:
        *reinterpret_cast<DecimalValue*>(dst) = *reinterpret_cast<const DecimalValue*>(value);
        break;

    case TYPE_DECIMALV2: {
        int128_t tmp = unaligned_load<int128_t>(value);
        unaligned_store<int128_t>(dst, tmp);
        break;
    }

    default:
        DCHECK(false) << "RawValue::write(): bad type: " << type.debug_string();
    }
}

bool RawValue::lt(const void* v1, const void* v2, const TypeDescriptor& type) {
    const StringValue* string_value1;
    const StringValue* string_value2;

    switch (type.type) {
    case TYPE_BOOLEAN:
        return *reinterpret_cast<const bool*>(v1) < *reinterpret_cast<const bool*>(v2);

    case TYPE_TINYINT:
        return *reinterpret_cast<const int8_t*>(v1) < *reinterpret_cast<const int8_t*>(v2);

    case TYPE_SMALLINT:
        return *reinterpret_cast<const int16_t*>(v1) < *reinterpret_cast<const int16_t*>(v2);

    case TYPE_INT:
        return *reinterpret_cast<const int32_t*>(v1) < *reinterpret_cast<const int32_t*>(v2);

    case TYPE_BIGINT:
        return *reinterpret_cast<const int64_t*>(v1) < *reinterpret_cast<const int64_t*>(v2);

    case TYPE_FLOAT:
        return *reinterpret_cast<const float*>(v1) < *reinterpret_cast<const float*>(v2);

    case TYPE_DOUBLE:
        return *reinterpret_cast<const double*>(v1) < *reinterpret_cast<const double*>(v2);

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
        string_value1 = reinterpret_cast<const StringValue*>(v1);
        string_value2 = reinterpret_cast<const StringValue*>(v2);
        return string_value1->lt(*string_value2);

    case TYPE_DATE:
    case TYPE_DATETIME:
        return *reinterpret_cast<const DateTimeValue*>(v1) < *reinterpret_cast<const DateTimeValue*>(v2);

    case TYPE_DECIMAL:
        return *reinterpret_cast<const DecimalValue*>(v1) < *reinterpret_cast<const DecimalValue*>(v2);

    case TYPE_DECIMALV2: {
        int128_t r = unaligned_load<int128_t>(v2);
        int128_t l = unaligned_load<int128_t>(v1);
        return l < r;
    }

    case TYPE_LARGEINT: {
        int128_t r = unaligned_load<int128_t>(v2);
        int128_t l = unaligned_load<int128_t>(v1);
        return l < r;
    }

    default:
        DCHECK(false) << "invalid type: " << type;
        return false;
    };
}
bool RawValue::eq(const void* v1, const void* v2, const TypeDescriptor& type) {
    const StringValue* string_value1;
    const StringValue* string_value2;

    switch (type.type) {
    case TYPE_BOOLEAN:
        return *reinterpret_cast<const bool*>(v1) == *reinterpret_cast<const bool*>(v2);

    case TYPE_TINYINT:
        return *reinterpret_cast<const int8_t*>(v1) == *reinterpret_cast<const int8_t*>(v2);

    case TYPE_SMALLINT:
        return *reinterpret_cast<const int16_t*>(v1) == *reinterpret_cast<const int16_t*>(v2);

    case TYPE_INT:
        return *reinterpret_cast<const int32_t*>(v1) == *reinterpret_cast<const int32_t*>(v2);

    case TYPE_BIGINT:
        return *reinterpret_cast<const int64_t*>(v1) == *reinterpret_cast<const int64_t*>(v2);

    case TYPE_FLOAT:
        return *reinterpret_cast<const float*>(v1) == *reinterpret_cast<const float*>(v2);

    case TYPE_DOUBLE:
        return *reinterpret_cast<const double*>(v1) == *reinterpret_cast<const double*>(v2);

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
        string_value1 = reinterpret_cast<const StringValue*>(v1);
        string_value2 = reinterpret_cast<const StringValue*>(v2);
        return string_value1->eq(*string_value2);

    case TYPE_DATE:
    case TYPE_DATETIME:
        return *reinterpret_cast<const DateTimeValue*>(v1) == *reinterpret_cast<const DateTimeValue*>(v2);

    case TYPE_DECIMAL:
        return *reinterpret_cast<const DecimalValue*>(v1) == *reinterpret_cast<const DecimalValue*>(v2);

    case TYPE_DECIMALV2: {
        int128_t r = unaligned_load<int128_t>(v2);
        int128_t l = unaligned_load<int128_t>(v1);
        return r == l;
    }

    case TYPE_LARGEINT: {
        int128_t r = unaligned_load<int128_t>(v2);
        int128_t l = unaligned_load<int128_t>(v1);
        return r == l;
    }

    default:
        DCHECK(false) << "invalid type: " << type;
        return false;
    };
}

// Use HashUtil::hash_combine for corner cases.  HashUtil::hash_combine is reimplemented
// here to use int32t's (instead of size_t)
// HashUtil::hash_combine does:
//  seed ^= v + 0x9e3779b9 + (seed << 6) + (seed >> 2);
uint32_t RawValue::get_hash_value(const void* v, const PrimitiveType& type, uint32_t seed) {
    // Hash_combine with v = 0
    if (v == nullptr) {
        uint32_t value = 0x9e3779b9;
        return seed ^ (value + (seed << 6) + (seed >> 2));
    }

    switch (type) {
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_HLL: {
        const StringValue* string_value = reinterpret_cast<const StringValue*>(v);
        return HashUtil::hash(string_value->ptr, string_value->len, seed);
    }

    case TYPE_BOOLEAN: {
        uint32_t value = *reinterpret_cast<const bool*>(v) + 0x9e3779b9;
        return seed ^ (value + (seed << 6) + (seed >> 2));
    }

    case TYPE_TINYINT:
        return HashUtil::hash(v, 1, seed);

    case TYPE_SMALLINT:
        return HashUtil::hash(v, 2, seed);

    case TYPE_INT:
        return HashUtil::hash(v, 4, seed);

    case TYPE_BIGINT:
        return HashUtil::hash(v, 8, seed);

    case TYPE_FLOAT:
        return HashUtil::hash(v, 4, seed);

    case TYPE_DOUBLE:
        return HashUtil::hash(v, 8, seed);

    case TYPE_DATE:
    case TYPE_DATETIME:
        return HashUtil::hash(v, 16, seed);

    case TYPE_DECIMAL:
        return HashUtil::hash(v, 40, seed);

    case TYPE_DECIMALV2:
        return HashUtil::hash(v, 16, seed);

    case TYPE_LARGEINT:
        return HashUtil::hash(v, 16, seed);

    default:
        DCHECK(false) << "invalid type: " << type;
        return 0;
    }
}

uint32_t RawValue::get_hash_value_fvn(const void* v, const PrimitiveType& type, uint32_t seed) {
    // Hash_combine with v = 0
    if (v == nullptr) {
        uint32_t value = 0x9e3779b9;
        return seed ^ (value + (seed << 6) + (seed >> 2));
    }

    switch (type) {
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_HLL: {
        const StringValue* string_value = reinterpret_cast<const StringValue*>(v);
        return HashUtil::fnv_hash(string_value->ptr, string_value->len, seed);
    }

    case TYPE_BOOLEAN: {
        uint32_t value = *reinterpret_cast<const bool*>(v) + 0x9e3779b9;
        return seed ^ (value + (seed << 6) + (seed >> 2));
    }

    case TYPE_TINYINT:
        return HashUtil::fnv_hash(v, 1, seed);

    case TYPE_SMALLINT:
        return HashUtil::fnv_hash(v, 2, seed);

    case TYPE_INT:
        return HashUtil::fnv_hash(v, 4, seed);

    case TYPE_BIGINT:
        return HashUtil::fnv_hash(v, 8, seed);

    case TYPE_FLOAT:
        return HashUtil::fnv_hash(v, 4, seed);

    case TYPE_DOUBLE:
        return HashUtil::fnv_hash(v, 8, seed);

    case TYPE_DATE:
    case TYPE_DATETIME:
        return HashUtil::fnv_hash(v, 16, seed);

    case TYPE_DECIMAL:
        return ((DecimalValue*)v)->hash(seed);

    case TYPE_DECIMALV2:
        return HashUtil::fnv_hash(v, 16, seed);

    case TYPE_LARGEINT:
        return HashUtil::fnv_hash(v, 16, seed);

    default:
        DCHECK(false) << "invalid type: " << type;
        return 0;
    }
}

// NOTE: this is just for split data, decimal use old starrocks hash function
// Because crc32 hardware is not equal with zlib crc32
uint32_t RawValue::zlib_crc32(const void* v, const TypeDescriptor& type, uint32_t seed) {
    // Hash_combine with v = 0
    if (v == nullptr) {
        uint32_t value = 0x9e3779b9;
        return seed ^ (value + (seed << 6) + (seed >> 2));
    }

    switch (type.type) {
    case TYPE_VARCHAR:
    case TYPE_HLL: {
        const StringValue* string_value = reinterpret_cast<const StringValue*>(v);
        return HashUtil::zlib_crc_hash(string_value->ptr, string_value->len, seed);
    }
    case TYPE_CHAR: {
        // TODO(zc): ugly, use actual value to compute hash value
        const StringValue* string_value = reinterpret_cast<const StringValue*>(v);
        int len = 0;
        while (len < string_value->len) {
            if (string_value->ptr[len] == '\0') {
                break;
            }
            len++;
        }
        return HashUtil::zlib_crc_hash(string_value->ptr, len, seed);
    }
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        return HashUtil::zlib_crc_hash(v, 1, seed);
    case TYPE_SMALLINT:
        return HashUtil::zlib_crc_hash(v, 2, seed);
    case TYPE_INT:
        return HashUtil::zlib_crc_hash(v, 4, seed);
    case TYPE_BIGINT:
        return HashUtil::zlib_crc_hash(v, 8, seed);
    case TYPE_LARGEINT:
        return HashUtil::zlib_crc_hash(v, 16, seed);
    case TYPE_FLOAT:
        return HashUtil::zlib_crc_hash(v, 4, seed);
    case TYPE_DOUBLE:
        return HashUtil::zlib_crc_hash(v, 8, seed);
    case TYPE_DATE:
    case TYPE_DATETIME: {
        const DateTimeValue* date_val = (const DateTimeValue*)v;
        char buf[64];
        char* end = date_val->to_string(buf);

        return HashUtil::zlib_crc_hash(buf, end - buf - 1, seed);
    }
    case TYPE_DECIMAL: {
        const DecimalValue* dec_val = (const DecimalValue*)v;
        int64_t int_val = dec_val->int_value();
        int32_t frac_val = dec_val->frac_value();
        seed = HashUtil::zlib_crc_hash(&int_val, sizeof(int_val), seed);
        return HashUtil::zlib_crc_hash(&frac_val, sizeof(frac_val), seed);
    }

    case TYPE_DECIMALV2: {
        const DecimalV2Value* dec_val = (const DecimalV2Value*)v;
        int64_t int_val = dec_val->int_value();
        int32_t frac_val = dec_val->frac_value();
        seed = HashUtil::zlib_crc_hash(&int_val, sizeof(int_val), seed);
        return HashUtil::zlib_crc_hash(&frac_val, sizeof(frac_val), seed);
    }
    default:
        DCHECK(false) << "invalid type: " << type;
        return 0;
    }
}

int RawValue::compare(const void* v1, const void* v2, const TypeDescriptor& type) {
    const StringValue* string_value1;
    const StringValue* string_value2;
    const DateTimeValue* ts_value1;
    const DateTimeValue* ts_value2;
    const DecimalValue* decimal_value1;
    const DecimalValue* decimal_value2;
    float f1 = 0;
    float f2 = 0;
    double d1 = 0;
    double d2 = 0;
    int32_t i1;
    int32_t i2;
    int64_t b1;
    int64_t b2;

    if (nullptr == v1 && nullptr == v2) {
        return 0;
    } else if (nullptr == v1 && nullptr != v2) {
        return -1;
    } else if (nullptr != v1 && nullptr == v2) {
        return 1;
    }

    switch (type.type) {
    case TYPE_NULL:
        return 0;

    case TYPE_BOOLEAN:
        return *reinterpret_cast<const bool*>(v1) - *reinterpret_cast<const bool*>(v2);

    case TYPE_TINYINT:
        return *reinterpret_cast<const int8_t*>(v1) - *reinterpret_cast<const int8_t*>(v2);

    case TYPE_SMALLINT:
        return *reinterpret_cast<const int16_t*>(v1) - *reinterpret_cast<const int16_t*>(v2);

    case TYPE_INT:
        i1 = *reinterpret_cast<const int32_t*>(v1);
        i2 = *reinterpret_cast<const int32_t*>(v2);
        return i1 > i2 ? 1 : (i1 < i2 ? -1 : 0);

    case TYPE_BIGINT:
        b1 = *reinterpret_cast<const int64_t*>(v1);
        b2 = *reinterpret_cast<const int64_t*>(v2);
        return b1 > b2 ? 1 : (b1 < b2 ? -1 : 0);

    case TYPE_FLOAT:
        // TODO: can this be faster? (just returning the difference has underflow problems)
        f1 = *reinterpret_cast<const float*>(v1);
        f2 = *reinterpret_cast<const float*>(v2);
        return f1 > f2 ? 1 : (f1 < f2 ? -1 : 0);

    case TYPE_DOUBLE:
        // TODO: can this be faster?
        d1 = *reinterpret_cast<const double*>(v1);
        d2 = *reinterpret_cast<const double*>(v2);
        return d1 > d2 ? 1 : (d1 < d2 ? -1 : 0);

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
        string_value1 = reinterpret_cast<const StringValue*>(v1);
        string_value2 = reinterpret_cast<const StringValue*>(v2);
        return string_value1->compare(*string_value2);

    case TYPE_DATE:
    case TYPE_DATETIME:
        ts_value1 = reinterpret_cast<const DateTimeValue*>(v1);
        ts_value2 = reinterpret_cast<const DateTimeValue*>(v2);
        return *ts_value1 > *ts_value2 ? 1 : (*ts_value1 < *ts_value2 ? -1 : 0);

    case TYPE_DECIMAL:
        decimal_value1 = reinterpret_cast<const DecimalValue*>(v1);
        decimal_value2 = reinterpret_cast<const DecimalValue*>(v2);
        return (*decimal_value1 > *decimal_value2) ? 1 : (*decimal_value1 < *decimal_value2 ? -1 : 0);

    case TYPE_DECIMALV2: {
        DecimalV2Value decimal_value1(unaligned_load<int128_t>(v1));
        DecimalV2Value decimal_value2(unaligned_load<int128_t>(v2));
        return (decimal_value1 > decimal_value2) ? 1 : (decimal_value1 < decimal_value2 ? -1 : 0);
    }

    case TYPE_LARGEINT: {
        __int128 large_int_value1 = unaligned_load<int128_t>(v1);
        __int128 large_int_value2 = unaligned_load<int128_t>(v2);
        return large_int_value1 > large_int_value2 ? 1 : (large_int_value1 < large_int_value2 ? -1 : 0);
    }

    default:
        DCHECK(false) << "invalid type: " << type.type;
        return 0;
    };
}

} // namespace starrocks
