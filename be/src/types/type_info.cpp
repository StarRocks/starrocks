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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/types.cpp

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

#include "types/type_info.h"

#include <cassert>
#include <cinttypes>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <limits>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/hash/unaligned_access.h"
#include "base/string/slice.h"
#include "base/types/decimal12.h"
#include "base/types/int128.h"
#include "base/utility/guard.h"
#include "base/utility/mem_util.hpp"
#include "base/utility/mysql_global.h"
#include "common/config.h"
#include "gutil/casts.h"
#include "gutil/strings/numbers.h"
#include "types/date_value.hpp"
#include "types/datetime_value.h"
#include "types/datum.h"
#include "types/decimalv2_value.h"
#include "types/time_types.h"
#include "types/type_descriptor.h"
#include "types/type_traits.h"

namespace starrocks {

#define APPLY_FOR_TYPE_INTEGER(M) \
    M(TYPE_TINYINT)               \
    M(TYPE_SMALLINT)              \
    M(TYPE_BIGINT)                \
    M(TYPE_LARGEINT)              \
    M(TYPE_INT)                   \
    M(TYPE_INT256)

#define APPLY_FOR_TYPE_DECIMAL(M) \
    M(TYPE_DECIMAL)               \
    M(TYPE_DECIMALV2)             \
    M(TYPE_DECIMAL32)             \
    M(TYPE_DECIMAL64)             \
    M(TYPE_DECIMAL128)            \
    M(TYPE_DECIMAL256)

#define APPLY_FOR_TYPE_TIME(M) \
    M(TYPE_DATE_V1)            \
    M(TYPE_DATE)               \
    M(TYPE_DATETIME_V1)        \
    M(TYPE_DATETIME)

#define APPLY_FOR_BASIC_LOGICAL_TYPE(M) \
    APPLY_FOR_TYPE_INTEGER(M)           \
    APPLY_FOR_TYPE_TIME(M)              \
    M(TYPE_UNSIGNED_INT)                \
    M(TYPE_FLOAT)                       \
    M(TYPE_DOUBLE)                      \
    M(TYPE_CHAR)                        \
    M(TYPE_VARCHAR)                     \
    M(TYPE_BOOLEAN)                     \
    APPLY_FOR_TYPE_DECIMAL(M)           \
    M(TYPE_JSON)                        \
    M(TYPE_VARBINARY)

#define APPLY_FOR_SUPPORTED_FIELD_TYPE(M) \
    APPLY_FOR_BASIC_LOGICAL_TYPE(M)       \
    M(TYPE_UNSIGNED_TINYINT)              \
    M(TYPE_UNSIGNED_SMALLINT)             \
    M(TYPE_UNSIGNED_BIGINT)               \
    M(TYPE_HLL)                           \
    M(TYPE_OBJECT)                        \
    M(TYPE_PERCENTILE)

inline uint32_t type_info_string_max_length() {
    return config::olap_string_max_length;
}

int TypeInfo::cmp(const Datum& left, const Datum& right) const {
    if (left.is_null() || right.is_null()) {
        return right.is_null() - left.is_null();
    }
    return _datum_cmp_impl(left, right);
}

class ScalarTypeInfo final : public TypeInfo {
public:
    ~ScalarTypeInfo() override = default;

    void shallow_copy(void* dest, const void* src) const override { _shallow_copy(dest, src); }

    void deep_copy(void* dest, const void* src, const TypeInfoAllocator* allocator) const override {
        _deep_copy(dest, src, allocator);
    }

    void direct_copy(void* dest, const void* src) const override { _direct_copy(dest, src); }

    Status from_string(void* buf, const std::string& scan_key) const override { return _from_string(buf, scan_key); }

    std::string to_string(const void* src) const override { return _to_string(src); }

    void set_to_max(void* buf) const override { _set_to_max(buf); }
    void set_to_min(void* buf) const override { _set_to_min(buf); }

    size_t size() const override { return _size; }

    LogicalType type() const override { return _field_type; }

protected:
    int _datum_cmp_impl(const Datum& left, const Datum& right) const override { return _datum_cmp(left, right); }

private:
    void (*_shallow_copy)(void* dest, const void* src);
    void (*_deep_copy)(void* dest, const void* src, const TypeInfoAllocator* allocator);
    void (*_direct_copy)(void* dest, const void* src);

    Status (*_from_string)(void* buf, const std::string& scan_key);
    std::string (*_to_string)(const void* src);

    void (*_set_to_max)(void* buf);
    void (*_set_to_min)(void* buf);

    // Datum based methods.
    int (*_datum_cmp)(const Datum& left, const Datum& right);

    const size_t _size;
    const LogicalType _field_type;

    friend class ScalarTypeInfoResolver;
    template <typename TypeTraitsClass>
    ScalarTypeInfo(TypeTraitsClass t);
};

// ScalarTypeInfoImplBase
// Base implementation for ScalarTypeInfo, use as default
template <LogicalType field_type>
struct ScalarTypeInfoImplBase {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    static const LogicalType type = field_type;
    static const int32_t size = sizeof(CppType);

    static bool equal(const void* left, const void* right) {
        auto l_value = unaligned_load<CppType>(left);
        auto r_value = unaligned_load<CppType>(right);
        return l_value == r_value;
    }

    static int cmp(const void* left, const void* right) {
        auto left_int = unaligned_load<CppType>(left);
        auto right_int = unaligned_load<CppType>(right);
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

    static void deep_copy(void* dest, const void* src, const TypeInfoAllocator* allocator __attribute__((unused))) {
        unaligned_store<CppType>(dest, unaligned_load<CppType>(src));
    }

    static void direct_copy(void* dest, const void* src) {
        unaligned_store<CppType>(dest, unaligned_load<CppType>(src));
    }

    static void set_to_max(void* buf) { unaligned_store<CppType>(buf, std::numeric_limits<CppType>::max()); }

    static void set_to_min(void* buf) { unaligned_store<CppType>(buf, std::numeric_limits<CppType>::lowest()); }

    static std::string to_string(const void* src) {
        std::stringstream stream;
        stream << unaligned_load<CppType>(src);
        return stream.str();
    }

    static Status from_string(void* buf, const std::string& scan_key) {
        CppType value = 0;
        if (scan_key.length() > 0) {
            value = static_cast<CppType>(strtol(scan_key.c_str(), nullptr, 10));
        }
        unaligned_store<CppType>(buf, value);
        return Status::OK();
    }

    static int datum_cmp(const Datum& left, const Datum& right) {
        CppType v1 = left.get<CppType>();
        CppType v2 = right.get<CppType>();
        return (v1 < v2) ? -1 : (v2 < v1) ? 1 : 0;
    }
};

// Default template does nothing but inherit from ScalarTypeInfoImplBase
template <LogicalType field_type>
struct ScalarTypeInfoImpl : public ScalarTypeInfoImplBase<field_type> {};

// ScalarTypeInfoResolver
// Manage all type-info instances, providing getter
class ScalarTypeInfoResolver {
public:
    static ScalarTypeInfoResolver* instance() {
        static ScalarTypeInfoResolver resolver;
        return &resolver;
    }

    ScalarTypeInfoResolver(const ScalarTypeInfoResolver&) = delete;
    ScalarTypeInfoResolver& operator=(const ScalarTypeInfoResolver&) = delete;

    ScalarTypeInfoResolver();
    ~ScalarTypeInfoResolver();

    const TypeInfoPtr get_type_info(const LogicalType t) {
        if (this->_mapping.find(t) == this->_mapping.end()) {
            return std::make_shared<ScalarTypeInfo>(*this->_mapping[TYPE_NONE].get());
        }
        return std::make_shared<ScalarTypeInfo>(*this->_mapping[t].get());
    }

    const ScalarTypeInfo* get_scalar_type_info(const LogicalType t) {
        DCHECK(is_scalar_field_type(t));
        return this->_mapping[t].get();
    }

private:
    template <LogicalType field_type>
    void add_mapping() {
        ScalarTypeInfoImpl<field_type> traits;
        std::unique_ptr<ScalarTypeInfo> scalar_type_info(new ScalarTypeInfo(traits));
        _mapping[field_type] = std::move(scalar_type_info);
    }

    // item_type_info -> list_type_info
    std::unordered_map<LogicalType, std::unique_ptr<ScalarTypeInfo>, std::hash<size_t>> _mapping;
};

template <typename TypeInfoImpl>
ScalarTypeInfo::ScalarTypeInfo([[maybe_unused]] TypeInfoImpl t)
        : _shallow_copy(TypeInfoImpl::shallow_copy),
          _deep_copy(TypeInfoImpl::deep_copy),
          _direct_copy(TypeInfoImpl::direct_copy),
          _from_string(TypeInfoImpl::from_string),
          _to_string(TypeInfoImpl::to_string),
          _set_to_max(TypeInfoImpl::set_to_max),
          _set_to_min(TypeInfoImpl::set_to_min),
          _datum_cmp(TypeInfoImpl::datum_cmp),
          _size(TypeInfoImpl::size),
          _field_type(TypeInfoImpl::type) {}

ScalarTypeInfoResolver::ScalarTypeInfoResolver() {
#define M(ftype) add_mapping<ftype>();
    APPLY_FOR_SUPPORTED_FIELD_TYPE(M)
#undef M
    add_mapping<TYPE_NONE>();
}

ScalarTypeInfoResolver::~ScalarTypeInfoResolver() = default;

TypeInfoPtr get_type_info(LogicalType field_type) {
    return ScalarTypeInfoResolver::instance()->get_type_info(field_type);
}

TypeInfoPtr get_type_info(const TypeDescriptor& type_desc) {
    if (type_desc.is_array_type()) {
        const TypeDescriptor& child = type_desc.children[0];
        TypeInfoPtr child_type_info = get_type_info(child);
        return get_array_type_info(child_type_info);
    } else if (type_desc.is_map_type()) {
        const TypeDescriptor& key_desc = type_desc.children[0];
        const TypeDescriptor& value_desc = type_desc.children[1];
        TypeInfoPtr key_type_info = get_type_info(key_desc);
        TypeInfoPtr value_type_info = get_type_info(value_desc);
        return get_map_type_info(std::move(key_type_info), std::move(value_type_info));
    } else if (type_desc.is_struct_type()) {
        std::vector<TypeInfoPtr> field_types;
        field_types.reserve(type_desc.children.size());
        for (const auto& child_desc : type_desc.children) {
            field_types.emplace_back(get_type_info(child_desc));
        }
        return get_struct_type_info(std::move(field_types));
    } else {
        return get_type_info(type_desc.type, type_desc.precision, type_desc.scale);
    }
}

TypeInfoPtr get_type_info(LogicalType field_type, [[maybe_unused]] int precision, [[maybe_unused]] int scale) {
    if (is_scalar_field_type(field_type)) {
        return get_type_info(field_type);
    } else if (field_type == TYPE_DECIMAL32 || field_type == TYPE_DECIMAL64 || field_type == TYPE_DECIMAL128 ||
               field_type == TYPE_DECIMAL256) {
        return get_decimal_type_info(field_type, precision, scale);
    } else {
        return nullptr;
    }
}

TypeInfoPtr get_type_info(const TypeInfo* type_info) {
    return get_type_info(type_info->type(), type_info->precision(), type_info->scale());
}

const TypeInfo* get_scalar_type_info(LogicalType type) {
    DCHECK(is_scalar_field_type(type));
    return ScalarTypeInfoResolver::instance()->get_scalar_type_info(type);
}

template <>
struct ScalarTypeInfoImpl<TYPE_BOOLEAN> : public ScalarTypeInfoImplBase<TYPE_BOOLEAN> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", *reinterpret_cast<const bool*>(src));
        return {buf};
    }

    static void set_to_max(void* buf) { (*(bool*)buf) = true; }
    static void set_to_min(void* buf) { (*(bool*)buf) = false; }

    static int datum_cmp(const Datum& left, const Datum& right) {
        uint8_t v1 = left.get<uint8_t>();
        uint8_t v2 = right.get<uint8_t>();
        return (v1 < v2) ? -1 : (v2 < v1) ? 1 : 0;
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_NONE> : public ScalarTypeInfoImplBase<TYPE_NONE> {};

template <>
struct ScalarTypeInfoImpl<TYPE_TINYINT> : public ScalarTypeInfoImplBase<TYPE_TINYINT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", *reinterpret_cast<const int8_t*>(src));
        return {buf};
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_SMALLINT> : public ScalarTypeInfoImplBase<TYPE_SMALLINT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", unaligned_load<int16_t>(src));
        return {buf};
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_INT> : public ScalarTypeInfoImplBase<TYPE_INT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", unaligned_load<int32_t>(src));
        return {buf};
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_BIGINT> : public ScalarTypeInfoImplBase<TYPE_BIGINT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%" PRId64, unaligned_load<int64_t>(src));
        return {buf};
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_LARGEINT> : public ScalarTypeInfoImplBase<TYPE_LARGEINT> {
    static Status from_string(void* buf, const std::string& scan_key) {
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

            value = is_negative ? (~current + 1) : current;
        }
        unaligned_store<int128_t>(buf, value);
        return Status::OK();
    }
    static std::string to_string(const void* src) {
        char buf[1024];
        auto value = unaligned_load<int128_t>(src);
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
            } else if (middle > 0) {
                current += snprintf(current, end - current, "%" PRIu64, middle);
                current += snprintf(current, end - current, "%.19" PRIu64, suffix);
                (void)current; // avoid unused value warning.
            } else {
                current += snprintf(current, end - current, "%" PRIu64, suffix);
                (void)current; // avoid unused value warning.
            }
        }

        return {buf};
    }

    // GCC7.3 will generate movaps instruction, which will lead to SEGV when buf is
    // not aligned to 16 byte
    static void shallow_copy(void* dest, const void* src) {
        unaligned_store<int128_t>(dest, unaligned_load<int128_t>(src));
    }
    static void deep_copy(void* dest, const void* src, const TypeInfoAllocator* allocator __attribute__((unused))) {
        unaligned_store<int128_t>(dest, unaligned_load<int128_t>(src));
    }

    static void direct_copy(void* dest, const void* src) {
        unaligned_store<int128_t>(dest, unaligned_load<int128_t>(src));
    }
    static void set_to_max(void* buf) { unaligned_store<int128_t>(buf, ~((int128_t)(1) << 127)); }
    static void set_to_min(void* buf) { unaligned_store<int128_t>(buf, (int128_t)(1) << 127); }

    static int datum_cmp(const Datum& left, const Datum& right) {
        const int128_t& v1 = left.get_int128();
        const int128_t& v2 = right.get_int128();
        return (v1 < v2) ? -1 : (v2 < v1) ? 1 : 0;
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_INT256> : public ScalarTypeInfoImplBase<TYPE_INT256> {
    static Status from_string(void* buf, const std::string& scan_key) {
        try {
            int256_t value = parse_int256(scan_key);
            unaligned_store<int256_t>(buf, value);
            return Status::OK();
        } catch (const std::exception& e) {
            // Fallback to zero on parse error
            unaligned_store<int256_t>(buf, int256_t(0));
            return Status::InternalError(fmt::format("Fail to parse int256 from string: {}", scan_key));
        }
    }

    static std::string to_string(const void* src) {
        auto value = unaligned_load<int256_t>(src);
        return value.to_string();
    }

    static void shallow_copy(void* dest, const void* src) {
        unaligned_store<int256_t>(dest, unaligned_load<int256_t>(src));
    }

    static void deep_copy(void* dest, const void* src, const TypeInfoAllocator* allocator __attribute__((unused))) {
        unaligned_store<int256_t>(dest, unaligned_load<int256_t>(src));
    }

    static void direct_copy(void* dest, const void* src) {
        unaligned_store<int256_t>(dest, unaligned_load<int256_t>(src));
    }

    static void set_to_max(void* buf) { unaligned_store<int256_t>(buf, INT256_MAX); }
    static void set_to_min(void* buf) { unaligned_store<int256_t>(buf, INT256_MIN); }

    static int datum_cmp(const Datum& left, const Datum& right) {
        const int256_t& v1 = left.get<int256_t>();
        const int256_t& v2 = right.get<int256_t>();
        return (v1 < v2) ? -1 : (v2 < v1) ? 1 : 0;
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_FLOAT> : public ScalarTypeInfoImplBase<TYPE_FLOAT> {
    static Status from_string(void* buf, const std::string& scan_key) {
        CppType value = 0.0f;
        if (scan_key.length() > 0) {
            value = static_cast<CppType>(atof(scan_key.c_str()));
        }
        unaligned_store<CppType>(buf, value);
        return Status::OK();
    }
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        int length = FloatToBuffer(unaligned_load<CppType>(src), MAX_FLOAT_STR_LENGTH, buf);
        DCHECK(length >= 0) << "gcvt float failed, float value=" << unaligned_load<CppType>(src);
        return {buf};
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_DOUBLE> : public ScalarTypeInfoImplBase<TYPE_DOUBLE> {
    static Status from_string(void* buf, const std::string& scan_key) {
        CppType value = 0.0;
        if (scan_key.length() > 0) {
            value = atof(scan_key.c_str());
        }
        unaligned_store<CppType>(buf, value);
        return Status::OK();
    }
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        int length = DoubleToBuffer(unaligned_load<CppType>(src), MAX_DOUBLE_STR_LENGTH, buf);
        DCHECK(length >= 0) << "gcvt float failed, float value=" << unaligned_load<CppType>(src);
        return {buf};
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_DECIMAL> : public ScalarTypeInfoImplBase<TYPE_DECIMAL> {
    static Status from_string(void* buf, const std::string& scan_key) {
        CppType t;
        RETURN_IF_ERROR(t.from_string(scan_key));
        unaligned_store<CppType>(buf, t);
        return Status::OK();
    }
    static std::string to_string(const void* src) {
        auto t = unaligned_load<CppType>(src);
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
struct ScalarTypeInfoImpl<TYPE_DECIMALV2> : public ScalarTypeInfoImplBase<TYPE_DECIMALV2> {
    static Status from_string(void* buf, const std::string& scan_key) {
        CppType val;
        if (val.parse_from_str(scan_key.c_str(), scan_key.size()) != E_DEC_OK) {
            return Status::InternalError("Fail to cast to decimal.");
        }
        unaligned_store<CppType>(buf, val);
        return Status::OK();
    }
    static std::string to_string(const void* src) {
        auto tmp = unaligned_load<CppType>(src);
        return tmp.to_string();
    }
    // GCC7.3 will generate movaps instruction, which will lead to SEGV when buf is
    // not aligned to 16 byte
    static void shallow_copy(void* dest, const void* src) { memcpy(dest, src, sizeof(CppType)); }
    static void deep_copy(void* dest, const void* src, const TypeInfoAllocator* allocator __attribute__((unused))) {
        memcpy(dest, src, sizeof(CppType));
    }

    static void direct_copy(void* dest, const void* src) { memcpy(dest, src, sizeof(CppType)); }

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

    static int datum_cmp(const Datum& left, const Datum& right) {
        const DecimalV2Value& v1 = left.get_decimal();
        const DecimalV2Value& v2 = right.get_decimal();
        return (v1 < v2) ? -1 : (v1 > v2) ? 1 : 0;
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_DATE_V1> : public ScalarTypeInfoImplBase<TYPE_DATE_V1> {
    static Status from_string(void* buf, const std::string& scan_key) {
        tm time_tm;
        char* res = strptime(scan_key.c_str(), "%Y-%m-%d", &time_tm);

        if (nullptr != res) {
            int value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
            unaligned_store<CppType>(buf, value);
        } else {
            // 1400 - 01 - 01
            unaligned_store<CppType>(buf, 716833);
        }

        return Status::OK();
    }
    static std::string to_string(const void* src) {
        auto v = unaligned_load<CppType>(src);
        return v.to_string();
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
struct ScalarTypeInfoImpl<TYPE_DATE> : public ScalarTypeInfoImplBase<TYPE_DATE> {
    static Status from_string(void* buf, const std::string& scan_key) {
        DateValue date;
        if (!date.from_string(scan_key.data(), scan_key.size())) {
            // Compatible with TYPE_DATE_V1
            date.from_string("1400-01-01", sizeof("1400-01-01") - 1);
        }
        unaligned_store<DateValue>(buf, date);
        return Status::OK();
    }

    static std::string to_string(const void* src) {
        auto src_val = unaligned_load<DateValue>(src);
        return src_val.to_string();
    }
    static void set_to_max(void* buf) {
        // max is 9999 * 16 * 32 + 12 * 32 + 31;
        unaligned_store<CppType>(buf, date::MAX_DATE);
    }
    static void set_to_min(void* buf) {
        // min is 0 * 16 * 32 + 1 * 32 + 1;
        unaligned_store<CppType>(buf, date::MIN_DATE);
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_DATETIME_V1> : public ScalarTypeInfoImplBase<TYPE_DATETIME_V1> {
    static Status from_string(void* buf, const std::string& scan_key) {
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

        return Status::OK();
    }
    static std::string to_string(const void* src) {
        tm time_tm;
        auto tmp = unaligned_load<CppType>(src);
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
        return {buf};
    }
    static void set_to_max(void* buf) { unaligned_store<CppType>(buf, 99991231235959L); }
    static void set_to_min(void* buf) { unaligned_store<CppType>(buf, 101000000); }
};

template <>
struct ScalarTypeInfoImpl<TYPE_DATETIME> : public ScalarTypeInfoImplBase<TYPE_DATETIME> {
    static Status from_string(void* buf, const std::string& scan_key) {
        auto timestamp = unaligned_load<TimestampValue>(buf);
        if (!timestamp.from_string(scan_key.data(), scan_key.size())) {
            // Compatible with TYPE_DATETIME_V1
            timestamp.from_string("1400-01-01 00:00:00", sizeof("1400-01-01 00:00:00") - 1);
        }
        unaligned_store<TimestampValue>(buf, timestamp);
        return Status::OK();
    }

    static std::string to_string(const void* src) {
        auto timestamp = unaligned_load<TimestampValue>(src);
        return timestamp.to_string();
    }
    static void set_to_max(void* buf) { unaligned_store<CppType>(buf, timestamp::MAX_TIMESTAMP); }
    static void set_to_min(void* buf) { unaligned_store<CppType>(buf, timestamp::MIN_TIMESTAMP); }
};

template <>
struct ScalarTypeInfoImpl<TYPE_CHAR> : public ScalarTypeInfoImplBase<TYPE_CHAR> {
    static Status from_string(void* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > type_info_string_max_length()) {
            return Status::InvalidArgument(fmt::format("String(length={}) is too long, the max length is: {}",
                                                       value_len, type_info_string_max_length()));
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
        return Status::OK();
    }
    static std::string to_string(const void* src) {
        auto slice = unaligned_load<Slice>(src);
        return slice.to_string();
    }
    static void deep_copy(void* dest, const void* src, const TypeInfoAllocator* allocator) {
        auto l_slice = unaligned_load<Slice>(dest);
        auto r_slice = unaligned_load<Slice>(src);
        DCHECK_NE(allocator, nullptr);
        l_slice.data = reinterpret_cast<char*>(allocator->allocate(r_slice.size));
        assert(l_slice.data != nullptr);
        memory_copy(l_slice.data, r_slice.data, r_slice.size);
        l_slice.size = r_slice.size;
        unaligned_store<Slice>(dest, l_slice);
    }

    static void direct_copy(void* dest, const void* src) {
        auto l_slice = unaligned_load<Slice>(dest);
        auto r_slice = unaligned_load<Slice>(src);
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

    static int datum_cmp(const Datum& left, const Datum& right) {
        const Slice& v1 = left.get_slice();
        const Slice& v2 = right.get_slice();
        return v1.compare(v2);
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_VARCHAR> : public ScalarTypeInfoImpl<TYPE_CHAR> {
    static const LogicalType type = TYPE_VARCHAR;
    static const int32_t size = TypeTraits<TYPE_VARCHAR>::size;

    static Status from_string(void* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > type_info_string_max_length()) {
            LOG(WARNING) << "String(length=" << value_len << ") is too long, the max length is "
                         << type_info_string_max_length();
            return Status::InternalError(fmt::format("String(length={}) is too long, the max length is: {}", value_len,
                                                     type_info_string_max_length()));
        }

        auto slice = unaligned_load<Slice>(buf);
        memory_copy(slice.data, scan_key.c_str(), value_len);
        slice.size = value_len;
        unaligned_store<Slice>(buf, slice);
        return Status::OK();
    }

    static void set_to_min(void* buf) {
        auto slice = reinterpret_cast<Slice*>(buf);
        slice->size = 0;
    }

    static int datum_cmp(const Datum& left, const Datum& right) {
        const Slice& v1 = left.get_slice();
        const Slice& v2 = right.get_slice();
        return v1.compare(v2);
    }
};

template <>
struct ScalarTypeInfoImpl<TYPE_HLL> : public ScalarTypeInfoImpl<TYPE_VARCHAR> {
    static const LogicalType type = TYPE_HLL;
    static const int32_t size = TypeTraits<TYPE_HLL>::size;
};

template <>
struct ScalarTypeInfoImpl<TYPE_OBJECT> : public ScalarTypeInfoImpl<TYPE_VARCHAR> {
    static const LogicalType type = TYPE_OBJECT;
    static const int32_t size = TypeTraits<TYPE_OBJECT>::size;
};

template <>
struct ScalarTypeInfoImpl<TYPE_PERCENTILE> : public ScalarTypeInfoImpl<TYPE_VARCHAR> {
    static const LogicalType type = TYPE_PERCENTILE;
    static const int32_t size = TypeTraits<TYPE_PERCENTILE>::size;
};

template <>
struct ScalarTypeInfoImpl<TYPE_JSON> : public ScalarTypeInfoImpl<TYPE_OBJECT> {
    static const LogicalType type = TYPE_JSON;
    static const int32_t size = TypeTraits<TYPE_JSON>::size;
};

template <>
struct ScalarTypeInfoImpl<TYPE_VARBINARY> : public ScalarTypeInfoImpl<TYPE_VARCHAR> {
    static const LogicalType type = TYPE_VARBINARY;
    static const int32_t size = TypeTraits<TYPE_VARBINARY>::size;
};

void (*ScalarTypeInfoImpl<TYPE_CHAR>::set_to_max)(void*) = nullptr;

// NOTE
// These code could not be moved proceeding ScalarTypeInfoImpl specialization, otherwise
// will encounter `specialization after instantiation` error
template <LogicalType ftype>
int TypeComparator<ftype>::cmp(const void* lhs, const void* rhs) {
    return ScalarTypeInfoImpl<ftype>::cmp(lhs, rhs);
}
#define M(ftype) template struct TypeComparator<ftype>;
APPLY_FOR_SUPPORTED_FIELD_TYPE(M)
#undef M
} // namespace starrocks
