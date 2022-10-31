// This file is made available under Elastic License 2.0.
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

#include "storage/types.h"

#include "exec/vectorized/join_hash_map.h"
#include "gutil/strings/numbers.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/large_int_value.h"
#include "runtime/mem_pool.h"
#include "runtime/time_types.h"
#include "storage/array_type_info.h"
#include "storage/collection.h"
#include "storage/convert_helper.h"
#include "storage/decimal12.h"
#include "storage/decimal_type_info.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/olap_type_infra.h"
#include "storage/tablet_schema.h" // for TabletColumn
#include "storage/type_traits.h"
#include "storage/uint24.h"
#include "types/date_value.hpp"
#include "util/hash_util.hpp"
#include "util/mem_util.hpp"
#include "util/slice.h"
#include "util/string_parser.hpp"
#include "util/unaligned_access.h"

namespace starrocks {

// NOLINTNEXTLINE
static const std::vector<std::string> DATE_FORMATS{
        "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d", "%y/%m/%d",
};

template <typename T>
static Status convert_int_from_varchar(void* dest, const void* src) {
    using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_VARCHAR>::CppType;
    auto src_value = unaligned_load<SrcType>(src);
    StringParser::ParseResult parse_res;
    T result = StringParser::string_to_int<T>(src_value.get_data(), src_value.get_size(), &parse_res);
    if (UNLIKELY(parse_res != StringParser::PARSE_SUCCESS)) {
        return Status::InternalError(fmt::format("Fail to cast to int from string: {}",
                                                 std::string(src_value.get_data(), src_value.get_size())));
    }
    memcpy(dest, &result, sizeof(T));
    return Status::OK();
}

template <typename T>
static Status convert_float_from_varchar(void* dest, const void* src) {
    using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_VARCHAR>::CppType;
    auto src_value = unaligned_load<SrcType>(src);
    StringParser::ParseResult parse_res;
    T result = StringParser::string_to_float<T>(src_value.get_data(), src_value.get_size(), &parse_res);
    if (UNLIKELY(parse_res != StringParser::PARSE_SUCCESS)) {
        return Status::InternalError(fmt::format("Fail to cast to float from string: {}",
                                                 std::string(src_value.get_data(), src_value.get_size())));
    }
    unaligned_store<T>(dest, result);
    return Status::OK();
}

int TypeInfo::cmp(const Datum& left, const Datum& right) const {
    if (left.is_null() || right.is_null()) {
        return right.is_null() - left.is_null();
    }
    return _datum_cmp_impl(left, right);
}

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

// ScalarTypeInfoImplBase
// Base implementation for ScalarTypeInfo, use as default
template <FieldType field_type>
struct ScalarTypeInfoImplBase {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    static const FieldType type = field_type;
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

    static void deep_copy(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        unaligned_store<CppType>(dest, unaligned_load<CppType>(src));
    }

    static void copy_object(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        unaligned_store<CppType>(dest, unaligned_load<CppType>(src));
    }

    static void direct_copy(void* dest, const void* src, MemPool* mem_pool) {
        unaligned_store<CppType>(dest, unaligned_load<CppType>(src));
    }

    static Status convert_from(void* dest __attribute__((unused)), const void* src __attribute__((unused)),
                               const TypeInfoPtr& src_type __attribute__((unused)),
                               MemPool* mem_pool __attribute__((unused))) {
        return Status::NotSupported("Not supported function");
    }

    static void set_to_max(void* buf) { unaligned_store<CppType>(buf, std::numeric_limits<CppType>::max()); }

    static void set_to_min(void* buf) { unaligned_store<CppType>(buf, std::numeric_limits<CppType>::lowest()); }

    static uint32_t hash_code(const void* data, uint32_t seed) { return HashUtil::hash(data, sizeof(CppType), seed); }

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

    static int datum_cmp(const vectorized::Datum& left, const vectorized::Datum& right) {
        CppType v1 = left.get<CppType>();
        CppType v2 = right.get<CppType>();
        return (v1 < v2) ? -1 : (v2 < v1) ? 1 : 0;
    }
};

// Default template does nothing but inherit from ScalarTypeInfoImplBase
template <FieldType field_type>
struct ScalarTypeInfoImpl : public ScalarTypeInfoImplBase<field_type> {};

// ScalarTypeInfoResolver
// Manage all type-info instances, prodivding getter
class ScalarTypeInfoResolver {
    DECLARE_SINGLETON(ScalarTypeInfoResolver);

public:
    const TypeInfoPtr get_type_info(const FieldType t) {
        if (this->_mapping.find(t) == this->_mapping.end()) {
            return std::make_shared<ScalarTypeInfo>(*this->_mapping[OLAP_FIELD_TYPE_NONE].get());
        }
        return std::make_shared<ScalarTypeInfo>(*this->_mapping[t].get());
    }

    const ScalarTypeInfo* get_scalar_type_info(const FieldType t) {
        DCHECK(is_scalar_field_type(t));
        return this->_mapping[t].get();
    }

private:
    template <FieldType field_type>
    void add_mapping() {
        ScalarTypeInfoImpl<field_type> traits;
        std::unique_ptr<ScalarTypeInfo> scalar_type_info(new ScalarTypeInfo(traits));
        _mapping[field_type] = std::move(scalar_type_info);
    }

    // item_type_info -> list_type_info
    std::unordered_map<FieldType, std::unique_ptr<ScalarTypeInfo>, std::hash<size_t>> _mapping;

    ScalarTypeInfoResolver(const ScalarTypeInfoResolver&) = delete;
    const ScalarTypeInfoResolver& operator=(const ScalarTypeInfoResolver&) = delete;
};

template <typename TypeInfoImpl>
ScalarTypeInfo::ScalarTypeInfo([[maybe_unused]] TypeInfoImpl t)
        : _equal(TypeInfoImpl::equal),
          _cmp(TypeInfoImpl::cmp),
          _shallow_copy(TypeInfoImpl::shallow_copy),
          _deep_copy(TypeInfoImpl::deep_copy),
          _copy_object(TypeInfoImpl::copy_object),
          _direct_copy(TypeInfoImpl::direct_copy),
          _convert_from(TypeInfoImpl::convert_from),
          _from_string(TypeInfoImpl::from_string),
          _to_string(TypeInfoImpl::to_string),
          _set_to_max(TypeInfoImpl::set_to_max),
          _set_to_min(TypeInfoImpl::set_to_min),
          _hash_code(TypeInfoImpl::hash_code),
          _datum_cmp(TypeInfoImpl::datum_cmp),
          _size(TypeInfoImpl::size),
          _field_type(TypeInfoImpl::type) {}

ScalarTypeInfoResolver::ScalarTypeInfoResolver() {
#define M(ftype) add_mapping<ftype>();
    APPLY_FOR_SUPPORTED_FIELD_TYPE(M)
#undef M
    add_mapping<OLAP_FIELD_TYPE_NONE>();
}

ScalarTypeInfoResolver::~ScalarTypeInfoResolver() = default;

bool is_scalar_field_type(FieldType field_type) {
    switch (field_type) {
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_ARRAY:
    case OLAP_FIELD_TYPE_MAP:
    case OLAP_FIELD_TYPE_DECIMAL32:
    case OLAP_FIELD_TYPE_DECIMAL64:
    case OLAP_FIELD_TYPE_DECIMAL128:
        return false;
    default:
        return true;
    }
}

bool is_complex_metric_type(FieldType field_type) {
    switch (field_type) {
    case OLAP_FIELD_TYPE_OBJECT:
    case OLAP_FIELD_TYPE_PERCENTILE:
    case OLAP_FIELD_TYPE_HLL:
        return true;
    default:
        return false;
    }
}

TypeInfoPtr get_type_info(FieldType field_type) {
    return ScalarTypeInfoResolver::instance()->get_type_info(field_type);
}

TypeInfoPtr get_type_info(const ColumnMetaPB& column_meta_pb) {
    auto type = static_cast<FieldType>(column_meta_pb.type());
    TypeInfoPtr type_info;
    if (type == OLAP_FIELD_TYPE_ARRAY) {
        const ColumnMetaPB& child = column_meta_pb.children_columns(0);
        TypeInfoPtr child_type_info = get_type_info(child);
        type_info = get_array_type_info(child_type_info);
        return type_info;
    } else {
        return get_type_info(delegate_type(type));
    }
}

TypeInfoPtr get_type_info(const TabletColumn& col) {
    TypeInfoPtr type_info;
    if (col.type() == OLAP_FIELD_TYPE_ARRAY) {
        const TabletColumn& child = col.subcolumn(0);
        TypeInfoPtr child_type_info = get_type_info(child);
        type_info = get_array_type_info(child_type_info);
        return type_info;
    } else {
        return get_type_info(col.type(), col.precision(), col.scale());
    }
}

TypeInfoPtr get_type_info(FieldType field_type, [[maybe_unused]] int precision, [[maybe_unused]] int scale) {
    if (is_scalar_field_type(field_type)) {
        return get_type_info(field_type);
    } else if (field_type == OLAP_FIELD_TYPE_DECIMAL32 || field_type == OLAP_FIELD_TYPE_DECIMAL64 ||
               field_type == OLAP_FIELD_TYPE_DECIMAL128) {
        return get_decimal_type_info(field_type, precision, scale);
    } else {
        return nullptr;
    }
}

TypeInfoPtr get_type_info(const TypeInfo* type_info) {
    return get_type_info(type_info->type(), type_info->precision(), type_info->scale());
}

const TypeInfo* get_scalar_type_info(FieldType type) {
    DCHECK(is_scalar_field_type(type));
    return ScalarTypeInfoResolver::instance()->get_scalar_type_info(type);
}

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_BOOL> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_BOOL> {
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
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_NONE> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_NONE> {};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_TINYINT> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_TINYINT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", *reinterpret_cast<const int8_t*>(src));
        return std::string(buf);
    }

    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                               MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_int_from_varchar<CppType>(dest, src);
        }
        return Status::InternalError("Fail to cast to tinyint.");
    }
};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_SMALLINT> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_SMALLINT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", unaligned_load<int16_t>(src));
        return std::string(buf);
    }
    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                               MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_int_from_varchar<CppType>(dest, src);
        }
        return Status::InternalError("Fail to cast to smallint.");
    }
};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_INT> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_INT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%d", unaligned_load<int32_t>(src));
        return std::string(buf);
    }
    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                               MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_int_from_varchar<CppType>(dest, src);
        }
        return Status::InternalError("Fail to cast to int.");
    }
};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_BIGINT> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_BIGINT> {
    static std::string to_string(const void* src) {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%" PRId64, unaligned_load<int64_t>(src));
        return std::string(buf);
    }
    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                               MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_int_from_varchar<CppType>(dest, src);
        }
        return Status::InternalError("Fail to cast to bigint.");
    }
};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_LARGEINT> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_LARGEINT> {
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

            value = is_negative ? -current : current;
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
    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                               MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_int_from_varchar<CppType>(dest, src);
        }
        return Status::InternalError("Fail to cast to largeint.");
    }

    static int datum_cmp(const vectorized::Datum& left, const vectorized::Datum& right) {
        const int128_t& v1 = left.get_int128();
        const int128_t& v2 = right.get_int128();
        return (v1 < v2) ? -1 : (v2 < v1) ? 1 : 0;
    }
};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_FLOAT> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_FLOAT> {
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
        return std::string(buf);
    }
    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                               MemPool* mem_pool __attribute__((unused))) {
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_float_from_varchar<CppType>(dest, src);
        }
        return Status::InternalError("Fail to cast to float.");
    }
};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_DOUBLE> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_DOUBLE> {
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
        return std::string(buf);
    }
    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
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
            return Status::OK();
        }
        if (src_type->type() == OLAP_FIELD_TYPE_VARCHAR) {
            return convert_float_from_varchar<CppType>(dest, src);
        }
        return Status::InternalError("Fail to cast to double.");
    }
};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_DECIMAL> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_DECIMAL> {
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
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_DECIMAL_V2> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_DECIMAL_V2> {
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
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_DATE> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_DATE> {
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

    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
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
            return Status::OK();
        }

        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_TIMESTAMP) {
            int year, month, day, hour, minute, second, usec;
            auto src_value = unaligned_load<vectorized::TimestampValue>(src);
            src_value.to_timestamp(&year, &month, &day, &hour, &minute, &second, &usec);
            unaligned_store<CppType>(dest, (year << 9) + (month << 5) + day);
            return Status::OK();
        }

        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_INT) {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_INT>::CppType;
            auto src_value = unaligned_load<SrcType>(src);
            DateTimeValue dt;
            if (!dt.from_date_int64(src_value)) {
                return Status::InternalError("Fail to cast to date.");
            }
            CppType year = static_cast<CppType>(src_value / 10000);
            CppType month = static_cast<CppType>((src_value % 10000) / 100);
            CppType day = static_cast<CppType>(src_value % 100);
            unaligned_store<CppType>(dest, (year << 9) + (month << 5) + day);
            return Status::OK();
        }

        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_VARCHAR>::CppType;
            auto src_value = unaligned_load<SrcType>(src);
            DateTimeValue dt;
            for (const auto& format : DATE_FORMATS) {
                if (dt.from_date_format_str(format.c_str(), format.length(), src_value.get_data(),
                                            src_value.get_size())) {
                    unaligned_store<CppType>(dest, (dt.year() << 9) + (dt.month() << 5) + dt.day());
                    return Status::OK();
                }
            }
            return Status::InternalError("Fail to cast to date.");
        }

        return Status::InternalError("Fail to cast to date.");
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
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_DATE_V2> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_DATE_V2> {
    static Status from_string(void* buf, const std::string& scan_key) {
        vectorized::DateValue date;
        if (!date.from_string(scan_key.data(), scan_key.size())) {
            // Compatible with OLAP_FIELD_TYPE_DATE
            date.from_string("1400-01-01", sizeof("1400-01-01") - 1);
        }
        unaligned_store<vectorized::DateValue>(buf, date);
        return Status::OK();
    }

    static std::string to_string(const void* src) {
        auto src_val = unaligned_load<vectorized::DateValue>(src);
        return src_val.to_string();
    }

    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                               MemPool* mem_pool __attribute__((unused))) {
        auto converter = vectorized::get_type_converter(src_type->type(), OLAP_FIELD_TYPE_DATE_V2);
        RETURN_IF_ERROR(converter->convert(dest, src, mem_pool));
        return Status::OK();
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
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_DATETIME> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_DATETIME> {
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
        return std::string(buf);
    }

    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                               MemPool* memPool __attribute__((unused))) {
        // when convert date to datetime, automatic padding zero
        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_DATE) {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_DATE>::CppType;
            auto value = unaligned_load<SrcType>(src);
            int day = static_cast<int>(value & 31);
            int mon = static_cast<int>(value >> 5 & 15);
            int year = static_cast<int>(value >> 9);
            unaligned_store<CppType>(dest, (year * 10000L + mon * 100L + day) * 1000000);
            return Status::OK();
        }

        // when convert date to datetime, automatic padding zero
        if (src_type->type() == FieldType::OLAP_FIELD_TYPE_DATE_V2) {
            auto src_value = unaligned_load<vectorized::DateValue>(src);
            int year, month, day;
            src_value.to_date(&year, &month, &day);
            unaligned_store<CppType>(dest, (year * 10000L + month * 100L + day) * 1000000);
            return Status::OK();
        }

        return Status::InternalError("Fail to cast to datetime.");
    }
    static void set_to_max(void* buf) { unaligned_store<CppType>(buf, 99991231235959L); }
    static void set_to_min(void* buf) { unaligned_store<CppType>(buf, 101000000); }
};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_TIMESTAMP> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_TIMESTAMP> {
    static Status from_string(void* buf, const std::string& scan_key) {
        auto timestamp = unaligned_load<vectorized::TimestampValue>(buf);
        if (!timestamp.from_string(scan_key.data(), scan_key.size())) {
            // Compatible with OLAP_FIELD_TYPE_DATETIME
            timestamp.from_string("1400-01-01 00:00:00", sizeof("1400-01-01 00:00:00") - 1);
        }
        unaligned_store<vectorized::TimestampValue>(buf, timestamp);
        return Status::OK();
    }

    static std::string to_string(const void* src) {
        auto timestamp = unaligned_load<vectorized::TimestampValue>(src);
        return timestamp.to_string();
    }

    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type, MemPool* mem_pool) {
        vectorized::TimestampValue value;
        auto converter = vectorized::get_type_converter(src_type->type(), OLAP_FIELD_TYPE_TIMESTAMP);
        auto st = converter->convert(&value, src, mem_pool);
        unaligned_store<vectorized::TimestampValue>(dest, value);
        RETURN_IF_ERROR(st);
        return Status::OK();
    }
    static void set_to_max(void* buf) { unaligned_store<CppType>(buf, vectorized::timestamp::MAX_TIMESTAMP); }
    static void set_to_min(void* buf) { unaligned_store<CppType>(buf, vectorized::timestamp::MIN_TIMESTAMP); }
};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_CHAR> : public ScalarTypeInfoImplBase<OLAP_FIELD_TYPE_CHAR> {
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
    static Status from_string(void* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > OLAP_STRING_MAX_LENGTH) {
            return Status::InvalidArgument(fmt::format("String(length={}) is too long, the max length is: {}",
                                                       value_len, OLAP_STRING_MAX_LENGTH));
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
    static void deep_copy(void* dest, const void* src, MemPool* mem_pool) {
        auto l_slice = unaligned_load<Slice>(dest);
        auto r_slice = unaligned_load<Slice>(src);
        l_slice.data = reinterpret_cast<char*>(mem_pool->allocate(r_slice.size));
        assert(l_slice.data != nullptr);
        memory_copy(l_slice.data, r_slice.data, r_slice.size);
        l_slice.size = r_slice.size;
        unaligned_store<Slice>(dest, l_slice);
    }

    static void copy_object(void* dest, const void* src, MemPool* mem_pool) { deep_copy(dest, src, mem_pool); }

    static void direct_copy(void* dest, const void* src, MemPool* mem_pool) {
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
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_VARCHAR> : public ScalarTypeInfoImpl<OLAP_FIELD_TYPE_CHAR> {
    static const FieldType type = OLAP_FIELD_TYPE_VARCHAR;
    static const int32_t size = TypeTraits<OLAP_FIELD_TYPE_VARCHAR>::size;

    static Status from_string(void* buf, const std::string& scan_key) {
        size_t value_len = scan_key.length();
        if (value_len > OLAP_STRING_MAX_LENGTH) {
            LOG(WARNING) << "String(length=" << value_len << ") is too long, the max length is "
                         << OLAP_STRING_MAX_LENGTH;
            return Status::InternalError(fmt::format("String(length={}) is too long, the max length is: {}", value_len,
                                                     OLAP_STRING_MAX_LENGTH));
        }

        auto slice = unaligned_load<Slice>(buf);
        memory_copy(slice.data, scan_key.c_str(), value_len);
        slice.size = value_len;
        unaligned_store<Slice>(buf, slice);
        return Status::OK();
    }

    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type, MemPool* mem_pool) {
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
                return Status::InternalError("Fail to malloc memory");
            }
            memcpy(slice->data, result.c_str(), result.size());
            slice->size = result.size();
            return Status::OK();
        }
        return Status::InternalError("Fail to cast to varchar.");
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
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_HLL> : public ScalarTypeInfoImpl<OLAP_FIELD_TYPE_VARCHAR> {
    static const FieldType type = OLAP_FIELD_TYPE_HLL;
    static const int32_t size = TypeTraits<OLAP_FIELD_TYPE_HLL>::size;
    /*
     * Hyperloglog type only used as value, so
     * cmp/from_string/set_to_max/set_to_min function
     * in this struct has no significance
     */

    // See copy_row_in_memtable() in olap/row.h, will be removed in future.
    static void copy_object(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        Slice dst_slice;
        auto src_slice = unaligned_load<Slice>(src);
        DCHECK_EQ(src_slice.size, 0);
        dst_slice.data = src_slice.data;
        dst_slice.size = 0;
        unaligned_store<Slice>(dest, dst_slice);
    }
};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_OBJECT> : public ScalarTypeInfoImpl<OLAP_FIELD_TYPE_VARCHAR> {
    static const FieldType type = OLAP_FIELD_TYPE_OBJECT;
    static const int32_t size = TypeTraits<OLAP_FIELD_TYPE_OBJECT>::size;
    /*
     * Object type only used as value, so
     * cmp/from_string/set_to_max/set_to_min function
     * in this struct has no significance
     */

    // See copy_row_in_memtable() in olap/row.h, will be removed in future.
    static void copy_object(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        Slice dst_slice;
        auto src_slice = unaligned_load<Slice>(src);
        DCHECK_EQ(src_slice.size, 0);
        dst_slice.data = src_slice.data;
        dst_slice.size = 0;
        unaligned_store<Slice>(dest, dst_slice);
    }
};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_PERCENTILE> : public ScalarTypeInfoImpl<OLAP_FIELD_TYPE_VARCHAR> {
    static const FieldType type = OLAP_FIELD_TYPE_PERCENTILE;
    static const int32_t size = TypeTraits<OLAP_FIELD_TYPE_PERCENTILE>::size;
    // See copy_row_in_memtable() in olap/row.h, will be removed in future.
    static void copy_object(void* dest, const void* src, MemPool* mem_pool __attribute__((unused))) {
        auto dst_slice = reinterpret_cast<Slice*>(dest);
        auto src_slice = reinterpret_cast<const Slice*>(src);
        DCHECK_EQ(src_slice->size, 0);
        dst_slice->data = src_slice->data;
        dst_slice->size = 0;
    }
};

template <>
struct ScalarTypeInfoImpl<OLAP_FIELD_TYPE_JSON> : public ScalarTypeInfoImpl<OLAP_FIELD_TYPE_OBJECT> {
    static const FieldType type = OLAP_FIELD_TYPE_JSON;
    static const int32_t size = TypeTraits<OLAP_FIELD_TYPE_JSON>::size;

    static Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type, MemPool* mem_pool) {
        // TODO(mofei)
        return Status::InternalError("Not supported");
    }
};

void (*ScalarTypeInfoImpl<OLAP_FIELD_TYPE_CHAR>::set_to_max)(void*) = nullptr;

// NOTE
// These code could not be moved proceeding ScalarTypeInfoImpl speciliazation, otherwise
// will encounter `specialization after instantiation` error
template <FieldType ftype>
int TypeComparator<ftype>::cmp(const void* lhs, const void* rhs) {
    return ScalarTypeInfoImpl<ftype>::cmp(lhs, rhs);
}
#define M(ftype) template struct TypeComparator<ftype>;
APPLY_FOR_SUPPORTED_FIELD_TYPE(M)
#undef M

} // namespace starrocks
