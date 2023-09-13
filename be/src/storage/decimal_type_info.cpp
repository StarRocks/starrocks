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

#include "storage/decimal_type_info.h"

#include "column/datum.h"
#include "gutil/casts.h"
#include "runtime/decimalv3.h"
#include "storage/type_traits.h"
#include "storage/types.h"
#include "util/guard.h"

namespace starrocks {

VALUE_GUARD(LogicalType, DecimalFTGuard, ft_is_decimal, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128)

VALUE_GUARD(LogicalType, InvalidFTGuard, ft_is_invalid, TYPE_MAX_VALUE);

template <LogicalType TYPE, typename = DecimalFTGuard<TYPE>>
class DecimalTypeInfo final : public TypeInfo {
public:
    virtual ~DecimalTypeInfo() = default;

    using CppType = typename CppTypeTraits<TYPE>::CppType;
    DecimalTypeInfo(int precision, int scale)
            : _delegate(get_scalar_type_info(DelegateType<TYPE>)), _precision(precision), _scale(scale) {
        static_assert(!ft_is_invalid<DelegateType<TYPE>>);
    }

    void shallow_copy(void* dest, const void* src) const override { return _delegate->shallow_copy(dest, src); }

    void deep_copy(void* dest, const void* src, MemPool* mem_pool) const override {
        return _delegate->deep_copy(dest, src, mem_pool);
    }

    void direct_copy(void* dest, const void* src) const override { _delegate->direct_copy(dest, src); }

    template <typename From, typename To>
    static inline Status to_decimal(const From* src, To* dst, int src_precision, int src_scale, int dst_precision,
                                    int dst_scale) {
        if (dst_scale < src_scale || dst_precision - dst_scale < src_precision - src_scale) {
            return Status::InvalidArgument("Fail to cast to decimal.");
        }
        int adjust_scale = dst_scale - src_scale;
        if (adjust_scale == 0) {
            DecimalV3Cast::to_decimal_trivial<From, To, false>(*src, dst);
        } else if (adjust_scale > 0) {
            const auto scale_factor = get_scale_factor<To>(adjust_scale);
            DecimalV3Cast::to_decimal<From, To, To, true, false>(*src, scale_factor, dst);
        } else {
            const auto scale_factor = get_scale_factor<From>(-adjust_scale);
            DecimalV3Cast::to_decimal<From, To, From, false, false>(*src, scale_factor, dst);
        }
        return Status::OK();
    }

    static inline Status to_decimal(LogicalType src_type, LogicalType dst_type, const void* src, void* dst,
                                    int src_precision, int src_scale, int dst_precision, int dst_scale) {
#define TO_DECIMAL_MACRO(n, m)                                                                               \
                                                                                                             \
    if (src_type == TYPE_DECIMAL##n && dst_type == TYPE_DECIMAL##m) {                                        \
        int##n##_t src_datum = 0;                                                                            \
        int##m##_t dst_datum = 0;                                                                            \
        src_datum = unaligned_load<typeof(src_datum)>(src);                                                  \
        auto overflow = to_decimal<int##n##_t, int##m##_t>(&src_datum, &dst_datum, src_precision, src_scale, \
                                                           dst_precision, dst_scale);                        \
        unaligned_store<typeof(dst_datum)>(dst, dst_datum);                                                  \
        return overflow;                                                                                     \
    }
        DIAGNOSTIC_PUSH

#if defined(__GNUC__) && !defined(__clang__)
        DIAGNOSTIC_IGNORE("-Wmaybe-uninitialized")
#endif
        TO_DECIMAL_MACRO(32, 32)
        TO_DECIMAL_MACRO(32, 64)
        TO_DECIMAL_MACRO(32, 128)
        TO_DECIMAL_MACRO(64, 32)
        TO_DECIMAL_MACRO(64, 64)
        TO_DECIMAL_MACRO(64, 128)
        TO_DECIMAL_MACRO(128, 32)
        TO_DECIMAL_MACRO(128, 64)
        TO_DECIMAL_MACRO(128, 128)

        DIAGNOSTIC_POP
#undef TO_DECIMAL_MACRO
        return Status::InvalidArgument("Fail to cast to decimal.");
    }

    static inline Status to_decimal(LogicalType src_type, LogicalType dst_type, const Datum& src_datum,
                                    Datum& dst_datum, int src_precision, int src_scale, int dst_precision,
                                    int dst_scale) {
#define TO_DECIMAL_MACRO(n, m)                                                                           \
                                                                                                         \
    if (src_type == TYPE_DECIMAL##n && dst_type == TYPE_DECIMAL##m) {                                    \
        int##m##_t dst_val = 0;                                                                          \
        int##n##_t src_val = src_datum.get_int##n();                                                     \
        auto overflow = to_decimal<int##n##_t, int##m##_t>(&src_val, &dst_val, src_precision, src_scale, \
                                                           dst_precision, dst_scale);                    \
        dst_datum.set_int##m(dst_val);                                                                   \
        return overflow;                                                                                 \
    }

        DIAGNOSTIC_PUSH

#if defined(__GNUC__) && !defined(__clang__)
        DIAGNOSTIC_IGNORE("-Wmaybe-uninitialized")
#endif
        TO_DECIMAL_MACRO(32, 32)
        TO_DECIMAL_MACRO(32, 64)
        TO_DECIMAL_MACRO(32, 128)
        TO_DECIMAL_MACRO(64, 32)
        TO_DECIMAL_MACRO(64, 64)
        TO_DECIMAL_MACRO(64, 128)
        TO_DECIMAL_MACRO(128, 32)
        TO_DECIMAL_MACRO(128, 64)
        TO_DECIMAL_MACRO(128, 128)

        DIAGNOSTIC_POP

#undef TO_DECIMAL_MACRO

        return Status::InvalidArgument("Fail to cast to decimal.");
    }

    Status from_string(void* buf, const std::string& scan_key) const override {
        auto* data_ptr = reinterpret_cast<CppType*>(buf);
        // Decimal strings in some predicates use decimal_precision_limit as precision,
        // when converted into decimal values, a smaller precision is used, DecimalTypeInfo::from_string
        // fail to convert these decimal strings and report errors; so use decimal_precision_limit
        // instead of smaller precision in DecimalTypeInfo::from_string.
        auto err = DecimalV3Cast::from_string<CppType>(data_ptr, decimal_precision_limit<CppType>, _scale,
                                                       scan_key.c_str(), scan_key.size());
        if (err) {
            return Status::InvalidArgument("Fail to cast to decimal.");
        }
        return Status::OK();
    }

    std::string to_string(const void* src) const override {
        const auto* data_ptr = reinterpret_cast<const CppType*>(src);
        return DecimalV3Cast::to_string<CppType>(*data_ptr, _precision, _scale);
    }

    void set_to_max(void* buf) const override {
        auto* data = reinterpret_cast<CppType*>(buf);
        *data = get_scale_factor<CppType>(_precision) - 1;
    }

    void set_to_min(void* buf) const override {
        auto* data = reinterpret_cast<CppType*>(buf);
        *data = 1 - get_scale_factor<CppType>(_precision);
    }

    size_t size() const override { return _delegate->size(); }

    int precision() const override { return _precision; }

    int scale() const override { return _scale; }

    LogicalType type() const override { return TYPE; }

    std::string to_zone_map_string(const void* src) { return _delegate->to_string(src); }

protected:
    int _datum_cmp_impl(const Datum& left, const Datum& right) const override {
        const auto& lhs = left.get<CppType>();
        const auto& rhs = right.get<CppType>();
        return (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
    }

private:
    const TypeInfo* _delegate;
    const int _precision;
    const int _scale;
};

TypeInfoPtr get_decimal_type_info(LogicalType type, int precision, int scale) {
    switch (type) {
    case TYPE_DECIMAL32:
        return std::make_shared<DecimalTypeInfo<TYPE_DECIMAL32>>(precision, scale);
    case TYPE_DECIMAL64:
        return std::make_shared<DecimalTypeInfo<TYPE_DECIMAL64>>(precision, scale);
    case TYPE_DECIMAL128:
        return std::make_shared<DecimalTypeInfo<TYPE_DECIMAL128>>(precision, scale);
    default:
        return nullptr;
    }
}

std::string get_decimal_zone_map_string(TypeInfo* type_info, const void* value) {
    switch (type_info->type()) {
    case TYPE_DECIMAL32: {
        auto* decimal_type_info = down_cast<DecimalTypeInfo<TYPE_DECIMAL32>*>(type_info);
        return decimal_type_info->to_zone_map_string(value);
    }
    case TYPE_DECIMAL64: {
        auto* decimal_type_info = down_cast<DecimalTypeInfo<TYPE_DECIMAL64>*>(type_info);
        return decimal_type_info->to_zone_map_string(value);
    }
    case TYPE_DECIMAL128: {
        auto* decimal_type_info = down_cast<DecimalTypeInfo<TYPE_DECIMAL128>*>(type_info);
        return decimal_type_info->to_zone_map_string(value);
    }
    default:
        DCHECK(false);
        return {};
    }
}

} //namespace starrocks
