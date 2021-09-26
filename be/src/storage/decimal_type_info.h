// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "runtime/decimalv3.h"
#include "storage/types.h"
#include "util/guard.h"

namespace starrocks {

VALUE_GUARD(FieldType, DecimalFTGuard, ft_is_decimal, OLAP_FIELD_TYPE_DECIMAL32, OLAP_FIELD_TYPE_DECIMAL64,
            OLAP_FIELD_TYPE_DECIMAL128)

VALUE_GUARD(FieldType, InvalidFTGuard, ft_is_invalid, OLAP_FIELD_TYPE_MAX_VALUE);

template <FieldType TYPE, typename = DecimalFTGuard<TYPE>>
class DecimalTypeInfo final : public TypeInfo {
public:
    using CppType = typename CppTypeTraits<TYPE>::CppType;
    using Datum = vectorized::Datum;
    DecimalTypeInfo(int precision, int scale)
            : _delegate(ScalarTypeInfoResolver::instance()->get_scalar_type_info(DelegateType<TYPE>)),
              _precision(precision),
              _scale(scale) {
        static_assert(!ft_is_invalid<DelegateType<TYPE>>);
    }

    bool equal(const void* left, const void* right) const override { return _delegate->equal(left, right); }

    int cmp(const void* left, const void* right) const override { return _delegate->cmp(left, right); }

    void shallow_copy(void* dest, const void* src) const override { return _delegate->shallow_copy(dest, src); }

    void deep_copy(void* dest, const void* src, MemPool* mem_pool) const override {
        return _delegate->deep_copy(dest, src, mem_pool);
    }

    // See copy_row_in_memtable() in olap/row.h, will be removed in future.
    // It is same with deep_copy() for all type except for HLL and OBJECT type
    void copy_object(void* dest, const void* src, MemPool* mem_pool) const override {
        return _delegate->copy_object(dest, src, mem_pool);
    }

    void direct_copy(void* dest, const void* src, MemPool* mem_pool) const override {
        _delegate->direct_copy(dest, src, mem_pool);
    }

    template <typename From, typename To>
    static inline OLAPStatus to_decimal(const From* src, To* dst, int src_precision, int src_scale, int dst_precision,
                                        int dst_scale) {
        if (dst_scale < src_scale || dst_precision - dst_scale < src_precision - src_scale) {
            return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
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
        return OLAPStatus::OLAP_SUCCESS;
    }

    static inline OLAPStatus to_decimal(FieldType src_type, FieldType dst_type, const void* src, void* dst,
                                        int src_precision, int src_scale, int dst_precision, int dst_scale) {
#define TO_DECIMAL_MACRO(n, m)                                                                               \
                                                                                                             \
    if (src_type == OLAP_FIELD_TYPE_DECIMAL##n && dst_type == OLAP_FIELD_TYPE_DECIMAL##m) {                  \
        int##n##_t src_datum = 0;                                                                            \
        int##m##_t dst_datum = 0;                                                                            \
        src_datum = unaligned_load<typeof(src_datum)>(src);                                                  \
        auto overflow = to_decimal<int##n##_t, int##m##_t>(&src_datum, &dst_datum, src_precision, src_scale, \
                                                           dst_precision, dst_scale);                        \
        unaligned_store<typeof(dst_datum)>(dst, dst_datum);                                                  \
        return overflow;                                                                                     \
    }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
        TO_DECIMAL_MACRO(32, 32)
        TO_DECIMAL_MACRO(32, 64)
        TO_DECIMAL_MACRO(32, 128)
        TO_DECIMAL_MACRO(64, 32)
        TO_DECIMAL_MACRO(64, 64)
        TO_DECIMAL_MACRO(64, 128)
        TO_DECIMAL_MACRO(128, 32)
        TO_DECIMAL_MACRO(128, 64)
        TO_DECIMAL_MACRO(128, 128)
#pragma GCC diagnostic pop
#undef TO_DECIMAL_MACRO
        return OLAP_ERR_INVALID_SCHEMA;
    }

    //convert and deep copy value from other type's source
    OLAPStatus convert_from(void* dest, const void* src, const TypeInfoPtr& src_type,
                            MemPool* mem_pool) const override {
        switch (src_type->type()) {
        case OLAP_FIELD_TYPE_CHAR:
        case OLAP_FIELD_TYPE_VARCHAR: {
            using SrcType = typename CppTypeTraits<OLAP_FIELD_TYPE_VARCHAR>::CppType;
            auto src_value = reinterpret_cast<const SrcType*>(src);
            CppType result;
            auto fail = DecimalV3Cast::from_string<CppType>(&result, precision(), scale(), src_value->data,
                                                            src_value->size);
            if (UNLIKELY(fail)) {
                return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
            }
            memcpy(dest, &result, sizeof(CppType));
            return OLAPStatus::OLAP_SUCCESS;
        }
        case OLAP_FIELD_TYPE_DECIMAL32:
        case OLAP_FIELD_TYPE_DECIMAL64:
        case OLAP_FIELD_TYPE_DECIMAL128:
            return to_decimal(src_type->type(), type(), src, dest, src_type->precision(), src_type->scale(),
                              precision(), scale());
        default:
            return OLAPStatus::OLAP_ERR_INVALID_SCHEMA;
        }
    }

    OLAPStatus from_string(void* buf, const std::string& scan_key) const override {
        CppType* data_ptr = reinterpret_cast<CppType*>(buf);
        // Decimal strings in some predicates use decimal_precision_limit as precision,
        // when converted into decimal values, a smaller precision is used, DecimalTypeInfo::from_string
        // fail to convert these decimal strings and report errors; so use decimal_precision_limit
        // instead of smaller precision in DecimalTypeInfo::from_string.
        auto err = DecimalV3Cast::from_string<CppType>(data_ptr, decimal_precision_limit<CppType>, _scale,
                                                       scan_key.c_str(), scan_key.size());
        if (err) {
            return OLAP_ERR_INVALID_SCHEMA;
        }
        return OLAP_SUCCESS;
    }

    std::string to_string(const void* src) const override {
        const CppType* data_ptr = reinterpret_cast<const CppType*>(src);
        return DecimalV3Cast::to_string<CppType>(*data_ptr, _precision, _scale);
    }

    void set_to_max(void* buf) const override {
        CppType* data = reinterpret_cast<CppType*>(buf);
        *data = get_scale_factor<CppType>(_precision) - 1;
    }

    void set_to_min(void* buf) const override {
        CppType* data = reinterpret_cast<CppType*>(buf);
        *data = 1 - get_scale_factor<CppType>(_precision);
    }

    uint32_t hash_code(const void* data, uint32_t seed) const override { return _delegate->hash_code(data, seed); }

    size_t size() const override { return _delegate->size(); }

    int precision() const override { return _precision; }

    int scale() const override { return _scale; }

    FieldType type() const override { return TYPE; }

    std::string to_zone_map_string(const void* src) { return _delegate->to_string(src); }

protected:
    int _datum_cmp_impl(const Datum& left, const Datum& right) const override {
        const CppType& lhs = left.get<CppType>();
        const CppType& rhs = right.get<CppType>();
        return (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
    }

private:
    const ScalarTypeInfo* _delegate;
    const int _precision;
    const int _scale;
};
} //namespace starrocks
