#pragma once

#include "column/type_traits.h"

namespace starrocks {
namespace vectorized {
// NOTE(zc): now CppColumnTraits is only used for this class, so I move it here.
// Someday if it is used by others, please move it into a single file.
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

} // namespace vectorized
} // namespace starrocks
