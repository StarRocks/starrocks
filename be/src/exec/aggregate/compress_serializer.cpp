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

#include <any>
#include <optional>

#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/decimalv3_column.h"
#include "column/nullable_column.h"
#include "common/status.h"
#include "exprs/literal.h"
#include "types/logical_type_infra.h"
#include "util/unaligned_access.h"

namespace starrocks {

template <size_t N>
struct int_type {};

template <>
struct int_type<1> {
    using type = int8_t;
};
template <>
struct int_type<2> {
    using type = int16_t;
};
template <>
struct int_type<4> {
    using type = int32_t;
};
template <>
struct int_type<8> {
    using type = int64_t;
};
template <>
struct int_type<16> {
    using type = __int128;
};

template <class T>
int leading_zeros(T v) {
    if (v == 0) return sizeof(T) * 8;
    typename std::make_unsigned<T>::type uv = v;
    return __builtin_clzll(static_cast<size_t>(uv)) - (sizeof(size_t) * 8 - sizeof(T) * 8);
}

template <>
int leading_zeros<int128_t>(int128_t v) {
    uint64_t high = (uint64_t)(v >> 64);
    uint64_t low = (uint64_t)v;

    if (high != 0) {
        return leading_zeros(high);
    } else {
        return 64 + leading_zeros(low);
    }
}

template <class T>
int get_used_bits(T min, T max) {
    using IntType = typename int_type<sizeof(T)>::type;
    auto vmin = unaligned_load<IntType>(&min);
    auto vmax = unaligned_load<IntType>(&max);
    IntType delta = vmax - vmin;
    return sizeof(T) * 8 - (leading_zeros<IntType>(delta));
}

std::optional<int> get_used_bits(LogicalType ltype, const VectorizedLiteral& begin, const VectorizedLiteral& end,
                                 std::any& base) {
    size_t used_bits = 0;
    bool applied = scalar_type_dispatch(ltype, [&]<LogicalType Type>() {
        if constexpr ((lt_is_integer<Type> || lt_is_decimal<Type> ||
                       lt_is_date<Type>)&&(sizeof(RunTimeCppType<Type>) <= 16)) {
            RunTimeCppType<Type> cs_min = ColumnHelper::get_const_value<Type>(begin.value().get());
            RunTimeCppType<Type> cs_max = ColumnHelper::get_const_value<Type>(end.value().get());
            base = cs_min;
            used_bits = get_used_bits(cs_min, cs_max);
            return true;
        }
        return false;
    });
    if (applied) {
        return used_bits;
    }
    return {};
}

template <class TSrc, class TDst>
void bitcompress_serialize(const TSrc* __restrict val, const uint8_t* __restrict nulls, TSrc base, size_t n, int offset,
                           TDst* __restrict dst) {
    using UTSrc = typename std::make_unsigned<TSrc>::type;
    if (nulls == nullptr) {
        for (size_t i = 0; i < n; ++i) {
            TDst v = UTSrc(val[i] - base);
            dst[i] |= v << offset;
        }
    } else {
        for (size_t i = 0; i < n; ++i) {
            TDst v = UTSrc(val[i] - base) & ~(-static_cast<TSrc>(nulls[i]));
            dst[i] |= TDst(nulls[i]) << offset;
            dst[i] |= v << (offset + 1);
        }
    }
}

template <class Dst>
class CompressSerializer : public ColumnVisitorAdapter<CompressSerializer<Dst>> {
public:
    using Base = ColumnVisitorAdapter<CompressSerializer<Dst>>;
    CompressSerializer(Dst* dst, const std::any& base, int offset)
            : Base(this), _dst(dst), _base(base), _offset(offset) {}

    Status do_visit(const NullableColumn& column) {
        _null_data = column.null_column_data().data();
        return column.data_column()->accept(this);
    }

    template <typename Column, typename T>
    void bit_compress(const Column& column) {
        if constexpr (sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8 || sizeof(T) == 16) {
            using SrcType = typename int_type<sizeof(T)>::type;
            const auto container = column.immutable_data();
            const auto& raw_data = container.data();
            size_t n = container.size();
            auto base = std::any_cast<T>(_base);
            auto tbase = unaligned_load<SrcType>(&base);
            bitcompress_serialize((SrcType*)raw_data, _null_data, tbase, n, _offset, _dst);
        } else {
            CHECK(false) << "unreachable";
        }
    }

    template <typename T>
    Status do_visit(const FixedLengthColumn<T>& column) {
        bit_compress<FixedLengthColumn<T>, T>(column);
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const DecimalV3Column<T>& column) {
        bit_compress<DecimalV3Column<T>, T>(column);
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const T& column) {
        CHECK(false) << "unreachable";
        return Status::NotSupported("unsupported type");
    }

private:
    Dst* _dst;
    const std::any& _base;
    int _offset;
    const uint8_t* _null_data = nullptr;
};

template <class T>
T mask(T bits) {
    if (bits == sizeof(T) * 8) return ~T(0);
    return (T(1) << bits) - 1;
}

template <class TSrc, class TDst>
void bitcompress_deserialize(const TSrc* __restrict src, uint8_t* __restrict nulls, TDst base, int n, int used_bits,
                             int offset, TDst* __restrict dst) {
    typename std::make_unsigned<TSrc>::type* usrc = (typename std::make_unsigned<TSrc>::type*)src;
    const uint8_t mask1 = mask<uint8_t>(1);
    const TSrc mask2 = mask<TSrc>(used_bits - offset - (nulls != nullptr));
    if (nulls == nullptr) {
        for (size_t i = 0; i < n; ++i) {
            dst[i] = ((usrc[i] >> (offset)) & mask2) + base;
        }
    } else {
        for (size_t i = 0; i < n; ++i) {
            nulls[i] = (usrc[i] >> offset) & mask1;
            dst[i] = ((usrc[i] >> (offset + 1)) & mask2) + base;
        }
    }
}

template <class Src>
class CompressDeserializer final : public ColumnVisitorMutableAdapter<CompressDeserializer<Src>> {
public:
    using Base = ColumnVisitorMutableAdapter<CompressDeserializer<Src>>;
    explicit CompressDeserializer(size_t num_rows, Src* src, const std::any& base, int offset, int used_bits)
            : Base(this), _num_rows(num_rows), _src(src), _base(base), _offset(offset), _used_bits(used_bits) {}

    Status do_visit(NullableColumn* column) {
        // TODO: opt me
        column->null_column_data().resize(_num_rows);
        _null_data = column->null_column_data().data();
        RETURN_IF_ERROR(column->mutable_data_column()->accept_mutable(this));
        column->update_has_null();
        return Status::OK();
    }

    template <typename Column, typename T>
    void bit_decompress(Column* column) {
        if constexpr (sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8 || sizeof(T) == 16) {
            using DstType = typename int_type<sizeof(T)>::type;
            column->resize(_num_rows);
            auto& container = column->get_data();
            auto* raw_data = container.data();
            auto base = std::any_cast<T>(_base);
            auto tbase = unaligned_load<DstType>(&base);
            bitcompress_deserialize(_src, _null_data, tbase, _num_rows, _used_bits, _offset, (DstType*)raw_data);
        } else {
            CHECK(false) << "unreachable";
        }
    }

    template <typename T>
    Status do_visit(FixedLengthColumn<T>* column) {
        bit_decompress<FixedLengthColumn<T>, T>(column);
        return Status::OK();
    }

    template <typename T>
    Status do_visit(DecimalV3Column<T>* column) {
        bit_decompress<DecimalV3Column<T>, T>(column);
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const T& column) {
        DCHECK(false) << "unreachable";
        return Status::NotSupported("unsupported type");
    }

private:
    size_t _num_rows;
    const Src* _src;
    const std::any& _base;
    int _offset;
    int _used_bits;
    uint8_t* _null_data = nullptr;
};

void bitcompress_serialize(const Columns& columns, const std::vector<std::any>& bases, const std::vector<int>& offsets,
                           size_t num_rows, size_t fixed_key_size, void* buffer) {
    for (size_t i = 0; i < columns.size(); ++i) {
        if (fixed_key_size == 1) {
            CompressSerializer<uint8_t> serializer((uint8_t*)buffer, bases[i], offsets[i]);
            (void)columns[i]->accept(&serializer);
        } else if (fixed_key_size == 4) {
            CompressSerializer<int> serializer((int*)buffer, bases[i], offsets[i]);
            (void)columns[i]->accept(&serializer);
        } else if (fixed_key_size == 8) {
            CompressSerializer<int64_t> serializer((int64_t*)buffer, bases[i], offsets[i]);
            (void)columns[i]->accept(&serializer);
        } else if (fixed_key_size == 16) {
            CompressSerializer<int128_t> serializer((int128_t*)buffer, bases[i], offsets[i]);
            (void)columns[i]->accept(&serializer);
        } else {
            DCHECK(false) << "unreachable path";
        }
    }
}

void bitcompress_deserialize(MutableColumns& columns, const std::vector<std::any>& bases, const std::vector<int>& offsets,
                             const std::vector<int>& used_bits, size_t num_rows, size_t fixed_key_size, void* buffer) {
    for (size_t i = 0; i < columns.size(); ++i) {
        if (fixed_key_size == 1) {
            CompressDeserializer<uint8_t> deserializer(num_rows, (uint8_t*)buffer, bases[i], offsets[i], used_bits[i]);
            (void)columns[i]->accept_mutable(&deserializer);
        } else if (fixed_key_size == 4) {
            CompressDeserializer<int> deserializer(num_rows, (int*)buffer, bases[i], offsets[i], used_bits[i]);
            (void)columns[i]->accept_mutable(&deserializer);
        } else if (fixed_key_size == 8) {
            CompressDeserializer<int64_t> deserializer(num_rows, (int64_t*)buffer, bases[i], offsets[i], used_bits[i]);
            (void)columns[i]->accept_mutable(&deserializer);
        } else if (fixed_key_size == 16) {
            CompressDeserializer<int128_t> deserializer(num_rows, (int128_t*)buffer, bases[i], offsets[i],
                                                        used_bits[i]);
            (void)columns[i]->accept_mutable(&deserializer);
        } else {
            DCHECK(false) << "unreachable path";
        }
    }
}

} // namespace starrocks