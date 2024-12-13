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

#include "runtime/type_pack.h"

#include "column/datum.h"
#include "column/type_traits.h"
#include "gutil/strings/substitute.h"
#include "types/logical_type.h"
#include "util/faststring.h"
#include "util/meta_macro.h"

namespace starrocks {
template <LogicalType LT, typename = guard::Guard>
struct Packer {
    using CppType = RunTimeCppType<LT>;
    static void pack(faststring* s, const Datum& datum) {
        const auto& value = datum.get<CppType>();
        const auto& str = value.to_string();
        put_fixed64_le(s, str.size());
        s->append(str.data(), str.size());
    }
};

template <LogicalType LT>
struct Packer<LT, NumericLTGuard<LT>> {
    using CppType = RunTimeCppType<LT>;
    static constexpr auto CppTypeSize = sizeof(CppType);
    static void pack(faststring* s, const Datum& datum) {
        const auto& value = datum.get<CppType>();
        if constexpr (CppTypeSize == 1) {
            const auto* u8 = reinterpret_cast<const uint8_t*>(&value);
            s->push_back(*u8);
        } else if constexpr (CppTypeSize == 2) {
            const auto* u16 = reinterpret_cast<const uint16_t*>(&value);
            uint8_t buff[2];
            encode_fixed16_le(buff, *u16);
            s->append(buff, 2);
        } else if constexpr (CppTypeSize == 4) {
            const auto* u32 = reinterpret_cast<const uint32_t*>(&value);
            put_fixed32_le(s, *u32);
        } else if constexpr (CppTypeSize == 8) {
            const auto* u64 = reinterpret_cast<const uint64_t*>(&value);
            put_fixed64_le(s, *u64);
        } else if constexpr (CppTypeSize == 16) {
            const auto* u128 = reinterpret_cast<const uint128_t*>(&value);
            put_fixed128_le(s, *u128);
        } else {
            static_assert(lt_is_numeric<LT>, "Logical Type must be numeric type");
        }
    }
};

UNION_VALUE_GUARD(LogicalType, BinaryOrStringLTGuard, lt_binary_or_string, lt_is_binary_struct, lt_is_string_struct);
template <LogicalType LT>
struct Packer<LT, BinaryOrStringLTGuard<LT>> {
    using CppType = RunTimeCppType<LT>;
    static void pack(faststring* s, const Datum& datum) {
        static_assert(std::is_same_v<Slice, CppType>, "CppType must be Slice");
        const auto& slice = datum.get<Slice>();
        put_fixed64_le(s, slice.size);
        s->append(slice.data, slice.size);
    }
};

template <>
struct Packer<TYPE_JSON> {
    using CppType = RunTimeCppType<TYPE_JSON>;
    static void pack(faststring* s, const Datum& datum) {
        static_assert(std::is_same_v<JsonValue*, CppType>, "CppType must be JsonValue*");
        const auto& json = datum.get<JsonValue*>();
        const auto& str = json->to_string_uncheck();
        put_fixed64_le(s, str.size());
        s->append(str.data(), str.size());
    }
};

template <LogicalType LT>
struct Packer<LT, DateOrDateTimeLTGuard<LT>> {
    using CppType = RunTimeCppType<LT>;
    static void pack(faststring* s, const Datum& datum) {
        const auto& value = datum.get<CppType>();
        uint64_t seconds = 0;
        if constexpr (lt_is_date<LT>) {
            seconds = static_cast<TimestampValue>(value).to_unix_second();
        } else if constexpr (lt_is_datetime<LT>) {
            seconds = value.to_unix_second();
        }
        auto* u64 = reinterpret_cast<uint64_t*>(&seconds);
        put_fixed64_le(s, *u64);
    }
};

#define LT_TO_PACKER_SINGLE_ENTRY_CTOR(nop, lt) \
    { lt, Packer<lt>::pack }

#define LT_TO_PACKER_ENTRY(nop, ...) \
    DEF_BINARY_RELATION_ENTRY_SEP_COMMA(LT_TO_PACKER_SINGLE_ENTRY_CTOR, nop, ##__VA_ARGS__)

static const std::unordered_map<LogicalType, PackFunc> g_pack_func_table = {
        LT_TO_PACKER_ENTRY(nop, TYPE_BOOLEAN, TYPE_TINYINT),
        LT_TO_PACKER_ENTRY(nop, TYPE_SMALLINT),
        LT_TO_PACKER_ENTRY(nop, TYPE_INT, TYPE_DECIMAL32, TYPE_FLOAT),
        LT_TO_PACKER_ENTRY(nop, TYPE_BIGINT, TYPE_DECIMAL64, TYPE_DOUBLE),
        LT_TO_PACKER_ENTRY(nop, TYPE_LARGEINT, TYPE_DECIMAL128),
        LT_TO_PACKER_ENTRY(nop, TYPE_VARCHAR, TYPE_CHAR, TYPE_BINARY, TYPE_VARBINARY),
        LT_TO_PACKER_ENTRY(nop, TYPE_JSON),
        LT_TO_PACKER_ENTRY(nop, TYPE_DATE, TYPE_DATETIME),
};

StatusOr<PackFunc> get_pack(LogicalType lt) {
    auto it = g_pack_func_table.find(lt);
    if (it != g_pack_func_table.end()) {
        return it->second;
    } else {
        return Status::NotFound(strings::Substitute("Packer for type $0 is absent", type_to_string(lt)));
    }
}

} // namespace starrocks