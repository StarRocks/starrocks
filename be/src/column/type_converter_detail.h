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

#pragma once

#include "base/hash/unaligned_access.h"
#include "gutil/strings/fastmem.h"
#include "types/decimalv2_value.h"

namespace starrocks {

template <typename SrcType, typename DstType>
struct ConvFunction {};

template <>
struct ConvFunction<decimal12_t, int128_t> {
    static inline void apply(const decimal12_t* src, int128_t* dst) {
        auto src_val = unaligned_load<decimal12_t>(src);
        DecimalV2Value tmp;
        tmp.from_olap_decimal(src_val.integer, src_val.fraction);
        unaligned_store<DecimalV2Value>(dst, tmp);
    }
};

template <>
struct ConvFunction<int128_t, decimal12_t> {
    static inline void apply(const int128_t* src, decimal12_t* dst) {
        auto src_val = unaligned_load<DecimalV2Value>(src);
        decimal12_t tmp;
        tmp.integer = src_val.int_value();
        tmp.fraction = src_val.frac_value();
        unaligned_store<decimal12_t>(dst, tmp);
    }
};

template <>
struct ConvFunction<DecimalV2Value, int128_t> {
    static inline void apply(const DecimalV2Value* src, int128_t* dst) {
        strings::memcpy_inlined(dst, src, sizeof(int128_t));
    }
};

template <>
struct ConvFunction<int128_t, DecimalV2Value> {
    static inline void apply(const int128_t* src, DecimalV2Value* dst) {
        strings::memcpy_inlined(dst, src, sizeof(int128_t));
    }
};

} // namespace starrocks
