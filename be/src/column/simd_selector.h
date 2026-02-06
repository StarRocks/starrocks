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

#include "base/simd/selector.h"
#include "column/type_traits.h"
#include "types/logical_type.h"

namespace starrocks {

// SIMD selector
// only support PrimaryType Arithmetic and DATE
template <LogicalType TYPE>
class SIMD_selector {
public:
    using Container = typename RunTimeColumnType<TYPE>::Container;
    using CppType = RunTimeCppType<TYPE>;
    using SelectVec = uint8_t*;

    // select if var var
    // dst[i] = select_vec[i] ? a[i] : b[i]
    static void select_if(SelectVec select_vec, Container& dst, const Container& a, const Container& b) {
        int size = dst.size();
        auto* start_dst = dst.data();
        auto* end_dst = dst.data() + size;

        auto* start_a = a.data();
        auto* start_b = b.data();

#ifdef __AVX2__
        if constexpr (sizeof(CppType) == 1) {
            avx2_select_if(select_vec, start_dst, start_a, start_b, size);
        } else if constexpr (sizeof(CppType) == 4) {
            avx2_select_if(select_vec, start_dst, start_a, start_b, size);
        } else if constexpr (could_use_common_select_if<CppType>()) {
            avx2_select_if_common_implement(select_vec, start_dst, start_a, start_b, size);
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        if constexpr (neon_could_use_common_select_if<CppType>()) {
            neon_select_if_common_implement(select_vec, start_dst, start_a, start_b, size);
        }
#endif

        while (start_dst < end_dst) {
            *start_dst = *select_vec ? *start_a : *start_b;
            select_vec++;
            start_dst++;
            start_a++;
            start_b++;
        }
    }

    // select if const var
    // dst[i] = select_vec[i] ? a : b[i]
    static void select_if(SelectVec select_vec, Container& dst, CppType a, const Container& b) {
        int size = dst.size();
        auto* start_dst = dst.data();
        auto* end_dst = dst.data() + size;

        [[maybe_unused]] const CppType* start_a = &a;
        auto* start_b = b.data();

#ifdef __AVX2__
        if constexpr (sizeof(RunTimeCppType<TYPE>) == 1) {
            avx2_select_if<CppType, true, false>(select_vec, start_dst, start_a, start_b, size);
        } else if constexpr (could_use_common_select_if<CppType>()) {
            avx2_select_if_common_implement<CppType, true, false>(select_vec, start_dst, start_a, start_b, size);
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        if constexpr (neon_could_use_common_select_if<CppType>()) {
            neon_select_if_common_implement<CppType, true, false>(select_vec, start_dst, start_a, start_b, size);
        }
#endif

        while (start_dst < end_dst) {
            *start_dst = *select_vec ? a : *start_b;
            select_vec++;
            start_dst++;
            start_b++;
        }
    }

    // select if var const
    // dst[i] = select_vec[i] ? a[i] : b
    static void select_if(SelectVec select_vec, Container& dst, const Container& a, const CppType b) {
        int size = dst.size();
        auto* start_dst = dst.data();
        auto* end_dst = dst.data() + size;

        auto* start_a = a.data();
        [[maybe_unused]] const CppType* start_b = &b;

#ifdef __AVX2__
        if constexpr (sizeof(RunTimeCppType<TYPE>) == 1) {
            avx2_select_if<CppType, false, true>(select_vec, start_dst, start_a, start_b, size);
        } else if constexpr (could_use_common_select_if<CppType>()) {
            avx2_select_if_common_implement<CppType, false, true>(select_vec, start_dst, start_a, start_b, size);
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        if constexpr (neon_could_use_common_select_if<CppType>()) {
            neon_select_if_common_implement<CppType, false, true>(select_vec, start_dst, start_a, start_b, size);
        }
#endif

        while (start_dst < end_dst) {
            *start_dst = *select_vec ? *start_a : b;
            select_vec++;
            start_dst++;
            start_a++;
        }
    }

    // select if const const
    // dst[i] = select_vec[i] ? a : b
    static void select_if(SelectVec select_vec, Container& dst, CppType a, CppType b) {
        int size = dst.size();
        auto* start_dst = dst.data();
        auto* end_dst = dst.data() + size;

        [[maybe_unused]] const CppType* start_a = &a;
        [[maybe_unused]] const CppType* start_b = &b;

#ifdef __AVX2__
        if constexpr (sizeof(RunTimeCppType<TYPE>) == 1) {
            avx2_select_if<CppType, true, true>(select_vec, start_dst, start_a, start_b, size);
        } else if constexpr (could_use_common_select_if<CppType>()) {
            avx2_select_if_common_implement<CppType, true, true>(select_vec, start_dst, start_a, start_b, size);
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        if constexpr (neon_could_use_common_select_if<CppType>()) {
            neon_select_if_common_implement<CppType, true, true>(select_vec, start_dst, start_a, start_b, size);
        }
#endif
        while (start_dst < end_dst) {
            *start_dst = *select_vec ? a : b;
            select_vec++;
            start_dst++;
        }
    }
};

} // namespace starrocks
