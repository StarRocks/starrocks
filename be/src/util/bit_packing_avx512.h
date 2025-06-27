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

#include "util/bit_packing_default.h"
#include "util/bitpacking/unpack_16u.h"
#include "util/bitpacking/unpack_32u.h"
#include "util/bitpacking/unpack_8u.h"

namespace starrocks::util::bitpacking_avx512 {

template <typename T>
constexpr bool IsSupportedUnpackingType() {
    return std::is_same<T, uint8_t>::value || std::is_same<T, uint16_t>::value || std::is_same<T, uint32_t>::value ||
           std::is_same<T, uint64_t>::value;
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValues(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes,
                                                int64_t num_values, OutType* __restrict__ out) {
    static_assert(IsSupportedUnpackingType<OutType>(), "Only unsigned integers are supported.");

    const int64_t values_to_read = bitpacking_default::NumValuesToUnpack(bit_width, in_bytes, num_values);
    const uint8_t* in_pos = in;
    OutType* out_pos = out;

#define HANDLE_8U(...)                   \
    case 0: {                            \
        return unpack_0u8u(__VA_ARGS__); \
    }                                    \
    case 1: {                            \
        return unpack_1u8u(__VA_ARGS__); \
    }                                    \
    case 2: {                            \
        return unpack_2u8u(__VA_ARGS__); \
    }                                    \
    case 3: {                            \
        return unpack_3u8u(__VA_ARGS__); \
    }                                    \
    case 4: {                            \
        return unpack_4u8u(__VA_ARGS__); \
    }                                    \
    case 5: {                            \
        return unpack_5u8u(__VA_ARGS__); \
    }                                    \
    case 6: {                            \
        return unpack_6u8u(__VA_ARGS__); \
    }                                    \
    case 7: {                            \
        return unpack_7u8u(__VA_ARGS__); \
    }                                    \
    case 8: {                            \
        return unpack_8u8u(__VA_ARGS__); \
    }

#define HANDLE_16U(...)                    \
    case 9: {                              \
        return unpack_9u16u(__VA_ARGS__);  \
    }                                      \
    case 10: {                             \
        return unpack_10u16u(__VA_ARGS__); \
    }                                      \
    case 11: {                             \
        return unpack_11u16u(__VA_ARGS__); \
    }                                      \
    case 12: {                             \
        return unpack_12u16u(__VA_ARGS__); \
    }                                      \
    case 13: {                             \
        return unpack_13u16u(__VA_ARGS__); \
    }                                      \
    case 14: {                             \
        return unpack_14u16u(__VA_ARGS__); \
    }                                      \
    case 15: {                             \
        return unpack_15u16u(__VA_ARGS__); \
    }                                      \
    case 16: {                             \
        return unpack_16u16u(__VA_ARGS__); \
    }

#define HANDLE_32U(...)                    \
    case 17: {                             \
        return unpack_17u32u(__VA_ARGS__); \
    }                                      \
    case 18: {                             \
        return unpack_18u32u(__VA_ARGS__); \
    }                                      \
    case 19: {                             \
        return unpack_19u32u(__VA_ARGS__); \
    }                                      \
    case 20: {                             \
        return unpack_20u32u(__VA_ARGS__); \
    }                                      \
    case 21: {                             \
        return unpack_21u32u(__VA_ARGS__); \
    }                                      \
    case 22: {                             \
        return unpack_22u32u(__VA_ARGS__); \
    }                                      \
    case 23: {                             \
        return unpack_23u32u(__VA_ARGS__); \
    }                                      \
    case 24: {                             \
        return unpack_24u32u(__VA_ARGS__); \
    }                                      \
    case 25: {                             \
        return unpack_25u32u(__VA_ARGS__); \
    }                                      \
    case 26: {                             \
        return unpack_26u32u(__VA_ARGS__); \
    }                                      \
    case 27: {                             \
        return unpack_27u32u(__VA_ARGS__); \
    }                                      \
    case 28: {                             \
        return unpack_28u32u(__VA_ARGS__); \
    }                                      \
    case 29: {                             \
        return unpack_29u32u(__VA_ARGS__); \
    }                                      \
    case 30: {                             \
        return unpack_30u32u(__VA_ARGS__); \
    }                                      \
    case 31: {                             \
        return unpack_31u32u(__VA_ARGS__); \
    }                                      \
    case 32: {                             \
        return unpack_32u32u(__VA_ARGS__); \
    }

#define HANDLE_DEFAULT                        \
    default: {                                \
        return (const unsigned char*)nullptr; \
    }

    if constexpr (sizeof(OutType) == 1) {
        auto BitUnpackKernel = [&]() {
            switch (bit_width) {
                HANDLE_8U(in_pos, values_to_read, out_pos);
                HANDLE_DEFAULT
            }
        };
        in_pos = BitUnpackKernel();
    } else if constexpr (sizeof(OutType) == 2) {
        auto BitUnpackKernel = [&]() {
            switch (bit_width) {
                HANDLE_8U(in_pos, values_to_read, out_pos);
                HANDLE_16U(in_pos, values_to_read, out_pos);
                HANDLE_DEFAULT
            }
        };
        in_pos = BitUnpackKernel();
    } else if constexpr (sizeof(OutType) == 4 || sizeof(OutType) == 8) {
        auto BitUnpackKernel = [&]() {
            switch (bit_width) {
                HANDLE_8U(in_pos, values_to_read, out_pos);
                HANDLE_16U(in_pos, values_to_read, out_pos);
                HANDLE_32U(in_pos, values_to_read, out_pos);
                HANDLE_DEFAULT
            }
        };
        in_pos = BitUnpackKernel();
    } else {
        static_assert(sizeof(OutType) == 1 || sizeof(OutType) == 2 || sizeof(OutType) == 4 || sizeof(OutType) == 8,
                      "Unsupported output type size");
    }

#undef BitUnpackKernel

    return std::make_pair(in_pos, values_to_read);
}

} // namespace starrocks::util::bitpacking_avx512
