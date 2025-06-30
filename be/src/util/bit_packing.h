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

#include "common/config.h"
#include "util/bit_packing_avx2.h"
#include "util/bit_packing_default.h"

namespace starrocks {

class BitPacking {
public:
    template <typename OutType>
    static std::pair<const uint8_t*, int64_t> UnpackValues(int bit_width, const uint8_t* __restrict__ in,
                                                           int64_t in_bytes, int64_t num_values,
                                                           OutType* __restrict__ out) {
        if (bit_width == 0) {
            std::memset(out, 0, sizeof(OutType) * num_values);
            return std::make_pair(in, num_values);
        }
        if (PREDICT_FALSE(bit_width > sizeof(OutType) * 8)) {
            return std::make_pair(in, 0);
        }
        if (config::enable_bit_unpack_simd) {
            return starrocks::util::bitpacking_avx2::UnpackValues(bit_width, in, in_bytes, num_values, out);
        } else {
            return starrocks::util::bitpacking_default::UnpackValues(bit_width, in, in_bytes, num_values, out);
        }
    }
};

} // namespace starrocks
