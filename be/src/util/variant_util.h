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

#include <arrow/util/endian.h>
#include <cctz/time_zone.h>

#include "formats/parquet/variant.h"
#include "types/variant_value.h"

namespace starrocks {
struct VariantUtil {
    // Get int64_t value from INT8/INT16/INT32/INT64 variant
    static StatusOr<int64_t> get_long(const Variant* variant);

    static std::string type_to_string(VariantType type);

    static Status variant_to_json(std::string_view metadata, std::string_view value, std::stringstream& json_str,
                                  cctz::time_zone timezone = cctz::local_time_zone());

    static inline uint32_t read_little_endian_unsigned32(const void* from, uint8_t size) {
        DCHECK_LE(size, 4);
        DCHECK_GE(size, 1);

        uint32_t result = 0;
        memcpy(&result, from, size);
        return arrow::bit_util::FromLittleEndian(result);
    }

    static std::string decimal4_to_string(DecimalValue<int32_t> decimal);
    static std::string decimal8_to_string(DecimalValue<int64_t> decimal);
    static std::string decimal16_to_string(DecimalValue<int128_t> decimal);

    static uint8_t primitiveHeader(VariantPrimitiveType primitive);
};

} // namespace starrocks
