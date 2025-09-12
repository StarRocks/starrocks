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
#include <cctz/time_zone.h>

#include "formats/parquet/variant.h"
#include "types/variant_value.h"

namespace starrocks {
struct VariantUtil {
    static std::string type_to_string(VariantType type);

    static Status variant_to_json(std::string_view metadata, std::string_view value, std::stringstream& json_str,
                                  cctz::time_zone timezone = cctz::local_time_zone());
    static uint32_t readLittleEndianUnsigned(const void* from, uint8_t size);

    static std::string decimal4_to_string(DecimalValue<int32_t> decimal);
    static std::string decimal8_to_string(DecimalValue<int64_t> decimal);
    static std::string decimal16_to_string(DecimalValue<int128_t> decimal);

    static uint8_t primitiveHeader(VariantPrimitiveType primitive);
};

} // namespace starrocks
