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

#include <avrocpp/GenericDatum.hh>
#include <utility>

#include "common/statusor.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks {

class AvroUtils {
public:
    static std::string logical_type_to_string(const avro::LogicalType& logical_type);

    static int128_t bytes_to_decimal_integer(const std::vector<uint8_t>& from);
    static DateValue int_to_date_value(int32_t from);

    // Splits a count of `units_per_second` units since the epoch into whole seconds and a non-negative microsecond
    // part, so that values before 1970 keep their instant: -1 us is (-1 s, 999999 us), not (0 s, -1 us).
    static std::pair<int64_t, int64_t> split_epoch_to_second_and_micros(int64_t value, int64_t units_per_second);
    static StatusOr<TimestampValue> long_to_timestamp_value(const avro::GenericDatum& datum,
                                                            const cctz::time_zone& timezone);

    static Status datum_to_json(const avro::GenericDatum& datum, std::string* json_str, bool use_logical_type = false,
                                const cctz::time_zone& timezone = cctz::local_time_zone());
};

} // namespace starrocks
