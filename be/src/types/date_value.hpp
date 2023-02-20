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

#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks {
inline DateValue::operator TimestampValue() const {
    return TimestampValue{date::to_timestamp(_julian)};
}

inline TimestampValue::operator DateValue() const {
    return DateValue{timestamp::to_julian(_timestamp)};
}

inline void DateValue::to_date(int* year, int* month, int* day) const {
    date::to_date_with_cache(_julian, year, month, day);
}
} // namespace starrocks
