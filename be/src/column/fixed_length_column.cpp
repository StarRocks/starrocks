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

#include "column/fixed_length_column.h"

namespace starrocks {
template class FixedLengthColumn<uint8_t>;
template class FixedLengthColumn<uint16_t>;
template class FixedLengthColumn<uint32_t>;
template class FixedLengthColumn<uint64_t>;

template class FixedLengthColumn<int8_t>;
template class FixedLengthColumn<int16_t>;
template class FixedLengthColumn<int32_t>;
template class FixedLengthColumn<int64_t>;
template class FixedLengthColumn<int96_t>;
template class FixedLengthColumn<int128_t>;

template class FixedLengthColumn<float>;
template class FixedLengthColumn<double>;

template class FixedLengthColumn<uint24_t>;
template class FixedLengthColumn<decimal12_t>;

template class FixedLengthColumn<DateValue>;
template class FixedLengthColumn<DecimalV2Value>;
template class FixedLengthColumn<TimestampValue>;
} // namespace starrocks
