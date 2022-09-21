// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#include "column/fixed_length_column.h"

namespace starrocks::vectorized {
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
template class FixedLengthColumn<Ipv4Value>;
} // namespace starrocks::vectorized
