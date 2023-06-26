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

#include <gutil/strings/fastmem.h>

#include "column/column_builder.h"
#include "runtime/decimalv3.h"

namespace starrocks {

UNION_VALUE_GUARD(LogicalType, FixedNonDecimalTypeGuard, lt_is_fixed_non_decimal, lt_is_boolean_struct,
                  lt_is_integer_struct, lt_is_float_struct, lt_is_date_struct, lt_is_datetime_struct,
                  lt_is_decimalv2_struct, lt_is_time_struct)

template <bool check_overflow, LogicalType Type, LogicalType ResultType>
struct DecimalDecimalCast {
    static inline ColumnPtr evaluate(const ColumnPtr& column, int to_precision, int to_scale) {
        using FromCppType = RunTimeCppType<Type>;
        using FromColumnType = RunTimeColumnType<Type>;
        using ToCppType = RunTimeCppType<ResultType>;
        using ToColumnType = RunTimeColumnType<ResultType>;
        auto num_rows = column->size();
        auto data_column = ColumnHelper::cast_to_raw<Type>(column);
        int from_scale = data_column->scale();

        // source type and target type has the same logical type and scale
        if (to_scale == from_scale && Type == ResultType) {
            auto result = column->clone_shared();
            ColumnHelper::cast_to_raw<Type>(result)->set_precision(to_precision);
            return result;
        }

        const auto data = &data_column->get_data().front();

        auto result = ToColumnType::create(to_precision, to_scale, num_rows);
        NullColumnPtr null_column;
        NullColumn::ValueType* nulls = nullptr;
        auto has_null = false;
        auto result_data = &ColumnHelper::cast_to_raw<ResultType>(result)->get_data().front();

        if constexpr (check_overflow) {
            null_column = NullColumn::create();
            null_column->resize(num_rows);
            nulls = &null_column->get_data().front();
        }

        if (to_scale == from_scale) {
            if constexpr (sizeof(FromCppType) <= sizeof(ToCppType)) {
                for (auto i = 0; i < num_rows; ++i) {
                    (void)DecimalV3Cast::to_decimal_trivial<FromCppType, ToCppType, check_overflow>(data[i],
                                                                                                    &result_data[i]);
                }
            } else {
                for (auto i = 0; i < num_rows; ++i) {
                    auto overflow = DecimalV3Cast::to_decimal_trivial<FromCppType, ToCppType, check_overflow>(
                            data[i], &result_data[i]);
                    if constexpr (check_overflow) {
                        if (overflow) {
                            has_null = true;
                            nulls[i] = DATUM_NULL;
                        }
                    }
                }
            }
        } else if (to_scale > from_scale) {
            const auto scale_factor = get_scale_factor<ToCppType>(to_scale - from_scale);
            for (auto i = 0; i < num_rows; ++i) {
                auto overflow = DecimalV3Cast::to_decimal<FromCppType, ToCppType, ToCppType, true, check_overflow>(
                        data[i], scale_factor, &result_data[i]);
                if constexpr (check_overflow) {
                    if (overflow) {
                        has_null = true;
                        nulls[i] = DATUM_NULL;
                    }
                }
            }
        } else {
            const auto scale_factor = get_scale_factor<FromCppType>(from_scale - to_scale);
            for (auto i = 0; i < num_rows; ++i) {
                auto overflow = DecimalV3Cast::to_decimal<FromCppType, ToCppType, FromCppType, false, check_overflow>(
                        data[i], scale_factor, &result_data[i]);
                if constexpr (check_overflow) {
                    if (overflow) {
                        has_null = true;
                        nulls[i] = DATUM_NULL;
                    }
                }
            }
        }
        if constexpr (check_overflow) {
            ColumnBuilder<ResultType> builder(result, null_column, has_null);
            return builder.build(column->is_constant());
        } else {
            return result;
        }
    }
};
template <bool check_overflow, LogicalType, LogicalType, typename = guard::Guard, typename = guard::Guard>
struct DecimalNonDecimalCast {};

// cast: bool,integer,float,datetime,date, time <-> decimal
template <bool check_overflow, LogicalType DecimalType, LogicalType NonDecimalType>
struct DecimalNonDecimalCast<check_overflow, DecimalType, NonDecimalType, DecimalLTGuard<DecimalType>,
                             FixedNonDecimalTypeGuard<NonDecimalType>> {
    using DecimalCppType = RunTimeCppType<DecimalType>;
    using DecimalColumnType = RunTimeColumnType<DecimalType>;
    using NonDecimalCppType = RunTimeCppType<NonDecimalType>;
    using NonDecimalColumnType = RunTimeColumnType<NonDecimalType>;

    static inline ColumnPtr decimal_from(const ColumnPtr& column, int precision, int scale) {
        const auto num_rows = column->size();
        auto result = DecimalColumnType::create(precision, scale, num_rows);
        const auto data = &ColumnHelper::cast_to_raw<NonDecimalType>(column)->get_data().front();
        auto result_data = &ColumnHelper::cast_to_raw<DecimalType>(result)->get_data().front();
        NullColumnPtr null_column;
        NullColumn::ValueType* nulls = nullptr;
        bool has_null = false;
        if constexpr (check_overflow) {
            null_column = NullColumn::create();
            null_column->resize(num_rows);
            nulls = &null_column->get_data().front();
        }

        [[maybe_unused]] const auto scale_factor = get_scale_factor<DecimalCppType>(scale);

        for (auto i = 0; i < num_rows; ++i) {
            bool overflow = false;
            if constexpr (lt_is_boolean<NonDecimalType>) {
                using SignedBooleanType = std::make_signed_t<NonDecimalCppType>;
                overflow = DecimalV3Cast::from_integer<SignedBooleanType, DecimalCppType, check_overflow>(
                        (SignedBooleanType)(data[i]), scale_factor, &result_data[i]);
            } else if constexpr (lt_is_integer<NonDecimalType>) {
                overflow = DecimalV3Cast::from_integer<NonDecimalCppType, DecimalCppType, check_overflow>(
                        data[i], scale_factor, &result_data[i]);
            } else if constexpr (lt_is_float<NonDecimalType>) {
                overflow = DecimalV3Cast::from_float<NonDecimalCppType, DecimalCppType>(data[i], scale_factor,
                                                                                        &result_data[i]);
            } else if constexpr (lt_is_time<NonDecimalType>) {
                double datum = timestamp::time_to_literal(data[i]);
                overflow = DecimalV3Cast::from_float<NonDecimalCppType, DecimalCppType>(datum, scale_factor,
                                                                                        &result_data[i]);
            } else if constexpr (lt_is_date<NonDecimalType>) {
                static_assert(std::is_same_v<NonDecimalCppType, DateValue>, "DateValue type is required");
                int32_t datum = ((DateValue&)data[i]).to_date_literal();
                overflow = DecimalV3Cast::from_integer<int32_t, DecimalCppType, check_overflow>(datum, scale_factor,
                                                                                                &result_data[i]);
            } else if constexpr (lt_is_datetime<NonDecimalType>) {
                static_assert(std::is_same_v<NonDecimalCppType, TimestampValue>, "TimestampValue type is required");
                auto datum = (int64_t)(((TimestampValue&)data[i]).to_timestamp_literal());
                overflow = DecimalV3Cast::from_integer<int64_t, DecimalCppType, check_overflow>(datum, scale_factor,
                                                                                                &result_data[i]);
            } else if constexpr (lt_is_decimalv2<NonDecimalType>) {
                static_assert(std::is_same_v<NonDecimalCppType, DecimalV2Value>, "TimestampValue type is required");
                auto datum = (int128_t)(((DecimalV2Value&)data[i]).value());
                if (scale == DecimalV2Value::SCALE) {
                    overflow = DecimalV3Cast::to_decimal_trivial<int128_t, DecimalCppType, check_overflow>(
                            datum, &result_data[i]);
                } else if (scale < DecimalV2Value::SCALE) {
                    const auto scale_up_factor = get_scale_factor<int128_t>(DecimalV2Value::SCALE - scale);
                    overflow = DecimalV3Cast::to_decimal<int128_t, DecimalCppType, int128_t, false, check_overflow>(
                            datum, scale_up_factor, &result_data[i]);
                } else {
                    const auto scale_down_factor = get_scale_factor<DecimalCppType>(scale - DecimalV2Value::SCALE);
                    overflow =
                            DecimalV3Cast::to_decimal<int128_t, DecimalCppType, DecimalCppType, true, check_overflow>(
                                    datum, scale_down_factor, &result_data[i]);
                }
            } else {
                static_assert(lt_is_fixed_non_decimal<NonDecimalType>,
                              "Only number type is a legal argument type to template parameter");
            }

            if constexpr (check_overflow) {
                if (overflow) {
                    has_null = true;
                    nulls[i] = DATUM_NULL;
                }
            }
        }
        if constexpr (check_overflow) {
            ColumnBuilder<DecimalType> builder(result, null_column, has_null);
            return builder.build(column->is_constant());
        } else {
            return result;
        }
    }

    static inline ColumnPtr decimal_to(const ColumnPtr& column) {
        const auto num_rows = column->size();
        auto result = NonDecimalColumnType::create();
        result->resize(num_rows);
        const auto data_column = ColumnHelper::cast_to_raw<DecimalType>(column);
        int scale = data_column->scale();
        const auto data = &data_column->get_data().front();
        auto result_data = &ColumnHelper::cast_to_raw<NonDecimalType>(result)->get_data().front();

        NullColumnPtr null_column;
        NullColumn::ValueType* nulls = nullptr;
        bool has_null = false;
        if constexpr (check_overflow) {
            null_column = NullColumn::create();
            null_column->resize(num_rows);
            nulls = &null_column->get_data().front();
        }

        [[maybe_unused]] const auto scale_factor = get_scale_factor<DecimalCppType>(scale);
        for (int i = 0; i < num_rows; ++i) {
            bool overflow = false;
            if constexpr (lt_is_boolean<NonDecimalType>) {
                static constexpr auto zero = DecimalCppType(0);
                result_data[i] = (data[i] != zero);
            } else if constexpr (lt_is_integer<NonDecimalType>) {
                overflow = DecimalV3Cast::to_integer<DecimalCppType, NonDecimalCppType, check_overflow>(
                        data[i], scale_factor, &result_data[i]);
            } else if constexpr (lt_is_float<NonDecimalType>) {
                DecimalV3Cast::to_float<DecimalCppType, NonDecimalCppType>(data[i], scale_factor, &result_data[i]);
            } else if constexpr (lt_is_time<NonDecimalType>) {
                double datumf;
                DecimalV3Cast::to_float<DecimalCppType, NonDecimalCppType>(data[i], scale_factor, &datumf);
                uint64_t datum = datumf;
                uint64_t hour = datum / 10000;
                uint64_t min = (datum / 100) % 100;
                uint64_t sec = datum % 100;
                result_data[i] = (hour * 60 + min) * 60 + sec;
                if constexpr (check_overflow) {
                    overflow = overflow || (min > 59 || sec > 59);
                }
            } else if constexpr (lt_is_date<NonDecimalType>) {
                static_assert(std::is_same_v<NonDecimalCppType, DateValue>, "DateValue type is required");
                int64_t datum;
                overflow = DecimalV3Cast::to_integer<DecimalCppType, int64_t, check_overflow>(data[i], scale_factor,
                                                                                              &datum);
                auto& date_value = (DateValue&)result_data[i];
                if constexpr (check_overflow) {
                    overflow = overflow || !date_value.from_date_literal_with_check(datum);
                } else {
                    date_value.from_date_literal(datum);
                }
            } else if constexpr (lt_is_datetime<NonDecimalType>) {
                static_assert(std::is_same_v<NonDecimalCppType, TimestampValue>, "TimestampValue is required");
                int64_t datum;
                overflow = DecimalV3Cast::to_integer<DecimalCppType, int64_t, check_overflow>(data[i], scale_factor,
                                                                                              &datum);
                auto& timestamp_value = (TimestampValue&)result_data[i];
                if constexpr (check_overflow) {
                    overflow = overflow || !timestamp_value.from_timestamp_literal_with_check((uint64_t)datum);
                } else {
                    timestamp_value.from_timestamp_literal((uint64_t)datum);
                }
            } else if constexpr (lt_is_decimalv2<NonDecimalType>) {
                static_assert(std::is_same_v<NonDecimalCppType, DecimalV2Value>, "TimestampValue type is required");
                int128_t datum;
                if (scale == DecimalV2Value::SCALE) {
                    overflow = DecimalV3Cast::to_decimal_trivial<DecimalCppType, int128_t, check_overflow>(data[i],
                                                                                                           &datum);
                } else if (scale < DecimalV2Value::SCALE) {
                    const auto scale_up_factor = get_scale_factor<int128_t>(DecimalV2Value::SCALE - scale);
                    overflow = DecimalV3Cast::to_decimal<DecimalCppType, int128_t, int128_t, true, check_overflow>(
                            data[i], scale_up_factor, &datum);
                } else {
                    const auto scale_down_factor = get_scale_factor<DecimalCppType>(scale - DecimalV2Value::SCALE);
                    overflow =
                            DecimalV3Cast::to_decimal<DecimalCppType, int128_t, DecimalCppType, false, check_overflow>(
                                    data[i], scale_down_factor, &datum);
                }
                auto& decimalV2_value = (DecimalV2Value&)result_data[i];
                decimalV2_value.set_value(datum);
            } else {
                static_assert(lt_is_number<NonDecimalType>,
                              "Only number type is a legal argument type to template parameter");
            }

            if (check_overflow) {
                if (overflow) {
                    nulls[i] = DATUM_NULL;
                    has_null = true;
                }
            }
        }
        if constexpr (check_overflow) {
            ColumnBuilder<NonDecimalType> builder(result, null_column, has_null);
            return builder.build(false);
        } else {
            return result;
        }
    }
};

// cast: char/varchar <-> decimal
template <bool check_overflow, LogicalType DecimalType, LogicalType StringType>
struct DecimalNonDecimalCast<check_overflow, DecimalType, StringType, DecimalLTGuard<DecimalType>,
                             StringLTGuard<StringType>> {
    using DecimalCppType = RunTimeCppType<DecimalType>;
    using DecimalColumnType = RunTimeColumnType<DecimalType>;
    using StringCppType = RunTimeCppType<StringType>;
    using StringColumnType = RunTimeColumnType<StringType>;

    static inline ColumnPtr decimal_from(const ColumnPtr& column, int precision, int scale) {
        const auto num_rows = column->size();
        auto result = DecimalColumnType::create(precision, scale, num_rows);
        auto result_data = &ColumnHelper::cast_to_raw<DecimalType>(result)->get_data().front();
        NullColumnPtr null_column;
        NullColumn::ValueType* nulls = nullptr;
        auto has_null = false;
        if constexpr (check_overflow) {
            null_column = NullColumn::create();
            null_column->resize(num_rows);
            nulls = &null_column->get_data().front();
        }
        const auto binary_data = ColumnHelper::cast_to_raw<StringType>(column);
        for (auto i = 0; i < num_rows; ++i) {
            auto slice = binary_data->get_slice(i);
            auto overflow = DecimalV3Cast::from_string<DecimalCppType>(
                    &result_data[i], decimal_precision_limit<DecimalCppType>, scale, slice.data, slice.size);
            if constexpr (check_overflow) {
                if (overflow) {
                    has_null = true;
                    nulls[i] = DATUM_NULL;
                }
            }
        }
        if constexpr (check_overflow) {
            ColumnBuilder<DecimalType> builder(result, null_column, has_null);
            return builder.build(column->is_constant());
        } else {
            return result;
        }
    }

    static inline ColumnPtr decimal_to(const ColumnPtr& column) {
        const auto num_rows = column->size();
        auto result = BinaryColumn::create();
        auto& bytes = result->get_bytes();
        auto& offsets = result->get_offset();
        raw::make_room(&offsets, num_rows + 1);
        offsets[0] = 0;
        size_t max_length = decimal_precision_limit<DecimalCppType> + 4;
        bytes.resize(num_rows * max_length);
        auto bytes_data = &bytes.front();
        auto data_column = ColumnHelper::cast_to_raw<DecimalType>(column);
        auto data = &data_column->get_data().front();
        int precision = data_column->precision();
        int scale = data_column->scale();
        size_t bytes_off = 0;
        for (auto i = 0; i < num_rows; ++i) {
            auto s = DecimalV3Cast::to_string<DecimalCppType>(data[i], precision, scale);
            strings::memcpy_inlined(bytes_data + bytes_off, s.data(), s.size());
            bytes_off += s.size();
            offsets[i + 1] = bytes_off;
        }
        bytes.resize(bytes_off);
        return result;
    }
};

template <bool check_overflow>
struct DecimalToDecimal {
    template <LogicalType Type, LogicalType ResultType, typename... Args>
    static inline ColumnPtr evaluate(const ColumnPtr& column, Args&&... args) {
        return DecimalDecimalCast<check_overflow, Type, ResultType>::evaluate(column, std::forward<Args>(args)...);
    }
};

// cast: convert to decimal type from other types
template <bool check_overflow>
struct DecimalFrom {
    template <LogicalType Type, LogicalType ResultType, typename... Args>
    static inline ColumnPtr evaluate(const ColumnPtr& column, Args&&... args) {
        return DecimalNonDecimalCast<check_overflow, ResultType, Type>::decimal_from(column,
                                                                                     std::forward<Args>(args)...);
    }
};

// cast: convert to other types from decimal type
template <bool check_overflow>
struct DecimalTo {
    template <LogicalType Type, LogicalType ResultType>
    static inline ColumnPtr evaluate(const ColumnPtr& column) {
        return DecimalNonDecimalCast<check_overflow, Type, ResultType>::decimal_to(column);
    }
};

} // namespace starrocks
