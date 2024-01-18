// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <gutil/strings/fastmem.h>

#include "column/column_builder.h"
#include "exprs/overflow.h"
#include "runtime/decimalv3.h"

namespace starrocks::vectorized {

UNION_VALUE_GUARD(PrimitiveType, FixedNonDecimalTypeGuard, pt_is_fixed_non_decimal, pt_is_boolean_struct,
                  pt_is_integer_struct, pt_is_float_struct, pt_is_date_struct, pt_is_datetime_struct,
                  pt_is_decimalv2_struct, pt_is_time_struct)

template <OverflowMode overflow_mode, PrimitiveType Type, PrimitiveType ResultType>
struct DecimalDecimalCast {
    static inline ColumnPtr evaluate(const ColumnPtr& column, int to_precision, int to_scale) {
        using FromCppType = RunTimeCppType<Type>;
        using FromColumnType = RunTimeColumnType<Type>;
        using ToCppType = RunTimeCppType<ResultType>;
        using ToColumnType = RunTimeColumnType<ResultType>;
        auto num_rows = column->size();
        auto data_column = ColumnHelper::cast_to_raw<Type>(column);
        int from_scale = data_column->scale();

        // source type and target type has the same primitive type and scale
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

        if constexpr (check_overflow<overflow_mode>) {
            null_column = NullColumn::create();
            null_column->resize(num_rows);
            nulls = &null_column->get_data().front();
        }

        if (to_scale == from_scale) {
            if constexpr (sizeof(FromCppType) <= sizeof(ToCppType)) {
                for (auto i = 0; i < num_rows; ++i) {
                    (void)DecimalV3Cast::to_decimal_trivial<FromCppType, ToCppType, check_overflow<overflow_mode>>(
                            data[i], &result_data[i]);
                }
            } else {
                for (auto i = 0; i < num_rows; ++i) {
                    auto overflow =
                            DecimalV3Cast::to_decimal_trivial<FromCppType, ToCppType, check_overflow<overflow_mode>>(
                                    data[i], &result_data[i]);
                    if constexpr (check_overflow<overflow_mode>) {
                        if (overflow) {
                            if constexpr (error_if_overflow<overflow_mode>) {
                                throw std::overflow_error("The type cast from decimal to decimal overflows");
                            } else {
                                static_assert(null_if_overflow<overflow_mode>);
                                has_null = true;
                                nulls[i] = DATUM_NULL;
                            }
                        }
                    }
                }
            }
        } else if (to_scale > from_scale) {
            const auto scale_factor = get_scale_factor<ToCppType>(to_scale - from_scale);
            for (auto i = 0; i < num_rows; ++i) {
                auto overflow = DecimalV3Cast::to_decimal<FromCppType, ToCppType, ToCppType, true,
                                                          check_overflow<overflow_mode>>(data[i], scale_factor,
                                                                                         &result_data[i]);
                if constexpr (check_overflow<overflow_mode>) {
                    if (overflow) {
                        if constexpr (error_if_overflow<overflow_mode>) {
                            throw std::overflow_error("The type cast from decimal to decimal overflows");
                        } else {
                            static_assert(null_if_overflow<overflow_mode>);
                            has_null = true;
                            nulls[i] = DATUM_NULL;
                        }
                    }
                }
            }
        } else {
            const auto scale_factor = get_scale_factor<FromCppType>(from_scale - to_scale);
            for (auto i = 0; i < num_rows; ++i) {
                auto overflow = DecimalV3Cast::to_decimal<FromCppType, ToCppType, FromCppType, false,
                                                          check_overflow<overflow_mode>>(data[i], scale_factor,
                                                                                         &result_data[i]);
                if constexpr (check_overflow<overflow_mode>) {
                    if (overflow) {
                        if constexpr (error_if_overflow<overflow_mode>) {
                            throw std::overflow_error("The type cast from decimal to decimal overflows");
                        } else {
                            static_assert(null_if_overflow<overflow_mode>);
                            has_null = true;
                            nulls[i] = DATUM_NULL;
                        }
                    }
                }
            }
        }
        if constexpr (check_overflow<overflow_mode>) {
            ColumnBuilder<ResultType> builder(result, null_column, has_null);
            return builder.build(column->is_constant());
        } else {
            return result;
        }
    }
};
template <OverflowMode overflow_mode, PrimitiveType, PrimitiveType, typename = guard::Guard, typename = guard::Guard>
struct DecimalNonDecimalCast {};

// cast: bool,integer,float,datetime,date, time <-> decimal
template <OverflowMode overflow_mode, PrimitiveType DecimalType, PrimitiveType NonDecimalType>
struct DecimalNonDecimalCast<overflow_mode, DecimalType, NonDecimalType, DecimalPTGuard<DecimalType>,
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
        if constexpr (check_overflow<overflow_mode>) {
            null_column = NullColumn::create();
            null_column->resize(num_rows);
            nulls = &null_column->get_data().front();
        }

        [[maybe_unused]] const auto scale_factor = get_scale_factor<DecimalCppType>(scale);

        for (auto i = 0; i < num_rows; ++i) {
            bool overflow = false;
            if constexpr (pt_is_boolean<NonDecimalType>) {
                using SignedBooleanType = std::make_signed_t<NonDecimalCppType>;
                overflow =
                        DecimalV3Cast::from_integer<SignedBooleanType, DecimalCppType, check_overflow<overflow_mode>>(
                                (SignedBooleanType)(data[i]), scale_factor, &result_data[i]);
            } else if constexpr (pt_is_integer<NonDecimalType>) {
                overflow =
                        DecimalV3Cast::from_integer<NonDecimalCppType, DecimalCppType, check_overflow<overflow_mode>>(
                                data[i], scale_factor, &result_data[i]);
            } else if constexpr (pt_is_float<NonDecimalType>) {
                overflow = DecimalV3Cast::from_float<NonDecimalCppType, DecimalCppType>(data[i], scale_factor,
                                                                                        &result_data[i]);
            } else if constexpr (pt_is_time<NonDecimalType>) {
                double datum = timestamp::time_to_literal(data[i]);
                overflow = DecimalV3Cast::from_float<NonDecimalCppType, DecimalCppType>(datum, scale_factor,
                                                                                        &result_data[i]);
            } else if constexpr (pt_is_date<NonDecimalType>) {
                static_assert(std::is_same_v<NonDecimalCppType, DateValue>, "DateValue type is required");
                int32_t datum = ((DateValue&)data[i]).to_date_literal();
                overflow = DecimalV3Cast::from_integer<int32_t, DecimalCppType, check_overflow<overflow_mode>>(
                        datum, scale_factor, &result_data[i]);
            } else if constexpr (pt_is_datetime<NonDecimalType>) {
                static_assert(std::is_same_v<NonDecimalCppType, TimestampValue>, "TimestampValue type is required");
                int64_t datum = (int64_t)(((TimestampValue&)data[i]).to_timestamp_literal());
                overflow = DecimalV3Cast::from_integer<int64_t, DecimalCppType, check_overflow<overflow_mode>>(
                        datum, scale_factor, &result_data[i]);
            } else if constexpr (pt_is_decimalv2<NonDecimalType>) {
                static_assert(std::is_same_v<NonDecimalCppType, DecimalV2Value>, "TimestampValue type is required");
                int128_t datum = (int128_t)(((DecimalV2Value&)data[i]).value());
                if (scale == DecimalV2Value::SCALE) {
                    overflow =
                            DecimalV3Cast::to_decimal_trivial<int128_t, DecimalCppType, check_overflow<overflow_mode>>(
                                    datum, &result_data[i]);
                } else if (scale < DecimalV2Value::SCALE) {
                    const auto scale_up_factor = get_scale_factor<int128_t>(DecimalV2Value::SCALE - scale);
                    overflow = DecimalV3Cast::to_decimal<int128_t, DecimalCppType, int128_t, false,
                                                         check_overflow<overflow_mode>>(datum, scale_up_factor,
                                                                                        &result_data[i]);
                } else {
                    const auto scale_down_factor = get_scale_factor<DecimalCppType>(scale - DecimalV2Value::SCALE);
                    overflow = DecimalV3Cast::to_decimal<int128_t, DecimalCppType, DecimalCppType, true,
                                                         check_overflow<overflow_mode>>(datum, scale_down_factor,
                                                                                        &result_data[i]);
                }
            } else {
                static_assert(pt_is_fixed_non_decimal<NonDecimalType>,
                              "Only number type is a legal argument type to template parameter");
            }

            if constexpr (check_overflow<overflow_mode>) {
                if (overflow) {
                    if constexpr (error_if_overflow<overflow_mode>) {
                        throw std::overflow_error("The type cast from other types to decimal overflows");
                    } else {
                        static_assert(null_if_overflow<overflow_mode>);
                        has_null = true;
                        nulls[i] = DATUM_NULL;
                    }
                }
            }
        }
        if constexpr (check_overflow<overflow_mode>) {
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
        if constexpr (check_overflow<overflow_mode>) {
            null_column = NullColumn::create();
            null_column->resize(num_rows);
            nulls = &null_column->get_data().front();
        }

        [[maybe_unused]] const auto scale_factor = get_scale_factor<DecimalCppType>(scale);
        for (int i = 0; i < num_rows; ++i) {
            bool overflow = false;
            if constexpr (pt_is_boolean<NonDecimalType>) {
                static constexpr DecimalCppType zero = DecimalCppType(0);
                result_data[i] = (data[i] != zero);
            } else if constexpr (pt_is_integer<NonDecimalType>) {
                overflow = DecimalV3Cast::to_integer<DecimalCppType, NonDecimalCppType, check_overflow<overflow_mode>>(
                        data[i], scale_factor, &result_data[i]);
            } else if constexpr (pt_is_float<NonDecimalType>) {
                DecimalV3Cast::to_float<DecimalCppType, NonDecimalCppType>(data[i], scale_factor, &result_data[i]);
            } else if constexpr (pt_is_time<NonDecimalType>) {
                double datumf;
                DecimalV3Cast::to_float<DecimalCppType, NonDecimalCppType>(data[i], scale_factor, &datumf);
                uint64_t datum = datumf;
                uint64_t hour = datum / 10000;
                uint64_t min = (datum / 100) % 100;
                uint64_t sec = datum % 100;
                result_data[i] = (hour * 60 + min) * 60 + sec;
                if constexpr (check_overflow<overflow_mode>) {
                    overflow = overflow || (min > 59 || sec > 59);
                }
            } else if constexpr (pt_is_date<NonDecimalType>) {
                static_assert(std::is_same_v<NonDecimalCppType, DateValue>, "DateValue type is required");
                int64_t datum;
                overflow = DecimalV3Cast::to_integer<DecimalCppType, int64_t, check_overflow<overflow_mode>>(
                        data[i], scale_factor, &datum);
                DateValue& date_value = (DateValue&)result_data[i];
                if constexpr (check_overflow<overflow_mode>) {
                    overflow = overflow || !date_value.from_date_literal_with_check(datum);
                } else {
                    date_value.from_date_literal(datum);
                }
            } else if constexpr (pt_is_datetime<NonDecimalType>) {
                static_assert(std::is_same_v<NonDecimalCppType, TimestampValue>, "TimestampValue is required");
                int64_t datum;
                overflow = DecimalV3Cast::to_integer<DecimalCppType, int64_t, check_overflow<overflow_mode>>(
                        data[i], scale_factor, &datum);
                TimestampValue& timestamp_value = (TimestampValue&)result_data[i];
                if constexpr (check_overflow<overflow_mode>) {
                    overflow = overflow || !timestamp_value.from_timestamp_literal_with_check((uint64_t)datum);
                } else {
                    timestamp_value.from_timestamp_literal((uint64_t)datum);
                }
            } else if constexpr (pt_is_decimalv2<NonDecimalType>) {
                static_assert(std::is_same_v<NonDecimalCppType, DecimalV2Value>, "TimestampValue type is required");
                int128_t datum;
                if (scale == DecimalV2Value::SCALE) {
                    overflow =
                            DecimalV3Cast::to_decimal_trivial<DecimalCppType, int128_t, check_overflow<overflow_mode>>(
                                    data[i], &datum);
                } else if (scale < DecimalV2Value::SCALE) {
                    const auto scale_up_factor = get_scale_factor<int128_t>(DecimalV2Value::SCALE - scale);
                    overflow =
                            DecimalV3Cast::to_decimal<DecimalCppType, int128_t, int128_t, true,
                                                      check_overflow<overflow_mode>>(data[i], scale_up_factor, &datum);
                } else {
                    const auto scale_down_factor = get_scale_factor<DecimalCppType>(scale - DecimalV2Value::SCALE);
                    overflow = DecimalV3Cast::to_decimal<DecimalCppType, int128_t, DecimalCppType, false,
                                                         check_overflow<overflow_mode>>(data[i], scale_down_factor,
                                                                                        &datum);
                }
                auto& decimalV2_value = (DecimalV2Value&)result_data[i];
                decimalV2_value.set_value(datum);
            } else {
                static_assert(pt_is_number<NonDecimalType>,
                              "Only number type is a legal argument type to template parameter");
            }

            if (check_overflow<overflow_mode>) {
                if (overflow) {
                    if constexpr (error_if_overflow<overflow_mode>) {
                        throw std::overflow_error("The type cast from decimal to other types overflows");
                    } else {
                        static_assert(null_if_overflow<overflow_mode>);
                        nulls[i] = DATUM_NULL;
                        has_null = true;
                    }
                }
            }
        }
        if constexpr (check_overflow<overflow_mode>) {
            ColumnBuilder<NonDecimalType> builder(result, null_column, has_null);
            return builder.build(false);
        } else {
            return result;
        }
    }
};

// cast: char/varchar <-> decimal
template <OverflowMode overflow_mode, PrimitiveType DecimalType, PrimitiveType StringType>
struct DecimalNonDecimalCast<overflow_mode, DecimalType, StringType, DecimalPTGuard<DecimalType>,
                             StringPTGuard<StringType>> {
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
        if constexpr (check_overflow<overflow_mode>) {
            null_column = NullColumn::create();
            null_column->resize(num_rows);
            nulls = &null_column->get_data().front();
        }
        const auto binary_data = ColumnHelper::cast_to_raw<StringType>(column);
        for (auto i = 0; i < num_rows; ++i) {
            auto slice = binary_data->get_slice(i);
            auto overflow = DecimalV3Cast::from_string<DecimalCppType>(
                    &result_data[i], decimal_precision_limit<DecimalCppType>, scale, slice.data, slice.size);
            if constexpr (check_overflow<overflow_mode>) {
                if (overflow) {
                    if constexpr (error_if_overflow<overflow_mode>) {
                        throw std::overflow_error("The type cast from string to decimal overflows");
                    } else {
                        static_assert(null_if_overflow<overflow_mode>);
                        has_null = true;
                        nulls[i] = DATUM_NULL;
                    }
                }
            }
        }
        if constexpr (check_overflow<overflow_mode>) {
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

template <OverflowMode overflow_mode>
struct DecimalToDecimal {
    template <PrimitiveType Type, PrimitiveType ResultType, typename... Args>
    static inline ColumnPtr evaluate(const ColumnPtr& column, Args&&... args) {
        return DecimalDecimalCast<overflow_mode, Type, ResultType>::evaluate(column, std::forward<Args>(args)...);
    }
};

// cast: convert to decimal type from other types
template <OverflowMode overflow_mode>
struct DecimalFrom {
    template <PrimitiveType Type, PrimitiveType ResultType, typename... Args>
    static inline ColumnPtr evaluate(const ColumnPtr& column, Args&&... args) {
        return DecimalNonDecimalCast<overflow_mode, ResultType, Type>::decimal_from(column,
                                                                                    std::forward<Args>(args)...);
    }
};

// cast: convert to other types from decimal type
template <OverflowMode overflow_mode>
struct DecimalTo {
    template <PrimitiveType Type, PrimitiveType ResultType>
    static inline ColumnPtr evaluate(const ColumnPtr& column) {
        return DecimalNonDecimalCast<overflow_mode, Type, ResultType>::decimal_to(column);
    }
};

} // namespace starrocks::vectorized
