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

#include <fmt/format.h>
#include <iomanip>
#include <sstream>
#include <string>
#include <type_traits>

#include <cmath>
#include <cstddef>
#include <cstdint>

#include "runtime/integer_overflow_arithmetics.h"
#include "util/decimal_types.h"
#include "util/raw_container.h"
#include "util/string_parser.hpp"

typedef unsigned __int128 uint128_t;

namespace starrocks {
TYPE_GUARD(Decimal32Guard, is_decimal32, int32_t)
TYPE_GUARD(Decimal64Guard, is_decimal64, int64_t)
TYPE_GUARD(Decimal128Guard, is_decimal128, int128_t)
TYPE_GUARD(DecimalGuard, is_decimal, int32_t, int64_t, int128_t)

template <typename ST>
struct unsigned_type {
    using type = typename std::make_unsigned<ST>::type;
};
template <>
struct unsigned_type<int128_t> {
    using type = uint128_t;
};

template <typename T, bool check_overflow>
class DecimalV3Arithmetics {
public:
    using Type = std::enable_if_t<starrocks::is_underlying_type_of_decimal<T>, T>;
    static inline bool add(Type const& a, Type const& b, Type* c) {
        if (check_overflow) {
            return add_overflow(a, b, c);
        } else {
            *c = a + b;
            return false;
        }
    }

    static inline bool sub(Type const& a, Type const& b, Type* c) {
        if constexpr (check_overflow) {
            return sub_overflow(a, b, c);
        } else {
            *c = a - b;
            return false;
        }
    }

    static inline bool mul(Type const& a, Type const& b, Type* c) {
        if constexpr (check_overflow) {
            return mul_overflow(a, b, c);
        } else {
            *c = a * b;
            return false;
        }
    }

    // check divide-by-zero before calling div and mod
    static inline bool div(Type const& a, Type const& b, Type* c) {
        *c = a / b;
        return false;
    }

    // check divide-by-zero before calling div and mod
    static inline bool div_round(Type const& a, Type const& b, Type* c) {
        *c = a / b;
        Type r = a % b;
        // case 1: |b| is odd. if [|b|/2] < |r|, then add carry; otherwise add 0.
        // case 2: |b| is even. if [|b|/2] <= |r|, then add carry; otherwise add 0. here
        // [b/2] == r means round half to up.
        // carry depends on sign of a^b.
        Type carry = ((a ^ b) >> (sizeof(Type) * 8 - 1)) | 1;
        Type abs_b = std::abs(b);
        Type abs_r = std::abs(r);
        bool need_carry = ((abs_b >> 1) + (abs_b & 1)) <= abs_r;
        *c += carry & -Type(need_carry);
        return false;
    }

    static inline bool mod(Type const& a, Type const& b, Type* c) {
        *c = a % b;
        return false;
    }
};

enum DecimalRoundRule {
    ROUND_HALF_UP,
    ROUND_HALF_EVEN,
    ROUND_FLOOR,
    ROUND_CEILING,
    ROUND_TRUNCATE,
};

class DecimalV3Cast {
public:
    template <typename T>
    static inline bool from_string(DecimalType<T>* value, int precision, int scale, const char* s, size_t n) {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        *value = StringParser::string_to_decimal<T>(s, n, precision, scale, &result);
        return result == StringParser::PARSE_FAILURE || result == StringParser::PARSE_OVERFLOW;
    }

    // If a decimal string is too large so that it can not be represented in decimal, then try to convert it into
    // double value, and the double value shall be greater than the integer part of max decimal or less than the
    // integer part of min decimal, in such situations, (max decimal + 1) and (min decimal - 1) are final result
    // respectively. this function is used in `IN` predicates and for the purpose that convert sets of decimal
    // strings into valid decimal values, in these scenarios, that overflow values are handled as max + 1 or min - 1
    // values is accepted.
    template <typename T>
    static inline bool from_string_with_overflow_allowed(DecimalType<T>* value, int scale, const char* s, size_t n) {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        *value = StringParser::string_to_decimal<T>(s, n, decimal_precision_limit<T>, scale, &result);
        if (UNLIKELY(StringParser::PARSE_FAILURE == result)) {
            return true;
        }
        if (UNLIKELY(StringParser::PARSE_OVERFLOW == result)) {
            auto double_value = StringParser::string_to_float<double>(s, n, &result);
            if (result != StringParser::PARSE_SUCCESS) {
                return true;
            }
            const auto max_integer = get_scale_factor<T>(decimal_precision_limit<T> - scale);
            const auto min_integer = -max_integer;
            if (double_value >= max_integer) {
                *value = get_scale_factor<T>(decimal_precision_limit<T>);
            } else if (double_value <= min_integer) {
                *value = -get_scale_factor<T>(decimal_precision_limit<T>);
            } else {
                return true;
            }
        }
        return false;
    }

    template <typename T>
    static inline std::string to_string(const DecimalType<T>& value, int precision, int scale) {
        DecimalType<T> abs_value = value;
        bool is_negative = value < 0;
        if (is_negative) {
            abs_value = -value;
        }
        
        T integer_part = abs_value / DecimalType<T>(1);
        T fractional_part = abs_value % DecimalType<T>(1);
        
        T divisor = 1;
        for (int i = 0; i < scale; i++) {
            divisor *= 10;
        }
        fractional_part = (fractional_part * divisor) / DecimalType<T>(1);
        
        std::stringstream ss;
        if (is_negative) {
            ss << "-";
        }
        
        if constexpr (std::is_same_v<T, int256_t> || std::is_same_v<T, __int128>) {
            std::string int_str;
            if (integer_part == 0) {
                int_str = "0";
            } else {
                DecimalType<T> temp = integer_part;
                DecimalType<T> ten(10);
                while (temp != 0) {
                    int_str = std::to_string(static_cast<int64_t>(temp % ten)) + int_str;
                    temp /= ten;
                }
            }
            ss << int_str;
        } else {
            ss << integer_part;
        }
        
        ss << ".";
        
        if constexpr (std::is_same_v<T, int256_t> || std::is_same_v<T, __int128>) {
            std::string frac_str;
            if (fractional_part == 0) {
                frac_str = std::string(scale, '0');
            } else {
                DecimalType<T> temp = fractional_part;
                DecimalType<T> ten(10);
                while (temp != 0) {
                    frac_str = std::to_string(static_cast<int64_t>(temp % ten)) + frac_str;
                    temp /= ten;
                }
                while (frac_str.length() < scale) {
                    frac_str = "0" + frac_str;
                }
            }
            ss << frac_str;
        } else {
            ss << std::setw(scale) << std::setfill('0') << fractional_part;
        }
        
        return ss.str();
    }

    template <typename T>
    static constexpr T float_lower_overflow_indicator = std::numeric_limits<T>::max();
    template <typename T>
    static constexpr T float_upper_overflow_indicator = std::numeric_limits<T>::min();

    template <typename From, typename To>
    static inline bool from_float(FloatType<From> value, DecimalType<To> const& scale_factor,
                                  DecimalType<To>* dec_value) {
        double delta = value >= 0 ? 0.5 : -0.5; // go to the nearest integer
        *dec_value = static_cast<To>(scale_factor * static_cast<double>(value) + delta);
        if constexpr (is_decimal32<To> || is_decimal64<To>) {
            // Depending on the compiler implement, std::numeric_limits<T>::max() or std::numeric_limits<T>::max() both could be returned,
            // when overflow is happenning in casting.

            // With GCC-10.3.0, the cast on aarch64 uses fcvtzs instruction, behaving as "carries all overflows to the output precision's largest finite number with the sign of the result before rounding".
            // (https://developer.arm.com/documentation/ddi0487/latest)

            // Meanwhile, the cast on x86_64 uses cvttsd2siq instruction, behaving as "the indefinite integer value (80000000H) is returned".
            // (https://www.felixcloutier.com/x86/cvttsd2si)
            return (*dec_value == float_lower_overflow_indicator<To>) ||
                   (*dec_value == float_upper_overflow_indicator<To>);
        } else if constexpr (is_decimal128<To>) {
            // std::abs(value)<1.0 -> 0: Acceptable
            // std::abs(value)>=1.0 -> 0 or different sign: Overflow!!
            return std::abs(value) >= From(1) && (*dec_value == To(0) || ((value < From(0)) ^ (*dec_value < To(0))));
        } else {
            static_assert(is_decimal<To>, "invalid decimal type");
        }
    }

    template <typename From, typename To, bool check_overflow>
    static inline bool scale_up(IntegerType<From> from_value, DecimalType<To> const& scale_factor,
                                DecimalType<To>* dec_value) {
        if constexpr (check_overflow) {
            auto overflow =
                    DecimalV3Arithmetics<To, check_overflow>::mul(static_cast<To>(from_value), scale_factor, dec_value);
            if constexpr (sizeof(From) <= sizeof(To)) {
                return overflow;
            } else {
                static constexpr auto max_value = static_cast<From>(get_max<To>());
                static constexpr auto min_value = static_cast<From>(get_min<To>());
                return overflow || from_value > max_value || from_value < min_value;
            }
        } else {
            *dec_value = static_cast<To>(from_value * scale_factor);
            return false;
        }
    }

    template <typename From, typename To>
    static inline bool to_float(DecimalType<From> const& value, DecimalType<From> const& scale_factor,
                                FloatType<To>* to_value) {
        *to_value = static_cast<To>(static_cast<double>(value) / static_cast<double>(scale_factor));
        return false;
    }

    template <typename From, typename To, bool check_overflow>
    static inline bool scale_down(DecimalType<From> const& value, DecimalType<From> const& scale_factor,
                                  IntegerType<To>* to_value) {
        if constexpr (check_overflow) {
            From quotient;
            DecimalV3Arithmetics<From, check_overflow>::div(value, scale_factor, &quotient);
            *to_value = static_cast<To>(quotient);
            if constexpr (sizeof(From) <= sizeof(To)) {
                return false;
            } else {
                static constexpr auto max_value = static_cast<From>(get_max<To>());
                static constexpr auto min_value = static_cast<From>(get_min<To>());
                return quotient > max_value || quotient < min_value;
            }
        } else {
            *to_value = static_cast<To>(value / scale_factor);
            return false;
        }
    }

    template <typename From, DecimalRoundRule rule, bool check_overflow>
    static inline bool round_quotient(const DecimalType<From>& carry, DecimalType<From>& quotient,
                                      DecimalType<From> const& divisor, DecimalType<From> const& remainder) {
        bool need_round = false;

        if constexpr (rule == ROUND_HALF_UP || rule == ROUND_HALF_EVEN) {
            //TODO(by satanson): ROUND_HALF_UP is different from ROUND_HALF_EVEN
            need_round = std::abs(remainder) >= (divisor >> 1);
        } else if constexpr (rule == ROUND_FLOOR) {
            need_round = remainder > 0 && quotient > 0;
        } else if constexpr (rule == ROUND_CEILING) {
            need_round = remainder < 0 && quotient < 0;
        } else {
            // do nothing here
        }
        if (need_round) {
            return DecimalV3Arithmetics<From, check_overflow>::add(quotient, carry, &quotient);
        } else {
            return false;
        }
    }

    template <typename From, typename To, DecimalRoundRule rule, bool check_overflow>
    static inline bool scale_down_round(DecimalType<From> const& value, DecimalType<From> const& scale_factor,
                                        IntegerType<To>* to_value) {
        if (UNLIKELY(scale_factor == 1)) {
            return to_decimal_trivial<From, To, check_overflow>(value, to_value);
        }

        From quotient, remainder;
        bool overflow;

        // if value < 0, then carry = -1; otherwise, carry = 1.
        From carry = (value >> (sizeof(From) * 8 - 1)) | 1;

        if constexpr (check_overflow) {
            DecimalV3Arithmetics<From, check_overflow>::div(value, scale_factor, &quotient);
            DecimalV3Arithmetics<From, check_overflow>::mod(value, scale_factor, &remainder);
            overflow = round_quotient<From, rule, true>(carry, quotient, scale_factor, remainder);
            *to_value = static_cast<To>(quotient);
            if constexpr (sizeof(From) > sizeof(To)) {
                static constexpr auto max_value = static_cast<From>(get_max<To>());
                static constexpr auto min_value = static_cast<From>(get_min<To>());
                return overflow || quotient > max_value || quotient < min_value;
            } else {
                return overflow;
            }
        } else {
            quotient = value / scale_factor;
            remainder = value % scale_factor;
            round_quotient<From, rule, false>(carry, quotient, scale_factor, remainder);
            *to_value = static_cast<To>(quotient);
            return false;
        }
    }

    template <typename From, typename To, bool check_overflow>
    static inline bool from_integer(From from_value, To const& scale_factor, To* dec_value) {
        return scale_up<From, To, check_overflow>(from_value, scale_factor, dec_value);
    }

    template <typename From, typename To, bool check_overflow>
    static inline bool to_integer(From const& dec_value, From const& scale_factor, To* to_value) {
        return scale_down<From, To, check_overflow>(dec_value, scale_factor, to_value);
    }

    template <typename From, typename To, typename S, bool is_scale_up, bool check_overflow>
    static inline bool to_decimal_truncate(From const& value, S const& scale_factor, To* to_value) {
        if constexpr (is_scale_up) {
            static_assert(std::is_same_v<S, To>, "S and U must be the same type");
            return scale_up<From, To, check_overflow>(value, scale_factor, to_value);
        } else {
            static_assert(std::is_same_v<From, S>, "T and S must be the same type");
            return scale_down<From, To, check_overflow>(value, scale_factor, to_value);
        }
    }

    template <typename From, typename To, typename S, bool is_scale_up, bool check_overflow>
    static inline bool to_decimal(From const& value, S const& scale_factor, To* to_value) {
        return round<From, To, S, ROUND_HALF_EVEN, is_scale_up, check_overflow>(value, scale_factor, to_value);
    }

    template <typename From, typename To, bool check_overflow>
    static inline bool to_decimal_trivial(From const& value, To* to_value) {
        *to_value = static_cast<To>(value);
        if constexpr (check_overflow) {
            if constexpr (sizeof(From) <= sizeof(To)) {
                return false;
            } else {
                static constexpr auto max_value = static_cast<From>(get_max<To>());
                static constexpr auto min_value = static_cast<From>(get_min<To>());
                return value > max_value || value < min_value;
            }
        } else {
            return false;
        }
    }

    template <typename From, typename To, typename S, DecimalRoundRule rule, bool is_scale_up, bool check_overflow>
    static inline bool round(From const& value, S const& scale_factor, To* to_value) {
        if constexpr (is_scale_up) {
            static_assert(std::is_same_v<S, To>, "S and U must be the same type");
            return scale_up<From, To, check_overflow>(value, scale_factor, to_value);
        } else {
            static_assert(std::is_same_v<From, S>, "T and S must be the same type");
            return scale_down_round<From, To, rule, check_overflow>(value, scale_factor, to_value);
        }
    }

    template <typename T, DecimalRoundRule rule, bool is_scale_up, bool check_overflow>
    static inline bool round(T const& value, T const& scale_factor, T* to_value) {
        return round<T, T, T, rule, is_scale_up, check_overflow>(value, scale_factor, to_value);
    }
};

} // namespace starrocks
