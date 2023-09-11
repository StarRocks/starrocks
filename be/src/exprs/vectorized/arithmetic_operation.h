// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/type_traits.h"
#include "runtime/decimalv3.h"
#include "runtime/primitive_type.h"
#include "util/guard.h"

namespace starrocks::vectorized {
struct AddOp {};
struct SubOp {};
struct ReverseSubOp {};
struct ReverseModOp {};
struct MulOp {};
struct DivOp {};
struct IntDivOp {};
struct ModOp {};
struct BitAndOp {};
struct BitOrOp {};
struct BitXorOp {};
struct BitNotOp {};
struct BitShiftLeftOp {};
struct BitShiftRightOp {};
struct BitShiftRightLogicalOp {};

struct MulOp64x64_128 {};
struct MulOp32x64_128 {};
struct MulOp32x32_64 {};
struct MulOp32x32_128 {};

TYPE_GUARD(AddOpGuard, is_add_op, AddOp)
TYPE_GUARD(SubOpGuard, is_sub_op, SubOp)
TYPE_GUARD(ReverseSubOpGuard, is_reverse_sub_op, ReverseSubOp)
TYPE_GUARD(ReverseModOpGuard, is_reverse_mod_op, ReverseModOp)
TYPE_GUARD(MulOpGuard, is_mul_op, MulOp)
TYPE_GUARD(DivOpGuard, is_div_op, DivOp)
TYPE_GUARD(IntDivOpGuard, is_intdiv_op, IntDivOp)
TYPE_GUARD(ModOpGuard, is_mod_op, ModOp)
TYPE_GUARD(BitAndOpGuard, is_bitand_op, BitAndOp)
TYPE_GUARD(BitOrOpGuard, is_bitor_op, BitOrOp)
TYPE_GUARD(BitXorOpGuard, is_bitxor_op, BitXorOp)
TYPE_GUARD(BitNotOpGuard, is_bitnot_op, BitNotOp)
TYPE_GUARD(BitShiftLeftOpGuard, is_bit_shift_left_op, BitShiftLeftOp)
TYPE_GUARD(BitShiftRightOpGuard, is_bit_shift_right_op, BitShiftRightOp)
TYPE_GUARD(BitShiftRightLogicalOpGuard, is_bit_shift_right_logical_op, BitShiftRightLogicalOp)
TYPE_GUARD(BinaryOpGuard, is_binary_op, AddOp, SubOp, MulOp, DivOp, ModOp, BitAndOp, BitOrOp, BitXorOp, BitShiftLeftOp,
           BitShiftRightOp, BitShiftRightLogicalOp)
TYPE_GUARD(UnaryOpGuard, is_unary_op, BitNotOp)

template <PrimitiveType Type, typename ResultType>
using ReturnType = std::enable_if_t<std::is_same_v<RunTimeCppType<Type>, ResultType>, ResultType>;

TYPE_GUARD(MayCauseFPEGuard, may_cause_fpe, int32_t, int64_t)
template <typename T, typename = MayCauseFPEGuard<T>>
constexpr T signed_minimum = T(1) << (sizeof(T) * 8 - 1);

template <typename LType, typename RType>
bool check_fpe_of_min_div_by_minus_one(LType lhs, RType rhs) {
    static constexpr auto minus_one = RType(-1);
    if constexpr (may_cause_fpe<LType> && may_cause_fpe<RType>) {
        return lhs == signed_minimum<LType> && rhs == minus_one;
    } else {
        return false;
    }
}

template <typename Op>
std::string get_op_name() {
    if constexpr (is_add_op<Op>) {
        return "add";
    } else if constexpr (is_sub_op<Op>) {
        return "sub";
    } else if constexpr (is_reverse_sub_op<Op>) {
        return "reverse_sub";
    } else if constexpr (is_reverse_mod_op<Op>) {
        return "reverse_mod";
    } else if constexpr (is_mul_op<Op>) {
        return "mul";
    } else if constexpr (is_div_op<Op>) {
        return "div";
    } else if constexpr (is_intdiv_op<Op>) {
        return "intdiv";
    } else if constexpr (is_mod_op<Op>) {
        return "mod";
    } else if constexpr (is_bitand_op<Op>) {
        return "bitand";
    } else if constexpr (is_bitor_op<Op>) {
        return "bitor";
    } else if constexpr (is_bitxor_op<Op>) {
        return "bitxor";
    } else if constexpr (is_bitnot_op<Op>) {
        return "bitnot";
    } else if constexpr (is_bit_shift_left_op<Op>) {
        return "bit_shift_left";
    } else if constexpr (is_bit_shift_right_op<Op>) {
        return "bit_shift_right";
    } else if constexpr (is_bit_shift_right_logical_op<Op>) {
        return "bit_shift_right_logical";
    } else {
        return "unknown";
    }
}

template <typename Op, PrimitiveType Type, typename = guard::Guard, typename = guard::Guard>
struct ArithmeticBinaryOperator {
    template <typename LType, typename RType, typename ResultType>
    static inline ReturnType<Type, ResultType> apply(const LType& l, const RType& r) {
        if constexpr (is_add_op<Op>) {
            return l + r;
        } else if constexpr (is_sub_op<Op>) {
            return l - r;
        } else if constexpr (is_mul_op<Op>) {
            // avoid 0 mul a negative num, make result -0
            if constexpr (std::is_floating_point<LType>::value) {
                if (UNLIKELY(l == LType(0) || r == RType(0))) {
                    return LType(0);
                }
            }
            return l * r;
        } else if constexpr (is_div_op<Op>) {
            // avoid 0 div a negative num, make result -0
            if constexpr (std::is_floating_point<LType>::value) {
                if (UNLIKELY(l == 0)) {
                    return 0;
                }
            }
            if constexpr (may_cause_fpe<ResultType>) {
                if (UNLIKELY(check_fpe_of_min_div_by_minus_one(l, r))) {
                    return signed_minimum<ResultType>;
                } else {
                    return l / (r + (r == 0));
                }
            } else {
                return l / (r + (r == 0));
            }
        } else if constexpr (is_mod_op<Op>) {
            if constexpr (may_cause_fpe<ResultType>) {
                if (UNLIKELY(check_fpe_of_min_div_by_minus_one(l, r))) {
                    return 0;
                } else {
                    return l % (r + (r == 0));
                }
            } else {
                return l % (r + (r == 0));
            }
        } else if constexpr (is_bitand_op<Op>) {
            return l & r;
        } else if constexpr (is_bitor_op<Op>) {
            return l | r;
        } else if constexpr (is_bitxor_op<Op>) {
            return l ^ r;
        } else if constexpr (is_bit_shift_left_op<Op>) {
            return l << r;
        } else if constexpr (is_bit_shift_right_op<Op>) {
            return l >> r;
        } else if constexpr (is_bit_shift_right_logical_op<Op>) {
            if constexpr (std::is_same_v<LType, int8_t>) {
                return uint8_t(l) >> r;
            } else if constexpr (std::is_same_v<LType, int16_t>) {
                return uint16_t(l) >> r;
            } else if constexpr (std::is_same_v<LType, int32_t>) {
                return uint32_t(l) >> r;
            } else if constexpr (std::is_same_v<LType, int64_t>) {
                return uint64_t(l) >> r;
            } else if constexpr (std::is_same_v<LType, __int128_t>) {
                return uint128_t(l) >> r;
            }
        } else {
            static_assert(is_binary_op<Op>, "Invalid binary operators");
        }
    }
};

TYPE_GUARD(DivModOpGuard, is_divmod_op, DivOp, ModOp)

template <typename Op>
struct ArithmeticBinaryOperator<Op, TYPE_DECIMALV2, DivModOpGuard<Op>, guard::Guard> {
    template <typename LType, typename RType, typename ResultType>
    static inline ReturnType<TYPE_DECIMALV2, ResultType> apply(const LType& l, const RType& r) {
        if constexpr (is_div_op<Op>) {
            // avoid 0 div a negative num, make result -0
            if constexpr (std::is_floating_point<LType>::value) {
                if (UNLIKELY(l == DecimalV2Value::ZERO)) {
                    return DecimalV2Value::ZERO;
                }
            }
            return (r == DecimalV2Value::ZERO) ? l : (l / r);
        } else if constexpr (is_mod_op<Op>) {
            return (r == DecimalV2Value::ZERO) ? l : (l % r);
        } else {
            static_assert(is_divmod_op<Op>, "Invalid float operators");
        }
    }
};

template <PrimitiveType Type>
struct ArithmeticBinaryOperator<ModOp, Type, guard::Guard, FloatPTGuard<Type>> {
    template <typename LType, typename RType, typename ResultType>
    static inline ReturnType<Type, ResultType> apply(const LType& l, const RType& r) {
        auto result = fmod(l, (r + (r == 0)));
        if (UNLIKELY(result == -0)) {
            return 0;
        }
        return result;
    }
};

TYPE_GUARD(DecimalOpGuard, is_decimal_op, AddOp, SubOp, ReverseSubOp, MulOp, DivOp, ModOp, ReverseModOp)
TYPE_GUARD(DecimalFastMulOpGuard, is_decimal_fast_mul_op, MulOp32x32_64, MulOp32x32_128, MulOp32x64_128, MulOp64x64_128)

// compute precision, scale and adjust for result of binary operation
// scale(a add/sub b) = max(scale(a), scale(b))
// scale(a mul b) = scale(a) + scale(b)
// scale(a div b) = case scale(a) when 0..6 then scale(a)+6 when 7..12 then 12 when 12..38 then scale(a)
// scale(a mod b) = max(scale(a), scale(b))
template <typename T, typename Op>
static inline std::tuple<int, int, int> compute_decimal_result_type(int lhs_scale, int rhs_scale) {
    int precision = decimal_precision_limit<T>;
    int scale = 0;
    int adjust_scale = 0;
    if constexpr (is_add_op<Op> || is_sub_op<Op> || is_mod_op<Op>) {
        scale = std::max(lhs_scale, rhs_scale);
        adjust_scale = scale - std::min(lhs_scale, rhs_scale);
    } else if constexpr (is_mul_op<Op> || is_decimal_fast_mul_op<Op>) {
        int max_precision = decimal_precision_limit<T>;
        scale = lhs_scale + rhs_scale;
        adjust_scale = 0;
        DCHECK(scale <= max_precision);
    } else if constexpr (is_div_op<Op>) {
        precision = decimal_precision_limit<int128_t>;
        if (lhs_scale <= 6) {
            scale = lhs_scale + 6;
        } else if (lhs_scale <= 12) {
            scale = 12;
        } else {
            scale = lhs_scale;
        }
        adjust_scale = scale + rhs_scale - lhs_scale;
    } else {
        static_assert(is_decimal_op<Op>, "Invalid decimal Op");
    }
    return {precision, scale, adjust_scale};
}

template <typename T>
T decimal_div_integer(const T& dividend, const T& divisor, int dividend_scale) {
    // compute adjust_scale_factor
    auto [_1, _2, adjust_scale] = compute_decimal_result_type<T, DivOp>(dividend_scale, 0);
    T adjust_scale_factor = get_scale_factor<T>(adjust_scale);
    // scale dividend up by adjust_scale
    T scaled_dividend = 0;
    DecimalV3Cast::to_decimal<T, T, T, true, false>(dividend, adjust_scale_factor, &scaled_dividend);
    // compute the quotient
    T quotient = 0;
    DecimalV3Arithmetics<T, false>::div_round(scaled_dividend, divisor, &quotient);
    return quotient;
}

class Decimal128P38S9 {
public:
    using Type = int128_t;
    static constexpr Type scale_factor = get_scale_factor<Type>(9);

    explicit Decimal128P38S9(const Type& value) : _value(value) {}
    explicit Decimal128P38S9(const double& value) {
        DecimalV3Cast::from_float<double, Type>(value, scale_factor, &_value);
    }
    template <typename U>
    Decimal128P38S9(const U& value, int) : _value(T(value) * scale_factor) {}

    Decimal128P38S9 operator+(const Decimal128P38S9& rhs) const { return Decimal128P38S9(this->_value + rhs._value); }

    Decimal128P38S9 operator-(const Decimal128P38S9& rhs) const { return Decimal128P38S9(this->_value - rhs._value); }

    Decimal128P38S9 operator/(const Decimal128P38S9& rhs) const {
        Type mul_result = 0;
        DecimalV3Arithmetics<Type, false>::mul(this->_value, scale_factor, &mul_result);
        Type div_result = 0;
        DecimalV3Arithmetics<Type, false>::div_round(mul_result, rhs._value, &div_result);
        return Decimal128P38S9(div_result);
    }

    Decimal128P38S9 operator/(const int64_t& rhs) const {
        Type div_result = 0;
        DecimalV3Arithmetics<Type, false>::div_round(this->_value, rhs, &div_result);
        return Decimal128P38S9(div_result);
    }

    Decimal128P38S9 operator*(const Decimal128P38S9& rhs) const {
        Type mul_result = 0;
        DecimalV3Arithmetics<Type, false>::mul(this->_value, rhs._value, &mul_result);
        Type div_result = 0;
        DecimalV3Arithmetics<Type, false>::div_round(mul_result, scale_factor, &div_result);
        return Decimal128P38S9(div_result);
    }

    Type value() const { return _value; }
    double double_value() const {
        double result = 0;
        DecimalV3Cast::to_float<Type, double>(_value, scale_factor, &result);
        return result;
    }

private:
    Type _value;
};

template <typename Op, PrimitiveType Type>
struct ArithmeticBinaryOperator<Op, Type, DecimalOpGuard<Op>, DecimalPTGuard<Type>> {
    template <bool check_overflow, typename LType, typename RType, typename ResultType>
    static inline bool apply(const LType& l, const RType& r, ResultType* result) {
        [[maybe_unused]] static constexpr ResultType zero = ResultType(0);
        using DecimalV3Operators = DecimalV3Arithmetics<ResultType, check_overflow>;
        [[maybe_unused]] bool overflow = false;
        if constexpr (is_add_op<Op>) {
            overflow = DecimalV3Operators::add(l, r, result);
        } else if constexpr (is_sub_op<Op>) {
            overflow = DecimalV3Operators::sub(l, r, result);
        } else if constexpr (is_reverse_sub_op<Op>) {
            overflow = DecimalV3Operators::sub(r, l, result);
        } else if constexpr (is_mul_op<Op>) {
            overflow = DecimalV3Operators::mul(l, r, result);
        } else if constexpr (is_div_op<Op>) {
            if constexpr (may_cause_fpe<ResultType>) {
                if (UNLIKELY(check_fpe_of_min_div_by_minus_one(l, r))) {
                    *result = signed_minimum<ResultType>;
                    overflow = true;
                } else {
                    overflow = DecimalV3Operators::div_round(l, r + (r == zero), result);
                }
            } else {
                overflow = DecimalV3Operators::div_round(l, r + (r == zero), result);
            }
        } else if constexpr (is_mod_op<Op>) {
            if constexpr (may_cause_fpe<ResultType>) {
                if (UNLIKELY(check_fpe_of_min_div_by_minus_one(l, r))) {
                    *result = 0;
                    overflow = false;
                } else {
                    overflow = DecimalV3Operators::mod(l, r + (r == zero), result);
                }
            } else {
                overflow = DecimalV3Operators::mod(l, r + (r == zero), result);
            }
        } else if constexpr (is_reverse_mod_op<Op>) {
            if constexpr (may_cause_fpe<ResultType>) {
                if (UNLIKELY(check_fpe_of_min_div_by_minus_one(r, l))) {
                    *result = 0;
                    overflow = false;
                } else {
                    overflow = DecimalV3Operators::mod(r, l + (l == zero), result);
                }
            } else {
                overflow = DecimalV3Operators::mod(r, l + (l == zero), result);
            }
        } else {
            static_assert(is_decimal_op<Op>, "Invalid decimal binary operator");
        }
        if constexpr (check_overflow) {
            return overflow;
        } else {
            return false;
        }
    }

    template <bool check_overflow, bool adjust_left, typename LType, typename RType, typename ResultType>
    static inline bool apply(const LType& l, const RType& r, ResultType* result,
                             [[maybe_unused]] const RType& scale_factor) {
        // ugly code for divide-by-zero processing
        if constexpr (is_div_op<Op> || is_mod_op<Op>) {
            if (r == RType(0)) {
                if constexpr (is_div_op<Op>) {
                    *result = get_max<ResultType>();
                } else {
                    *result = 0;
                }
                return check_overflow;
            }
        }

        if constexpr (adjust_left) {
            LType ll;
            [[maybe_unused]] auto overflow =
                    DecimalV3Cast::scale_up<LType, LType, check_overflow>(l, scale_factor, &ll);
            if constexpr (check_overflow) {
                if (overflow) {
                    return true;
                }
            }
            return apply<check_overflow, LType, RType, ResultType>(ll, r, result);
        } else {
            return apply<check_overflow, LType, RType, ResultType>(l, r, result);
        }
    }
};

template <typename Op, PrimitiveType Type>
struct ArithmeticBinaryOperator<Op, Type, DecimalFastMulOpGuard<Op>, DecimalPTGuard<Type>> {
    template <bool check_overflow, bool adjust_left, typename LType, typename RType, typename ResultType>
    static inline bool apply(const LType& l, const RType& r, ResultType* result,
                             [[maybe_unused]] const RType& scale_factor) {
        if constexpr (pt_is_decimal128<Type>) {
            *result = i64_x_i64_produce_i128(l, r);
        } else {
            *result = i32_x_i32_produce_i64(l, r);
        }
        return false;
    }
};

template <typename Op, PrimitiveType Type, typename = guard::Guard, typename = guard::Guard>
struct ArithmeticUnaryOperator {
    template <typename UType, typename ResultType>
    static inline ReturnType<Type, ResultType> apply(const UType& l) {
        if constexpr (is_bitnot_op<Op>) {
            return ~l;
        } else {
            static_assert(is_bitnot_op<Op>, "Invalid unary operators");
        }
    }
};

template <PrimitiveType Type, typename = guard::Guard>
struct ArithmeticRightZeroCheck {
    template <typename LType, typename RType, typename ResultType>
    static inline uint8_t apply(const LType& l, const RType& r) {
        return r == RType(0);
    }
};

template <>
struct ArithmeticRightZeroCheck<TYPE_DECIMALV2, guard::Guard> {
    template <typename LType, typename RType, typename ResultType>
    static inline uint8_t apply(const LType& l, const RType& r) {
        return r == DecimalV2Value::ZERO;
    }
};
} //namespace starrocks::vectorized
