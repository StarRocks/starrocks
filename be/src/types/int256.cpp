#include "types/int256.h"
#include <glog/logging.h>
#include <algorithm> // for std::reverse
#include "util/stack_util.h"

namespace starrocks {

std::string to_string(const int256_t& value) {
    LOG(ERROR) << "11111111111111";
    if (value.high == 0 && value.low == 0) {
        return "0";
    }
    int256_t temp = value;
    bool negative = false;
    if (temp.high < 0) {
        negative = true;
        temp = -temp;
    }
    std::string result;
    while (temp.high != 0 || temp.low != 0) {
        uint32_t rem;
        int256_t quot;
        divmod_u32(temp, 10, &quot, &rem);
        result.push_back('0' + rem);
        temp = quot;
    }
    if (negative) result.push_back('-');
    std::reverse(result.begin(), result.end());
    return result;
}

// =============================================================================
// Division Algorithm Implementations - CORRECTED VERSION
// =============================================================================

int256_t int256_t::operator/(const int256_t& other) const {
    LOG(ERROR) << "|dividend|" << to_string(*this) << "|divisor|" << to_string(other) << std::endl << get_stack_trace() << std::endl;

    if (other.high == 0 && other.low == 0) {
        throw std::domain_error("Division by zero");
    }

    // Handle special cases first
    if (high == 0 && low == 0) return int256_t(0);
    if (*this == other) return int256_t(1);

    bool negative = (high < 0) != (other.high < 0);
    int256_t dividend = (high < 0) ? -*this : *this;
    int256_t divisor = (other.high < 0) ? -other : other;

    if (dividend < divisor) return int256_t(0);
    if (divisor.high == 0 && divisor.low == 1) {
        return negative ? -dividend : dividend;
    }

    // Use corrected adaptive algorithm selection
    int256_t res = adaptive_divide(dividend, divisor, negative);
    LOG(ERROR) << "rrrrrrrrrrrrrrrrrrrrrrrrrrrrrr" << to_string(res);
    return res;
}

int256_t int256_t::adaptive_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const {
    if (is_power_of_2(divisor)) {
        return power_of_2_divide(dividend, divisor, negative);
    }

    if (divisor.high == 0 && divisor.low <= 0xFFFFFFFFULL) {
        return small_divisor_divide(dividend, divisor, negative);
    }

    return fast_binary_divide(dividend, divisor, negative);
}


int int256_t::find_msb_64(uint64_t value) const {
    if (value == 0) return -1;
    int msb = 0;
    if (value >= 0x100000000ULL) { msb += 32; value >>= 32; }
    if (value >= 0x10000) { msb += 16; value >>= 16; }
    if (value >= 0x100) { msb += 8; value >>= 8; }
    if (value >= 0x10) { msb += 4; value >>= 4; }
    if (value >= 0x4) { msb += 2; value >>= 2; }
    if (value >= 0x2) { msb += 1; }
    return msb;
}

int int256_t::find_msb(const int256_t& value) const {
    int256_t abs_value = value;
    if (abs_value.high < 0) {
        abs_value = -abs_value;
    }

    if (abs_value.high != 0) {
        uint128_t h = static_cast<uint128_t>(abs_value.high);
        if (h >> 64) {
            uint64_t high_part = static_cast<uint64_t>(h >> 64);
            return 192 + find_msb_64(high_part);
        } else {
            uint64_t low_part = static_cast<uint64_t>(h);
            return 128 + find_msb_64(low_part);
        }
    } else if (abs_value.low != 0) {
        if (abs_value.low >> 64) {
            uint64_t high_part = static_cast<uint64_t>(abs_value.low >> 64);
            return 64 + find_msb_64(high_part);
        } else {
            uint64_t low_part = static_cast<uint64_t>(abs_value.low);
            return find_msb_64(low_part);
        }
    }
    return -1; // value is 0
}

// Check if value is power of 2
bool int256_t::is_power_of_2(const int256_t& value) const {
    if (value.high < 0 || (value.high == 0 && value.low == 0)) return false;
    if (value.high == 0) {
        return (value.low & (value.low - 1)) == 0;
    }
    if (value.low != 0) return false;
    uint128_t h = static_cast<uint128_t>(value.high);
    return (h & (h - 1)) == 0;
}

// Count trailing zeros
int int256_t::count_trailing_zeros(const int256_t& value) const {
    if (value.low != 0) {
        return count_trailing_zeros_128(value.low);
    } else if (value.high != 0) {
        return 128 + count_trailing_zeros_128(static_cast<uint128_t>(value.high));
    }
    return 256;
}

int int256_t::count_trailing_zeros_128(uint128_t value) const {
    if (value == 0) return 128;

    int count = 0;
    if ((value & 0xFFFFFFFFFFFFFFFFULL) == 0) {
        count += 64;
        value >>= 64;
    }

    uint64_t v = static_cast<uint64_t>(value);
    if ((v & 0xFFFFFFFF) == 0) { count += 32; v >>= 32; }
    if ((v & 0xFFFF) == 0) { count += 16; v >>= 16; }
    if ((v & 0xFF) == 0) { count += 8; v >>= 8; }
    if ((v & 0xF) == 0) { count += 4; v >>= 4; }
    if ((v & 0x3) == 0) { count += 2; v >>= 2; }
    if ((v & 0x1) == 0) { count += 1; }

    return count;
}

int256_t int256_t::fast_binary_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const {
    int256_t quotient(0);
    int256_t remainder(0);

    int dividend_bits = find_msb(dividend) + 1;
    if (dividend_bits <= 0) dividend_bits = 1;

    for (int i = dividend_bits - 1; i >= 0; --i) {
        remainder = remainder << 1;

        bool bit_set = false;
        if (i >= 128) {
            int bit_pos = i - 128;
            if (bit_pos < 128 && dividend.high >= 0) {
                uint128_t high_part = static_cast<uint128_t>(dividend.high);
                bit_set = (high_part >> bit_pos) & 1;
            } else if (bit_pos < 128) {
                LOG(ERROR) << "Unexpected negative high in division";
                bit_set = false;
            }
        } else {
            bit_set = (dividend.low >> i) & 1;
        }

        if (bit_set) {
            remainder.low |= 1;
        }

        if (remainder >= divisor) {
            remainder = remainder - divisor;

            if (i >= 128) {
                int bit_pos = i - 128;
                if (bit_pos < 128) {
                    quotient.high |= (int128_t(1) << bit_pos);
                }
            } else {
                quotient.low |= (uint128_t(1) << i);
            }
        }
    }

    return negative ? -quotient : quotient;
}

int256_t int256_t::small_divisor_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const {
    if (divisor.high == 0 && divisor.low <= 0xFFFFFFFFULL) {
        uint32_t div32 = static_cast<uint32_t>(divisor.low);
        if (div32 == 0) {
            throw std::domain_error("Division by zero in small divisor divide");
        }

        int256_t quotient;
        uint32_t remainder;
        divmod_u32(dividend, div32, &quotient, &remainder);
        return negative ? -quotient : quotient;
    } else if (divisor.high == 0) {
        return fast_divide_by_128(dividend, divisor.low, negative);
    }

    return fast_binary_divide(dividend, divisor, negative);
}

int256_t int256_t::fast_small_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const {
    return small_divisor_divide(dividend, divisor, negative);
}

int256_t int256_t::fast_divide_by_128(const int256_t& dividend, uint128_t divisor, bool negative) const {
    if (divisor == 0) {
        throw std::domain_error("Division by zero in 128-bit divide");
    }

    // Handle power-of-2 divisors with bit shifts
    if ((divisor & (divisor - 1)) == 0) {
        int shift = count_trailing_zeros_128(divisor);
        int256_t result = dividend >> shift;
        return negative ? -result : result;
    }

    int256_t quotient(0);

    if (dividend.high > 0) {
        // High part division
        uint128_t high_abs = static_cast<uint128_t>(dividend.high);
        uint128_t high_q = high_abs / divisor;
        uint128_t high_r = high_abs % divisor;

        quotient.high = static_cast<int128_t>(high_q);

        // Combined remainder + low part division
        if (high_r == 0) {
            quotient.low = dividend.low / divisor;
        } else {
            quotient.low = divide_256_by_128(high_r, dividend.low, divisor);
        }
    } else {
        quotient.low = dividend.low / divisor;
    }

    return negative ? -quotient : quotient;
}

int256_t int256_t::power_of_2_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const {
    int shift = count_trailing_zeros(divisor);
    int256_t result = dividend >> shift;
    return negative ? -result : result;
}

int256_t int256_t::barrett_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const {
    return fast_binary_divide(dividend, divisor, negative);
}

int256_t int256_t::burnikel_ziegler_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const {
    return fast_binary_divide(dividend, divisor, negative);
}

uint128_t int256_t::divide_256_by_128(uint128_t high, uint128_t low, uint128_t divisor) const {
    if (divisor == 0) {
        throw std::domain_error("Division by zero in 256/128 divide");
    }

    uint64_t divisor_high = static_cast<uint64_t>(divisor >> 64);
    uint64_t divisor_low = static_cast<uint64_t>(divisor);

    if (divisor_high == 0) {
        return divide_256_by_64(high, low, divisor_low);
    }

    uint128_t quotient = 0;
    uint128_t remainder = high;

    if (remainder >= divisor) {
        quotient = remainder / divisor;
        remainder = remainder % divisor;
    }

    remainder = (remainder << 64) | (low >> 64);
    quotient = (quotient << 64) | (remainder / divisor);
    remainder = remainder % divisor;

    remainder = (remainder << 64) | (low & 0xFFFFFFFFFFFFFFFFULL);
    quotient = (quotient << 64) | (remainder / divisor);

    return quotient;
}

uint128_t int256_t::divide_256_by_64(uint128_t high, uint128_t low, uint64_t divisor) const {
    if (divisor == 0) {
        throw std::domain_error("Division by zero in 256/64 divide");
    }

    uint64_t parts[4] = {
        static_cast<uint64_t>(low),
        static_cast<uint64_t>(low >> 64),
        static_cast<uint64_t>(high),
        static_cast<uint64_t>(high >> 64)
    };

    uint64_t quotient_parts[2] = {0, 0};
    uint64_t remainder = 0;

    for (int i = 3; i >= 2; --i) {
        uint128_t temp = (static_cast<uint128_t>(remainder) << 64) | parts[i];
        quotient_parts[i-2] = static_cast<uint64_t>(temp / divisor);
        remainder = static_cast<uint64_t>(temp % divisor);
    }

    return (static_cast<uint128_t>(quotient_parts[1]) << 64) | quotient_parts[0];
}

int256_t int256_t::reciprocal_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const {
    return fast_binary_divide(dividend, divisor, negative);
}

int256_t int256_t::calculate_barrett_constant(const int256_t& divisor, int k) const {
    int divisor_bits = find_msb(divisor) + 1;
    return int256_t(1) << (k - divisor_bits);
}

int256_t int256_t::multiply_high_precision(const int256_t& a, const int256_t& b) const {
    int256_t result = a * b;
    return result >> 128;
}

int256_t int256_t::newton_raphson_reciprocal(const int256_t& divisor, const int256_t& power_of_2) const {
    return power_of_2 / divisor;
}

void int256_t::recursive_divide_blocks(uint64_t* dividend_blocks, int dividend_size,
                                     const uint64_t* divisor_blocks, int divisor_size,
                                     uint64_t* quotient_blocks) const {
    if (divisor_size == 1 && dividend_size > 0) {
        divide_by_single_block(dividend_blocks, dividend_size, divisor_blocks[0], quotient_blocks);
    }
}

void int256_t::divide_by_single_block(uint64_t* dividend_blocks, int dividend_size,
                                    uint64_t divisor_block, uint64_t* quotient_blocks) const {
    if (divisor_block == 0) return;

    uint64_t remainder = 0;
    for (int i = dividend_size - 1; i >= 0; --i) {
        uint128_t temp = (static_cast<uint128_t>(remainder) << 64) | dividend_blocks[i];
        quotient_blocks[i] = static_cast<uint64_t>(temp / divisor_block);
        remainder = static_cast<uint64_t>(temp % divisor_block);
    }
}

void int256_t::divide_2n_by_n(uint64_t* dividend_2n, const uint64_t* divisor_n, int n, uint64_t* quotient) const {
    if (n == 1 && divisor_n[0] != 0) {
        uint128_t dividend_128 = (static_cast<uint128_t>(dividend_2n[1]) << 64) | dividend_2n[0];
        quotient[0] = static_cast<uint64_t>(dividend_128 / divisor_n[0]);
    }
}

void int256_t::divide_3half_by_2half(uint64_t* dividend, const uint64_t* divisor, int half_n, uint64_t* quotient) const {
    if (half_n == 1) {
        uint128_t high_dividend = (static_cast<uint128_t>(dividend[1]) << 64) | dividend[0];
        uint128_t high_divisor = (static_cast<uint128_t>(divisor[1]) << 64) | divisor[0];
        if (high_divisor != 0) {
            quotient[0] = static_cast<uint64_t>(high_dividend / high_divisor);
        }
    }
}

void int256_t::refine_quotient_estimate(uint64_t* dividend, const uint64_t* divisor, int half_n,
                                      uint64_t estimated_quotient, uint64_t* quotient) const {
    quotient[0] = estimated_quotient;
}

void int256_t::calculate_remainder_and_continue(uint64_t* dividend_2n, const uint64_t* divisor_n, int n,
                                              uint64_t* quotient, uint64_t* temp_remainder) const {
    for (int i = 0; i < n; ++i) {
        temp_remainder[i] = 0;
    }
}

uint64_t int256_t::modular_inverse_64(uint64_t value) const {
    if (value <= 1) return value;

    uint64_t result = 1;
    for (int i = 0; i < 6; ++i) {
        result = result * (2 - value * result);
    }
    return result;
}

int256_t int256_t::montgomery_divide(const int256_t& dividend, uint64_t small_divisor, uint64_t inverse) const {
    return fast_divide_by_128(dividend, static_cast<uint128_t>(small_divisor), false);
}

} // namespace starrocks
