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
// Division Algorithm Implementations
// =============================================================================

int256_t int256_t::operator/(const int256_t& other) const {
    LOG(ERROR) << "|dividend|" << to_string(*this) << "|divisor|" << to_string(other) << std::endl << get_stack_trace() << std::endl;

    if (other.high == 0 && other.low == 0) {
        throw std::domain_error("Division by zero");
    }

    // Handle special cases first
    if (high == 0 && low == 0) return int256_t(0);
    if (*this == other) return int256_t(1);

    // 符号处理提取到外层
    bool negative = (high < 0) != (other.high < 0);
    int256_t dividend = (high < 0) ? -*this : *this;
    int256_t divisor = (other.high < 0) ? -other : other;

    if (dividend < divisor) return int256_t(0);
    if (divisor.high == 0 && divisor.low == 1) {
        return negative ? -dividend : dividend;
    }

    int256_t res = adaptive_divide(dividend, divisor);

    if (negative) {
        res = -res;
    }

    LOG(ERROR) << "rrrrrrrrrrrrrrrrrrrrrrrrrrrrrr" << to_string(res);
    return res;
}

int256_t int256_t::adaptive_divide(const int256_t& dividend, const int256_t& divisor) const {
    if (is_power_of_2(divisor)) {
        return power_of_2_divide(dividend, divisor);
    }

    const int divisor_bits = 256 - divisor.count_leading_zeros();
    if (divisor_bits <= 64) {
        return divide_by_small_divisor(dividend, static_cast<uint64_t>(divisor.low));
    } else if (divisor_bits <= 128) {
        return divide_by_128bit(dividend, divisor.low);
    }
    return burnikel_ziegler_divide(dividend, divisor);
}

// Check if value is power of 2
bool int256_t::is_power_of_2(const int256_t& value) {
    if (value.high < 0 || (value.high == 0 && value.low == 0)) return false;
    if (value.high == 0) {
        return (value.low & (value.low - 1)) == 0;
    }
    if (value.low != 0) return false;
    uint128_t h = static_cast<uint128_t>(value.high);
    return (h & (h - 1)) == 0;
}

int int256_t::count_trailing_zeros(int256_t divisor) const {
    if (divisor.low != 0) {
        return __builtin_ctzll(static_cast<uint64_t>(divisor.low))
             + (__builtin_ctzll(divisor.low >> 64) & 0x3F);
    }
    if (divisor.high != 0) {
        return 128 + __builtin_ctzll(static_cast<uint64_t>(divisor.high))
             + (__builtin_ctzll(divisor.high >> 64) & 0x3F);
    }
    return 256;
}

int256_t int256_t::safe_abs() const {
    if (high == std::numeric_limits<int128_t>::min() && low == 0) {
        return INT256_MIN;
    }
    return (high < 0) ? -*this : *this;
}

int int256_t::count_leading_zeros() const {
    if (high != 0) {
        uint128_t h = static_cast<uint128_t>(high);
        return __builtin_clzll(h >> 64) + (h >> 64 ? 0 : 64 + __builtin_clzll(static_cast<uint64_t>(h)));
    }
    if (low != 0) {
        return 128 + __builtin_clzll(low >> 64) + (low >> 64 ? 0 : 64 + __builtin_clzll(static_cast<uint64_t>(low)));
    }
    return 256;
}

int256_t int256_t::power_of_2_divide(const int256_t& dividend, const int256_t& divisor) const {
    int shift = count_trailing_zeros(divisor);
    return dividend >> shift;
}

int256_t int256_t::divide_by_small_divisor(const int256_t& dividend, uint64_t divisor) {
    uint64_t parts[4] = {
        static_cast<uint64_t>(dividend.low),
        static_cast<uint64_t>(dividend.low >> 64),
        static_cast<uint64_t>(dividend.high),
        static_cast<uint64_t>(dividend.high >> 64)
    };

    uint64_t quotient_parts[4] = {0};
    __uint128_t remainder = 0;

    for (int i = 3; i >= 0; i--) {
        remainder = (remainder << 64) | parts[i];
        quotient_parts[i] = static_cast<uint64_t>(remainder / divisor);
        remainder %= divisor;
    }

    int256_t result;
    result.low = (static_cast<__uint128_t>(quotient_parts[1]) << 64) | quotient_parts[0];
    result.high = (static_cast<__int128_t>(static_cast<__uint128_t>(quotient_parts[3]) << 64) | quotient_parts[2]);

    return result;
}

    int256_t int256_t::divide_by_128bit(const int256_t& dividend, uint128_t divisor) const {
    if (dividend.high < 0) return int256_t(0);

    if (dividend.high == 0) {
        if (dividend.low < divisor) return int256_t(0);
        return int256_t(0, dividend.low / divisor);
    }

    uint128_t high_part = static_cast<uint128_t>(dividend.high);
    uint128_t high_quotient = high_part / divisor;
    uint128_t high_remainder = high_part % divisor;

    uint64_t low_high = static_cast<uint64_t>(dividend.low >> 64);
    uint64_t low_low = static_cast<uint64_t>(dividend.low);

    struct uint192_t {
        uint128_t high;
        uint64_t low;
    };

    uint192_t current;
    current.high = high_remainder;
    current.low = low_high;

    uint128_t q1 = 0;
    uint128_t r1 = 0;

    if (current.high >= divisor) {
        q1 = current.high / divisor;
        r1 = current.high % divisor;
    } else {
        q1 = 0;
        r1 = current.high;
    }

    uint128_t next = (r1 << 64) | current.low;
    if (next >= divisor) {
        uint128_t q1_low = next / divisor;
        r1 = next % divisor;
        q1 = (q1 << 64) | q1_low;
    } else {
        q1 = q1 << 64;
    }

    next = (r1 << 64) | low_low;
    uint128_t q0 = next / divisor;
    r1 = next % divisor;

    uint128_t low_quotient = (q1 << 64) | q0;

    constexpr uint128_t INT128_MAX_POS = ...;
    if (high_quotient > INT128_MAX_POS) {
        throw std::overflow_error(...);
    }

    return int256_t(static_cast<int128_t>(high_quotient), low_quotient);
}



int256_t int256_t::divide_by_256bit(const int256_t& dividend, const int256_t& divisor) const {
    constexpr int BLOCK_SIZE = 64;
    constexpr int BLOCKS = 4;

    uint64_t dividend_blocks[BLOCKS] = {
        static_cast<uint64_t>(dividend.low),
        static_cast<uint64_t>(dividend.low >> 64),
        static_cast<uint64_t>(dividend.high),
        static_cast<uint64_t>(dividend.high >> 64)
    };

    uint64_t divisor_blocks[BLOCKS] = {
        static_cast<uint64_t>(divisor.low),
        static_cast<uint64_t>(divisor.low >> 64),
        static_cast<uint64_t>(divisor.high),
        static_cast<uint64_t>(divisor.high >> 64)
    };

    int divisor_len = BLOCKS;
    while (divisor_len > 1 && divisor_blocks[divisor_len-1] == 0) {
        divisor_len--;
    }

    if (divisor_len == 1) {
        return divide_by_small_divisor(dividend, divisor_blocks[0]);
    }

    int dividend_len = BLOCKS;
    while (dividend_len > 1 && dividend_blocks[dividend_len-1] == 0) {
        dividend_len--;
    }

    if (compare_blocks(dividend_blocks, dividend_len, divisor_blocks, divisor_len) < 0) {
        return int256_t(0);
    }

    uint64_t quotient[BLOCKS] = {0};
    uint64_t remainder[BLOCKS + 1] = {0};

    for (int i = dividend_len - divisor_len; i >= 0; i--) {
        __uint128_t dividend_top = (static_cast<__uint128_t>(dividend_blocks[i + divisor_len]) << BLOCK_SIZE) |
                                 dividend_blocks[i + divisor_len - 1];

        __uint128_t divisor_top = (static_cast<__uint128_t>(divisor_blocks[divisor_len-1]) << BLOCK_SIZE) |
                                (divisor_len > 1 ? divisor_blocks[divisor_len-2] : 0);

        __uint128_t q_estimate = dividend_top / divisor_top;
        if (q_estimate > 0xFFFFFFFFFFFFFFFF) {
            q_estimate = 0xFFFFFFFFFFFFFFFF;
        }

        uint64_t q = static_cast<uint64_t>(q_estimate);

        __uint128_t borrow = 0;
        __uint128_t carry = 0;

        for (int j = 0; j < divisor_len; j++) {
            __uint128_t product = static_cast<__uint128_t>(q) * divisor_blocks[j];
            __uint128_t digit = static_cast<__uint128_t>(dividend_blocks[i + j]) + carry;

            __uint128_t diff = digit - (product & 0xFFFFFFFFFFFFFFFF) - borrow;

            borrow = (product >> BLOCK_SIZE) + (diff >> BLOCK_SIZE ? 1 : 0);
            carry = product >> BLOCK_SIZE;

            remainder[j] = static_cast<uint64_t>(diff);
        }

        while (borrow != 0) {
            q--;

            __uint128_t carry_add = 0;
            for (int j = 0; j < divisor_len; j++) {
                __uint128_t sum = static_cast<__uint128_t>(remainder[j]) +
                                 divisor_blocks[j] + carry_add;
                remainder[j] = static_cast<uint64_t>(sum);
                carry_add = sum >> BLOCK_SIZE;
            }
            borrow -= carry_add;
        }

        quotient[i] = q;

        for (int j = 0; j < divisor_len; j++) {
            dividend_blocks[i + j] = remainder[j];
        }
    }

    int256_t result;
    result.low = (static_cast<__uint128_t>(quotient[1]) << BLOCK_SIZE) | quotient[0];
    result.high = (static_cast<__int128_t>(static_cast<__uint128_t>(quotient[3]) << BLOCK_SIZE) | quotient[2]);

    return result;
}


} // namespace starrocks
