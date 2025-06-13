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

#include "types/int256.h"

#include <glog/logging.h>

#include <algorithm>

namespace starrocks {

// =============================================================================
// Type Conversion Operators Implementation
// =============================================================================

int256_t::operator double() const {
    if (*this == 0) return 0.0;

    bool negative = (high < 0);
    int256_t abs_val = negative ? -*this : *this;

    // Find the position of the most significant bit
    int bit_count = 0;
    int256_t temp = abs_val;
    while (temp > 0) {
        temp >>= 1;
        bit_count++;
    }

    if (bit_count <= 53) {
        // Can represent exactly in double
        double result = static_cast<double>(static_cast<uint64_t>(abs_val.low));
        if (abs_val.high != 0) {
            result += static_cast<double>(static_cast<uint64_t>(abs_val.high)) * (1ULL << 32) * (1ULL << 32) *
                      (1ULL << 32) * (1ULL << 32);
        }
        return negative ? -result : result;
    } else {
        // Need to round to fit in double precision
        int shift = bit_count - 53;
        int256_t rounded = (abs_val + (int256_t(1) << (shift - 1))) >> shift;
        double mantissa = static_cast<double>(static_cast<uint64_t>(rounded.low));
        double result = mantissa * pow(2.0, shift);
        return negative ? -result : result;
    }
}

// =============================================================================
// Division Implementation
// =============================================================================

// TODO(stephen): optimize this operator in the next patch
int256_t int256_t::operator/(const int256_t& other) const {
    if (other.high == 0 && other.low == 0) {
        throw std::domain_error("Division by zero");
    }

    if (high == 0 && low == 0) return int256_t(0);
    if (*this == other) return int256_t(1);

    bool negative = (high < 0) != (other.high < 0);
    int256_t dividend = (high < 0) ? -*this : *this;
    int256_t divisor = (other.high < 0) ? -other : other;

    if (dividend < divisor) return int256_t(0);

    if (divisor.high == 0 && divisor.low == 1) {
        return negative ? -dividend : dividend;
    }

    int256_t quotient(0);
    int256_t remainder(0);

    for (int i = 255; i >= 0; i--) {
        remainder = remainder << 1;

        if (i >= 128) {
            if ((dividend.high >> (i - 128)) & 1) {
                remainder.low |= 1;
            }
        } else {
            if ((dividend.low >> i) & 1) {
                remainder.low |= 1;
            }
        }

        if (remainder >= divisor) {
            remainder = remainder - divisor;
            if (i >= 128) {
                quotient.high |= (static_cast<int128_t>(1) << (i - 128));
            } else {
                quotient.low |= (static_cast<uint128_t>(1) << i);
            }
        }
    }

    return negative ? -quotient : quotient;
}

// =============================================================================
// Modulo Implementation
// =============================================================================

int256_t int256_t::operator%(const int256_t& other) const {
    if (other.high == 0 && other.low == 0) {
        throw std::domain_error("Division by zero");
    }

    if (high == 0 && low == 0) return int256_t(0);
    if (*this == other) return int256_t(0);

    int256_t quotient = *this / other;
    int256_t remainder = *this - (quotient * other);
    return remainder;
}

// =============================================================================
// Utility Functions Implementation
// =============================================================================

std::string int256_t::to_string() const {
    if (high == 0 && low == 0) {
        return "0";
    }
    int256_t temp = *this;
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

void divmod_u32(const int256_t& value, uint32_t divisor, int256_t* quotient, uint32_t* remainder) {
    uint64_t parts[4];
    parts[0] = static_cast<uint64_t>(value.low);
    parts[1] = static_cast<uint64_t>(value.low >> 64);
    parts[2] = static_cast<uint64_t>(value.high);
    parts[3] = static_cast<uint64_t>(value.high >> 64);

    uint64_t q[4] = {0, 0, 0, 0};
    uint64_t r = 0;
    for (int i = 3; i >= 0; --i) {
        __uint128_t acc = (static_cast<__uint128_t>(r) << 64) | parts[i];
        q[i] = static_cast<uint64_t>(acc / divisor);
        r = static_cast<uint64_t>(acc % divisor);
    }
    *quotient = int256_t((static_cast<__int128_t>(q[3]) << 64) | q[2], (static_cast<__uint128_t>(q[1]) << 64) | q[0]);
    *remainder = r;
}

int256_t parse_int256(const std::string& str) {
    if (str.empty()) {
        throw std::invalid_argument("empty string");
    }

    int256_t result = 0;
    bool negative = false;
    size_t i = 0;

    if (str[0] == '-') {
        negative = true;
        i = 1;
        if (i >= str.size()) {
            throw std::invalid_argument("only minus sign");
        }
    }

    bool has_digits = false;

    for (; i < str.size(); ++i) {
        if (str[i] < '0' || str[i] > '9') {
            throw std::invalid_argument("invalid digit");
        }
        has_digits = true;
        result = result * 10 + (str[i] - '0');
    }

    if (!has_digits) {
        throw std::invalid_argument("no valid digits");
    }

    return negative ? -result : result;
}

} // namespace starrocks