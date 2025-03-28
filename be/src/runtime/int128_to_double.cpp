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

#include "runtime/int128_to_double.h"

#include <glog/logging.h>

#include <climits>
#include <cstdint>

#include "integer_overflow_arithmetics.h"
namespace starrocks {
double __wrap___floattidf(__int128 a) {
    typedef double dst_t;
    typedef uint64_t dst_rep_t;
    typedef __uint128_t usrc_t;
#define DST_REP_C UINT64_C

    enum {
        dstSigBits = 52,
    };

    [[maybe_unused]] __int128 original_value = a;

    if (a == 0) return 0.0;

    enum {
        dstMantDig = dstSigBits + 1,
        srcBits = sizeof(__int128) * CHAR_BIT,
        srcIsSigned = ((__int128)-1) < 0,
    };

    const __int128 s = srcIsSigned ? a >> (srcBits - 1) : 0;

    a = (usrc_t)(a ^ s) - s;
    int sd = srcBits - clz128(a); // number of significant digits
    int e = sd - 1;               // exponent
    if (sd > dstMantDig) {
        //  start:  0000000000000000000001xxxxxxxxxxxxxxxxxxxxxxPQxxxxxxxxxxxxxxxxxx
        //  finish: 000000000000000000000000000000000000001xxxxxxxxxxxxxxxxxxxxxxPQR
        //                                                12345678901234567890123456
        //  1 = msb 1 bit
        //  P = bit dstMantDig-1 bits to the right of 1
        //  Q = bit dstMantDig bits to the right of 1
        //  R = "or" of all bits to the right of Q
        if (sd == dstMantDig + 1) {
            a <<= 1;
        } else if (sd == dstMantDig + 2) {
            // Do nothing.
        } else {
            a = ((usrc_t)a >> (sd - (dstMantDig + 2))) |
                ((a & ((usrc_t)(-1) >> ((srcBits + dstMantDig + 2) - sd))) != 0);
        }
        // finish:
        a |= (a & 4) != 0; // Or P into R
        ++a;               // round - this step may add a significant bit
        a >>= 2;           // dump Q and R
        // a is now rounded to dstMantDig or dstMantDig+1 bits
        if (a & ((usrc_t)1 << dstMantDig)) {
            a >>= 1;
            ++e;
        }
        // a is now rounded to dstMantDig bits
    } else {
        a <<= (dstMantDig - sd);
        // a is now rounded to dstMantDig bits
    }
    const int dstBits = sizeof(dst_t) * CHAR_BIT;
    const dst_rep_t dstSignMask = DST_REP_C(1) << (dstBits - 1);
    const int dstExpBits = dstBits - dstSigBits - 1;
    const int dstExpBias = (1 << (dstExpBits - 1)) - 1;
    const dst_rep_t dstSignificandMask = (DST_REP_C(1) << dstSigBits) - 1;
    // Combine sign, exponent, and mantissa.
    const dst_rep_t result = ((dst_rep_t)s & dstSignMask) | ((dst_rep_t)(e + dstExpBias) << dstSigBits) |
                             ((dst_rep_t)(a)&dstSignificandMask);

    const union {
        dst_t f;
        dst_rep_t i;
    } rep = {.i = result};

    DCHECK(std::abs(rep.f - __real___floattidf(original_value)) < 0.001);
    return rep.f;
}
} // namespace starrocks