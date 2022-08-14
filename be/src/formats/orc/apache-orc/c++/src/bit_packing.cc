// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "bit_packing.h"

#include <algorithm>
namespace orc {

void bit_unpack_tail(const uint8_t* in, int fb, int64_t* data, int nums) {
    if (nums == 0) return;
    int64_t t = 0;
    uint8_t c = 0;
    int cb = 0;
    for (int i = 0; i < nums; i++) {
        int bits = fb;
        t = 0;
        while (bits) {
            if (cb == 0) {
                c = (*in++);
                cb = 8;
            }
            int lb = std::min(cb, bits);
            t = (t << lb) | ((c >> (cb - lb)) & ((1 << lb) - 1));
            bits -= lb;
            cb -= lb;
        }
        *data = t;
        data++;
    }
}

#include "bit_packing_gen.inc"

} // namespace orc
