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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <vector>

namespace starrocks {

class BitMask {
public:
    BitMask(size_t size) : _bits((size + 7 / 8), 0) {}
    ~BitMask() = default;

    void set_bit(size_t pos) { _bits[pos >> 3] |= (1 << (pos & 7)); }
    void clear_bit(size_t pos) { _bits[pos >> 3] &= ~(1 << (pos & 7)); }

    bool is_bit_set(size_t pos) { return (_bits[pos >> 3] & (1 << (pos & 7))) != 0; }

    bool all_bits_zero() const {
        for (uint8_t byte : _bits) {
            if (byte != 0) {
                return false;
            }
        }
        return true;
    }

private:
    std::vector<uint8_t> _bits;
};
} // namespace starrocks