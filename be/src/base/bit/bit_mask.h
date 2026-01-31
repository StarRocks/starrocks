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

#include <cstdint>
#include <cstdlib>
#include <cstring>

namespace starrocks {

class BitMask {
public:
    BitMask(size_t size) {
        _size = (size + 7) / 8;
        _bits = new uint8_t[_size];
        memset(_bits, 0, _size);
    }
    ~BitMask() {
        if (_bits) {
            delete[] _bits;
        }
    }

    void set_bit(size_t pos) { _bits[pos >> 3] |= (1 << (pos & 7)); }
    // try to set bit in pos, if bit is already set, return false, otherwise return true
    bool try_set_bit(size_t pos) {
        if (is_bit_set(pos)) {
            return false;
        }
        set_bit(pos);
        return true;
    }
    void clear_bit(size_t pos) { _bits[pos >> 3] &= ~(1 << (pos & 7)); }

    bool is_bit_set(size_t pos) { return (_bits[pos >> 3] & (1 << (pos & 7))) != 0; }

    bool all_bits_zero() const {
        for (size_t i = 0; i < _size; i++) {
            if (_bits[i] != 0) {
                return false;
            }
        }
        return true;
    }

private:
    size_t _size;
    uint8_t* _bits;
};
} // namespace starrocks