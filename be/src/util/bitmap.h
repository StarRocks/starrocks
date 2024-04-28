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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/bitmap.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "util/bit_util.h"

namespace starrocks {

// Return the number of bytes necessary to store the given number of bits.
inline size_t BitmapSize(const size_t num_bits) {
    return (num_bits + 7) / 8;
}

class BitMap {
public:
    BitMap() : _size(0), _bits(BitmapSize(0), 0) {}
    explicit BitMap(const size_t n) : _size(n), _bits(BitmapSize(n), 0) {}

    void resize(const size_t n) {
        _size = n;
        _bits.resize(BitmapSize(n), 0);
    }

    void set(const size_t pos, const bool value) {
        if (pos >= _size) return;
        if (value) {
            _bits[pos / 8] |= (1 << (pos % 8));
        } else {
            _bits[pos / 8] &= ~(1 << (pos % 8));
        }
    }

    bool test(const size_t pos) const {
        if (pos >= _size) return false;
        return (_bits[pos / 8] & (1 << (pos % 8))) != 0;
    }

    // count the number of bits set to true (vectorized version)
    size_t count() const {
        size_t count = 0;
        for (size_t i = 0; i < _bits.size(); ++i) {
            count += BitUtil::popcount(_bits[i]);
        }
        return count;
    }

    size_t size() const { return _size; }

    std::string debug_string() const {
        std::stringstream ss;
        for (size_t i = 0; i < _size; ++i) {
            ss << test(i);
        }
        return ss.str();
    }

private:
    size_t _size;
    std::vector<uint8_t> _bits;
};

} // namespace starrocks
