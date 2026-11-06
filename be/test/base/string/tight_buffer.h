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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string_view>

namespace starrocks {

// Exact-size heap copy. Unlike std::string (capacity slack, SSO, NUL terminator), any read outside
// [begin(), end()) is a heap-buffer-overflow under BUILD_TYPE=ASAN, which pins the "zero over-read" invariant.
class TightBuffer {
public:
    explicit TightBuffer(std::string_view s) : _size(s.size()), _data(new uint8_t[s.empty() ? 1 : s.size()]) {
        if (!s.empty()) {
            std::memcpy(_data.get(), s.data(), s.size());
        }
    }
    const uint8_t* begin() const { return _data.get(); }
    const uint8_t* end() const { return _data.get() + _size; }
    size_t size() const { return _size; }

private:
    size_t _size;
    std::unique_ptr<uint8_t[]> _data;
};

} // namespace starrocks
