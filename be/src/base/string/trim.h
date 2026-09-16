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
#include <string_view>

namespace starrocks {

// Removes the leading and trailing ASCII spaces (0x20) of `str` and returns the remaining
// substring, which still points into the buffer owned by the caller.
//
// Only the space character is removed. Callers such as the CSV `trim_space` option and URL
// parsing treat a tab, a newline or a carriage return as data, so widening this to the whole
// whitespace set would silently change what they parse. Use StripWhiteSpace() from
// gutil/strings/strip.h when the full whitespace set should be removed instead.
inline std::string_view trim_spaces(std::string_view str) {
    size_t begin = 0;
    size_t end = str.size();
    while (begin < end && str[begin] == ' ') {
        ++begin;
    }
    while (end > begin && str[end - 1] == ' ') {
        --end;
    }
    return str.substr(begin, end - begin);
}

} // namespace starrocks
