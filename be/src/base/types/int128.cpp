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

#include "base/types/int128.h"

#include <string>

#include "base/hash/hash_util.hpp"
#include "base/string/string_parser.hpp"

namespace starrocks {

std::istream& operator>>(std::istream& is, __int128& value) {
    std::string str;
    is >> str;
    StringParser::ParseResult result;
    value = StringParser::string_to_int<__int128>(str.c_str(), str.size(), &result);
    if (result != StringParser::PARSE_SUCCESS) {
        is.setstate(std::ios_base::failbit);
    }
    return is;
}

std::size_t hash_value(__int128 const& value) {
    return HashUtil::hash(&value, sizeof(value), 0);
}

} // namespace starrocks
