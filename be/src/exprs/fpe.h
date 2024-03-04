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

#include <string>
#include <string_view>
#include "util/slice.h"
#include "common/status.h"

namespace starrocks {

class FPE {
public:
    FPE() = delete;

    static std::string_view trim_leading_zeros(const std::string_view& str);
    static std::string_view trim_trailing_zeros(const std::string_view& str);
    static Status encrypt(const std::string_view& num_str, const std::string_view& key, char* buffer, size_t* len, int radix);
    static Status encrypt_num(const std::string_view& num_str, const std::string_view& key, std::string& value);
    static Status decrypt(const std::string_view& num_str, const std::string_view& key, std::string& value, int radix);
    static Status decrypt_num(const std::string_view& num_str,const std::string_view& key, std::string& value);

public:
    static const std::string_view DEFAULT_KEY;
    static const int DEFAULT_RADIX = 10;

private:
    static const int MIN_LENGTH = 6;
    static const char FIXED_NUM = '1';
    constexpr static const double EXPANDED = 100000.0 * 1000000000.0;
    constexpr static const uint8_t TWEAK[] = {};
};

} // namespace starrocks
