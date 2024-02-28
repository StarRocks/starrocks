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

#include "common/status.h"
#include "util/slice.h"

namespace starrocks {

class FPE {
public:
    FPE() = delete;

    static std::string trim_zeros(const std::string& str, size_t num_flag_pos);
    static Status encrypt(std::string_view num_str, std::string_view key, char* buffer, int radix);
    static Status encrypt_num(std::string_view num_str, std::string_view key, std::string& value);
    static Status decrypt(std::string_view num_str, std::string_view key, char* buffer, int radix);
    static Status decrypt_num(std::string_view num_str, std::string_view key, std::string& value);

public:
    static const std::string_view DEFAULT_KEY;
    static const int DEFAULT_RADIX = 10;
    static const int MIN_LENGTH = 6;

private:
    static std::string current_key;
    static std::vector<uint8_t> fpe_key;
    static const char FIXED_NUM = '1';
    constexpr static const double EXPANDED = 100000.0 * 1000000000.0;
    constexpr static const int EXPANDED_LENGTH = 14;
    constexpr static const uint8_t TWEAK[] = {};
};

} // namespace starrocks
