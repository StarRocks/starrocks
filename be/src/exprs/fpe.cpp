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

#include <iostream>
#include <string>
#include <sstream>
#include <cmath>
#include <vector>
#include <ubiq/fpe/ff1.h>
#include <iomanip>

#include "util/string_parser.hpp"
#include "exprs/fpe.h"
#include "util/defer_op.h"
#include "gutil/strings/fastmem.h"

namespace starrocks {

const std::string_view FPE::DEFAULT_KEY = "abcdefghijk12345abcdefghijk12345";
std::vector<uint8_t> FPE::fpe_key;
std::string FPE::current_key;

std::string FPE::trim_zeros(const std::string& str, size_t num_flag_pos) {
    int start = num_flag_pos;
    int end = str.length() - 1;

    while (start <= end && str[start] == '0') {
        ++start;
    }
    while (end >= start && str[end] == '0') {
        --end;
    }
    if (start > end) {
        return "";
    }
    return num_flag_pos == 0 ? str.substr(start, end - start + 1)
    : str.substr(start, end - start + 1).insert(0,1,'-');
}

Status FPE::encrypt(std::string_view num_str, std::string_view key, char* buffer, int radix = 10) {
    int fpe_key_length = key.length();
    if (key != current_key) {
        current_key = std::string(key);
        fpe_key.resize(fpe_key_length);
        for (size_t i = 0; i < key.length(); ++i) {
            fpe_key[i] = static_cast<uint8_t>(key[i]);
        }
    }

    int num_str_length = num_str.length();
    std::string fixed_num_str;
    if (num_str_length < MIN_LENGTH) {
        fixed_num_str.resize(MIN_LENGTH);
        int padding_pos = MIN_LENGTH - num_str_length;
        std::fill(fixed_num_str.begin(), fixed_num_str.begin() + padding_pos, '0');
        strings::memcpy_inlined(fixed_num_str.data()+padding_pos,  num_str.data(), num_str.size());
    }

    struct ff1_ctx* ctx = nullptr;
    DeferOp op([&]{ if (ctx != nullptr) ff1_ctx_destroy(ctx);});

    int res = ff1_ctx_create(&ctx, fpe_key.data(),
                             fpe_key_length, TWEAK, sizeof(TWEAK),
                             0, SIZE_MAX,
                             radix);
    if (res != 0) {
        return Status::RuntimeError("ff1_ctx_create failed");
    }
    res = ff1_encrypt(ctx, buffer,
                      fixed_num_str.empty() ? std::string(num_str).c_str() : fixed_num_str.c_str(),
                      NULL, 0);
    if (res != 0) {
        return Status::RuntimeError("ff1_encrypt failed");
    }

    return Status::OK();
}

Status FPE::decrypt(std::string_view num_str, std::string_view key, char* buffer, int radix= 10) {
    int fpe_key_length = key.length();
    if (key != current_key) {
        current_key = std::string(key);
        fpe_key.resize(fpe_key_length);
        for (size_t i = 0; i < key.length(); ++i) {
            fpe_key[i] = static_cast<uint8_t>(key[i]);
        }
    }

    struct ff1_ctx* ctx = nullptr;
    DeferOp op([&]{ if (ctx != nullptr) ff1_ctx_destroy(ctx);});

    int res = ff1_ctx_create(&ctx, fpe_key.data(),
                             fpe_key_length, TWEAK, sizeof(TWEAK),
                             0, SIZE_MAX,
                             radix);
    if (res != 0) {
        return Status::RuntimeError("ff1_ctx_create failed");
    }
    res = ff1_decrypt(ctx, buffer, std::string(num_str).c_str(), NULL, 0);
    if (res != 0) {
        return Status::RuntimeError("ff1_decrypt failed");
    }

    return Status::OK();
}

Status FPE::encrypt_num(std::string_view num_str,std::string_view key, std::string& value) {
    std::string result;
    result.resize(100);

    size_t result_len = 0;
    size_t num_flag_pos = 0 ;
    std::string num_flag;

    if (num_str[0] == '-') {
        result[0] = '-';
        result_len = 1;
        num_flag_pos = 1;
    }

    result[result_len] = FIXED_NUM;
    ++result_len;

    size_t dot_pos = num_str.find('.');
    std::string_view int_part;
    std::string_view dec_part;

    if (dot_pos != std::string_view::npos) {
        int_part = num_str.substr(num_flag_pos, dot_pos - num_flag_pos);
        dec_part = num_str.substr(dot_pos + num_flag_pos + 1);
    } else {
        int_part = num_str.substr(num_flag_pos);
    }

    RETURN_IF_ERROR(encrypt(int_part,
                            key, result.data() + result_len, DEFAULT_RADIX));
    auto int_part_size = int_part.size();
    int_part_size = int_part_size > FPE::MIN_LENGTH ? int_part_size : FPE::MIN_LENGTH;
    result_len += int_part_size;

    if (!dec_part.empty()) {
        std::string_view dec_with_dot_part = num_str.substr(dot_pos);
        StringParser::ParseResult parse_result;
        double dec_part_num = StringParser::string_to_float<double>(dec_with_dot_part.data(), dec_with_dot_part.length(), &parse_result);
        if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
            return Status::InternalError("Fail to cast to int from string");
        }

        dec_part_num = dec_part_num * EXPANDED;
        auto dec_int_part = static_cast<long long>(dec_part_num);

        result[result_len] = '.';
        ++result_len;

        auto dec_int_part_str = std::to_string(dec_int_part);
        RETURN_IF_ERROR(encrypt(dec_int_part_str, key, result.data() + result_len, DEFAULT_RADIX));
        auto dec_int_part_size = dec_int_part_str.length() > FPE::MIN_LENGTH ? dec_int_part_str.length() : FPE::MIN_LENGTH;
        result_len += dec_int_part_size;

        result[result_len] = FIXED_NUM;
        ++result_len;
    }

    result.resize(result_len);
    value = result;

    return Status::OK();
}

Status FPE::decrypt_num(std::string_view num_str, std::string_view key, std::string& value) {
    std::string result;
    result.resize(100);

    size_t result_len = 0;
    size_t num_flag_pos = 0 ;

    std::string num_flag;
    if (num_str[0] == '-') {
        result[0] = '-';
        result_len = 1;
        num_flag_pos = 1;
    }

    size_t dot_pos = num_str.find('.');
    std::string_view int_part;
    std::string_view dec_part;

    // Remove the added FIXED_NUM
    if (dot_pos != std::string_view::npos) {
        int_part = num_str.substr(num_flag_pos + 1, dot_pos - num_flag_pos - 1);
        dec_part = num_str.substr(dot_pos + 1, num_str.length() - dot_pos -2);
    } else {
        int_part = num_str.substr(num_flag_pos + 1);
    }

    RETURN_IF_ERROR(decrypt(int_part, key, result.data() + result_len, DEFAULT_RADIX));
    result_len += int_part.size();

   if (!dec_part.empty()) {
        result[result_len] = '.';
        result_len++;

        auto dec_part_len = dec_part.size();
        RETURN_IF_ERROR(decrypt(dec_part, key, result.data() + result_len, DEFAULT_RADIX));

        int expanded_length = EXPANDED_LENGTH - dec_part_len;
        if (expanded_length > 0) {
            result.insert(result_len, expanded_length, '0');
            result_len += expanded_length;
        }

        result_len += dec_part_len;
    }

    result.resize(result_len);
    value = trim_zeros(result, num_flag_pos);

    return Status::OK();
}

} // namespace starrocks
