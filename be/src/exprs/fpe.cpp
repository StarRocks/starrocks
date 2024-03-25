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

#include "exprs/fpe.h"

#include <ubiq/fpe/ff1.h>

#include <cmath>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "gutil/strings/fastmem.h"
#include "util/defer_op.h"
#include "util/string_parser.hpp"

namespace starrocks {

const std::string_view FPE::DEFAULT_KEY = "abcdefghijk12345abcdefghijk12345";
static constexpr int DEC_MAX_SIZE = 15;

void FPE::trim_leading_zeros(const std::string& result, size_t num_flag_pos, std::string& value) {
    size_t start = num_flag_pos;
    size_t end = result.length() - 1;
    while (start <= end && result[start] == '0') {
        ++start;
    }
    if (start > end) {
        value = "0";
    } else {
        size_t length = end - start + 1;
        value.resize(num_flag_pos + length);
        if (num_flag_pos == 1) {
            value[0] = '-';
        }
        strings::memcpy_inlined(value.data() + num_flag_pos, result.data() + start, length);
    }
}

void FPE::trim_zeros(const std::string& result, size_t num_flag_pos, std::string& value) {
    size_t start = num_flag_pos;
    size_t end = result.length() - 1;

    while (start < end && result[start] == '0') {
        ++start;
    }

    if (result[start] == '.') {
        --start;
    }
    while (end > start && result[end] == '0') {
        --end;
    }
    if (start <= end && result[end] == '.') {
        --end;
    }

    if (start > end) {
        value = "0";
    } else {
        size_t length = end - start + 1;
        value.resize(num_flag_pos + length);
        if (num_flag_pos == 1) {
            value[0] = '-';
        }
        strings::memcpy_inlined(value.data() + num_flag_pos, result.data() + start, length);
    }
}

Status FPE::encrypt(std::string_view num_str, const std::vector<uint8_t>& key, char* buffer, int radix = 10) {
    int num_str_length = num_str.length();
    std::string fixed_num_str;
    if (num_str_length < MIN_LENGTH) {
        fixed_num_str.resize(MIN_LENGTH);
        int padding_pos = MIN_LENGTH - num_str_length;
        std::fill(fixed_num_str.begin(), fixed_num_str.begin() + padding_pos, '0');
        strings::memcpy_inlined(fixed_num_str.data() + padding_pos, num_str.data(), num_str.size());
    }

    struct ff1_ctx* ctx = nullptr;
    DeferOp op([&] {
        if (ctx != nullptr) ff1_ctx_destroy(ctx);
    });

    int res = ff1_ctx_create(&ctx, key.data(), key.size(), TWEAK, sizeof(TWEAK), 0, SIZE_MAX, radix);
    if (res != 0) {
        return Status::RuntimeError("ff1_ctx_create failed");
    }
    res = ff1_encrypt(ctx, buffer, fixed_num_str.empty() ? std::string(num_str).c_str() : fixed_num_str.c_str(),
                      nullptr, 0);
    if (res != 0) {
        return Status::RuntimeError("ff1_encrypt failed");
    }

    return Status::OK();
}

Status FPE::decrypt(std::string_view num_str, const std::vector<uint8_t>& key, char* buffer, int radix = 10) {
    struct ff1_ctx* ctx = nullptr;
    DeferOp op([&] {
        if (ctx != nullptr) ff1_ctx_destroy(ctx);
    });

    int res = ff1_ctx_create(&ctx, key.data(), key.size(), TWEAK, sizeof(TWEAK), 0, SIZE_MAX, radix);
    if (res != 0) {
        return Status::RuntimeError("ff1_ctx_create failed");
    }
    res = ff1_decrypt(ctx, buffer, std::string(num_str).c_str(), nullptr, 0);
    if (res != 0) {
        return Status::RuntimeError("ff1_decrypt failed");
    }

    return Status::OK();
}

Status FPE::encrypt_num(std::string_view num_str, const std::vector<uint8_t>& key, std::string& value) {
    std::string result;
    result.resize(40);
    size_t result_len = 0;
    size_t num_flag_pos = 0;
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
        dec_part = num_str.substr(dot_pos + 1);
    } else {
        int_part = num_str.substr(num_flag_pos);
    }

    RETURN_IF_ERROR(encrypt(int_part, key, result.data() + result_len, DEFAULT_RADIX));
    auto int_part_size = int_part.size();
    int_part_size = int_part_size > FPE::MIN_LENGTH ? int_part_size : FPE::MIN_LENGTH;
    result_len += int_part_size;

    if (dec_part.empty()) {
        result.resize(result_len);
        value = std::move(result);

        return Status::OK();
    } else {
        std::size_t first_not_zero = dec_part.find_first_not_of('0');
        if (first_not_zero == std::string::npos) {
            result.resize(result_len);
            value = std::move(result);
            return Status::OK();
        }

        std::string dec_int_part_str;
        dec_int_part_str = dec_part.substr(first_not_zero);
        if (UNLIKELY(FPE::EXPANDED_LENGTH - static_cast<int>(first_not_zero) <= 0)) {
            result.resize(result_len);
            value = std::move(result);
            return Status::OK();
        }
        dec_int_part_str.resize(FPE::EXPANDED_LENGTH - first_not_zero, '0');

        result[result_len] = '.';
        ++result_len;

        RETURN_IF_ERROR(encrypt(dec_int_part_str, key, result.data() + result_len, DEFAULT_RADIX));

        auto dec_int_part_size =
                dec_int_part_str.length() > FPE::MIN_LENGTH ? dec_int_part_str.length() : FPE::MIN_LENGTH;
        result_len += dec_int_part_size;

        result[result_len] = FIXED_NUM;
        ++result_len;

        result.resize(result_len);
        value = std::move(result);

        return Status::OK();
    }
}

Status FPE::decrypt_num(std::string_view num_str, const std::vector<uint8_t>& key, std::string& value) {
    std::string result;
    result.resize(40);

    size_t result_len = 0;
    size_t num_flag_pos = 0;
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
        dec_part = num_str.substr(dot_pos + 1, num_str.length() - dot_pos - 1);
        // only keep the first 15 digits
        dec_part = dec_part.substr(0, DEC_MAX_SIZE);
        // remove the tailing zero if exsit
        dec_part = dec_part.substr(0, dec_part.find_last_not_of('0') + 1);
        // remove the last "1" which is the FIXED_NUM
        dec_part = dec_part.substr(0, dec_part.size() - 1);
    } else {
        int_part = num_str.substr(num_flag_pos + 1);
    }

    RETURN_IF_ERROR(decrypt(int_part, key, result.data() + result_len, DEFAULT_RADIX));
    result_len += int_part.size();

    if (dec_part.empty()) {
        result.resize(result_len);
        trim_leading_zeros(result, num_flag_pos, value);

        return Status::OK();
    } else {
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
        result.resize(result_len);
        trim_zeros(result, num_flag_pos, value);

        return Status::OK();
    }
}

} // namespace starrocks
