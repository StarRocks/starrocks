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

#include "exprs/fpe.h"
#include "util/defer_op.h"

namespace starrocks {

const std::string_view FPE::DEFAULT_KEY = "abcdefghijk12345abcdefghijk12345";

std::string_view FPE::trim_leading_zeros(const std::string_view& str) {
    size_t start = str.find_first_not_of('0');
    if (start == std::string::npos) {
        return "0";
    }
    return str.substr(start);
}

std::string_view FPE::trim_trailing_zeros(const std::string_view& str) {
    size_t end = str.find_last_not_of('0');
    if (end == std::string::npos) {
        return str;
    }
    return str.substr(0, end + 1);
}

Status FPE::encrypt(const std::string_view& num_str, const std::string_view& key, char* buffer, size_t* len, int radix = 10) {
    int num_str_length = num_str.length();
    std::string fixed_num_str(num_str);
    if (num_str_length < MIN_LENGTH) {
        std::string padding(MIN_LENGTH - num_str_length, '0');
        fixed_num_str = padding + fixed_num_str;
    }

    int fpe_key_length = key.length();

    std::vector<uint8_t> fpe_key(fpe_key_length);
    for (size_t i = 0; i < fpe_key_length; ++i) {
        fpe_key[i] = static_cast<uint8_t>(key[i]);
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
    res = ff1_encrypt(ctx, buffer, fixed_num_str.c_str(), NULL, 0);
    if (res != 0) {
        return Status::RuntimeError("ff1_encrypt failed");
    }

    if (len != nullptr) {
        *len = strlen(buffer);
    }

    return Status::OK();
}

Status FPE::encrypt_num(const std::string_view& num_str,const std::string_view& key, std::string& value) {
    std::string result;
    size_t result_len = 0;
    result.resize(100);

    std::string num_flag;
    if (num_str[0] == '-') {
        result[0] = '-';
        result_len = 1;
    }

    result[result_len] = FIXED_NUM;
    ++result_len;

    size_t dot_pos = num_str.find('.');
    std::string_view int_part;
    std::string_view dec_part;

    if (dot_pos != std::string_view::npos) {
        int_part = num_str.substr(0, dot_pos);
        dec_part = num_str.substr(dot_pos + 1);
    } else {
        int_part = num_str.substr(0);
    }

    size_t int_part_len = 0;
    RETURN_IF_ERROR(encrypt(int_part, key, result.data() + result_len, &int_part_len, DEFAULT_RADIX));
    result_len += int_part_len;

    if (!dec_part.empty()) {
        double dec_part_num = std::stod(std::string(num_str.substr(dot_pos)), nullptr);
        dec_part_num = dec_part_num * EXPANDED;
        auto dec_int_part = static_cast<long long>(dec_part_num);

        result[result_len] = '.';
        ++result_len;

        size_t dec_part_len = 0;
        RETURN_IF_ERROR(encrypt(std::to_string(dec_int_part), key, result.data() + result_len, &dec_part_len, DEFAULT_RADIX));
        result_len += dec_part_len;

        result[result_len] = FIXED_NUM;
        ++result_len;
    }

    result.resize(result_len);
    value = result;

    return Status::OK();
}

Status FPE::decrypt(const std::string_view& num_str, const std::string_view& key, std::string& value, int radix= 10) {
    int num_str_length = num_str.length();
    int fpe_key_length = key.length();

    std::vector<uint8_t> fpe_key(fpe_key_length);
    for (size_t i = 0; i < fpe_key_length; ++i) {
        fpe_key[i] = static_cast<uint8_t>(key[i]);
    }

    struct ff1_ctx* ctx = nullptr;
    DeferOp op([&]{ if (ctx != nullptr) ff1_ctx_destroy(ctx);});

    std::string tmp;
    tmp.resize(num_str_length);
    char* out = tmp.data();

    int res = ff1_ctx_create(&ctx, fpe_key.data(),
                             fpe_key_length, TWEAK, sizeof(TWEAK),
                             0, SIZE_MAX,
                             radix);
    if (res != 0) {
        return Status::RuntimeError("ff1_ctx_create failed");
    }
    res = ff1_decrypt(ctx, out, num_str.data(), NULL, 0);
    if (res != 0) {
        return Status::RuntimeError("ff1_encrypt failed");
    }

    std::string result(out);
    value = result;

    return Status::OK();
}

Status FPE::decrypt_num(const std::string_view& num_str, const std::string_view& key, std::string& value) {
    std::string num_flag;
    std::string decrypt_num_str(num_str);
    if (decrypt_num_str[0] == '-') {
        num_flag = "-";
        decrypt_num_str = decrypt_num_str.substr(1);
    }

    Status status;
    std::string encrypted_dec_part;
    size_t dot_pos = decrypt_num_str.find('.');
    std::string int_part = decrypt_num_str.substr(1, dot_pos - 1);
    status = decrypt(int_part, key, encrypted_dec_part, DEFAULT_RADIX);
    if (!status.ok()) {
        return Status::RuntimeError("decrypt_num int_part failed");
    }
    std::string_view decrypted_int_part = trim_leading_zeros(encrypted_dec_part);

    std::string decrypted_dec_part;
    if (dot_pos != std::string::npos) {
        std::string dec_part = decrypt_num_str.substr(dot_pos + 1, decrypt_num_str.length() - dot_pos - 2);
        status = decrypt(dec_part, key, decrypted_dec_part, DEFAULT_RADIX);
        if (!status.ok()) {
            return Status::RuntimeError("decrypt_num dec_part failed");
        }
        int expanded_length = std::to_string(static_cast<long long>(EXPANDED)).length() - 1;
        std::string leading_zeros(expanded_length - decrypted_dec_part.length(), '0');
        decrypted_dec_part = leading_zeros + decrypted_dec_part;
        decrypted_dec_part = trim_trailing_zeros(decrypted_dec_part);
        decrypted_dec_part = '.' + decrypted_dec_part;
    }

    std::string decrypted_num = num_flag + std::string(decrypted_int_part) + decrypted_dec_part;
    value = decrypted_num;

    return Status::OK();
}

} // namespace starrocks
