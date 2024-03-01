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

namespace starrocks {

const std::string FPE::DEFAULT_KEY = "abcdefghijk12345abcdefghijk12345";

std::string FPE::trim_leading_zeros(const std::string &str) {
    size_t start = str.find_first_not_of('0');
    if (start == std::string::npos) {
        return "0";
    }
    return str.substr(start);
}

std::string FPE::trim_trailing_zeros(const std::string &str) {
    size_t end = str.find_last_not_of('0');
    if (end == std::string::npos) {
        return str;
    }
    return str.substr(0, end + 1);
}

Status FPE::encrypt(const std::string& num_str, const std::string key, std::string& value, int radix = 10) {
    int num_str_length = num_str.length();
    std::string fixed_num_str = num_str;
    if (num_str_length < MIN_LENGTH) {
        std::string padding(MIN_LENGTH - num_str_length, '0');
        fixed_num_str = padding + fixed_num_str;
    }

    int fpe_key_length = key.length();

    std::vector<uint8_t> fpe_key(fpe_key_length);
    for (size_t i = 0; i < fpe_key_length; ++i) {
        fpe_key[i] = static_cast<uint8_t>(key[i]);
    }

    struct ff1_ctx* ctx;
    char* out = (char *) calloc(num_str_length + 1, sizeof(char));
    if (out == NULL) {
        return Status::MemoryAllocFailed("encrypt calloc memory Failed");
    }

    int res = ff1_ctx_create(&ctx, fpe_key.data(),
                             fpe_key_length, TWEAK, sizeof(TWEAK),
                             0, SIZE_MAX,
                             radix);
    if (res != 0) {
        free(out);
        return Status::RuntimeError("ff1_ctx_create failed");
    }
    res = ff1_encrypt(ctx, out, fixed_num_str.c_str(), NULL, 0);
    if (res != 0) {
        free(out);
        return Status::RuntimeError("ff1_encrypt failed");
    }
    ff1_ctx_destroy(ctx);

    std::string result(out);
    value = result;
    free(out);

    return Status::OK();
}

Status FPE::encrypt_num(const std::string& num_str,const std::string key, std::string& value) {
    std::string num_flag;
    if (num_str[0] == '-') {
        num_flag = "-";
    }

    size_t dot_pos = num_str.find('.');
    std::string int_part = num_str.substr(0, dot_pos);
    Status status;
    std::string encrypted_int_part;
    status = encrypt(int_part, key, encrypted_int_part, DEFAULT_RADIX);
    if (!status.ok()) {
        return Status::RuntimeError("encrypt_num int_part failed");
    }

    std::string encrypted_dec_part;
    if (dot_pos != std::string::npos) {
        std::string dec_part = num_str.substr(dot_pos + 1);
        dec_part = "0." + dec_part;
        double dec_part_num = std::stod(dec_part);
        dec_part_num = dec_part_num * EXPANDED;
        auto dec_int_part = static_cast<long long>(dec_part_num);
        status = encrypt(std::to_string(dec_int_part), key, encrypted_dec_part, DEFAULT_RADIX);
        if (!status.ok()) {
            return Status::RuntimeError("encrypt_num dec_int_part failed");
        }
        encrypted_dec_part = "." + encrypted_dec_part + FIXED_NUM;
    }

    value = num_flag + FIXED_NUM + encrypted_int_part + encrypted_dec_part;

    return Status::OK();
}

Status FPE::decrypt(const std::string& num_str, const std::string key, std::string& value, int radix= 10) {
    int num_str_length = num_str.length();
    int fpe_key_length = key.length();

    std::vector<uint8_t> fpe_key(fpe_key_length);
    for (size_t i = 0; i < fpe_key_length; ++i) {
        fpe_key[i] = static_cast<uint8_t>(key[i]);
    }

    struct ff1_ctx* ctx;
    char* out = (char *) calloc(num_str_length + 1, sizeof(char));
    if (out == NULL) {
        return Status::MemoryAllocFailed("encrypt calloc memory Failed");
    }

    int res = ff1_ctx_create(&ctx, fpe_key.data(),
                             fpe_key_length, TWEAK, sizeof(TWEAK),
                             0, SIZE_MAX,
                             radix);
    if (res != 0) {
        free(out);
        return Status::RuntimeError("ff1_ctx_create failed");
    }
    res = ff1_decrypt(ctx, out, num_str.c_str(), NULL, 0);
    if (res != 0) {
        free(out);
        return Status::RuntimeError("ff1_encrypt failed");
    }
    ff1_ctx_destroy(ctx);

    std::string result(out);
    value = result;
    free(out);

    return Status::OK();
}

Status FPE::decrypt_num(const std::string& num_str, const std::string key, std::string& value) {
    std::string num_flag;
    std::string decrypt_num_str = num_str;
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
    std::string decrypted_int_part = trim_leading_zeros(encrypted_dec_part);

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

    std::string decrypted_num = num_flag + decrypted_int_part + decrypted_dec_part;
    value = decrypted_num;

    return Status::OK();
}

} // namespace starrocks
