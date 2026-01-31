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

#include "base/crypto/sha.h"

#include <iomanip>
#include <iostream>
#include <string>

namespace starrocks {

SHA224Digest::SHA224Digest() {
    SHA224_Init(&_sha224_ctx);
}

void SHA224Digest::update(const void* data, size_t length) {
    SHA224_Update(&_sha224_ctx, data, length);
}

void SHA224Digest::digest() {
    SHA224_Final(_hash, &_sha224_ctx);
}

std::string SHA224Digest::hex() const {
    std::stringstream ss;
    for (int i = 0; i < SHA224_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)_hash[i];
    }
    return ss.str();
}

SHA256Digest::SHA256Digest() {
    SHA256_Init(&_sha256_ctx);
}

void SHA256Digest::update(const void* data, size_t length) {
    SHA256_Update(&_sha256_ctx, data, length);
}

void SHA256Digest::digest() {
    SHA256_Final(_hash, &_sha256_ctx);
}

std::string SHA256Digest::hex() const {
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)_hash[i];
    }
    return ss.str();
}

SHA384Digest::SHA384Digest() {
    SHA384_Init(&_sha384_ctx);
}

void SHA384Digest::update(const void* data, size_t length) {
    SHA384_Update(&_sha384_ctx, data, length);
}

void SHA384Digest::digest() {
    SHA384_Final(_hash, &_sha384_ctx);
}

std::string SHA384Digest::hex() const {
    std::stringstream ss;
    for (int i = 0; i < SHA384_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)_hash[i];
    }
    return ss.str();
}

SHA512Digest::SHA512Digest() {
    SHA512_Init(&_sha512_ctx);
}

void SHA512Digest::update(const void* data, size_t length) {
    SHA512_Update(&_sha512_ctx, data, length);
}

void SHA512Digest::digest() {
    SHA512_Final(_hash, &_sha512_ctx);
}

std::string SHA512Digest::hex() const {
    std::stringstream ss;
    for (int i = 0; i < SHA512_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)_hash[i];
    }
    return ss.str();
}

} // namespace starrocks
