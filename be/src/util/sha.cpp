// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "util/sha.h"

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
    unsigned char hash[SHA224_DIGEST_LENGTH];
    SHA224_Final(hash, &_sha224_ctx);

    std::stringstream ss;
    for (int i = 0; i < SHA224_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }

    _hex.assign(ss.str());
}

SHA256Digest::SHA256Digest() {
    SHA256_Init(&_sha256_ctx);
}

void SHA256Digest::update(const void* data, size_t length) {
    SHA256_Update(&_sha256_ctx, data, length);
}

void SHA256Digest::digest() {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_Final(hash, &_sha256_ctx);

    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }

    _hex.assign(ss.str());
}

SHA384Digest::SHA384Digest() {
    SHA384_Init(&_sha384_ctx);
}

void SHA384Digest::update(const void* data, size_t length) {
    SHA384_Update(&_sha384_ctx, data, length);
}

void SHA384Digest::digest() {
    unsigned char hash[SHA384_DIGEST_LENGTH];
    SHA384_Final(hash, &_sha384_ctx);

    std::stringstream ss;
    for (int i = 0; i < SHA384_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }

    _hex.assign(ss.str());
}

SHA512Digest::SHA512Digest() {
    SHA512_Init(&_sha512_ctx);
}

void SHA512Digest::update(const void* data, size_t length) {
    SHA512_Update(&_sha512_ctx, data, length);
}

void SHA512Digest::digest() {
    unsigned char hash[SHA512_DIGEST_LENGTH];
    SHA512_Final(hash, &_sha512_ctx);

    std::stringstream ss;
    for (int i = 0; i < SHA512_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }

    _hex.assign(ss.str());
}

} // namespace starrocks
