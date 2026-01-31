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

#include <openssl/sha.h>

#include <string>

namespace starrocks {

class SHA224Digest {
public:
    SHA224Digest();

    void update(const void* data, size_t length);
    void digest();

    // Get hex-encoded result (56 characters for SHA224)
    std::string hex() const;

    // Get raw binary result (28 bytes for SHA224)
    const unsigned char* binary() const { return _hash; }
    size_t binary_size() const { return SHA224_DIGEST_LENGTH; }

private:
    SHA256_CTX _sha224_ctx;
    unsigned char _hash[SHA224_DIGEST_LENGTH];
};

class SHA256Digest {
public:
    SHA256Digest();

    void update(const void* data, size_t length);
    void digest();

    // Get hex-encoded result (64 characters for SHA256)
    std::string hex() const;

    // Get raw binary result (32 bytes for SHA256)
    const unsigned char* binary() const { return _hash; }
    size_t binary_size() const { return SHA256_DIGEST_LENGTH; }

private:
    SHA256_CTX _sha256_ctx;
    unsigned char _hash[SHA256_DIGEST_LENGTH];
};

class SHA384Digest {
public:
    SHA384Digest();

    void update(const void* data, size_t length);
    void digest();

    // Get hex-encoded result (96 characters for SHA384)
    std::string hex() const;

    // Get raw binary result (48 bytes for SHA384)
    const unsigned char* binary() const { return _hash; }
    size_t binary_size() const { return SHA384_DIGEST_LENGTH; }

private:
    SHA512_CTX _sha384_ctx;
    unsigned char _hash[SHA384_DIGEST_LENGTH];
};

class SHA512Digest {
public:
    SHA512Digest();

    void update(const void* data, size_t length);
    void digest();

    // Get hex-encoded result (128 characters for SHA512)
    std::string hex() const;

    // Get raw binary result (64 bytes for SHA512)
    const unsigned char* binary() const { return _hash; }
    size_t binary_size() const { return SHA512_DIGEST_LENGTH; }

private:
    SHA512_CTX _sha512_ctx;
    unsigned char _hash[SHA512_DIGEST_LENGTH];
};
} // namespace starrocks
