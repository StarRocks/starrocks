// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <openssl/sha.h>

#include <string>

namespace starrocks {

class SHA224Digest {
public:
    SHA224Digest();

    void update(const void* data, size_t length);
    void digest();

    const std::string& hex() const { return _hex; }

private:
    SHA256_CTX _sha224_ctx;
    std::string _hex;
};

class SHA256Digest {
public:
    SHA256Digest();

    void update(const void* data, size_t length);
    void digest();

    const std::string& hex() const { return _hex; }

private:
    SHA256_CTX _sha256_ctx;
    std::string _hex;
};

class SHA384Digest {
public:
    SHA384Digest();

    void update(const void* data, size_t length);
    void digest();

    const std::string& hex() const { return _hex; }

private:
    SHA512_CTX _sha384_ctx;
    std::string _hex;
};

class SHA512Digest {
public:
    SHA512Digest();

    void update(const void* data, size_t length);
    void digest();

    const std::string& hex() const { return _hex; }

private:
    SHA512_CTX _sha512_ctx;
    std::string _hex;
};
} // namespace starrocks
