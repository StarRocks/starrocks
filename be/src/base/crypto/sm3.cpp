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

#include "base/crypto/sm3.h"

#include <glog/logging.h>
#include <openssl/evp.h>

namespace starrocks {
namespace {

struct Sm3MdHolder {
    EVP_MD* md = nullptr;

    Sm3MdHolder() : md(EVP_MD_fetch(nullptr, "SM3", nullptr)) {}
    ~Sm3MdHolder() {
        if (md != nullptr) {
            EVP_MD_free(md);
        }
    }

    Sm3MdHolder(const Sm3MdHolder&) = delete;
    Sm3MdHolder& operator=(const Sm3MdHolder&) = delete;
};

struct Sm3CtxHolder {
    EVP_MD_CTX* ctx = nullptr;

    Sm3CtxHolder() : ctx(EVP_MD_CTX_new()) {}
    ~Sm3CtxHolder() {
        if (ctx != nullptr) {
            EVP_MD_CTX_free(ctx);
        }
    }

    Sm3CtxHolder(const Sm3CtxHolder&) = delete;
    Sm3CtxHolder& operator=(const Sm3CtxHolder&) = delete;
};

const EVP_MD* get_sm3_md() {
    static Sm3MdHolder holder;
    return holder.md != nullptr ? holder.md : EVP_sm3();
}

EVP_MD_CTX* get_sm3_ctx() {
    thread_local Sm3CtxHolder holder;
    return holder.ctx;
}

} // namespace

unsigned char* Sm3::sm3_compute(const unsigned char* message, unsigned long message_len,
                                unsigned char digest[SM3_HASH_SIZE]) {
    const EVP_MD* md = get_sm3_md();
    EVP_MD_CTX* ctx = get_sm3_ctx();
    CHECK(md != nullptr && ctx != nullptr);

    unsigned int digest_len = 0;
    CHECK_EQ(EVP_DigestInit_ex(ctx, md, nullptr), 1);
    if (message_len > 0) {
        CHECK_EQ(EVP_DigestUpdate(ctx, message, message_len), 1);
    }
    CHECK_EQ(EVP_DigestFinal_ex(ctx, digest, &digest_len), 1);
    DCHECK_EQ(digest_len, static_cast<unsigned int>(SM3_HASH_SIZE));
    return digest;
}

} // namespace starrocks
