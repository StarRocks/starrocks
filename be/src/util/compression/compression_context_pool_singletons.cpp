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

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "util/compression/compression_context_pool_singletons.h"

#include <cstdlib>
#include <memory>

namespace starrocks::compression {

StatusOr<ZSTDCompressionContext*> ZSTD_CCtx_Creator::operator()() const noexcept {
    auto* context = new (std::nothrow) ZSTDCompressionContext();
    if (context == nullptr) {
        return Status::InvalidArgument("Fail to init ZSTD compress context");
    }
    ZSTD_CCtx* ctx = ZSTD_createCCtx();
    if (ctx == nullptr) {
        delete context;
        return Status::InvalidArgument("Fail to init ZSTD compress context");
    }
    context->ctx = ctx;
    return context;
}

StatusOr<ZSTDDecompressContext*> ZSTD_DCtx_Creator::operator()() const noexcept {
    auto* context = new (std::nothrow) ZSTDDecompressContext();
    if (context == nullptr) {
        return Status::InvalidArgument("Fail to init ZSTD decompress context");
    }
    ZSTD_DCtx* ctx = ZSTD_createDCtx();
    if (ctx == nullptr) {
        delete context;
        return Status::InvalidArgument("Fail to init ZSTD decompress context");
    }
    context->ctx = ctx;
    return context;
}

void ZSTD_CCtx_Deleter::operator()(ZSTDCompressionContext* context) const noexcept {
    DCHECK(!context->compression_fail);
    ZSTD_freeCCtx(context->ctx);
    delete context;
}

void ZSTD_DCtx_Deleter::operator()(ZSTDDecompressContext* context) const noexcept {
    DCHECK(!context->decompression_fail);
    ZSTD_freeDCtx(context->ctx);
    delete context;
}

Status ZSTD_CCtx_Resetter::operator()(ZSTDCompressionContext* context) const noexcept {
    context->compression_count++;
    if (context->compression_fail) {
        context->compression_fail = false;
    }
    [[maybe_unused]] size_t const err = ZSTD_CCtx_reset(context->ctx, ZSTD_reset_session_and_parameters);
    assert(!ZSTD_isError(err)); // This function doesn't actually fail (void)err;
    return Status::OK();
}

Status ZSTD_DCtx_Resetter::operator()(ZSTDDecompressContext* context) const noexcept {
    context->decompression_count++;
    if (context->decompression_fail) {
        context->decompression_fail = false;
    }
    [[maybe_unused]] size_t const err = ZSTD_DCtx_reset(context->ctx, ZSTD_reset_session_and_parameters);
    assert(!ZSTD_isError(err)); // This function doesn't actually fail (void)err;
    return Status::OK();
}

ZSTD_CCtx_Pool& zstd_cctx_pool() {
    static std::string pool_name = "zstd_compress";
    static ZSTD_CCtx_Pool zstd_cctx_pool_singleton(pool_name);
    return zstd_cctx_pool_singleton;
}

ZSTD_DCtx_Pool& zstd_dctx_pool() {
    static std::string pool_name = "zstd_decompress";
    static ZSTD_DCtx_Pool zstd_dctx_pool_singleton(pool_name);
    return zstd_dctx_pool_singleton;
}

StatusOr<ZSTD_CCtx_Pool::Ref> getZSTD_CCtx() {
    return zstd_cctx_pool().get();
}

StatusOr<ZSTD_DCtx_Pool::Ref> getZSTD_DCtx() {
    return zstd_dctx_pool().get();
}

// ==============================================================

StatusOr<LZ4FCompressContext*> LZ4F_CCtx_Creator::operator()() const noexcept {
    auto* context = new (std::nothrow) LZ4FCompressContext();
    if (context == nullptr) {
        return Status::InvalidArgument("Fail to init LZ4FRAME compression context");
    }
    auto res = LZ4F_createCompressionContext(&context->ctx, LZ4F_VERSION);
    if (res != 0) {
        delete context;
        return Status::InvalidArgument(
                strings::Substitute("Fail to init LZ4FRAME compression context, res=$0", LZ4F_getErrorName(res)));
    }
    return context;
}

StatusOr<LZ4FDecompressContext*> LZ4F_DCtx_Creator::operator()() const noexcept {
    auto* context = new (std::nothrow) LZ4FDecompressContext();
    if (context == nullptr) {
        return Status::InvalidArgument("Fail to init LZ4FRAME decompression context");
    }
    auto res = LZ4F_createDecompressionContext(&context->ctx, LZ4F_VERSION);
    if (res != 0) {
        delete context;
        return Status::InvalidArgument(
                strings::Substitute("Fail to init LZ4FRAME decompression context, res=$0", LZ4F_getErrorName(res)));
    }
    return context;
}

void LZ4F_CCtx_Deleter::operator()(LZ4FCompressContext* context) const noexcept {
    DCHECK(!context->compression_fail);
    LZ4F_freeCompressionContext(context->ctx);
    delete context;
}

void LZ4F_DCtx_Deleter::operator()(LZ4FDecompressContext* context) const noexcept {
    DCHECK(!context->decompression_fail);
    LZ4F_freeDecompressionContext(context->ctx);
    delete context;
}

Status LZ4F_CCtx_Resetter::operator()(LZ4FCompressContext* context) const noexcept {
    context->compression_count++;
    if (context->compression_fail == true) {
        LZ4F_freeCompressionContext(context->ctx);
        LZ4F_compressionContext_t new_ctx = nullptr;
        auto res = LZ4F_createCompressionContext(&new_ctx, LZ4F_VERSION);
        if (res != 0) {
            delete context;
            context = nullptr;
            return Status::InvalidArgument(
                    strings::Substitute("Fail to reinit LZ4FRAME compress context, res=$0", LZ4F_getErrorName(res)));
        }
        context->ctx = new_ctx;
        context->compression_fail = false;
        context->compression_count = 0;
    }
    return Status::OK();
}

Status LZ4F_DCtx_Resetter::operator()(LZ4FDecompressContext* context) const noexcept {
    context->decompression_count++;
    LZ4F_resetDecompressionContext(context->ctx);
    if (context->decompression_fail == true) {
        context->decompression_fail = false;
    }
    return Status::OK();
}

LZ4F_CCtx_Pool& lz4f_cctx_pool() {
    static std::string pool_name = "lz4f_compress";
    static LZ4F_CCtx_Pool lz4f_cctx_pool_singleton(pool_name);
    return lz4f_cctx_pool_singleton;
}

LZ4F_DCtx_Pool& lz4f_dctx_pool() {
    static std::string pool_name = "lz4f_decompress";
    static LZ4F_DCtx_Pool lz4f_dctx_pool_singleton(pool_name);
    return lz4f_dctx_pool_singleton;
}

StatusOr<LZ4F_CCtx_Pool::Ref> getLZ4F_CCtx() {
    return lz4f_cctx_pool().get();
}

StatusOr<LZ4F_DCtx_Pool::Ref> getLZ4F_DCtx() {
    return lz4f_dctx_pool().get();
}

// ==============================================================

StatusOr<LZ4CompressContext*> LZ4_CCtx_Creator::operator()() const noexcept {
    auto* context = new (std::nothrow) LZ4CompressContext();
    if (context == nullptr) {
        return Status::InvalidArgument("Fail to init LZ4 compression context");
    }
    context->ctx = LZ4_createStream();
    if (context->ctx == nullptr) {
        delete context;
        return Status::InvalidArgument("Fail to init LZ4 compression context");
    }
    return context;
}

void LZ4_CCtx_Deleter::operator()(LZ4CompressContext* context) const noexcept {
    DCHECK(!context->compression_fail);
    LZ4_freeStream(context->ctx);
    delete context;
}

Status LZ4_CCtx_Resetter::operator()(LZ4CompressContext* context) const noexcept {
    context->compression_count++;
    if (context->compression_fail) {
        context->compression_fail = false;
    }

    LZ4_resetStream(context->ctx);

    return Status::OK();
}

LZ4_CCtx_Pool& lz4_cctx_pool() {
    static std::string pool_name = "lz4_compress";
    static LZ4_CCtx_Pool lz4_cctx_pool_singleton(pool_name);
    return lz4_cctx_pool_singleton;
}

StatusOr<LZ4_CCtx_Pool::Ref> getLZ4_CCtx() {
    return lz4_cctx_pool().get();
}
} // namespace starrocks::compression
