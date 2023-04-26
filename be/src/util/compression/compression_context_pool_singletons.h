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

#pragma once

#include "common/status.h"
#include "common/statusor.h"
#include "gutil/endian.h"
#include "gutil/strings/substitute.h"
#include "util/compression/compression_context.h"
#include "util/compression/compression_context_pool.h"
#include "util/faststring.h"

namespace starrocks::compression {

/**
 * There are totally four compression context pool singleton:
 *        ZSTD_CCtx_Pool zstd_cctx_pool_singleton
 *        ZSTD_DCtx_Pool zstd_dctx_pool_singleton
 *        LZ4F_CCtx_Pool lz4f_cctx_pool_singleton
 *        LZ4F_DCtx_Pool lz4f_dctx_pool_singleton
 *        LZ4_CCtx_Pool lz4_cctx_pool_singleton
 * 
 * Before compression, we will first apply for a compression context from the corresponding pool 
 * through getZSTD_CCtx(for ZSTD compression context).
 * After compression, we will return the compression context to the correspoding pool.
 * 
 * One thread at most has one compression context, so the maximum number of compression contexts 
 * in a pool cannot exceed the maximum number of concurrent threads in extreme cases.
 */

struct ZSTD_CCtx_Creator {
    StatusOr<ZSTDCompressionContext*> operator()() const noexcept;
};

struct ZSTD_DCtx_Creator {
    StatusOr<ZSTDDecompressContext*> operator()() const noexcept;
};

struct ZSTD_CCtx_Deleter {
    void operator()(ZSTDCompressionContext* ctx) const noexcept;
};

struct ZSTD_DCtx_Deleter {
    void operator()(ZSTDDecompressContext* ctx) const noexcept;
};

struct ZSTD_CCtx_Resetter {
    Status operator()(ZSTDCompressionContext* ctx) const noexcept;
};

struct ZSTD_DCtx_Resetter {
    Status operator()(ZSTDDecompressContext* ctx) const noexcept;
};

using ZSTD_CCtx_Pool =
        CompressionContextPool<ZSTDCompressionContext, ZSTD_CCtx_Creator, ZSTD_CCtx_Deleter, ZSTD_CCtx_Resetter>;

using ZSTD_DCtx_Pool =
        CompressionContextPool<ZSTDDecompressContext, ZSTD_DCtx_Creator, ZSTD_DCtx_Deleter, ZSTD_DCtx_Resetter>;

StatusOr<ZSTD_CCtx_Pool::Ref> getZSTD_CCtx();

StatusOr<ZSTD_DCtx_Pool::Ref> getZSTD_DCtx();

// ==============================================================

struct LZ4F_CCtx_Creator {
    StatusOr<LZ4FCompressContext*> operator()() const noexcept;
};

struct LZ4F_DCtx_Creator {
    StatusOr<LZ4FDecompressContext*> operator()() const noexcept;
};

struct LZ4F_CCtx_Deleter {
    void operator()(LZ4FCompressContext* ctx) const noexcept;
};

struct LZ4F_DCtx_Deleter {
    void operator()(LZ4FDecompressContext* ctx) const noexcept;
};

struct LZ4F_CCtx_Resetter {
    Status operator()(LZ4FCompressContext* ctx) const noexcept;
};

struct LZ4F_DCtx_Resetter {
    Status operator()(LZ4FDecompressContext* ctx) const noexcept;
};

using LZ4F_CCtx_Pool =
        CompressionContextPool<LZ4FCompressContext, LZ4F_CCtx_Creator, LZ4F_CCtx_Deleter, LZ4F_CCtx_Resetter>;

using LZ4F_DCtx_Pool =
        CompressionContextPool<LZ4FDecompressContext, LZ4F_DCtx_Creator, LZ4F_DCtx_Deleter, LZ4F_DCtx_Resetter>;

StatusOr<LZ4F_CCtx_Pool::Ref> getLZ4F_CCtx();

StatusOr<LZ4F_DCtx_Pool::Ref> getLZ4F_DCtx();

// ==============================================================

struct LZ4_CCtx_Creator {
    StatusOr<LZ4CompressContext*> operator()() const noexcept;
};

struct LZ4_CCtx_Deleter {
    void operator()(LZ4CompressContext* ctx) const noexcept;
};

struct LZ4_CCtx_Resetter {
    Status operator()(LZ4CompressContext* ctx) const noexcept;
};

using LZ4_CCtx_Pool = CompressionContextPool<LZ4CompressContext, LZ4_CCtx_Creator, LZ4_CCtx_Deleter, LZ4_CCtx_Resetter>;

StatusOr<LZ4_CCtx_Pool::Ref> getLZ4_CCtx();

} // namespace starrocks::compression
