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

// The advanced dictionary-construction API (ZSTD_createCDict_advanced,
// ZSTD_createDDict_advanced, ZSTD_dct_rawContent, ZSTD_dlm_byCopy,
// ZSTD_defaultCMem, ZSTD_getCParams) lives in the experimental section of
// <zstd.h>, guarded by ZSTD_STATIC_LINKING_ONLY. It MUST be defined before
// zstd.h is first pulled into this translation unit, so keep it above every
// include. Header layout follows compression_headers.h (flat on macOS/Homebrew,
// subdirectories for the Linux thirdparty install).
#define ZSTD_STATIC_LINKING_ONLY

#ifdef STARROCKS_MACOS_USE_FLAT_INCLUDES
#include <zstd.h>
#else
#include <zstd/zstd.h>
#endif

#include <atomic>
#include <cstring>

#include "util/compression/zstd_dict.h"

namespace starrocks::compression {

StatusOr<std::unique_ptr<ZstdCDict>> ZstdCDict::create(const Slice& dict_bytes, int level) {
    if (dict_bytes.size == 0) {
        return Status::InvalidArgument("cannot build shared ZSTD dict from empty bytes");
    }
    // level == -1 means "unset"; a CDict must bake a concrete level because the
    // per-page compress path skips ZSTD_c_compressionLevel once a CDict is
    // referenced (see block_compression.cpp _compress).
    const int effective_level = (level == -1) ? ZSTD_CLEVEL_DEFAULT : level;
    ZSTD_compressionParameters cparams = ZSTD_getCParams(effective_level, /*estimatedSrcSize=*/0, dict_bytes.size);
    // A raw sample must never be parsed as a structured dictionary (it is
    // user-controlled data and may begin with ZSTD_MAGIC_DICTIONARY).
    const ZSTD_dictContentType_e content_type = ZSTD_dct_rawContent;
    ZSTD_CDict* d = ZSTD_createCDict_advanced(dict_bytes.data, dict_bytes.size, ZSTD_dlm_byCopy, content_type, cparams,
                                              ZSTD_defaultCMem);
    if (d == nullptr) {
        return Status::InternalError("ZSTD_createCDict_advanced returned null");
    }
    return std::unique_ptr<ZstdCDict>(new ZstdCDict(d));
}

ZstdCDict::~ZstdCDict() {
    if (_dict != nullptr) {
        ZSTD_freeCDict(_dict);
        _dict = nullptr;
    }
}

StatusOr<std::unique_ptr<ZstdDDict>> ZstdDDict::create(const Slice& dict_bytes) {
    if (dict_bytes.size == 0) {
        return Status::InvalidArgument("cannot build shared ZSTD ddict from empty bytes");
    }
    // Raw content, always: the page holds a verbatim sample of the column's own data, and
    // ZSTD_dct_rawContent keeps a sample that happens to begin with ZSTD_MAGIC_DICTIONARY
    // from being misparsed as a structured dictionary.
    ZSTD_DDict* d = ZSTD_createDDict_advanced(dict_bytes.data, dict_bytes.size, ZSTD_dlm_byCopy, ZSTD_dct_rawContent,
                                              ZSTD_defaultCMem);
    if (d == nullptr) {
        return Status::InternalError("ZSTD_createDDict_advanced returned null");
    }
    return std::unique_ptr<ZstdDDict>(new ZstdDDict(d));
}

namespace {
std::atomic<uint64_t> g_ddict_id_seq{1};
} // namespace

ZstdDDict::ZstdDDict(ZSTD_DDict* d) : _dict(d), _id(g_ddict_id_seq.fetch_add(1, std::memory_order_relaxed)) {}

size_t ZstdDDict::mem_usage() const {
    return _dict != nullptr ? ZSTD_sizeof_DDict(_dict) : 0;
}

ZstdDDict::~ZstdDDict() {
    if (_dict != nullptr) {
        ZSTD_freeDDict(_dict);
        _dict = nullptr;
    }
}

} // namespace starrocks::compression
