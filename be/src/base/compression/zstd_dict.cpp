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
// ZDICT_trainFromBuffer_fastCover + ZDICT_fastCover_params_t are experimental too.
#define ZDICT_STATIC_LINKING_ONLY

#ifdef STARROCKS_MACOS_USE_FLAT_INCLUDES
#include <zdict.h>
#include <zstd.h>
#else
#include <zstd/zdict.h>
#include <zstd/zstd.h>
#endif

#include <atomic>
#include <cstring>

#include "base/compression/zstd_dict.h"

namespace starrocks::compression {

// ZDICT_DICTSIZE_MIN lives in zdict.h's static-linking-only section; keep our own
// floor so this file does not depend on that block.
static constexpr size_t kMinTrainedDictSize = 4096;
// The requested size is allocated up front, once per column per segment, so an
// absurd value would be an allocation failure on a flush or compaction thread
// rather than the documented "give up and write without a dict". Cap it well
// above any size that pays for itself (measurements plateaued below 128KB).
static constexpr size_t kMaxTrainedDictSize = 16 * 1024 * 1024;

StatusOr<std::unique_ptr<ZstdCDict>> ZstdCDict::create(const Slice& dict_bytes, int level, bool trained) {
    if (dict_bytes.size == 0) {
        return Status::InvalidArgument("cannot build a ZSTD compression dictionary from empty bytes");
    }
    // level == -1 means "unset"; a CDict must bake a concrete level because the
    // per-page compress path skips ZSTD_c_compressionLevel once a CDict is
    // referenced (see block_compression.cpp _compress).
    const int effective_level = (level == -1) ? ZSTD_CLEVEL_DEFAULT : level;
    ZSTD_compressionParameters cparams = ZSTD_getCParams(effective_level, /*estimatedSrcSize=*/0, dict_bytes.size);
    // A raw sample must never be parsed as a structured dictionary (it is
    // user-controlled data and may begin with ZSTD_MAGIC_DICTIONARY). A trained
    // dictionary, in contrast, MUST be parsed so its entropy tables are used.
    const ZSTD_dictContentType_e content_type = trained ? ZSTD_dct_auto : ZSTD_dct_rawContent;
    ZSTD_CDict* d = ZSTD_createCDict_advanced(dict_bytes.data, dict_bytes.size, ZSTD_dlm_byCopy, content_type, cparams,
                                              ZSTD_defaultCMem);
    if (d == nullptr) {
        return Status::InternalError("ZSTD_createCDict_advanced returned null");
    }
    return std::unique_ptr<ZstdCDict>(new ZstdCDict(d));
}

StatusOr<std::string> ZstdCDict::train(const Slice& sample_buf, const std::vector<size_t>& sample_sizes,
                                       size_t max_dict_size) {
    if (sample_buf.size == 0 || sample_sizes.empty()) {
        return Status::InvalidArgument("no samples to train a ZSTD compression dictionary from");
    }
    // ZDICT needs a meaningful amount of material; below this it reliably fails
    // (and a tiny dictionary would not pay for itself anyway).
    if (max_dict_size < kMinTrainedDictSize) {
        return Status::InvalidArgument("compression dict max size too small to train");
    }
    if (max_dict_size > kMaxTrainedDictSize) {
        return Status::InvalidArgument("compression dict max size too large to train");
    }
    std::string dict;
    dict.resize(max_dict_size);

    // Use fastCover with EXPLICIT k/d instead of the stable
    // ZDICT_trainFromBuffer, which redirects to the fastCover *optimizer*
    // (d=8, steps=4). Measured on six synthetic datasets: the optimizer is
    // 3-4x slower and can silently settle on a degenerate dictionary (20KB
    // instead of the requested 112KB, ~11% worse compression); pinning k/d was
    // never worse and much faster.
    ZDICT_fastCover_params_t params;
    memset(&params, 0, sizeof(params));
    params.k = 200; // segment size
    params.d = 8;   // dmer size
    // f, accel, shrinkDict and zParams stay 0 == zstd defaults; steps/nbThreads/
    // splitPoint only matter for the optimizer we are deliberately bypassing.
    size_t written = ZDICT_trainFromBuffer_fastCover(dict.data(), dict.size(), sample_buf.data, sample_sizes.data(),
                                                     static_cast<unsigned>(sample_sizes.size()), params);
    if (ZDICT_isError(written)) {
        // Fall back to the stable entry point before giving up.
        written = ZDICT_trainFromBuffer(dict.data(), dict.size(), sample_buf.data, sample_sizes.data(),
                                        static_cast<unsigned>(sample_sizes.size()));
    }
    if (ZDICT_isError(written)) {
        // Common and benign: "src size is incorrect" / "Dictionary training
        // failed" when the samples are too few or too homogeneous. The caller
        // degrades to no compression dict.
        return Status::InternalError(std::string("ZDICT training failed: ") + ZDICT_getErrorName(written));
    }
    dict.resize(written);
    return dict;
}

ZstdCDict::~ZstdCDict() {
    if (_dict != nullptr) {
        ZSTD_freeCDict(_dict);
        _dict = nullptr;
    }
}

StatusOr<std::unique_ptr<ZstdDDict>> ZstdDDict::create(const Slice& dict_bytes, bool trained) {
    if (dict_bytes.size == 0) {
        return Status::InvalidArgument("cannot build a ZSTD decompression dictionary from empty bytes");
    }
    const ZSTD_dictContentType_e content_type = trained ? ZSTD_dct_auto : ZSTD_dct_rawContent;
    ZSTD_DDict* d = ZSTD_createDDict_advanced(dict_bytes.data, dict_bytes.size, ZSTD_dlm_byCopy, content_type,
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
