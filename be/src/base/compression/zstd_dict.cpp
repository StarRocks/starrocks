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
// <zstd.h>, guarded by ZSTD_STATIC_LINKING_ONLY. It MUST be defined before the
// <zstd.h> include is first pulled into this translation unit. Keep this define
// and the direct <zstd.h> include above every other include.
#define ZSTD_STATIC_LINKING_ONLY
#include "base/compression/zstd_dict.h"

#include <zstd.h>

namespace starrocks::compression {

StatusOr<std::unique_ptr<ZstdCDict>> ZstdCDict::create(const Slice& sample, int level) {
    if (sample.size == 0) {
        return Status::InvalidArgument("cannot build shared ZSTD dict from an empty sample");
    }
    // level == -1 means "unset"; a CDict must bake a concrete level because the
    // per-page compress path skips ZSTD_c_compressionLevel once a CDict is
    // referenced (see block_compression.cpp _compress).
    const int effective_level = (level == -1) ? ZSTD_CLEVEL_DEFAULT : level;
    // Build cParams from the level. estimatedSrcSize == 0 lets zstd pick window
    // parameters sized to the (small) dictionary rather than a huge source.
    ZSTD_compressionParameters cparams = ZSTD_getCParams(effective_level, /*estimatedSrcSize=*/0, sample.size);
    // ZSTD_dct_rawContent: never parse the sample as a structured dictionary,
    // even if its first bytes happen to equal ZSTD_MAGIC_DICTIONARY (the sample
    // is user-controlled raw data and could be adversarially constructed).
    ZSTD_CDict* d = ZSTD_createCDict_advanced(sample.data, sample.size, ZSTD_dlm_byCopy, ZSTD_dct_rawContent, cparams,
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

StatusOr<std::unique_ptr<ZstdDDict>> ZstdDDict::create(const Slice& sample) {
    if (sample.size == 0) {
        return Status::InvalidArgument("cannot build shared ZSTD ddict from an empty sample");
    }
    ZSTD_DDict* d =
            ZSTD_createDDict_advanced(sample.data, sample.size, ZSTD_dlm_byCopy, ZSTD_dct_rawContent, ZSTD_defaultCMem);
    if (d == nullptr) {
        return Status::InternalError("ZSTD_createDDict_advanced returned null");
    }
    return std::unique_ptr<ZstdDDict>(new ZstdDDict(d));
}

ZstdDDict::~ZstdDDict() {
    if (_dict != nullptr) {
        ZSTD_freeDDict(_dict);
        _dict = nullptr;
    }
}

} // namespace starrocks::compression
