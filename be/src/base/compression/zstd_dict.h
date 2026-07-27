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

#include <memory>

#include "base/statusor.h"
#include "base/string/slice.h"

// Forward-declare the opaque zstd dictionary handles so this header never leaks
// <zstd.h> into its includers (page_io.h / column_reader.h etc.). The real
// definitions come from <zstd.h> inside zstd_dict.cpp; these typedefs are
// byte-identical to the ones in <zstd.h>, and redeclaring an identical typedef
// is legal in C++.
typedef struct ZSTD_CDict_s ZSTD_CDict;
typedef struct ZSTD_DDict_s ZSTD_DDict;

namespace starrocks::compression {

// RAII wrapper around a ZSTD compression dictionary (E4 column-level shared
// dictionary). Built from a raw-content sample with a baked-in compression
// level. Held by the writer and passed per-page into the codec via
// ZSTD_CCtx_refCDict; never enters the shared context pool.
class ZstdCDict {
public:
    // Build a raw-content (ZSTD_dct_rawContent) CDict from `sample`, baking
    // `level` (level == -1 falls back to ZSTD_CLEVEL_DEFAULT). Returns an error
    // Status on failure; the caller degrades to "no shared dict for this
    // column" and must never fail the segment flush.
    static StatusOr<std::unique_ptr<ZstdCDict>> create(const Slice& sample, int level);

    ~ZstdCDict();
    ZstdCDict(const ZstdCDict&) = delete;
    ZstdCDict& operator=(const ZstdCDict&) = delete;

    ZSTD_CDict* dict() const { return _dict; }

private:
    explicit ZstdCDict(ZSTD_CDict* d) : _dict(d) {}
    ZSTD_CDict* _dict = nullptr;
};

// RAII wrapper around a ZSTD decompression dictionary. Built once per
// (segment, column) on the read path and cached on the ColumnReader; passed
// per-page into the codec via ZSTD_DCtx_refDDict.
class ZstdDDict {
public:
    // Build a raw-content DDict from `sample` (the bytes of the shared-dict
    // page). ZSTD copies the sample internally, so the page handle may be
    // released afterward.
    static StatusOr<std::unique_ptr<ZstdDDict>> create(const Slice& sample);

    ~ZstdDDict();
    ZstdDDict(const ZstdDDict&) = delete;
    ZstdDDict& operator=(const ZstdDDict&) = delete;

    ZSTD_DDict* dict() const { return _dict; }

private:
    explicit ZstdDDict(ZSTD_DDict* d) : _dict(d) {}
    ZSTD_DDict* _dict = nullptr;
};

} // namespace starrocks::compression
