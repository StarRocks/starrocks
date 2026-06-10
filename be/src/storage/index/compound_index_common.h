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

#include <cstdint>
#include <string>
#include <vector>

namespace starrocks {

enum class CompoundIndexKind : uint8_t {
    UNKNOWN = 0,
    INVERTED_TANTIVY = 1,
};

// On-disk format magic + version constants. Kept here so writer and reader
// (and any tooling) share the same source of truth.
inline constexpr uint32_t COMPOUND_BIN_MAGIC = 0x53524342u; // "SRCB"
inline constexpr uint32_t COMPOUND_BIN_VERSION = 1u;

// ---------------------------------------------------------------------------
// Writer-side types
// ---------------------------------------------------------------------------

// One subfile contributed by a per-column index writer; the file is currently
// sitting in a temp directory on local disk and `local_path` is its absolute
// path. The CompoundIndexFileWriter will read it and append its bytes into
// the .idx during pack().
struct CompoundFileRef {
    std::string name;       // logical name inside the compound (e.g. "meta.json")
    std::string local_path; // current absolute path on disk
};

// One full index entry (i.e. one column's inverted index) ready to be packed.
// Returned by `InvertedWriter::finish_compound`.
struct CompoundIndexEntry {
    CompoundIndexKind kind = CompoundIndexKind::UNKNOWN;
    int64_t index_id = 0;
    std::string suffix; // e.g. column unique_id rendered as a string
    std::vector<CompoundFileRef> files;
};

// ---------------------------------------------------------------------------
// Reader-side types
// ---------------------------------------------------------------------------

// One subfile inside a `.idx`, parsed from the header.
struct CompoundFileEntry {
    std::string name;
    uint64_t offset = 0; // absolute position in the .idx file
    uint64_t length = 0;
};

// One index entry parsed from the .idx header. Mirrors CompoundIndexEntry on
// the writer side but the file list carries (offset, length) instead of a
// local path. Returned by `CompoundIndexFileReader::find_index`.
struct CompoundIndexLayout {
    CompoundIndexKind kind = CompoundIndexKind::UNKNOWN;
    int64_t index_id = 0;
    std::string suffix;
    std::vector<CompoundFileEntry> files;
};

} // namespace starrocks
