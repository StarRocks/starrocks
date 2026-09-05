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

#include <string_view>

#include "gutil/strings/util.h"

namespace starrocks::lake {

inline bool is_segment(std::string_view file_name) {
    return HasSuffixString(file_name, ".dat");
}

inline bool is_del(std::string_view file_name) {
    return HasSuffixString(file_name, ".del");
}

inline bool is_delvec(std::string_view file_name) {
    return HasSuffixString(file_name, ".delvec");
}

inline bool is_txn_log(std::string_view file_name) {
    return HasSuffixString(file_name, ".log");
}

inline bool is_txn_slog(std::string_view file_name) {
    return HasSuffixString(file_name, ".slog");
}

inline bool is_txn_vlog(std::string_view file_name) {
    return HasSuffixString(file_name, ".vlog");
}

inline bool is_tablet_metadata(std::string_view file_name) {
    return HasSuffixString(file_name, ".meta");
}

inline bool is_tablet_initial_metadata(std::string_view file_name) {
    return HasPrefixString(file_name, "0000000000000000_");
}

inline bool is_tablet_metadata_lock(std::string_view file_name) {
    return HasSuffixString(file_name, ".lock");
}

inline bool is_sst(std::string_view file_name) {
    return HasSuffixString(file_name, ".sst");
}

inline bool is_spcols(std::string_view file_name);

inline bool is_cols(std::string_view file_name) {
    // Guard against the ".spcols" suffix collision: ".spcols" ends with ".cols", so a naive
    // HasSuffixString would classify a sparse SDCG file as dense. is_cols() means "dense .cols only".
    return HasSuffixString(file_name, ".cols") && !is_spcols(file_name);
}

// Sparse Delta Column Group (SDCG) payload file: a standard Segment v2 carrying a leading
// source_rowid column plus update value columns for K updated rows, read as an overlay layer.
// Referenced from DeltaColumnGroupVerPB.column_files like a dense `.cols`, with the same lifecycle.
inline bool is_spcols(std::string_view file_name) {
    return HasSuffixString(file_name, ".spcols");
}

// Index Delta Group payload file produced by ADD INDEX fast-path schema change.
// One .idx per ADD INDEX alter per segment, holding bloom-filter / bitmap /
// ngram-bloom blobs (GIN keeps its own per-column inverted directory and is
// referenced by IndexDeltaGroupEntryPB.index_file by directory name).
inline bool is_idx(std::string_view file_name) {
    return HasSuffixString(file_name, ".idx");
}

// Check if file is a Lake Compaction Rows Mapper file.
// WHY: Need to distinguish between local (.crm) and remote (.lcrm) mapper files
// for correct cleanup behavior. Remote lcrm files must not be deleted immediately
// after use since they may be accessed by multiple nodes during parallel pk execution.
inline bool is_lcrm(std::string_view file_name) {
    return HasSuffixString(file_name, ".lcrm");
}

inline bool is_combined_txn_log(std::string_view file_name) {
    return HasSuffixString(file_name, ".logs");
}

inline bool is_vector_index(std::string_view file_name) {
    return HasSuffixString(file_name, ".vi");
}

} // namespace starrocks::lake
