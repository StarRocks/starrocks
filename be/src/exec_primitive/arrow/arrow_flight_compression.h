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

#include <arrow/util/compression.h>

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

namespace starrocks {

// Maps a user-facing codec name to an Arrow IPC compression type.
// "lz4" -> LZ4_FRAME (frame format, enum 6; NOT raw LZ4 block format, enum 5).
// "zstd" -> ZSTD. "none"/""/unrecognized -> UNCOMPRESSED. Case-insensitive.
inline arrow::Compression::type arrow_flight_compression_from_string(std::string_view name) {
    std::string lower(name);
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
    if (lower == "lz4") {
        return arrow::Compression::LZ4_FRAME;
    }
    if (lower == "zstd") {
        return arrow::Compression::ZSTD;
    }
    return arrow::Compression::UNCOMPRESSED;
}

// Resolves the effective codec: a non-empty per-connection session value wins
// (including an explicit "none"); an empty session value inherits the cluster default.
inline arrow::Compression::type resolve_arrow_flight_compression(std::string_view session_value,
                                                                 std::string_view config_default) {
    const std::string_view effective = session_value.empty() ? config_default : session_value;
    return arrow_flight_compression_from_string(effective);
}

} // namespace starrocks
