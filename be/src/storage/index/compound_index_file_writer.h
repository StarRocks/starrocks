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

#include <vector>

#include "common/status.h"
#include "storage/index/compound_index_common.h"

namespace starrocks {
class WritableFile;

// Pack one or more index entries into a single compound `.idx` file.
//
// On-disk layout (all multi-byte integers are big-endian):
//
//   Header (at offset 0):
//     u32  magic           = "SRCB" (COMPOUND_BIN_MAGIC)
//     u32  version         = 1     (COMPOUND_BIN_VERSION)
//     u32  num_indices
//     [per-index entry × num_indices]:
//       u8   kind           (CompoundIndexKind)
//       u8[3] reserved      = 0
//       u64  index_id
//       u32  suffix_len, bytes  (utf-8)
//       u32  num_files
//       [per-file entry × num_files]:
//         u32  name_len, bytes  (utf-8)
//         u64  offset           (absolute position in .idx)
//         u64  length
//
//   Data:
//     concatenated subfile bytes, contiguous, no padding.
//
// The writer first computes the total header size from the entries' metadata
// (so all subfile offsets can be assigned upfront), writes the header, then
// streams each subfile's bytes into the output file. No temp dir cleanup is
// performed here — callers that produced the temp files own that step.
class CompoundIndexFileWriter {
public:
    // Pack `entries` into `out`. `out` must be a freshly-opened WritableFile;
    // on success the caller is responsible for `out->close()`. On failure the
    // partially-written file should be deleted by the caller.
    static Status pack(const std::vector<CompoundIndexEntry>& entries, WritableFile* out);
};

} // namespace starrocks
