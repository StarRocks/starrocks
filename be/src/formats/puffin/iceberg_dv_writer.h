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

#include <roaring/roaring64.h>

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "common/statusor.h"

namespace starrocks {
class WritableFile;
}

namespace starrocks::formats {

// Commit metadata for one data file's deletion vector, produced by IcebergDvWriter::finish.
// The Puffin file path, partition, file_content and format are attached later by the sink /
// FE commit — this struct only carries what the writer knows about the blob it wrote.
struct IcebergDvCommitEntry {
    std::string referenced_data_file;
    int64_t content_offset = 0;
    int64_t content_size_in_bytes = 0;
    int64_t record_count = 0; // roaring64 cardinality (unique deleted positions)
};

// Accumulates deleted row positions per referenced data file, then writes them all as
// deletion-vector-v1 blobs into a single Puffin file (one blob per data file). Close-time
// finalize: bitmaps are held until finish(). Not thread-safe; use one instance per sink driver.
//
// An empty writer (no positions accumulated) writes nothing and returns no entries; callers
// should also check empty() before creating the output file to avoid orphan zero-blob Puffins.
class IcebergDvWriter {
public:
    IcebergDvWriter() = default;
    ~IcebergDvWriter();
    IcebergDvWriter(const IcebergDvWriter&) = delete;
    IcebergDvWriter& operator=(const IcebergDvWriter&) = delete;

    // Record one deleted row position for a data file (idempotent per position).
    void add(std::string_view referenced_data_file, uint64_t position);

    // Set-union an existing deletion bitmap into the accumulator for a data file (creates the
    // entry if absent). Lets a previously-written DV be merged in without expanding it to rows.
    // `other` is not modified or freed.
    void merge_bitmap(std::string_view referenced_data_file, const roaring64_bitmap_t* other);

    bool empty() const { return _bitmaps.empty(); }
    size_t num_data_files() const { return _bitmaps.size(); }

    // Write all accumulated bitmaps as DV blobs into `file` (one Puffin container), in ascending
    // referenced-data-file order. Returns one entry per data file. Single-shot: a second call
    // returns an error. An empty writer writes nothing and returns an empty vector. `file` must
    // be non-null when the writer is non-empty. The caller owns `file` and closes it afterwards.
    StatusOr<std::vector<IcebergDvCommitEntry>> finish(WritableFile* file);

private:
    // referenced_data_file -> owned roaring64 bitmap. std::less<> enables string_view lookup
    // without allocating a std::string on every add().
    std::map<std::string, roaring64_bitmap_t*, std::less<>> _bitmaps;
    bool _finished = false;
};

} // namespace starrocks::formats
