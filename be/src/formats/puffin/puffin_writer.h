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
#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"

namespace starrocks {
class WritableFile;
}

namespace starrocks::formats {

// Metadata for one blob written into a Puffin file. offset/length locate the blob
// within the file (offset measured from file start) and become the Iceberg
// DeleteFile content_offset / content_size_in_bytes.
struct PuffinBlobMetadata {
    std::string type;
    std::vector<int32_t> input_fields;
    int64_t offset = 0;
    int64_t length = 0;
    std::map<std::string, std::string> properties;
};

// Blob-type-agnostic Puffin container writer.
// Layout: "PFA1" | blob_0 | blob_1 | ... | footer.
// footer: "PFA1" | FileMetadata JSON | payloadSize(4B LE) | flags(4B, 0) | "PFA1".
// The writer does not own the WritableFile; the caller closes it after finish().
//
// Scope note: this container is "generic" over blob *bytes* (add_blob takes opaque
// data + type/fields/properties), but currently writes blobs UNCOMPRESSED and fixes
// snapshot-id / sequence-number to -1 in the footer (the Iceberg DV convention) and
// omits compression-codec. Future blob types (e.g. NDV theta-sketches) that need
// those exposed should extend add_blob / the footer serialization then.
//
// Lifecycle: init() exactly once, then add_blob() zero or more times, then finish()
// exactly once. Misuse (calling out of order / twice) returns Status::InternalError
// rather than corrupting the file.
class PuffinWriter {
public:
    explicit PuffinWriter(WritableFile* file) : _file(file) {}

    // Writes the 4-byte header magic. Must be called once, before any add_blob.
    Status init();

    // Appends one opaque blob and records its metadata for the footer.
    // Requires init() called and finish() not yet called.
    StatusOr<PuffinBlobMetadata> add_blob(const std::string& type, const std::vector<int32_t>& input_fields,
                                          const std::map<std::string, std::string>& properties, const uint8_t* data,
                                          size_t size);

    // Writes the footer. Must be called once, after the last add_blob.
    Status finish();

private:
    WritableFile* _file;
    int64_t _offset = 0; // offset of the next blob (== bytes written so far)
    std::vector<PuffinBlobMetadata> _blobs;
    bool _initialized = false;
    bool _finished = false;
};

} // namespace starrocks::formats
