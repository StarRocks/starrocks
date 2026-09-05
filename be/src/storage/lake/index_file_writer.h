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
#include <vector>

#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "gen_cpp/types.pb.h"

namespace starrocks {
class WritableFile;
}

namespace starrocks::lake {

// Helper for writing a single standalone Index Delta Group payload file
// (*.idx) containing one or more column indexes.
//
// File layout:
//     [ index blob 0 ]
//     [ index blob 1 ]
//     ...
//     [ IndexFileFooterPB (serialized protobuf) ]
//     [ footer_length: uint32 little-endian ]
//     [ MAGIC: uint32 ]
//
// Each blob is written by the underlying index writer (e.g.
// BitmapIndexWriter::finish(file, meta)). This helper owns the footer
// assembly: it collects per-column (col_uid, index_type, meta) tuples and
// serializes them into IndexFileFooterPB once all blobs are flushed.
//
// Usage:
//     IndexFileWriter w(std::move(writable_file));
//     // For each (col, index_type) build:
//     //   open an IndexWriter; add values; call its finish(file, &meta);
//     //   then append the resulting meta:
//     w.append_column_index(col_uid, index_type, meta);
//     // Finally:
//     RETURN_IF_ERROR(w.finalize());
//     uint64_t size = w.file_size();
//
// Concurrency: NOT thread-safe. One IndexFileWriter per .idx file.
class IndexFileWriter {
public:
    // 4-byte magic written at the end of the file. Chosen to avoid collision
    // with segment file magic. ASCII "IDX1".
    static constexpr uint32_t kMagic = 0x31584449;

    explicit IndexFileWriter(std::unique_ptr<WritableFile> file);
    ~IndexFileWriter();

    IndexFileWriter(const IndexFileWriter&) = delete;
    IndexFileWriter& operator=(const IndexFileWriter&) = delete;

    // Expose the underlying WritableFile so callers can pass it to index
    // writers' finish(WritableFile*, ColumnIndexMetaPB*). The returned
    // pointer is valid until finalize() is called.
    WritableFile* writable_file() { return _file.get(); }

    // Append the metadata for a column index that was just written via the
    // underlying WritableFile. Does not write to the file; only accumulates
    // footer state.
    void append_column_index(int32_t col_unique_id, IndexType index_type, const ColumnIndexMetaPB& meta);

    // Serialize the footer and write the trailing length + magic. After this
    // returns OK, file_size() reflects the final on-disk size.
    Status finalize();

    // Final byte size of the file once finalize() has succeeded; 0 before.
    uint64_t file_size() const { return _file_size; }

    // Number of column-index entries the footer will record.
    int num_entries() const { return _footer.column_indexes_size(); }

private:
    std::unique_ptr<WritableFile> _file;
    IndexFileFooterPB _footer;
    uint64_t _file_size = 0;
    bool _finalized = false;
};

} // namespace starrocks::lake
