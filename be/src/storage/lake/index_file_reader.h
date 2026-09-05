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
#include <string>

#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/segment.pb.h"
#include "gen_cpp/types.pb.h"

namespace starrocks {
class RandomAccessFile;
}

namespace starrocks::lake {

// Reader for the standalone .idx (Index Delta Group) payload file produced
// by IndexFileWriter. Parses the trailing IndexFileFooterPB and resolves
// (col_unique_id, index_type) lookups to the embedded ColumnIndexMetaPB
// payload. The per-column index blob (bitmap / bloom filter / ngram) remains
// in the same file and is loaded via the existing index reader paths which
// accept a RandomAccessFile + ColumnIndexMetaPB.
//
// Usage pattern:
//     IndexFileReader reader;
//     RETURN_IF_ERROR(reader.init(random_access_file));
//     const ColumnIndexMetaPB* meta = reader.find(col_uid, IndexType::BITMAP);
//     if (meta != nullptr) {
//         // pass `meta` and `random_access_file` to BitmapIndexReader.
//     }
//
// NOT thread-safe. One reader per open .idx file; multiple concurrent
// queries need separate instances.
class IndexFileReader {
public:
    IndexFileReader() = default;
    ~IndexFileReader() = default;

    IndexFileReader(const IndexFileReader&) = delete;
    IndexFileReader& operator=(const IndexFileReader&) = delete;

    // Parse the trailing footer by:
    //   - reading the last 8 bytes to extract footer_length + magic
    //   - reading `footer_length` bytes immediately before and parsing the
    //     IndexFileFooterPB from them
    // Reports a clear error on mismatched magic / truncated file / parse
    // failure so higher layers can fall back to segment-footer-embedded
    // indexes without silently serving stale data.
    Status init(RandomAccessFile* file);

    // Returns the ColumnIndexMetaPB for the given (col_uid, index_type) or
    // nullptr when the .idx file does not carry that pair. Lookup is linear
    // across `footer.column_indexes_size()`; we keep the footer small so a
    // linear scan stays under a few dozen entries in practice.
    const ColumnIndexMetaPB* find(int32_t col_unique_id, IndexType index_type) const;

    const IndexFileFooterPB& footer() const { return _footer; }

    int num_entries() const { return _footer.column_indexes_size(); }

private:
    IndexFileFooterPB _footer;
    bool _inited = false;
};

} // namespace starrocks::lake
