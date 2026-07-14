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

#include "formats/puffin/iceberg_dv_writer.h"

#include <string>

#include "formats/puffin/deletion_vector_blob.h"
#include "formats/puffin/puffin_writer.h"

namespace starrocks::formats {

IcebergDvWriter::~IcebergDvWriter() {
    for (auto& [ref_file, bitmap] : _bitmaps) {
        roaring::api::roaring64_bitmap_free(bitmap);
    }
}

void IcebergDvWriter::add(std::string_view referenced_data_file, uint64_t position) {
    auto it = _bitmaps.find(referenced_data_file);
    if (it == _bitmaps.end()) {
        it = _bitmaps.emplace(std::string(referenced_data_file), roaring64_bitmap_create()).first;
    }
    roaring64_bitmap_add(it->second, position);
}

void IcebergDvWriter::merge_bitmap(std::string_view referenced_data_file, const roaring64_bitmap_t* other) {
    if (other == nullptr) return;
    auto it = _bitmaps.find(referenced_data_file);
    if (it == _bitmaps.end()) {
        it = _bitmaps.emplace(std::string(referenced_data_file), roaring64_bitmap_create()).first;
    }
    roaring64_bitmap_or_inplace(it->second, other);
}

StatusOr<std::vector<IcebergDvCommitEntry>> IcebergDvWriter::finish(WritableFile* file) {
    if (_finished) {
        return Status::InternalError("IcebergDvWriter::finish called more than once");
    }

    // Nothing deleted: complete as a no-op, writing no bytes and returning no entries. Callers
    // should prefer checking empty() before even creating the output file (avoids orphan Puffins).
    if (_bitmaps.empty()) {
        _finished = true;
        return std::vector<IcebergDvCommitEntry>{};
    }

    // Pre-flight validation. These are caller/input errors that write nothing, so they leave the
    // writer un-finished and callable again (unlike a real write attempt, which is single-shot).
    if (file == nullptr) {
        return Status::InvalidArgument("IcebergDvWriter::finish requires a non-null WritableFile");
    }
    // An empty referenced-data-file would produce an unusable DeleteFile entry. The empty
    // string (if any position was added under it) sorts first in the map.
    if (_bitmaps.begin()->first.empty()) {
        return Status::InvalidArgument("IcebergDvWriter: empty referenced_data_file");
    }

    // Committed to writing: single-shot from here so a retry cannot append a second Puffin body.
    _finished = true;
    PuffinWriter puffin(file);
    RETURN_IF_ERROR(puffin.init());
    std::vector<IcebergDvCommitEntry> entries;
    entries.reserve(_bitmaps.size());
    for (const auto& [ref_file, bitmap] : _bitmaps) {
        ASSIGN_OR_RETURN(std::string blob, build_deletion_vector_blob(bitmap));
        const int64_t cardinality = static_cast<int64_t>(roaring64_bitmap_get_cardinality(bitmap));
        ASSIGN_OR_RETURN(PuffinBlobMetadata meta,
                         puffin.add_blob(kDeletionVectorBlobType, {kRowPositionFieldId},
                                         {{kDvPropReferencedDataFile, ref_file},
                                          {kDvPropCardinality, std::to_string(cardinality)}},
                                         reinterpret_cast<const uint8_t*>(blob.data()), blob.size()));
        entries.push_back(IcebergDvCommitEntry{ref_file, meta.offset, meta.length, cardinality});
    }
    RETURN_IF_ERROR(puffin.finish());
    return entries;
}

} // namespace starrocks::formats
