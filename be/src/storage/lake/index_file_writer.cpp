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

#include "storage/lake/index_file_writer.h"

#include "base/coding.h"
#include "fs/fs.h"

namespace starrocks::lake {

IndexFileWriter::IndexFileWriter(std::unique_ptr<WritableFile> file) : _file(std::move(file)) {}

IndexFileWriter::~IndexFileWriter() = default;

void IndexFileWriter::append_column_index(int32_t col_unique_id, IndexType index_type, const ColumnIndexMetaPB& meta) {
    auto* entry = _footer.add_column_indexes();
    entry->set_col_unique_id(col_unique_id);
    entry->set_index_type(index_type);
    entry->mutable_index_meta()->CopyFrom(meta);
}

Status IndexFileWriter::finalize() {
    if (_finalized) {
        return Status::InternalError("IndexFileWriter::finalize called twice");
    }
    if (_file == nullptr) {
        return Status::InternalError("IndexFileWriter has no underlying WritableFile");
    }

    // Serialize footer.
    std::string footer_buf;
    if (!_footer.SerializeToString(&footer_buf)) {
        return Status::InternalError("Failed to serialize IndexFileFooterPB");
    }
    RETURN_IF_ERROR(_file->append(Slice(footer_buf)));

    // Trailing length (uint32 LE) + magic.
    uint8_t tail[8];
    encode_fixed32_le(tail, static_cast<uint32_t>(footer_buf.size()));
    encode_fixed32_le(tail + 4, kMagic);
    RETURN_IF_ERROR(_file->append(Slice(reinterpret_cast<const char*>(tail), sizeof(tail))));

    RETURN_IF_ERROR(_file->close());
    _file_size = _file->size();
    _finalized = true;
    return Status::OK();
}

} // namespace starrocks::lake
