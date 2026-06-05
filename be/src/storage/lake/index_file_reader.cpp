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

#include "storage/lake/index_file_reader.h"

#include <string>
#include <vector>

#include "base/coding.h"
#include "fs/fs.h"
#include "storage/lake/index_file_writer.h" // for kMagic

namespace starrocks::lake {

Status IndexFileReader::init(RandomAccessFile* file) {
    if (file == nullptr) {
        return Status::InvalidArgument("IndexFileReader::init file is null");
    }
    if (_inited) {
        return Status::InternalError("IndexFileReader::init called twice");
    }

    ASSIGN_OR_RETURN(auto file_size, file->get_size());
    // Trailer layout (8 bytes): [ footer_length: u32 LE ][ MAGIC: u32 LE ].
    static constexpr int64_t kTrailerSize = 8;
    if (file_size < kTrailerSize) {
        return Status::Corruption("IndexFileReader: file too small for trailer");
    }

    uint8_t trailer[kTrailerSize];
    RETURN_IF_ERROR(file->read_at_fully(file_size - kTrailerSize, trailer, kTrailerSize));
    uint32_t footer_len = decode_fixed32_le(trailer);
    uint32_t magic = decode_fixed32_le(trailer + 4);
    if (magic != IndexFileWriter::kMagic) {
        return Status::Corruption("IndexFileReader: bad magic; file is not an IDG payload");
    }
    if (static_cast<int64_t>(footer_len) + kTrailerSize > file_size) {
        return Status::Corruption("IndexFileReader: footer length exceeds file size");
    }
    std::string footer_buf;
    footer_buf.resize(footer_len);
    RETURN_IF_ERROR(file->read_at_fully(file_size - kTrailerSize - static_cast<int64_t>(footer_len), footer_buf.data(),
                                        footer_len));
    if (!_footer.ParseFromString(footer_buf)) {
        return Status::Corruption("IndexFileReader: failed to parse IndexFileFooterPB");
    }
    _inited = true;
    return Status::OK();
}

const ColumnIndexMetaPB* IndexFileReader::find(int32_t col_unique_id, IndexType index_type) const {
    if (!_inited) {
        return nullptr;
    }
    // Linear scan is fine: a single .idx file carries at most a handful of
    // (col, type) pairs (bounded by what one ALTER can introduce).
    for (const auto& entry : _footer.column_indexes()) {
        if (entry.col_unique_id() == col_unique_id && entry.index_type() == index_type && entry.has_index_meta()) {
            return &entry.index_meta();
        }
    }
    return nullptr;
}

} // namespace starrocks::lake
