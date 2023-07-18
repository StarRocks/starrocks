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

#include "iceberg_delete_file_iterator.h"

namespace starrocks {

Status IcebergDeleteFileIterator::init(FileSystem* fs, const std::string& timezone, const std::string& file_path,
                                       int64_t file_length, const std::vector<SlotDescriptor*>& src_slot_descriptors,
                                       bool position_delete) {
    std::shared_ptr<RandomAccessFile> raw_file;
    ASSIGN_OR_RETURN(raw_file, fs->new_random_access_file(file_path));
    auto parquet_file = std::make_shared<ParquetChunkFile>(raw_file, 0);
    int num_of_columns_from_file = -1;
    if (position_delete) {
        num_of_columns_from_file = 2;
    }
    auto parquet_reader =
            std::make_shared<ParquetReaderWrap>(std::move(parquet_file), num_of_columns_from_file, 0, file_length);
    int64_t file_size;
    RETURN_IF_ERROR(parquet_reader->size(&file_size));
    if (file_size == 0) {
        parquet_reader->close();
        return Status::OK();
    }

    _file_reader = std::make_shared<ParquetChunkReader>(std::move(parquet_reader), src_slot_descriptors, timezone);
    return Status::OK();
}

StatusOr<bool> IcebergDeleteFileIterator::has_next() {
    if (_file_reader) {
        // @TODO what if error
        RETURN_IF_ERROR(_file_reader->next_batch(&_batch));
        if (_batch) {
            return true;
        }
    }
    return false;
}

std::shared_ptr<::arrow::RecordBatch> IcebergDeleteFileIterator::next() {
    return _batch;
}

} // namespace starrocks