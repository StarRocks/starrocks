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

#include "exec/parquet_scanner.h"

namespace starrocks {

class IcebergDeleteFileIterator {
public:
    IcebergDeleteFileIterator() = default;
    ~IcebergDeleteFileIterator() = default;

    Status init(FileSystem* fs, const std::string& timezone, const std::string& file_path, int64_t file_length,
                const std::vector<SlotDescriptor*>& src_slot_descriptors, bool position_delete);

    StatusOr<bool> has_next();

    std::shared_ptr<::arrow::RecordBatch> next();

private:
    std::shared_ptr<::arrow::RecordBatch> _batch;
    std::shared_ptr<ParquetChunkReader> _file_reader;
};

} // namespace starrocks