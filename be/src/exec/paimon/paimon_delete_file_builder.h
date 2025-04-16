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

#include "exec/hdfs_scanner.h"
#include "fs/fs.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

class PaimonDeleteFileBuilder {
public:
    PaimonDeleteFileBuilder(FileSystem* fs, SkipRowsContextPtr skip_rows_ctx)
            : _fs(fs), _skip_rows_ctx(std::move(skip_rows_ctx)) {}
    ~PaimonDeleteFileBuilder() = default;
    Status build(const TPaimonDeletionFile* paimon_deletion_file);

private:
    uint32_t swap_endian32(uint32_t val) {
        return ((val << 24) & 0xFF000000) | ((val << 8) & 0x00FF0000) | ((val >> 8) & 0x0000FF00) |
               ((val >> 24) & 0x000000FF);
    }

    FileSystem* _fs;
    SkipRowsContextPtr _skip_rows_ctx;

    // Structure of a deletion file is: 1 byte version num + n * {4 bytes deletion vector length + 4 bytes magic num
    // + (length - 4) bytes bitmap + 4 bytes CRC num}, n is equal to num of data files
    const int32_t MAGIC_NUMBER = 1581511376;
    const int32_t MAGIC_NUMBER_LENGTH = 4;
    const int32_t BITMAP_SIZE_LENGTH = 4;
};

} // namespace starrocks
