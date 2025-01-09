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

#include "common/status.h"
#include "exec/hdfs_scanner.h"
#include "fs/fs.h"

namespace starrocks {

struct DeletionVectorBuildStats {
    int64_t bitmap_deserialize_ns = 0;
};

class DeletionVector {
public:
    DeletionVector(const HdfsScannerParams& scanner_params)
            : _deletion_vector_descriptor(scanner_params.deletion_vector_descriptor), _params(scanner_params) {}

    Status fill_row_indexes(const SkipRowsContextPtr& skip_rows_ctx);
    Status deserialized_inline_dv(std::string& encoded_bitmap_data, const SkipRowsContextPtr& skip_rows_ctx);
    StatusOr<std::string> get_absolute_path(const std::string& table_location) const;

    const bool is_inline() {
        return _deletion_vector_descriptor->__isset.storageType && _deletion_vector_descriptor->storageType == "i";
    }

    static const int32_t DV_SIZE_LENGTH = 4;
    static const int32_t MAGIC_NUMBER_LENGTH = 4;
    static const uint32_t MAGIC_NUMBER = 1681511377;
    // UUIDs always encode into 20 characters.
    static const uint32_t ENCODED_UUID_LENGTH = 20;
    static const std::string DELETION_VECTOR;

private:
    StatusOr<std::unique_ptr<RandomAccessFile>> open_random_access_file(
            const std::string& file_path, HdfsScanStats& fs_scan_stats, HdfsScanStats& app_scan_stats,
            std::shared_ptr<io::SharedBufferedInputStream>& shared_buffered_input_stream,
            std::shared_ptr<io::CacheInputStream>& cache_input_stream) const;

    Status deserialized_deletion_vector(uint32_t magic_number, std::vector<char>& serialized_dv,
                                        int64_t serialized_bitmap_length, const SkipRowsContextPtr& skip_rows_ctx);

    std::string assemble_deletion_vector_path(const std::string& table_location, std::string&& uuid,
                                              std::string& prefix) const;

    void update_dv_file_io_counter(RuntimeProfile* parent_profile, const HdfsScanStats& app_stats,
                                   const HdfsScanStats& fs_stats,
                                   const std::shared_ptr<io::CacheInputStream>& cache_input_stream,
                                   const std::shared_ptr<io::SharedBufferedInputStream>& shared_buffered_input_stream);

    void update_dv_build_counter(RuntimeProfile* parent_profile, const DeletionVectorBuildStats& build_stats);

    const std::shared_ptr<TDeletionVectorDescriptor> _deletion_vector_descriptor;
    const HdfsScannerParams& _params;
    DeletionVectorBuildStats _build_stats;
};
} // namespace starrocks
