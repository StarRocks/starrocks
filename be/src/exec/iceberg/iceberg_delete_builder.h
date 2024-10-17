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
#include "runtime/descriptors.h"

namespace starrocks {
struct IcebergColumnMeta;

class IcebergDeleteBuilder {
public:
    IcebergDeleteBuilder(std::set<int64_t>* need_skip_rowids, RuntimeState* state,
                         const HdfsScannerParams& scanner_params)
            : _need_skip_rowids(need_skip_rowids), _params(scanner_params), _runtime_state(state) {}

    ~IcebergDeleteBuilder() = default;

    Status build_orc(const TIcebergDeleteFile& delete_file) const;

    Status build_parquet(const TIcebergDeleteFile& delete_file) const;

private:
    StatusOr<std::unique_ptr<RandomAccessFile>> open_random_access_file(
            const TIcebergDeleteFile& delete_file, HdfsScanStats& fs_scan_stats, HdfsScanStats& app_scan_stats,
            std::shared_ptr<io::SharedBufferedInputStream>& shared_buffered_input_stream,
            std::shared_ptr<io::CacheInputStream>& cache_input_stream) const;

    static void update_delete_file_io_counter(
            RuntimeProfile* parent_profile, const HdfsScanStats& app_stats, const HdfsScanStats& fs_stats,
            const std::shared_ptr<io::CacheInputStream>& cache_input_stream,
            const std::shared_ptr<io::SharedBufferedInputStream>& shared_buffered_input_stream);
    Status fill_skip_rowids(const ChunkPtr& chunk) const;

    std::set<int64_t>* _need_skip_rowids;
    const HdfsScannerParams& _params;
    RuntimeState* _runtime_state;
};

class IcebergDeleteFileMeta {
public:
    IcebergDeleteFileMeta() = default;
    ~IcebergDeleteFileMeta() = default;

    static SlotDescriptor& get_delete_file_path_slot();
    static SlotDescriptor& get_delete_file_pos_slot();

private:
    static SlotDescriptor gen_slot_helper(const IcebergColumnMeta& meta);
};
} // namespace starrocks