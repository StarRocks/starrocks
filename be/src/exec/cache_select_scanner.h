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
#include "formats/disk_range.hpp"
#include "io/cache_select_input_stream.hpp"

namespace starrocks {

// CacheSelectScanner is used by cache select
// For textfile file, it will download files directly
// For orc/parquet file, it will read and parse it's footer first, then according the column's offset and length, fetch it's data directly.
// For iceberg v2 delete file, it will fetch entire file directly
class CacheSelectScanner final : public HdfsScanner {
public:
    CacheSelectScanner() = default;
    ~CacheSelectScanner() override = default;
    Status do_open(RuntimeState* runtime_state) override;
    void do_update_counter(HdfsScanProfile* profile) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;

private:
    Status _fetch_orc();
    Status _fetch_parquet();
    Status _fetch_textfile();
    Status _fetch_iceberg_delete_files();
    Status _create_input_stream();
    Status _write_entire_file(const std::string& file_path, size_t file_size);
    static Status _write_disk_ranges(std::shared_ptr<io::SharedBufferedInputStream>& shared_input_stream,
                                     std::shared_ptr<io::CacheInputStream>& cache_input_stream,
                                     const std::vector<DiskRange>& disk_ranges);
};

} // namespace starrocks
