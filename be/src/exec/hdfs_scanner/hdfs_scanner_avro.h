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

#include "exec/file_scanner/file_scanner.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/avro/cpp/avro_reader.h"
#include "formats/avro/cpp/column_reader.h"

namespace starrocks {

// Native C++ Avro scanner for the Hive connector HDFS path.
// Replaces the JNI-based scanner for AVRO format files, enabling:
//   - DataCache integration (reads via C++ IO layer)
//   - Eliminated JNI boundary overhead
//   - Column selection at record level (unused fields not decoded)
//
// Avro format limitations: no block-level statistics, so predicate pushdown
// is row-level only (post-read conjunct evaluation).
class HdfsAvroScanner final : public HdfsScanner {
public:
    HdfsAvroScanner() = default;
    ~HdfsAvroScanner() override = default;

    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    void do_update_counter(HdfsScanProfile* profile) override;

private:
    void _materialize_nullable_columns(ChunkPtr& chunk);

    AvroReaderUniquePtr _avro_reader;
    std::vector<avrocpp::ColumnReaderUniquePtr> _column_readers;
    std::vector<SlotDescriptor*> _materialize_slot_descs;
    // Lightweight IO counter forwarded into AvroBufferInputStream for metric tracking.
    ScannerCounter _scanner_counter{};
    cctz::time_zone _timezone;
};

} // namespace starrocks
