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
#include "formats/avro/cpp/avro_reader.h"

namespace starrocks {

// Avro file scanner implementation that reads Avro files using the Avro C++ library.
//
// 1. schema extraction.
// 2. read chunk with adaptive nullable column encoding.
class AvroCppScanner final : public FileScanner {
public:
    explicit AvroCppScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                            ScannerCounter* counter, bool schema_only = false);
    ~AvroCppScanner() override = default;

    Status open() override;
    void close() override;

    StatusOr<ChunkPtr> get_next() override;

    Status get_schema(std::vector<SlotDescriptor>* schema) override;

private:
    Status create_column_readers();
    Status create_src_chunk(ChunkPtr* chunk);
    Status next_avro_chunk(ChunkPtr& chunk);
    Status open_next_reader();
    StatusOr<AvroReaderUniquePtr> open_avro_reader(const TBrokerRangeDesc& range_desc);
    void materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk);

    const TBrokerScanRange& _scan_range;
    const int _max_chunk_size;
    int _num_of_columns_from_file = 0;
    cctz::time_zone _timezone;
    std::vector<avrocpp::ColumnReaderUniquePtr> _column_readers;
    AvroReaderUniquePtr _cur_file_reader;
    bool _cur_file_eof = false;
    int _next_range = 0;
    ObjectPool _pool;
};

} // namespace starrocks