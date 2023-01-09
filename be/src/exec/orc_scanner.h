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

#include <orc/OrcFile.hh>

#include "exec/file_scanner.h"
#include "formats/orc/orc_chunk_reader.h"
#include "fs/fs.h"

namespace starrocks {

class ORCScanner : public FileScanner {
public:
    using FillColumnFunction = void (*)(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                        const TypeDescriptor& slot_desc);

    ORCScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
               starrocks::ScannerCounter* counter);

    ~ORCScanner() override = default;

    Status open() override;

    StatusOr<ChunkPtr> get_next() override;

    void close() override;

private:
    Status _next_orc_batch(ChunkPtr* result);

    StatusOr<ChunkPtr> _next_orc_chunk();

    Status _open_next_orc_reader();

    ChunkPtr _create_src_chunk();

    StatusOr<ChunkPtr> _transfer_chunk(ChunkPtr& src);

    ChunkPtr _materialize(ChunkPtr& src, ChunkPtr& cast);

private:
    const TBrokerScanRange& _scan_range;
    const uint64_t _max_chunk_size;
    int _next_range;
    int _error_counter;
    bool _status_eof;

    std::unique_ptr<OrcChunkReader> _orc_reader;
    std::vector<SlotDescriptor*> _orc_slot_descriptors;
};

} // namespace starrocks
