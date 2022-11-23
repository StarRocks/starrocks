// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <orc/OrcFile.hh>

#include "exec/vectorized/file_scanner.h"
#include "formats/orc/orc_chunk_reader.h"
#include "fs/fs.h"

namespace starrocks::vectorized {

class ORCScanner : public FileScanner {
public:
    using FillColumnFunction = void (*)(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                        const TypeDescriptor& slot_desc);

    ORCScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
               starrocks::vectorized::ScannerCounter* counter);

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

} // namespace starrocks::vectorized
