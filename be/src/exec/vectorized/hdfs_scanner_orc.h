// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <orc/OrcFile.hh>

#include "exec/vectorized/hdfs_scanner.h"
#include "formats/orc/orc_chunk_reader.h"

namespace starrocks::vectorized {

class OrcRowReaderFilter;

class HdfsOrcScanner final : public HdfsScanner {
public:
    HdfsOrcScanner() = default;
    ~HdfsOrcScanner() override = default;

    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;

    void disable_use_orc_sargs() { _use_orc_sargs = false; }

private:
    // it means if we can skip this file without reading.
    // Normally it happens when we peek file column statistics,
    // and if we are sure there is no row matches, we can skip this file.
    // by skipping this file, we return EOF when client try to get chunk.
    bool _should_skip_file;

    // disable orc search argument would be much easier for
    // writing unittest of customized filter
    bool _use_orc_sargs;
    std::vector<SlotDescriptor*> _src_slot_descriptors;
    OrcChunkReader::LazyLoadContext _lazy_load_ctx;
    std::unique_ptr<OrcChunkReader> _orc_reader;
    std::shared_ptr<OrcRowReaderFilter> _orc_row_reader_filter;
    Filter _dict_filter;
    Filter _chunk_filter;
};

} // namespace starrocks::vectorized