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

#include "exec/hdfs_scanner.h"
#include "formats/orc/orc_chunk_reader.h"

namespace starrocks {

class OrcRowReaderFilter;

class HdfsOrcScanner final : public HdfsScanner {
public:
    HdfsOrcScanner() = default;
    ~HdfsOrcScanner() override = default;

    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    void do_update_counter(HdfsScanProfile* profile) override;

    void disable_use_orc_sargs() { _use_orc_sargs = false; }

private:
    StatusOr<size_t> _do_get_next(ChunkPtr* chunk);

    // it means if we can skip this file without reading.
    // Normally it happens when we peek file column statistics,
    // and if we are sure there is no row matches, we can skip this file.
    // by skipping this file, we return EOF when client try to get chunk.
    bool _should_skip_file;

    // hdfs_scanner_orc will only eval conjunctions in _eval_conjunct_ctxs_by_materialized_slot
    // _eval_conjunct_ctxs_by_materialized_slot's slot must be existed in orc file
    std::unordered_map<SlotId, std::vector<ExprContext*>> _eval_conjunct_ctxs_by_materialized_slot{};

    // disable orc search argument would be much easier for
    // writing unittest of customized filter
    bool _use_orc_sargs;
    std::vector<SlotDescriptor*> _src_slot_descriptors;
    OrcChunkReader::LazyLoadContext _lazy_load_ctx;
    std::unique_ptr<OrcChunkReader> _orc_reader;
    std::shared_ptr<OrcRowReaderFilter> _orc_row_reader_filter;
    Filter _dict_filter;
    Filter _chunk_filter;
    std::set<int64_t> _need_skip_rowids;
};

} // namespace starrocks
