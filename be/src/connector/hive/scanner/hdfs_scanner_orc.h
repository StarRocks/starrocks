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
#include <unordered_set>

#include "connector/hive/scanner/hdfs_scanner.h"
#include "formats/disk_range.hpp"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_input_stream.h"

namespace starrocks {

class OrcRowReaderFilter;

class HdfsOrcScanner final : public HdfsScanner {
public:
    HdfsOrcScanner() : _skip_rows_ctx(std::make_shared<SkipRowsContext>()){};
    ~HdfsOrcScanner() override = default;

    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) override;
    void do_update_counter(HdfsScannerProfile* profile) override;
    void disable_use_orc_sargs() { _use_orc_sargs = false; }

private:
    StatusOr<size_t> _do_get_next(ChunkPtr* chunk);
    StatusOr<size_t> _do_get_next_count(ChunkPtr* chunk);

    // it means if we can skip this file without reading.
    // Normally it happens when we peek file column statistics,
    // and if we are sure there is no row matches, we can skip this file.
    // by skipping this file, we return EOF when client try to get chunk.
    bool _should_skip_file;

    // hdfs_scanner_orc will only eval conjunctions in _eval_conjunct_ctxs_by_materialized_slot
    // _eval_conjunct_ctxs_by_materialized_slot's slot must be existed in orc file, or be _pos, which the scanner fills
    std::unordered_map<SlotId, std::vector<ExprContext*>> _eval_conjunct_ctxs_by_materialized_slot{};

    Status build_iceberg_delete_builder();
    Status build_paimon_delete_file_builder();
    Status build_stripes(orc::Reader* reader, std::vector<DiskRange>* stripes);
    // Drops from `stripes` every stripe whose rows are all in the deletion bitmap; the row reader skips them.
    void skip_deleted_stripes(orc::Reader* reader, std::vector<DiskRange>* stripes);
    Status build_split_tasks(orc::Reader* reader, const std::vector<DiskRange>& stripes);
    // tiny_stripe_read: coalesce whole stripes; decided on all the stripes of the scan range, deleted ones
    // included, so skipping deleted stripes never switches the survivors to whole-stripe reads.
    Status build_io_ranges(ORCHdfsFileStream* file_stream, const std::vector<DiskRange>& stripes,
                           bool tiny_stripe_read);
    Status resolve_columns(orc::Reader* reader);
    const std::vector<std::string>* column_names() const {
        return _iceberg_column_names.empty() ? &_scanner_ctx->hive_column_names : &_iceberg_column_names;
    }
    bool use_orc_column_names() const {
        return _iceberg_column_names.empty() && _scanner_ctx->format_scan_context.options.orc_use_column_names;
    }

    // disable orc search argument would be much easier for
    // writing unittest of customized filter
    bool _use_orc_sargs;
    std::vector<SlotDescriptor*> _src_slot_descriptors;
    OrcChunkReader::LazyLoadContext _lazy_load_ctx;
    std::unique_ptr<OrcChunkReader> _orc_reader;
    std::shared_ptr<OrcRowReaderFilter> _orc_row_reader_filter;
    Filter _dict_filter;
    Filter _chunk_filter;
    SkipRowsContextPtr _skip_rows_ctx;
    // Indexes of the stripes skip_deleted_stripes dropped.
    std::unordered_set<uint64_t> _deleted_stripes;
    std::unique_ptr<ORCHdfsFileStream> _input_stream;
    // Current Iceberg name of each top-level ORC column, set when the file carries Iceberg field ids.
    std::vector<std::string> _iceberg_column_names;
    // Iceberg `_pos` slot (the row's position in the data file), filled by the scanner; null when not read.
    const SlotDescriptor* _row_position_slot = nullptr;
};

} // namespace starrocks
