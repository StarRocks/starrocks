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

#include "exec/hdfs_scanner/hdfs_scanner.h"

namespace starrocks {
namespace parquet {
class FileReader;
}

class HdfsParquetScanner final : public HdfsScanner {
public:
    HdfsParquetScanner() : _skip_rows_ctx(std::make_shared<SkipRowsContext>()){};
    ~HdfsParquetScanner() override = default;

    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) override;
    void do_update_counter(HdfsScannerProfile* profile) override;
    // Parquet handles single-slot predicates via row-group zone-map, page-index,
    // dict-filter, and lazy materialisation inside FileReader; the base class must
    // not apply them a second time.
    bool scanner_handles_predicate_by_slot_internally() const override { return true; }
    // Parquet evaluates multi-slot predicates at the end of do_get_next() after
    // FileReader has materialised all columns.  In the future, expression-driven
    // lazy materialisation will interleave this with column loading for deeper
    // optimisation — the override keeps the base class from double-applying them.
    bool scanner_handles_multi_slot_conjuncts_internally() const override { return true; }

private:
    std::shared_ptr<parquet::FileReader> _reader = nullptr;
    SkipRowsContextPtr _skip_rows_ctx;
};

} // namespace starrocks
