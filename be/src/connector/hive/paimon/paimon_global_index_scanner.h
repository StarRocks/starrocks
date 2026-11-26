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

#include <paimon/global_index/global_index_reader.h>
#include <paimon/global_index/global_index_result.h>
#include <paimon/memory/memory_pool.h>
#include <rapidjson/document.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "base/string/slice.h"
#include "connector/hive/paimon/paimon_file_system.h"
#include "connector/hive/scanner/hdfs_scanner.h"

namespace starrocks {

class PaimonFileSystem;
struct PaimonGlobalIndexOperation;
class PaimonGlobalIndexScannerTestAccessor;
class TPaimonGlobalIndexScanRange;

// Executes one snapshot-bound Paimon global-index row-id range and emits one
// serialized scalar bitmap or ANN scored row for FE aggregation.
class PaimonGlobalIndexScanner final : public HdfsScanner {
public:
    PaimonGlobalIndexScanner() = default;
    ~PaimonGlobalIndexScanner() override = default;

    Status do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) override;
    Status do_open(RuntimeState* runtime_state) override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    void do_update_counter(HdfsScannerProfile* profile) override;
    int64_t estimated_mem_usage() const override;

private:
    friend class PaimonGlobalIndexScannerTestAccessor;

    Status _parse_request();
    static Status _parse_request(const TPaimonGlobalIndexScanRange& scan_range, rapidjson::Document* request);
    StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> _evaluate(RuntimeState* runtime_state);
    StatusOr<std::shared_ptr<paimon::ScoredGlobalIndexResult>> _evaluate_vector(RuntimeState* runtime_state);
    Status _emit_result(Slice bytes, ChunkPtr* chunk);

    bool _emitted = false;
    int64_t _open_ns = 0;
    int64_t _evaluate_ns = 0;
    int64_t _serialize_ns = 0;
    int64_t _serialized_bytes = 0;
    rapidjson::Document _request;
    std::shared_ptr<paimon::MemoryPool> _memory_pool;
    std::shared_ptr<PaimonFileSystem> _paimon_file_system;
    std::unordered_map<int32_t, std::shared_ptr<paimon::GlobalIndexReader>> _readers;
    std::unordered_map<int32_t, std::string> _field_types;
    std::shared_ptr<PaimonGlobalIndexOperation> _operation;
    PaimonFileSystemStats::Snapshot _io_stats;
};

} // namespace starrocks
