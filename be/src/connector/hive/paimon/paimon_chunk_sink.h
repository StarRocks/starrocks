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

#include <memory>
#include <utility>
#include <vector>

#include "connector_primitive/connector_sink.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/Exprs_types.h"

namespace starrocks {

class ExprContext;
class PriorityThreadPool;

// Shim-side implementation of the SDK-free routing entrypoint (also used by focused SDK UTs).
StatusOr<std::vector<int32_t>> calculate_paimon_bucket_ids_in_shim(const TPaimonTableSink& sink, const ChunkPtr& chunk,
                                                                   const std::vector<ExprContext*>& output_exprs);

namespace connector {

// This contract is shared with the lazily loaded shim. No SDK or Arrow C++ types may escape it.
struct PaimonChunkSinkContext final : ConnectorSinkContext {
    TPaimonTableSink sink;
    std::vector<TExpr> output_exprs; // Full native schema order, including partition columns.
    RuntimeState* runtime_state = nullptr;
    PriorityThreadPool* executor = nullptr; // Injected, bounded pipeline sink I/O pool.
};

// Only paimon_chunk_sink.cpp (compiled in the shim) defines this class's implementation.
class PaimonChunkSink final : public ConnectorSink {
public:
    PaimonChunkSink(std::shared_ptr<PaimonChunkSinkContext> context, int32_t driver_id);
    ~PaimonChunkSink() override;

    Status init(formats::AsyncFlushStreamPoller*, RuntimeProfile*, SinkMemoryManager*) override;
    Status add(const ChunkPtr& chunk) override;
    Status finish() override;
    void rollback() override;
    bool is_finished() override;
    Status status() override;
    SinkOperatorMemoryManager* op_mem_mgr() const override;

private:
    class Impl;
    std::unique_ptr<Impl> _impl;
};

// Defined in the always-linked loader. A missing writer capability is deferred to init(), not
// returned from create_sink(): ConnectorSinkOperatorFactory currently calls create_sink().value().
class PaimonChunkSinkProvider final : public ConnectorSinkProvider {
public:
    explicit PaimonChunkSinkProvider(std::shared_ptr<PaimonChunkSinkContext> context) : _context(std::move(context)) {}
    StatusOr<std::unique_ptr<ConnectorSink>> create_sink(int32_t driver_id) override;

private:
    std::shared_ptr<PaimonChunkSinkContext> _context;
};

} // namespace connector
} // namespace starrocks
