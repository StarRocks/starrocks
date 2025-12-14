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

#include "exec/pipeline/fetch_task.h"
#include "exec/sorting/sort_permute.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/descriptors.h"
#include "storage/range.h"
#include "util/phmap/phmap.h"

namespace starrocks::pipeline {

// Describes the lifecycle of a single lookup request, regardless of whether
// it is served locally or remotely. Implementations collect the columns needed
// for the request, populate response columns, and notify the caller once the
// fetch finishes.
class LookUpRequestContext {
public:
    virtual ~LookUpRequestContext() = default;

    virtual TupleId request_tuple_id() const = 0;
    virtual Status collect_input_columns(ChunkPtr chunk) = 0;
    virtual StatusOr<size_t> fill_response(const ChunkPtr& result_chunk, SlotId source_id_slot,
                                           const std::vector<SlotDescriptor*>& slots, size_t start_offset) = 0;
    virtual void callback(const Status& status) = 0;

    int64_t receive_ts = 0;
};
using LookUpRequestContextPtr = std::shared_ptr<LookUpRequestContext>;

class LocalLookUpRequestContext final : public LookUpRequestContext {
public:
    LocalLookUpRequestContext(FetchTaskContextPtr ctx) : fetch_ctx(std::move(ctx)) {}
    ~LocalLookUpRequestContext() override = default;

    TupleId request_tuple_id() const override { return fetch_ctx->request_tuple_id; }
    Status collect_input_columns(ChunkPtr chunk) override;
    StatusOr<size_t> fill_response(const ChunkPtr& result_chunk, SlotId source_id_slot,
                                   const std::vector<SlotDescriptor*>& slots, size_t start_offset) override;
    void callback(const Status& status) override;

    FetchTaskContextPtr fetch_ctx;
};
using LocalLookUpRequestContextPtr = std::shared_ptr<LocalLookUpRequestContext>;

class RemoteLookUpRequestContext final : public LookUpRequestContext {
public:
    RemoteLookUpRequestContext(::google::protobuf::RpcController* cntl, const PLookUpRequest* request,
                               PLookUpResponse* response, ::google::protobuf::Closure* done)
            : cntl(cntl), request(request), response(response), done(done) {}
    ~RemoteLookUpRequestContext() override = default;

    TupleId request_tuple_id() const override { return request->request_tuple_id(); }
    Status collect_input_columns(ChunkPtr chunk) override;
    StatusOr<size_t> fill_response(const ChunkPtr& result_chunk, SlotId source_id_slot,
                                   const std::vector<SlotDescriptor*>& slots, size_t start_offset) override;
    void callback(const Status& status) override;

    ::google::protobuf::RpcController* cntl = nullptr;
    const PLookUpRequest* request = nullptr;
    PLookUpResponse* response = nullptr;
    ::google::protobuf::Closure* done = nullptr;
    ChunkPtr request_chunk;
};
using RemoteLookUpRequestContextPtr = std::shared_ptr<RemoteLookUpRequestContext>;

class LookUpProcessor;
class LookUpOperator;
class LookUpTaskContext {
public:
    TupleId request_tuple_id;
    SlotId row_source_slot_id;
    std::vector<SlotId> lookup_ref_slot_ids;
    std::vector<SlotId> fetch_ref_slot_ids;
    std::vector<LookUpRequestContextPtr> request_ctxs;

    RuntimeProfile* profile = nullptr;
    Permutation permutation;
    LookUpOperator* parent = nullptr;
};
using LookUpTaskContextPtr = std::shared_ptr<LookUpTaskContext>;

class LookUpTask;
using LookUpTaskPtr = std::shared_ptr<LookUpTask>;

// Groups lookup requests that share the same tuple descriptor and executes
// them as a batch. Each concrete data source provides its own task that knows
// how to process the request chunk and fill responses for its contexts.
class LookUpTask {
public:
    LookUpTask(LookUpTaskContextPtr ctx) : _ctx(std::move(ctx)) {}
    virtual ~LookUpTask() = default;

    virtual Status process(RuntimeState* state, const ChunkPtr& request_chunk) = 0;

protected:
    StatusOr<ChunkPtr> _sort_chunk(RuntimeState* state, const ChunkPtr& chunk, const Columns& order_by_columns);
    LookUpTaskContextPtr _ctx;
};

// Handles lookup batches against Iceberg v3 tables. The task expands the
// incoming position metadata into row-id ranges, reads the matching rows from
// storage, reorders them to the original request order, and forwards the data
// back to every pending request context.
class IcebergV3LookUpTask final : public LookUpTask {
public:
    IcebergV3LookUpTask(LookUpTaskContextPtr ctx) : LookUpTask(std::move(ctx)) {}
    ~IcebergV3LookUpTask() override = default;

    Status process(RuntimeState* state, const ChunkPtr& request_chunk) override;

private:
    StatusOr<ChunkPtr> _calculate_row_id_range(
            RuntimeState* state, const ChunkPtr& request_chunk,
            phmap::flat_hash_map<int32_t, std::shared_ptr<SparseRange<int64_t>>>* row_id_ranges,
            Buffer<uint32_t>* replicated_offsets);

    StatusOr<ChunkPtr> _get_data_from_storage(
            RuntimeState* state, const std::vector<SlotDescriptor*>& slots,
            const phmap::flat_hash_map<int32_t, std::shared_ptr<SparseRange<int64_t>>>& row_id_ranges);

    TExpr create_row_id_filter_expr(SlotId slot_id, const SparseRange<int64_t>& row_id_range);
};

} // namespace starrocks::pipeline
