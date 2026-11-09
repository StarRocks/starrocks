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

#include "exec/pipeline/paimon_bucket_operator.h"

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/util/thrift_util.h"
#include "connector/hive/paimon/paimon_cpp_loader.h"
#include "exprs/expr_executor.h"
#include "runtime/runtime_state.h"

namespace starrocks {

StatusOr<TPaimonTableSink> decode_paimon_bucket_routing(const TPaimonBucketRouting& routing) {
    if (!routing.__isset.protocol_version || routing.protocol_version != kPaimonBucketRoutingProtocolVersion) {
        return Status::NotSupported("Unsupported Paimon bucket routing protocol");
    }
    if (!routing.__isset.sink_payload || routing.sink_payload.empty() ||
        routing.sink_payload.size() > kMaxPaimonRoutingPayloadBytes) {
        return Status::InvalidArgument("Paimon bucket routing payload is empty or exceeds 1 MiB");
    }
    if (!routing.__isset.bucket_slot_id || routing.bucket_slot_id < 0 || !routing.__isset.schema_exprs ||
        routing.schema_exprs.empty()) {
        return Status::InvalidArgument(
                "Paimon bucket routing requires a bucket slot and all native schema expressions");
    }
    TPaimonTableSink sink;
    uint32_t bytes = routing.sink_payload.size();
    auto status = deserialize_thrift_msg(reinterpret_cast<const uint8_t*>(routing.sink_payload.data()), &bytes,
                                         TProtocolType::BINARY, &sink);
    if (!status.ok() || bytes != routing.sink_payload.size()) {
        // Do not propagate serializer errors: its input contains cloud credentials and table options.
        return Status::InvalidArgument("Malformed Paimon bucket routing write contract");
    }
    if (!sink.__isset.writer_protocol_version || sink.writer_protocol_version != 1 || !sink.__isset.bucket_count ||
        sink.bucket_count <= 0 || !sink.__isset.routing_bucket_slot_id ||
        sink.routing_bucket_slot_id != routing.bucket_slot_id || !sink.__isset.column_names ||
        sink.column_names.size() != routing.schema_exprs.size() || !sink.__isset.schema_id ||
        !sink.__isset.schema_json || sink.schema_json.empty() || !sink.__isset.table_identity ||
        sink.table_identity.empty() || !sink.__isset.location || sink.location.empty() || !sink.__isset.options ||
        !sink.__isset.partition_columns || !sink.__isset.cloud_configuration) {
        return Status::InvalidArgument("Incomplete or unsupported Paimon bucket routing write contract");
    }
    return sink;
}

Status append_paimon_bucket_column(const ChunkPtr& chunk, SlotId slot_id, int32_t bucket_count,
                                   const std::vector<int32_t>& bucket_ids) {
    if (chunk == nullptr || bucket_count <= 0 || slot_id < 0 || bucket_ids.size() != chunk->num_rows()) {
        return Status::InvalidArgument("Paimon native bucket calculation returned an invalid row count or layout");
    }
    if (chunk->get_slot_id_to_index_map().count(slot_id) != 0) {
        return Status::InvalidArgument("Paimon native bucket slot already exists before calculation");
    }
    auto buckets = Int32Column::create();
    buckets->reserve(bucket_ids.size());
    for (int32_t bucket : bucket_ids) {
        if (bucket < 0 || bucket >= bucket_count) {
            return Status::InvalidArgument("Paimon native bucket calculation returned an out-of-range bucket");
        }
        buckets->append(bucket);
    }
    chunk->append_column(std::move(buckets), slot_id);
    return Status::OK();
}

namespace pipeline {

Status PaimonBucketOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _bucket_compute_timer = ADD_TIMER(_unique_metrics, "NativeBucketComputeTime");
    return Status::OK();
}

void PaimonBucketOperator::close(RuntimeState* state) {
    _chunk.reset();
    Operator::close(state);
}

StatusOr<ChunkPtr> PaimonBucketOperator::pull_chunk(RuntimeState* state) {
    return std::move(_chunk);
}

Status PaimonBucketOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_bucket_compute_timer);
    if (chunk->is_empty()) {
        // Preserve empty terminal chunks and their owner_info without loading the native SDK.
        _chunk = chunk;
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto bucket_ids, calculate_paimon_cpp_bucket_ids(_sink, chunk, _schema_exprs));
    RETURN_IF_ERROR(append_paimon_bucket_column(chunk, _sink.routing_bucket_slot_id, _sink.bucket_count, bucket_ids));
    _chunk = chunk;
    return Status::OK();
}

Status PaimonBucketOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(ExprExecutor::prepare(_schema_exprs, state));
    RETURN_IF_ERROR(ExprExecutor::open(_schema_exprs, state));
    return Status::OK();
}

void PaimonBucketOperatorFactory::close(RuntimeState* state) {
    ExprExecutor::close(_schema_exprs, state);
    OperatorFactory::close(state);
}

} // namespace pipeline
} // namespace starrocks
