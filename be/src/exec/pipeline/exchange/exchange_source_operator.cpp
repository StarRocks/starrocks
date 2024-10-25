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

#include "exec/pipeline/exchange/exchange_source_operator.h"

#include "glog/logging.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status ExchangeSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    _stream_recvr = static_cast<ExchangeSourceOperatorFactory*>(_factory)->create_stream_recvr(state);
    _stream_recvr->bind_profile(_driver_sequence, _unique_metrics);
    return Status::OK();
}

bool ExchangeSourceOperator::has_output() const {
    return _stream_recvr->has_output_for_pipeline(_driver_sequence);
}

bool ExchangeSourceOperator::is_finished() const {
    return _stream_recvr->is_finished();
}

Status ExchangeSourceOperator::set_finishing(RuntimeState* state) {
    _is_finishing = true;
    _stream_recvr->short_circuit_for_pipeline(_driver_sequence);
    static_cast<ExchangeSourceOperatorFactory*>(_factory)->close_stream_recvr();
    return Status::OK();
}

StatusOr<ChunkPtr> ExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    auto chunk = std::make_unique<Chunk>();
    RETURN_IF_ERROR(_stream_recvr->get_chunk_for_pipeline(&chunk, _driver_sequence));
    RETURN_IF_ERROR(eval_no_eq_join_runtime_in_filters(chunk.get()));
    eval_runtime_bloom_filters(chunk.get());
    return std::move(chunk);
}

ExchangeSourceOperatorFactory::~ExchangeSourceOperatorFactory() {
    if (_stream_recvr != nullptr && _stream_recvr_cnt != 0) {
        // NOTE: it is possible that the ExchangeSourceOperator::prepare() is called, but the ExchangeSourceOperator::set_finishing()
        // is never called. The `_stream_recvr_cnt` can never count down to 0. It is not a good idea to call
        // ExchangeSourceOperatorFactory::close_stream_recvr() in ~ExchangeSourceOperator because the operator is held by shared_ptr,
        // and its _factory is a raw pointer. It is not guaranteed that the factory may live longer than the operator.
        // Ideally, the operator should take the responsibility to create and destroy the `_stream_recvr`. However, this shared
        // `_stream_recvr` design, moves the responsibility from the operator to the operator factory.
        LOG(INFO) << "ExchangeSourceOperatorFactory::_stream_recvr_cnt=" << _stream_recvr_cnt
                  << ", the _stream_recvr is created without properly cleaned. Force close it!";
        _stream_recvr->close();
    }
}

std::shared_ptr<DataStreamRecvr> ExchangeSourceOperatorFactory::create_stream_recvr(RuntimeState* state) {
    if (_stream_recvr != nullptr) {
        return _stream_recvr;
    }
    auto query_statistic_recv = state->query_recv();
    _stream_recvr = state->exec_env()->stream_mgr()->create_recvr(
            state, _row_desc, state->fragment_instance_id(), _plan_node_id, _num_sender,
            config::exchg_node_buffer_size_bytes, false, query_statistic_recv, true, _degree_of_parallelism, false);

    return _stream_recvr;
}

void ExchangeSourceOperatorFactory::close_stream_recvr() {
    if (--_stream_recvr_cnt == 0) {
        _stream_recvr->close();
    }
}

bool ExchangeSourceOperatorFactory::could_local_shuffle() const {
    DCHECK(_texchange_node.__isset.partition_type);
    if (!_enable_pipeline_level_shuffle) {
        return true;
    }
    // There are two ways of shuffle
    // 1. If previous op is ExchangeSourceOperator and its partition type is HASH_PARTITIONED or BUCKET_SHUFFLE_HASH_PARTITIONED
    // then pipeline level shuffle will be performed at sender side (ExchangeSinkOperator), so
    // there is no need to perform local shuffle again at receiver side
    // 2. Otherwise, add LocalExchangeOperator
    // to shuffle multi-stream into #degree_of_parallelism# streams each of that pipes.
    return _texchange_node.partition_type != TPartitionType::HASH_PARTITIONED &&
           _texchange_node.partition_type != TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED;
}

TPartitionType::type ExchangeSourceOperatorFactory::partition_type() const {
    DCHECK(_texchange_node.__isset.partition_type);
    if (_texchange_node.partition_type != TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {
        return SourceOperatorFactory::partition_type();
    }
    return _texchange_node.partition_type;
}

} // namespace starrocks::pipeline
