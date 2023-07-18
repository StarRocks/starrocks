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

#include "exec/pipeline/exchange/exchange_sink_operator.h"

#include <arpa/inet.h>

#include <functional>
#include <iostream>
#include <memory>
#include <random>
#include <utility>

#include "common/config.h"
#include "exec/pipeline/exchange/shuffler.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exprs/expr.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/local_pass_through_buffer.h"
#include "runtime/runtime_state.h"
#include "serde/protobuf_serde.h"
#include "service/brpc.h"
#include "util/compression/block_compression.h"
#include "util/compression/compression_utils.h"
#include "util/failpoint/fail_point.h"

namespace starrocks::pipeline {

class ExchangeSinkOperator::Channel {
public:
    // Create channel to send data to particular ipaddress/port/query/node
    // combination. buffer_size is specified in bytes and a soft limit on
    // how much tuple data is getting accumulated before being sent; it only applies
    // when data is added via add_row() and not sent directly via send_batch().
    Channel(ExchangeSinkOperator* parent, const TNetworkAddress& brpc_dest, const TUniqueId& fragment_instance_id,
            PlanNodeId dest_node_id, int32_t num_shuffles, bool enable_exchange_pass_through, bool enable_exchange_perf,
            PassThroughChunkBuffer* pass_through_chunk_buffer)
            : _parent(parent),
              _brpc_dest_addr(brpc_dest),
              _fragment_instance_id(fragment_instance_id),
              _dest_node_id(dest_node_id),
              _enable_exchange_pass_through(enable_exchange_pass_through),
              _enable_exchange_perf(enable_exchange_perf),
              _pass_through_context(pass_through_chunk_buffer, fragment_instance_id, dest_node_id),
              _chunks(num_shuffles) {}

    // Initialize channel.
    // Returns OK if successful, error indication otherwise.
    Status init(RuntimeState* state);

    // Send one chunk to remote, this chunk may be batched in this channel.
    Status send_one_chunk(RuntimeState* state, const Chunk* chunk, int32_t driver_sequence, bool eos);

    // Send one chunk to remote, this chunk may be batched in this channel.
    // When the chunk is sent really rather than bachend, *is_real_sent will
    // be set to true.
    Status send_one_chunk(RuntimeState* state, const Chunk* chunk, int32_t driver_sequence, bool eos,
                          bool* is_real_sent);

    // Channel will sent input request directly without batch it.
    // This function is only used when broadcast, because request can be reused
    // by all the channels.
    Status send_chunk_request(RuntimeState* state, PTransmitChunkParamsPtr chunk_request,
                              const butil::IOBuf& attachment, int64_t attachment_physical_bytes);

    // Used when doing shuffle.
    // This function will copy selective rows in chunks to batch.
    // indexes contains row index of chunk and this function will copy from input
    // 'from' and copy 'size' rows
    Status add_rows_selective(Chunk* chunk, int32_t driver_sequence, const uint32_t* row_indexes, uint32_t from,
                              uint32_t size, RuntimeState* state);

    // Flush buffered rows and close channel. This function don't wait the response
    // of close operation, client should call close_wait() to finish channel's close.
    // We split one close operation into two phases in order to make multiple channels
    // can run parallel.
    Status close(RuntimeState* state, FragmentContext* fragment_ctx);

    std::string get_fragment_instance_id_str() {
        UniqueId uid(_fragment_instance_id);
        return uid.to_string();
    }

    TUniqueId get_fragment_instance_id() { return _fragment_instance_id; }

    bool use_pass_through() const { return _use_pass_through; }

    bool is_local();

private:
    Status _close_internal(RuntimeState* state, FragmentContext* fragment_ctx);

    bool _check_use_pass_through();
    void _prepare_pass_through();

    ExchangeSinkOperator* _parent;

    const TNetworkAddress _brpc_dest_addr;
    const TUniqueId _fragment_instance_id;
    const PlanNodeId _dest_node_id;

    const bool _enable_exchange_pass_through;
    // enable it to profile exchange's performance, which ignores computing local data for exchange_speed/_bytes,
    // because local data isn't accessed by remote network.
    const bool _enable_exchange_perf;
    PassThroughContext _pass_through_context;

    bool _is_first_chunk = true;
    doris::PBackendService_Stub* _brpc_stub = nullptr;

    // If pipeline level shuffle is enable, the size of the _chunks
    // equals with dop of dest pipeline
    // If pipeline level shuffle is disable, the size of _chunks
    // always be 1
    std::vector<std::unique_ptr<Chunk>> _chunks;
    PTransmitChunkParamsPtr _chunk_request;
    size_t _current_request_bytes = 0;

    bool _is_inited = false;
    bool _use_pass_through = false;
    // local data is shuffled without really remote network, so it cannot be considered in computing exchange speed.
    bool _ignore_local_data = false;
};

bool ExchangeSinkOperator::Channel::is_local() {
    if (BackendOptions::get_localhost() != _brpc_dest_addr.hostname) {
        return false;
    }
    if (config::brpc_port != _brpc_dest_addr.port) {
        return false;
    }
    return true;
}

bool ExchangeSinkOperator::Channel::_check_use_pass_through() {
    if (!_enable_exchange_pass_through) {
        return false;
    }
    return is_local();
}

void ExchangeSinkOperator::Channel::_prepare_pass_through() {
    _pass_through_context.init();
    _use_pass_through = _check_use_pass_through();
}

Status ExchangeSinkOperator::Channel::init(RuntimeState* state) {
    FAIL_POINT_TRIGGER_RETURN_ERROR(rand_error_during_prepare);
    if (_is_inited) {
        return Status::OK();
    }

    if (_brpc_dest_addr.hostname.empty()) {
        LOG(WARNING) << "there is no brpc destination address's hostname"
                        ", maybe version is not compatible.";
        return Status::InternalError("no brpc destination");
    }

    // _brpc_timeout_ms = std::min(3600, state->query_options().query_timeout) * 1000;
    // For bucket shuffle, the dest is unreachable, there is no need to establish a connection
    if (_fragment_instance_id.lo == -1) {
        _is_inited = true;
        return Status::OK();
    }
    _brpc_stub = state->exec_env()->brpc_stub_cache()->get_stub(_brpc_dest_addr);

    if (_brpc_stub == nullptr) {
        auto msg = fmt::format("The brpc stub of {}:{} is null.", _brpc_dest_addr.hostname, _brpc_dest_addr.port);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    _prepare_pass_through();
    _ignore_local_data = _enable_exchange_perf && is_local();

    _is_inited = true;
    return Status::OK();
}

Status ExchangeSinkOperator::Channel::add_rows_selective(Chunk* chunk, int32_t driver_sequence, const uint32_t* indexes,
                                                         uint32_t from, uint32_t size, RuntimeState* state) {
    if (UNLIKELY(_chunks[driver_sequence] == nullptr)) {
        _chunks[driver_sequence] = chunk->clone_empty_with_slot(size);
    }

    if (_chunks[driver_sequence]->num_rows() + size > state->chunk_size()) {
        RETURN_IF_ERROR(send_one_chunk(state, _chunks[driver_sequence].get(), driver_sequence, false));
        // we only clear column data, because we need to reuse column schema
        _chunks[driver_sequence]->set_num_rows(0);
    }

    _chunks[driver_sequence]->append_selective(*chunk, indexes, from, size);
    return Status::OK();
}

Status ExchangeSinkOperator::Channel::send_one_chunk(RuntimeState* state, const Chunk* chunk, int32_t driver_sequence,
                                                     bool eos) {
    bool is_real_sent = false;
    return send_one_chunk(state, chunk, driver_sequence, eos, &is_real_sent);
}

Status ExchangeSinkOperator::Channel::send_one_chunk(RuntimeState* state, const Chunk* chunk, int32_t driver_sequence,
                                                     bool eos, bool* is_real_sent) {
    *is_real_sent = false;

    if (_ignore_local_data && !eos) {
        return Status::OK();
    }

    if (_chunk_request == nullptr) {
        _chunk_request = std::make_shared<PTransmitChunkParams>();
        _chunk_request->set_node_id(_dest_node_id);
        _chunk_request->set_sender_id(_parent->_sender_id);
        _chunk_request->set_be_number(_parent->_be_number);
        if (_parent->_is_pipeline_level_shuffle) {
            _chunk_request->set_is_pipeline_level_shuffle(true);
        }
    }

    // If chunk is not null, append it to request
    if (chunk != nullptr) {
        if (_use_pass_through) {
            size_t chunk_size = serde::ProtobufChunkSerde::max_serialized_size(*chunk);
            // -1 means disable pipeline level shuffle
            TRY_CATCH_BAD_ALLOC(
                    _pass_through_context.append_chunk(_parent->_sender_id, chunk, chunk_size,
                                                       _parent->_is_pipeline_level_shuffle ? driver_sequence : -1));
            _current_request_bytes += chunk_size;
            COUNTER_UPDATE(_parent->_bytes_pass_through_counter, chunk_size);
        } else {
            if (_parent->_is_pipeline_level_shuffle) {
                _chunk_request->add_driver_sequences(driver_sequence);
            }
            auto pchunk = _chunk_request->add_chunks();
            TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_parent->serialize_chunk(chunk, pchunk, &_is_first_chunk)));
            _current_request_bytes += pchunk->data().size();
        }
    }

    // Try to accumulate enough bytes before sending a RPC. When eos is true we should send
    // last packet
    if (_current_request_bytes > config::max_transmit_batched_bytes || eos) {
        _chunk_request->set_eos(eos);
        _chunk_request->set_use_pass_through(_use_pass_through);
        if (auto delta_statistic = state->intermediate_query_statistic()) {
            delta_statistic->to_pb(_chunk_request->mutable_query_statistics());
        }
        butil::IOBuf attachment;
        int64_t attachment_physical_bytes = _parent->construct_brpc_attachment(_chunk_request, attachment);
        TransmitChunkInfo info = {this->_fragment_instance_id, _brpc_stub,     std::move(_chunk_request), attachment,
                                  attachment_physical_bytes,   _brpc_dest_addr};
        RETURN_IF_ERROR(_parent->_buffer->add_request(info));
        _current_request_bytes = 0;
        _chunk_request.reset();
        *is_real_sent = true;
    }

    return Status::OK();
}

Status ExchangeSinkOperator::Channel::send_chunk_request(RuntimeState* state, PTransmitChunkParamsPtr chunk_request,
                                                         const butil::IOBuf& attachment,
                                                         int64_t attachment_physical_bytes) {
    if (_ignore_local_data) {
        return Status::OK();
    }
    chunk_request->set_node_id(_dest_node_id);
    chunk_request->set_sender_id(_parent->_sender_id);
    chunk_request->set_be_number(_parent->_be_number);
    chunk_request->set_eos(false);
    chunk_request->set_use_pass_through(_use_pass_through);

    if (auto delta_statistic = state->intermediate_query_statistic()) {
        delta_statistic->to_pb(chunk_request->mutable_query_statistics());
    }

    TransmitChunkInfo info = {this->_fragment_instance_id, _brpc_stub,     std::move(chunk_request), attachment,
                              attachment_physical_bytes,   _brpc_dest_addr};
    RETURN_IF_ERROR(_parent->_buffer->add_request(info));

    return Status::OK();
}

Status ExchangeSinkOperator::Channel::_close_internal(RuntimeState* state, FragmentContext* fragment_ctx) {
    // no need to send EOS packet to pseudo destinations in scenarios of bucket shuffle join.
    if (this->_fragment_instance_id.lo == -1) {
        return Status::OK();
    }
    Status res = Status::OK();

    DeferOp op([&res, &fragment_ctx]() {
        if (!res.ok()) {
            LOG(WARNING) << fmt::format("fragment id {} close channel error: {}",
                                        print_id(fragment_ctx->fragment_instance_id()), res.get_error_msg());
        }
    });

    if (!fragment_ctx->is_canceled()) {
        for (auto driver_sequence = 0; driver_sequence < _chunks.size(); ++driver_sequence) {
            if (_chunks[driver_sequence] != nullptr) {
                RETURN_IF_ERROR(res = send_one_chunk(state, _chunks[driver_sequence].get(), driver_sequence, false));
            }
        }
        RETURN_IF_ERROR(res = send_one_chunk(state, nullptr, ExchangeSinkOperator::DEFAULT_DRIVER_SEQUENCE, true));
    }

    return Status::OK();
}

Status ExchangeSinkOperator::Channel::close(RuntimeState* state, FragmentContext* fragment_ctx) {
    auto status = _close_internal(state, fragment_ctx);
    state->log_error(status.get_error_msg());
    return status;
}

ExchangeSinkOperator::ExchangeSinkOperator(
        OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
        const std::shared_ptr<SinkBuffer>& buffer, TPartitionType::type part_type,
        const std::vector<TPlanFragmentDestination>& destinations, bool is_pipeline_level_shuffle,
        const int32_t num_shuffles_per_channel, int32_t sender_id, PlanNodeId dest_node_id,
        const std::vector<ExprContext*>& partition_expr_ctxs, bool enable_exchange_pass_through,
        bool enable_exchange_perf, FragmentContext* const fragment_ctx, const std::vector<int32_t>& output_columns)
        : Operator(factory, id, "exchange_sink", plan_node_id, driver_sequence),
          _buffer(buffer),
          _part_type(part_type),
          _destinations(destinations),
          _num_shuffles_per_channel(num_shuffles_per_channel > 0 ? num_shuffles_per_channel : 1),
          _sender_id(sender_id),
          _dest_node_id(dest_node_id),
          _partition_expr_ctxs(partition_expr_ctxs),
          _fragment_ctx(fragment_ctx),
          _output_columns(output_columns) {
    std::map<int64_t, int64_t> fragment_id_to_channel_index;
    RuntimeState* state = fragment_ctx->runtime_state();
    PassThroughChunkBuffer* pass_through_chunk_buffer =
            state->exec_env()->stream_mgr()->get_pass_through_chunk_buffer(state->query_id());

    _channels.reserve(destinations.size());
    std::vector<int> driver_sequence_per_channel(destinations.size(), 0);
    for (int i = 0; i < destinations.size(); ++i) {
        const auto& destination = destinations[i];
        const auto& fragment_instance_id = destination.fragment_instance_id;

        auto it = _instance_id2channel.find(fragment_instance_id.lo);
        if (it != _instance_id2channel.end()) {
            _channels.emplace_back(it->second.get());
        } else {
            std::unique_ptr<Channel> channel = std::make_unique<Channel>(
                    this, destination.brpc_server, fragment_instance_id, dest_node_id, _num_shuffles_per_channel,
                    enable_exchange_pass_through, enable_exchange_perf, pass_through_chunk_buffer);
            _channels.emplace_back(channel.get());
            _instance_id2channel.emplace(fragment_instance_id.lo, std::move(channel));
        }

        if (destination.__isset.pipeline_driver_sequence) {
            driver_sequence_per_channel[i] = destination.pipeline_driver_sequence;
            _is_channel_bound_driver_sequence = true;
        }
    }

    // Each channel is bound to a specific driver sequence for the local bucket shuffle join.
    if (_is_channel_bound_driver_sequence) {
        _num_shuffles_per_channel = 1;
        _driver_sequence_per_shuffle = std::move(driver_sequence_per_channel);
    } else {
        _driver_sequence_per_shuffle.reserve(_channels.size() * _num_shuffles_per_channel);
        for (int channel_id = 0; channel_id < _channels.size(); ++channel_id) {
            for (int i = 0; i < _num_shuffles_per_channel; ++i) {
                _driver_sequence_per_shuffle.emplace_back(i);
            }
        }
    }
    _num_shuffles = _channels.size() * _num_shuffles_per_channel;

    _is_pipeline_level_shuffle = is_pipeline_level_shuffle && (_num_shuffles > 1);

    _shuffler = std::make_unique<Shuffler>(runtime_state()->func_version() <= 3, !_is_channel_bound_driver_sequence,
                                           _part_type, _channels.size(), _num_shuffles_per_channel);
}

Status ExchangeSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));

    _buffer->incr_sinker(state);

    _be_number = state->be_number();
    if (state->query_options().__isset.transmission_encode_level) {
        _encode_level = state->query_options().transmission_encode_level;
    }
    // Set compression type according to query options
    if (state->query_options().__isset.transmission_compression_type) {
        _compress_type = CompressionUtils::to_compression_pb(state->query_options().transmission_compression_type);
    } else if (config::compress_rowbatches) {
        // If transmission_compression_type is not set, use compress_rowbatches to check if
        // compress transmitted data.
        _compress_type = CompressionTypePB::LZ4;
    }
    RETURN_IF_ERROR(get_block_compression_codec(_compress_type, &_compress_codec));

    std::string instances;
    for (const auto& channel : _channels) {
        if (instances.empty()) {
            instances = channel->get_fragment_instance_id_str();
        } else {
            instances += ", ";
            instances += channel->get_fragment_instance_id_str();
        }
    }
    _unique_metrics->add_info_string("DestID", std::to_string(_dest_node_id));
    _unique_metrics->add_info_string("DestFragments", instances);
    _unique_metrics->add_info_string("PartType", to_string(_part_type));
    _unique_metrics->add_info_string("ChannelNum", std::to_string(_channels.size()));

    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {
        _partitions_columns.resize(_partition_expr_ctxs.size());
        _unique_metrics->add_info_string("ShuffleNumPerChannel", std::to_string(_num_shuffles_per_channel));
        _unique_metrics->add_info_string("TotalShuffleNum", std::to_string(_num_shuffles));
        _unique_metrics->add_info_string("PipelineLevelShuffle", _is_pipeline_level_shuffle ? "Yes" : "No");
    }

    // Randomize the order we open/transmit to channels to avoid thundering herd problems.
    _channel_indices.resize(_channels.size());
    std::iota(_channel_indices.begin(), _channel_indices.end(), 0);
    srand(reinterpret_cast<uint64_t>(this));
    std::shuffle(_channel_indices.begin(), _channel_indices.end(), std::mt19937(std::random_device()()));

    _bytes_pass_through_counter = ADD_COUNTER(_unique_metrics, "BytesPassThrough", TUnit::BYTES);
    _uncompressed_bytes_counter = ADD_COUNTER(_unique_metrics, "UncompressedBytes", TUnit::BYTES);
    _serialize_chunk_timer = ADD_TIMER(_unique_metrics, "SerializeChunkTime");
    _shuffle_hash_timer = ADD_TIMER(_unique_metrics, "ShuffleHashTime");
    _compress_timer = ADD_TIMER(_unique_metrics, "CompressTime");

    for (auto& [_, channel] : _instance_id2channel) {
        RETURN_IF_ERROR(channel->init(state));
    }

    _shuffle_channel_ids.resize(state->chunk_size());
    _row_indexes.resize(state->chunk_size());

    return Status::OK();
}

bool ExchangeSinkOperator::is_finished() const {
    return _is_finished;
}

bool ExchangeSinkOperator::need_input() const {
    return !is_finished() && _buffer != nullptr && !_buffer->is_full();
}

bool ExchangeSinkOperator::pending_finish() const {
    return _buffer != nullptr && !_buffer->is_finished();
}

Status ExchangeSinkOperator::set_cancelled(RuntimeState* state) {
    _buffer->cancel_one_sinker(state);
    return Status::OK();
}

StatusOr<ChunkPtr> ExchangeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't call pull_chunk from exchange sink.");
}

Status ExchangeSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    uint16_t num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }
    DCHECK_LE(num_rows, state->chunk_size());

    Chunk temp_chunk;
    Chunk* send_chunk = chunk.get();
    if (!_output_columns.empty()) {
        for (int32_t cid : _output_columns) {
            temp_chunk.append_column(chunk->get_column_by_slot_id(cid), cid);
        }
        send_chunk = &temp_chunk;
    }

    if (_part_type == TPartitionType::UNPARTITIONED || _num_shuffles == 1) {
        if (_chunk_request == nullptr) {
            _chunk_request = std::make_shared<PTransmitChunkParams>();
        }

        // If we have any channel which can pass through chunks, we use `send_one_chunk`(without serialization)
        int has_not_pass_through = false;
        for (auto idx : _channel_indices) {
            if (_channels[idx]->use_pass_through()) {
                RETURN_IF_ERROR(_channels[idx]->send_one_chunk(state, send_chunk, DEFAULT_DRIVER_SEQUENCE, false));
            } else {
                has_not_pass_through = true;
            }
        }

        // And if we find if there are other channels can not pass through, we have to use old way.
        // Do serialization once, and send serialized data.
        if (has_not_pass_through) {
            // We use sender request to avoid serialize chunk many times.
            // 1. create a new chunk PB to serialize
            ChunkPB* pchunk = _chunk_request->add_chunks();
            // 2. serialize input chunk to pchunk
            TRY_CATCH_BAD_ALLOC(
                    RETURN_IF_ERROR(serialize_chunk(send_chunk, pchunk, &_is_first_chunk, _channels.size())));
            _current_request_bytes += pchunk->data().size();
            // 3. if request bytes exceede the threshold, send current request
            if (_current_request_bytes > config::max_transmit_batched_bytes) {
                butil::IOBuf attachment;
                int64_t attachment_physical_bytes = construct_brpc_attachment(_chunk_request, attachment);
                for (auto idx : _channel_indices) {
                    if (!_channels[idx]->use_pass_through()) {
                        PTransmitChunkParamsPtr copy = std::make_shared<PTransmitChunkParams>(*_chunk_request);
                        RETURN_IF_ERROR(
                                _channels[idx]->send_chunk_request(state, copy, attachment, attachment_physical_bytes));
                    }
                }
                _current_request_bytes = 0;
                _chunk_request.reset();
            }
        }
    } else if (_part_type == TPartitionType::RANDOM) {
        // Round-robin batches among channels. Wait for the current channel to finish its
        // rpc before overwriting its batch.
        // 1. Get request of that channel
        std::vector<Channel*> local_channels;
        for (const auto& channel : _channels) {
            if (channel->is_local()) {
                local_channels.emplace_back(channel);
            }
        }

        if (local_channels.empty()) {
            local_channels = _channels;
        }

        auto& channel = local_channels[_curr_random_channel_idx];
        bool real_sent = false;
        RETURN_IF_ERROR(channel->send_one_chunk(state, send_chunk, DEFAULT_DRIVER_SEQUENCE, false, &real_sent));
        if (real_sent) {
            _curr_random_channel_idx = (_curr_random_channel_idx + 1) % local_channels.size();
        }
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {
        // hash-partition batch's rows across channels
        {
            SCOPED_TIMER(_shuffle_hash_timer);
            for (size_t i = 0; i < _partitions_columns.size(); ++i) {
                ASSIGN_OR_RETURN(_partitions_columns[i], _partition_expr_ctxs[i]->evaluate(chunk.get()));
                DCHECK(_partitions_columns[i] != nullptr);
            }

            // Compute hash for each partition column
            if (_part_type == TPartitionType::HASH_PARTITIONED) {
                _hash_values.assign(num_rows, HashUtil::FNV_SEED);
                for (const ColumnPtr& column : _partitions_columns) {
                    column->fnv_hash(&_hash_values[0], 0, num_rows);
                }
            } else {
                // The data distribution was calculated using CRC32_HASH,
                // and bucket shuffle need to use the same hash function when sending data
                _hash_values.assign(num_rows, 0);
                for (const ColumnPtr& column : _partitions_columns) {
                    column->crc32_hash(&_hash_values[0], 0, num_rows);
                }
            }

            // Compute row indexes for each channel's each shuffle
            _channel_row_idx_start_points.assign(_num_shuffles + 1, 0);
            _shuffler->exchange_shuffle(_shuffle_channel_ids, _hash_values, num_rows);

            for (size_t i = 0; i < num_rows; ++i) {
                _channel_row_idx_start_points[_shuffle_channel_ids[i]]++;
            }
            // NOTE:
            // we make the last item equal with number of rows of this chunk
            for (int32_t i = 1; i <= _num_shuffles; ++i) {
                _channel_row_idx_start_points[i] += _channel_row_idx_start_points[i - 1];
            }

            for (int32_t i = num_rows - 1; i >= 0; --i) {
                _row_indexes[_channel_row_idx_start_points[_shuffle_channel_ids[i]] - 1] = i;
                _channel_row_idx_start_points[_shuffle_channel_ids[i]]--;
            }
        }

        for (int32_t channel_id : _channel_indices) {
            if (_channels[channel_id]->get_fragment_instance_id().lo == -1) {
                // dest bucket is no used, continue
                continue;
            }

            for (int32_t i = 0; i < _num_shuffles_per_channel; ++i) {
                int shuffle_id = channel_id * _num_shuffles_per_channel + i;
                int driver_sequence = _driver_sequence_per_shuffle[shuffle_id];

                size_t from = _channel_row_idx_start_points[shuffle_id];
                size_t size = _channel_row_idx_start_points[shuffle_id + 1] - from;
                if (size == 0) {
                    // no data for this channel continue;
                    continue;
                }

                RETURN_IF_ERROR(_channels[channel_id]->add_rows_selective(send_chunk, driver_sequence,
                                                                          _row_indexes.data(), from, size, state));
            }
        }
    }
    return Status::OK();
}

void ExchangeSinkOperator::update_metrics(RuntimeState* state) {
    if (_driver_sequence == 0) {
        _buffer->update_profile(_unique_metrics.get());
    }
}

Status ExchangeSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    if (_chunk_request != nullptr) {
        butil::IOBuf attachment;
        int64_t attachment_physical_bytes = construct_brpc_attachment(_chunk_request, attachment);
        for (const auto& [_, channel] : _instance_id2channel) {
            PTransmitChunkParamsPtr copy = std::make_shared<PTransmitChunkParams>(*_chunk_request);
            channel->send_chunk_request(state, copy, attachment, attachment_physical_bytes);
        }
        _current_request_bytes = 0;
        _chunk_request.reset();
    }
    Status status = Status::OK();
    for (auto& [_, channel] : _instance_id2channel) {
        auto tmp_status = channel->close(state, _fragment_ctx);
        if (!tmp_status.ok()) {
            status = tmp_status;
        }
    }

    _buffer->set_finishing();
    return status;
}

void ExchangeSinkOperator::close(RuntimeState* state) {
    if (_driver_sequence == 0) {
        _buffer->update_profile(_unique_metrics.get());
    }
    Operator::close(state);
}

Status ExchangeSinkOperator::serialize_chunk(const Chunk* src, ChunkPB* dst, bool* is_first_chunk, int num_receivers) {
    VLOG_ROW << "[ExchangeSinkOperator] serializing " << src->num_rows() << " rows";
    {
        SCOPED_TIMER(_serialize_chunk_timer);
        // We only serialize chunk meta for first chunk
        if (*is_first_chunk) {
            _encode_context = serde::EncodeContext::get_encode_context_shared_ptr(src->columns().size(), _encode_level);
            StatusOr<ChunkPB> res = Status::OK();
            TRY_CATCH_BAD_ALLOC(res = serde::ProtobufChunkSerde::serialize(*src, _encode_context));
            RETURN_IF_ERROR(res);
            res->Swap(dst);
            *is_first_chunk = false;
        } else {
            StatusOr<ChunkPB> res = Status::OK();
            TRY_CATCH_BAD_ALLOC(res = serde::ProtobufChunkSerde::serialize_without_meta(*src, _encode_context));
            RETURN_IF_ERROR(res);
            res->Swap(dst);
        }
    }
    if (_encode_context) {
        _encode_context->set_encode_levels_in_pb(dst);
    }
    DCHECK(dst->has_uncompressed_size());
    DCHECK_EQ(dst->uncompressed_size(), dst->data().size());
    const size_t uncompressed_size = dst->uncompressed_size();

    if (_compress_codec != nullptr && _compress_codec->exceed_max_input_size(uncompressed_size)) {
        return Status::InternalError(strings::Substitute("The input size for compression should be less than $0",
                                                         _compress_codec->max_input_size()));
    }

    // try compress the ChunkPB data
    if (_compress_codec != nullptr && uncompressed_size > 0) {
        SCOPED_TIMER(_compress_timer);

        if (use_compression_pool(_compress_codec->type())) {
            Slice compressed_slice;
            Slice input(dst->data());
            RETURN_IF_ERROR(_compress_codec->compress(input, &compressed_slice, true, uncompressed_size, nullptr,
                                                      &_compression_scratch));
        } else {
            int max_compressed_size = _compress_codec->max_compressed_len(uncompressed_size);

            if (_compression_scratch.size() < max_compressed_size) {
                _compression_scratch.resize(max_compressed_size);
            }

            Slice compressed_slice{_compression_scratch.data(), _compression_scratch.size()};

            Slice input(dst->data());
            RETURN_IF_ERROR(_compress_codec->compress(input, &compressed_slice));
            _compression_scratch.resize(compressed_slice.size);
        }

        double compress_ratio = (static_cast<double>(uncompressed_size)) / _compression_scratch.size();
        if (LIKELY(compress_ratio > config::rpc_compress_ratio_threshold)) {
            dst->mutable_data()->swap(reinterpret_cast<std::string&>(_compression_scratch));
            dst->set_compress_type(_compress_type);
        }

        VLOG_ROW << "uncompressed size: " << uncompressed_size << ", compressed size: " << _compression_scratch.size();
    }
    size_t chunk_size = dst->data().size();
    VLOG_ROW << "chunk data size " << chunk_size;

    COUNTER_UPDATE(_uncompressed_bytes_counter, uncompressed_size * num_receivers);
    return Status::OK();
}

int64_t ExchangeSinkOperator::construct_brpc_attachment(const PTransmitChunkParamsPtr& chunk_request,
                                                        butil::IOBuf& attachment) {
    int64_t attachment_physical_bytes = 0;
    for (int i = 0; i < chunk_request->chunks().size(); ++i) {
        auto chunk = chunk_request->mutable_chunks(i);
        chunk->set_data_size(chunk->data().size());

        int64_t before_bytes = CurrentThread::current().get_consumed_bytes();
        attachment.append(chunk->data());
        attachment_physical_bytes += CurrentThread::current().get_consumed_bytes() - before_bytes;

        chunk->clear_data();
        // If the request is too big, free the memory in order to avoid OOM
        if (_is_large_chunk(chunk->data_size())) {
            chunk->mutable_data()->shrink_to_fit();
        }
    }

    return attachment_physical_bytes;
}

ExchangeSinkOperatorFactory::ExchangeSinkOperatorFactory(
        int32_t id, int32_t plan_node_id, std::shared_ptr<SinkBuffer> buffer, TPartitionType::type part_type,
        const std::vector<TPlanFragmentDestination>& destinations, bool is_pipeline_level_shuffle,
        int32_t num_shuffles_per_channel, int32_t sender_id, PlanNodeId dest_node_id,
        std::vector<ExprContext*> partition_expr_ctxs, bool enable_exchange_pass_through, bool enable_exchange_perf,
        FragmentContext* const fragment_ctx, std::vector<int32_t> output_columns)
        : OperatorFactory(id, "exchange_sink", plan_node_id),
          _buffer(std::move(buffer)),
          _part_type(part_type),
          _destinations(destinations),
          _is_pipeline_level_shuffle(is_pipeline_level_shuffle),
          _num_shuffles_per_channel(num_shuffles_per_channel),
          _sender_id(sender_id),
          _dest_node_id(dest_node_id),
          _partition_expr_ctxs(std::move(partition_expr_ctxs)),
          _enable_exchange_pass_through(enable_exchange_pass_through),
          _enable_exchange_perf(enable_exchange_perf),
          _fragment_ctx(fragment_ctx),
          _output_columns(std::move(output_columns)) {}

OperatorPtr ExchangeSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<ExchangeSinkOperator>(
            this, _id, _plan_node_id, driver_sequence, _buffer, _part_type, _destinations, _is_pipeline_level_shuffle,
            _num_shuffles_per_channel, _sender_id, _dest_node_id, _partition_expr_ctxs, _enable_exchange_pass_through,
            _enable_exchange_perf, _fragment_ctx, _output_columns);
}

Status ExchangeSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state));
        RETURN_IF_ERROR(Expr::open(_partition_expr_ctxs, state));
    }
    return Status::OK();
}

void ExchangeSinkOperatorFactory::close(RuntimeState* state) {
    _buffer.reset();
    Expr::close(_partition_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
