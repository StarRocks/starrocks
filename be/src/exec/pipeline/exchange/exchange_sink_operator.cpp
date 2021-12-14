// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/exchange/exchange_sink_operator.h"

#include <arpa/inet.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <boost/thread/thread.hpp>
#include <functional>
#include <iostream>
#include <memory>
#include <random>

#include "exec/pipeline/exchange/sink_buffer.h"
#include "exprs/expr.h"
#include "gen_cpp/Types_types.h"
#include "runtime/client_cache.h"
#include "runtime/descriptors.h"
#include "runtime/dpp_sink_internal.h"
#include "runtime/exec_env.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "service/brpc.h"
#include "util/block_compression.h"
#include "util/compression_utils.h"
#include "util/debug_util.h"
#include "util/network_util.h"
#include "util/thrift_client.h"
#include "util/thrift_util.h"

namespace starrocks::pipeline {

class ExchangeSinkOperator::Channel {
public:
    // Create channel to send data to particular ipaddress/port/query/node
    // combination. buffer_size is specified in bytes and a soft limit on
    // how much tuple data is getting accumulated before being sent; it only applies
    // when data is added via add_row() and not sent directly via send_batch().
    Channel(ExchangeSinkOperator* parent, const TNetworkAddress& brpc_dest, const TUniqueId& fragment_instance_id,
            PlanNodeId dest_node_id, size_t channel_id)
            : _parent(parent),
              _fragment_instance_id(fragment_instance_id),
              _dest_node_id(dest_node_id),
              _brpc_dest_addr(brpc_dest),
              _channel_id(channel_id) {}

    // Initialize channel.
    // Returns OK if successful, error indication otherwise.
    Status init(RuntimeState* state);

    // Send one chunk to remote, this chunk may be batched in this channel.
    Status send_one_chunk(const vectorized::Chunk* chunk, bool eos);

    // Send one chunk to remote, this chunk may be batched in this channel.
    // When the chunk is sent really rather than bachend, *is_real_sent will
    // be set to true.
    Status send_one_chunk(const vectorized::Chunk* chunk, bool eos, bool* is_real_sent);

    // Channel will sent input request directly without batch it.
    // This function is only used when broadcast, because request can be reused
    // by all the channels.
    Status send_chunk_request(PTransmitChunkParamsPtr chunk_request, const butil::IOBuf& attachment);

    // Used when doing shuffle.
    // This function will copy selective rows in chunks to batch.
    // indexes contains row index of chunk and this function will copy from input
    // 'from' and copy 'size' rows
    Status add_rows_selective(vectorized::Chunk* chunk, const uint32_t* row_indexes, uint32_t from, uint32_t size);

    // Flush buffered rows and close channel. This function don't wait the response
    // of close operation, client should call close_wait() to finish channel's close.
    // We split one close operation into two phases in order to make multiple channels
    // can run parallel.
    void close(RuntimeState* state, FragmentContext* fragment_ctx);

    std::string get_fragment_instance_id_str() {
        UniqueId uid(_fragment_instance_id);
        return uid.to_string();
    }

    TUniqueId get_fragment_instance_id() { return _fragment_instance_id; }

private:
    Status _close_internal(RuntimeState* state, FragmentContext* fragment_ctx);

    ExchangeSinkOperator* _parent;

    TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    std::unique_ptr<vectorized::Chunk> _chunk;
    bool _is_first_chunk = true;

    TNetworkAddress _brpc_dest_addr;

    PTransmitChunkParamsPtr _chunk_request;

    doris::PBackendService_Stub* _brpc_stub = nullptr;

    size_t _current_request_bytes = 0;

    bool _is_inited = false;
    size_t _channel_id;
};

Status ExchangeSinkOperator::Channel::init(RuntimeState* state) {
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

    _is_inited = true;
    return Status::OK();
}

Status ExchangeSinkOperator::Channel::add_rows_selective(vectorized::Chunk* chunk, const uint32_t* indexes,
                                                         uint32_t from, uint32_t size) {
    if (UNLIKELY(_chunk == nullptr)) {
        _chunk = chunk->clone_empty_with_tuple();
    }

    if (_chunk->num_rows() + size > config::vector_chunk_size) {
        RETURN_IF_ERROR(send_one_chunk(_chunk.get(), false));
        // we only clear column data, because we need to reuse column schema
        _chunk->set_num_rows(0);
    }

    _chunk->append_selective(*chunk, indexes, from, size);
    return Status::OK();
}

Status ExchangeSinkOperator::Channel::send_one_chunk(const vectorized::Chunk* chunk, bool eos) {
    bool is_real_sent = false;
    return send_one_chunk(chunk, eos, &is_real_sent);
}

Status ExchangeSinkOperator::Channel::send_one_chunk(const vectorized::Chunk* chunk, bool eos, bool* is_real_sent) {
    *is_real_sent = false;
    if (_chunk_request == nullptr) {
        _chunk_request = std::make_shared<PTransmitChunkParams>();
        _chunk_request->set_node_id(_dest_node_id);
        _chunk_request->set_sender_id(_parent->_sender_id);
        _chunk_request->set_be_number(_parent->_be_number);
    }

    // If chunk is not null, append it to request
    if (chunk != nullptr) {
        auto pchunk = _chunk_request->add_chunks();
        RETURN_IF_ERROR(_parent->serialize_chunk(chunk, pchunk, &_is_first_chunk));
        _current_request_bytes += pchunk->data().size();
    }

    // Try to accumulate enough bytes before sending a RPC. When eos is true we should send
    // last packet
    if (_current_request_bytes > _parent->_request_bytes_threshold || eos) {
        _chunk_request->set_eos(eos);
        butil::IOBuf attachment;
        _parent->construct_brpc_attachment(_chunk_request, attachment);
        TransmitChunkInfo info = {this->_fragment_instance_id, _brpc_stub, std::move(_chunk_request), attachment};
        _parent->_buffer->add_request(info);
        _current_request_bytes = 0;
        _chunk_request.reset();
        *is_real_sent = true;
    }

    return Status::OK();
}

Status ExchangeSinkOperator::Channel::send_chunk_request(PTransmitChunkParamsPtr chunk_request,
                                                         const butil::IOBuf& attachment) {
    chunk_request->set_node_id(_dest_node_id);
    chunk_request->set_sender_id(_parent->_sender_id);
    chunk_request->set_be_number(_parent->_be_number);
    chunk_request->set_eos(false);

    TransmitChunkInfo info = {this->_fragment_instance_id, _brpc_stub, std::move(chunk_request), attachment};
    _parent->_buffer->add_request(info);

    return Status::OK();
}

Status ExchangeSinkOperator::Channel::_close_internal(RuntimeState* state, FragmentContext* fragment_ctx) {
    // no need to send EOS packet to pseudo destinations in scenarios of bucket shuffle join.
    if (this->_fragment_instance_id.lo == -1) {
        return Status::OK();
    }

    if (!fragment_ctx->is_canceled()) {
        RETURN_IF_ERROR(send_one_chunk(_chunk != nullptr ? _chunk.get() : nullptr, true));
    }

    return Status::OK();
}

// TODO(lzh): Remove fragment_ctx from parameters.
//  Cancel flag should probably be owned by RuntimeState instead of FragmentContext.
void ExchangeSinkOperator::Channel::close(RuntimeState* state, FragmentContext* fragment_ctx) {
    state->log_error(_close_internal(state, fragment_ctx).get_error_msg());
}

ExchangeSinkOperator::ExchangeSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                           const std::shared_ptr<SinkBuffer>& buffer, TPartitionType::type part_type,
                                           const std::vector<TPlanFragmentDestination>& destinations, int sender_id,
                                           PlanNodeId dest_node_id,
                                           const std::vector<ExprContext*>& partition_expr_ctxs,
                                           FragmentContext* const fragment_ctx)
        : Operator(factory, id, "exchange_sink", plan_node_id),
          _buffer(buffer),
          _part_type(part_type),
          _destinations(destinations),
          _sender_id(sender_id),
          _dest_node_id(dest_node_id),
          _partition_expr_ctxs(partition_expr_ctxs),
          _fragment_ctx(fragment_ctx) {
    std::map<int64_t, int64_t> fragment_id_to_channel_index;
    // fragment_instance_id.lo == -1 indicates that the destination is pseudo for bucket shuffle join.
    std::optional<std::shared_ptr<Channel>> pseudo_channel;
    for (const auto& destination : destinations) {
        const auto& fragment_instance_id = destination.fragment_instance_id;
        if (fragment_instance_id.lo == -1 && pseudo_channel.has_value()) {
            _channels.emplace_back(pseudo_channel.value());
        } else {
            const auto channel_id = _channels.size();
            _channels.emplace_back(
                    new Channel(this, destination.brpc_server, fragment_instance_id, dest_node_id, channel_id));
            if (fragment_instance_id.lo == -1) {
                pseudo_channel = _channels.back();
            }
        }
    }
}

Status ExchangeSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));

    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _be_number = state->be_number();

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
    _runtime_profile->add_info_string("DestID", std::to_string(_dest_node_id));
    _runtime_profile->add_info_string("DestFragments", instances);
    _runtime_profile->add_info_string("PartType", _TPartitionType_VALUES_TO_NAMES.at(_part_type));

    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        _partitions_columns.resize(_partition_expr_ctxs.size());
    }

    // Randomize the order we open/transmit to channels to avoid thundering herd problems.
    _channel_indices.resize(_channels.size());
    std::iota(_channel_indices.begin(), _channel_indices.end(), 0);
    srand(reinterpret_cast<uint64_t>(this));
    std::shuffle(_channel_indices.begin(), _channel_indices.end(), std::mt19937(std::random_device()()));

    _bytes_sent_counter = ADD_COUNTER(_runtime_profile, "BytesSent", TUnit::BYTES);
    _uncompressed_bytes_counter = ADD_COUNTER(_runtime_profile, "UncompressedBytes", TUnit::BYTES);
    _ignore_rows = ADD_COUNTER(_runtime_profile, "IgnoreRows", TUnit::UNIT);
    _serialize_batch_timer = ADD_TIMER(_runtime_profile, "SerializeBatchTime");
    _compress_timer = ADD_TIMER(_runtime_profile, "CompressTime");
    _send_request_timer = ADD_TIMER(_runtime_profile, "SendRequestTime");
    _wait_response_timer = ADD_TIMER(_runtime_profile, "WaitResponseTime");
    _overall_throughput = _runtime_profile->add_derived_counter(
            "OverallThroughput", TUnit::BYTES_PER_SECOND,
            [capture0 = _bytes_sent_counter, capture1 = _runtime_profile->total_time_counter()] {
                return RuntimeProfile::units_per_second(capture0, capture1);
            },
            "");
    for (auto& _channel : _channels) {
        RETURN_IF_ERROR(_channel->init(state));
    }

    _row_indexes.resize(config::vector_chunk_size);

    return Status::OK();
}

bool ExchangeSinkOperator::is_finished() const {
    return _is_finished;
}

bool ExchangeSinkOperator::need_input() const {
    return !is_finished() && !_buffer->is_full();
}

bool ExchangeSinkOperator::pending_finish() const {
    return !_buffer->is_finished();
}

void ExchangeSinkOperator::set_cancelled(RuntimeState* state) {
    _buffer->cancel_one_sinker();
}

StatusOr<vectorized::ChunkPtr> ExchangeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't call pull_chunk from exchange sink.");
}

Status ExchangeSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    uint16_t num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }
    if (_part_type == TPartitionType::UNPARTITIONED || _channels.size() == 1) {
        if (_chunk_request == nullptr) {
            _chunk_request = std::make_shared<PTransmitChunkParams>();
        }

        // We use sender request to avoid serialize chunk many times.
        // 1. create a new chunk PB to serialize
        ChunkPB* pchunk = _chunk_request->add_chunks();
        // 2. serialize input chunk to pchunk
        RETURN_IF_ERROR(serialize_chunk(chunk.get(), pchunk, &_is_first_chunk, _channels.size()));
        _current_request_bytes += pchunk->data().size();
        // 3. if request bytes exceede the threshold, send current request
        if (_current_request_bytes > _request_bytes_threshold) {
            butil::IOBuf attachment;
            construct_brpc_attachment(_chunk_request, attachment);
            for (auto idx : _channel_indices) {
                PTransmitChunkParamsPtr copy = std::make_shared<PTransmitChunkParams>(*_chunk_request);
                RETURN_IF_ERROR(_channels[idx]->send_chunk_request(copy, attachment));
            }
            _current_request_bytes = 0;
            _chunk_request.reset();
        }
    } else if (_part_type == TPartitionType::RANDOM) {
        // Round-robin batches among channels. Wait for the current channel to finish its
        // rpc before overwriting its batch.
        // 1. Get request of that channel
        auto& channel = _channels[_curr_random_channel_idx];
        bool real_sent = false;
        RETURN_IF_ERROR(channel->send_one_chunk(chunk.get(), false, &real_sent));
        if (real_sent) {
            _curr_random_channel_idx = (_curr_random_channel_idx + 1) % _channels.size();
        }
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        // hash-partition batch's rows across channels
        int num_channels = _channels.size();
        {
            // SCOPED_TIMER(_shuffle_hash_timer);
            for (size_t i = 0; i < _partitions_columns.size(); ++i) {
                _partitions_columns[i] = _partition_expr_ctxs[i]->evaluate(chunk.get());
                DCHECK(_partitions_columns[i] != nullptr);
            }

            if (_part_type == TPartitionType::HASH_PARTITIONED) {
                _hash_values.assign(num_rows, HashUtil::FNV_SEED);
                for (const vectorized::ColumnPtr& column : _partitions_columns) {
                    column->fnv_hash(&_hash_values[0], 0, num_rows);
                }
            } else {
                // The data distribution was calculated using CRC32_HASH,
                // and bucket shuffle need to use the same hash function when sending data
                _hash_values.assign(num_rows, 0);
                for (const vectorized::ColumnPtr& column : _partitions_columns) {
                    column->crc32_hash(&_hash_values[0], 0, num_rows);
                }
            }

            // Compute row indexes for each channel
            _channel_row_idx_start_points.assign(num_channels + 1, 0);
            for (uint16_t i = 0; i < num_rows; ++i) {
                uint16_t channel_index = _hash_values[i] % num_channels;
                _channel_row_idx_start_points[channel_index]++;
                _hash_values[i] = channel_index;
            }
            // NOTE:
            // we make the last item equal with number of rows of this chunk
            for (int i = 1; i <= num_channels; ++i) {
                _channel_row_idx_start_points[i] += _channel_row_idx_start_points[i - 1];
            }

            for (int i = num_rows - 1; i >= 0; --i) {
                _row_indexes[_channel_row_idx_start_points[_hash_values[i]] - 1] = i;
                _channel_row_idx_start_points[_hash_values[i]]--;
            }
        }

        for (int i : _channel_indices) {
            size_t from = _channel_row_idx_start_points[i];
            size_t size = _channel_row_idx_start_points[i + 1] - from;
            if (size == 0) {
                // no data for this channel continue;
                continue;
            }

            if (_channels[i]->get_fragment_instance_id().lo == -1) {
                // dest bucket is no used, continue
                continue;
            }
            RETURN_IF_ERROR(_channels[i]->add_rows_selective(chunk.get(), _row_indexes.data(), from, size));
        }
    }
    return Status::OK();
}

void ExchangeSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    if (_chunk_request != nullptr) {
        butil::IOBuf attachment;
        construct_brpc_attachment(_chunk_request, attachment);
        for (const auto& channel : _channels) {
            PTransmitChunkParamsPtr copy = std::make_shared<PTransmitChunkParams>(*_chunk_request);
            channel->send_chunk_request(copy, attachment);
        }
        _current_request_bytes = 0;
        _chunk_request.reset();
    }

    for (auto& _channel : _channels) {
        _channel->close(state, _fragment_ctx);
    }
}

Status ExchangeSinkOperator::close(RuntimeState* state) {
    ScopedTimer<MonotonicStopWatch> close_timer(_runtime_profile != nullptr ? _runtime_profile->total_time_counter()
                                                                            : nullptr);
    Operator::close(state);
    return _close_status;
}

Status ExchangeSinkOperator::serialize_chunk(const vectorized::Chunk* src, ChunkPB* dst, bool* is_first_chunk,
                                             int num_receivers) {
    VLOG_ROW << "[ExchangeSinkOperator] serializing " << src->num_rows() << " rows";
    size_t uncompressed_size = 0;
    {
        SCOPED_TIMER(_serialize_batch_timer);
        dst->set_compress_type(CompressionTypePB::NO_COMPRESSION);
        // We only serialize chunk meta for first chunk
        if (*is_first_chunk) {
            uncompressed_size = src->serialize_with_meta(dst);
            *is_first_chunk = false;
        } else {
            dst->clear_is_nulls();
            dst->clear_is_consts();
            dst->clear_slot_id_map();
            uncompressed_size = src->serialize_size();
            // TODO(kks): resize without initializing the new bytes
            dst->mutable_data()->resize(uncompressed_size);
            src->serialize((uint8_t*)dst->mutable_data()->data());
        }
    }

    if (_compress_codec != nullptr && _compress_codec->exceed_max_input_size(uncompressed_size)) {
        return Status::InternalError("The input size for compression should be less than " +
                                     _compress_codec->max_input_size());
    }

    dst->set_uncompressed_size(uncompressed_size);
    // try compress the ChunkPB data
    if (_compress_codec != nullptr && uncompressed_size > 0) {
        SCOPED_TIMER(_compress_timer);

        // Try compressing data to _compression_scratch, swap if compressed data is smaller
        int max_compressed_size = _compress_codec->max_compressed_len(uncompressed_size);

        if (_compression_scratch.size() < max_compressed_size) {
            _compression_scratch.resize(max_compressed_size);
        }

        Slice compressed_slice{_compression_scratch.data(), _compression_scratch.size()};
        _compress_codec->compress(dst->data(), &compressed_slice);
        double compress_ratio = (static_cast<double>(uncompressed_size)) / compressed_slice.size;
        if (LIKELY(compress_ratio > config::rpc_compress_ratio_threshold)) {
            _compression_scratch.resize(compressed_slice.size);
            dst->mutable_data()->swap(reinterpret_cast<std::string&>(_compression_scratch));
            dst->set_compress_type(_compress_type);
        }

        VLOG_ROW << "uncompressed size: " << uncompressed_size << ", compressed size: " << compressed_slice.size;
    }
    size_t chunk_size = dst->data().size();
    VLOG_ROW << "chunk data size " << chunk_size;

    COUNTER_UPDATE(_bytes_sent_counter, chunk_size * num_receivers);
    COUNTER_UPDATE(_uncompressed_bytes_counter, uncompressed_size * num_receivers);
    return Status::OK();
}

void ExchangeSinkOperator::construct_brpc_attachment(PTransmitChunkParamsPtr chunk_request, butil::IOBuf& attachment) {
    for (int i = 0; i < chunk_request->chunks().size(); ++i) {
        auto chunk = chunk_request->mutable_chunks(i);
        chunk->set_data_size(chunk->data().size());
        attachment.append(chunk->data());
        chunk->clear_data();
    }
}

ExchangeSinkOperatorFactory::ExchangeSinkOperatorFactory(
        int32_t id, int32_t plan_node_id, std::shared_ptr<SinkBuffer> buffer, TPartitionType::type part_type,
        const std::vector<TPlanFragmentDestination>& destinations, int sender_id, PlanNodeId dest_node_id,
        std::vector<ExprContext*> partition_expr_ctxs, FragmentContext* const fragment_ctx)
        : OperatorFactory(id, "exchange_sink", plan_node_id),
          _buffer(std::move(buffer)),
          _part_type(part_type),
          _destinations(destinations),
          _sender_id(sender_id),
          _dest_node_id(dest_node_id),
          _partition_expr_ctxs(std::move(partition_expr_ctxs)),
          _fragment_ctx(fragment_ctx) {}

OperatorPtr ExchangeSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<ExchangeSinkOperator>(this, _id, _plan_node_id, _buffer, _part_type, _destinations,
                                                  _sender_id, _dest_node_id, _partition_expr_ctxs, _fragment_ctx);
}

Status ExchangeSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state, _row_desc));
        RETURN_IF_ERROR(Expr::open(_partition_expr_ctxs, state));
    }
    return Status::OK();
}

void ExchangeSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_partition_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
