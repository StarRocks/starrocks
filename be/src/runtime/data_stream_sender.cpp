// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/data_stream_sender.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/data_stream_sender.h"

#include <arpa/inet.h>
#include <fmt/format.h>

#include <functional>
#include <iostream>
#include <memory>
#include <random>

#include "column/chunk.h"
#include "common/logging.h"
#include "exprs/expr.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/Types_types.h"
#include "runtime/client_cache.h"
#include "runtime/current_thread.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/descriptors.h"
#include "runtime/dpp_sink_internal.h"
#include "runtime/exec_env.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "serde/protobuf_serde.h"
#include "service/backend_options.h"
#include "service/brpc.h"
#include "util/brpc_stub_cache.h"
#include "util/compression/block_compression.h"
#include "util/compression/compression_utils.h"
#include "util/ref_count_closure.h"
#include "util/thrift_client.h"
#include "util/uid_util.h"

namespace starrocks {

// A channel sends data asynchronously via calls to transmit_data
// to a single destination ipaddress/node.
// It has a fixed-capacity buffer and allows the caller either to add rows to
// that buffer individually (AddRow()), or circumvent the buffer altogether and send
// TRowBatches directly (SendBatch()). Either way, there can only be one in-flight RPC
// at any one time (ie, sending will block if the most recent rpc hasn't finished,
// which allows the receiver node to throttle the sender by withholding acks).
// *Not* thread-safe.
class DataStreamSender::Channel {
public:
    // Create channel to send data to particular ipaddress/port/query/node
    // combination. buffer_size is specified in bytes and a soft limit on
    // how much tuple data is getting accumulated before being sent; it only applies
    // when data is added via add_row() and not sent directly via send_batch().
    Channel(DataStreamSender* parent, const TNetworkAddress& brpc_dest, const TUniqueId& fragment_instance_id,
            PlanNodeId dest_node_id, int buffer_size, bool is_transfer_chain,
            bool send_query_statistics_with_every_batch)
            : _parent(parent),
              _fragment_instance_id(fragment_instance_id),
              _dest_node_id(dest_node_id),

              _brpc_dest_addr(brpc_dest),
              _is_transfer_chain(is_transfer_chain),
              _send_query_statistics_with_every_batch(send_query_statistics_with_every_batch) {}

    virtual ~Channel() {
        if (_closure != nullptr && _closure->unref()) {
            delete _closure;
        }

        if (_chunk_closure != nullptr && _chunk_closure->unref()) {
            delete _chunk_closure;
        }
    }

    // Initialize channel.
    // Returns OK if successful, error indication otherwise.
    Status init(RuntimeState* state);

    // Used when doing shuffle.
    // This function will copy selective rows in chunks to batch.
    // indexes contains row index of chunk and this function will copy from input
    // 'from' and copy 'size' rows
    Status add_rows_selective(RuntimeState* state, vectorized::Chunk* chunk, const uint32_t* row_indexes, uint32_t from,
                              uint32_t size);

    // Send one chunk to remote, this chunk may be batched in this channel.
    // When the chunk is sent really rather than backend, *is_real_sent will
    // be set to true.
    Status send_one_chunk(const vectorized::Chunk* chunk, bool eos, bool* is_real_sent);

    // Channel will sent input request directly without batch it.
    // This function is only used when broadcast, because request can be reused
    // by all the channels.
    Status send_chunk_request(PTransmitChunkParams* params, const butil::IOBuf& attachment);

    // Flush buffered rows and close channel. This function don't wait the response
    // of close operation, client should call close_wait() to finish channel's close.
    // We split one close operation into two phases in order to make multiple channels
    // can run parallel.
    void close(RuntimeState* state);

    // Get close wait's response, to finish channel close operation.
    void close_wait(RuntimeState* state);

    int64_t num_data_bytes_sent() const { return _num_data_bytes_sent; }

    std::string get_fragment_instance_id_str() { return print_id(_fragment_instance_id); }

    TUniqueId get_fragment_instance_id() { return _fragment_instance_id; }

private:
    inline Status _wait_prev_request() {
        SCOPED_TIMER(_parent->_wait_response_timer);
        if (_request_seq == 0) {
            return Status::OK();
        }
        auto cntl = &_chunk_closure->cntl;
        brpc::Join(cntl->call_id());
        if (cntl->Failed()) {
            LOG(WARNING) << "fail to send brpc batch, error=" << berror(cntl->ErrorCode())
                         << ", error_text=" << cntl->ErrorText();
            return Status::ThriftRpcError("fail to send batch");
        }
        return {_chunk_closure->result.status()};
    }

private:
    Status _send_current_chunk(bool eos);

    Status _do_send_chunk_rpc(PTransmitChunkParams* request, const butil::IOBuf& attachment);

    Status close_internal();

    DataStreamSender* _parent;

    TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    // the number of TRowBatch.data bytes sent successfully
    int64_t _num_data_bytes_sent{0};
    int64_t _request_seq{0};

    std::unique_ptr<vectorized::Chunk> _chunk;
    bool _is_first_chunk = true;

    bool _need_close{false};

    TNetworkAddress _brpc_dest_addr;

    // TODO(zc): initused for brpc
    PUniqueId _finst_id;

    PTransmitDataParams _brpc_request;

    // Used to transmit chunk. We use this struct in a round robin way.
    // When one request is being send, producer will construct the other one.
    // Which one is used is decided by _request_seq.
    PTransmitChunkParams _chunk_request;
    RefCountClosure<PTransmitChunkResult>* _chunk_closure = nullptr;

    size_t _current_request_bytes = 0;

    doris::PBackendService_Stub* _brpc_stub = nullptr;
    RefCountClosure<PTransmitDataResult>* _closure = nullptr;

    int32_t _brpc_timeout_ms = 500;
    // whether the dest can be treated as query statistics transfer chain.
    bool _is_transfer_chain;
    bool _send_query_statistics_with_every_batch;
    bool _is_inited = false;
};

Status DataStreamSender::Channel::init(RuntimeState* state) {
    if (_is_inited) {
        return Status::OK();
    }

    if (_brpc_dest_addr.hostname.empty()) {
        LOG(WARNING) << "there is no brpc destination address's hostname"
                        ", maybe version is not compatible.";
        return Status::InternalError("no brpc destination");
    }
    _brpc_stub = state->exec_env()->brpc_stub_cache()->get_stub(_brpc_dest_addr);
    if (UNLIKELY(_brpc_stub == nullptr)) {
        auto msg = fmt::format("The brpc stub of {}:{} is null.", _brpc_dest_addr.hostname, _brpc_dest_addr.port);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    // initialize brpc request
    _finst_id.set_hi(_fragment_instance_id.hi);
    _finst_id.set_lo(_fragment_instance_id.lo);
    *_brpc_request.mutable_finst_id() = _finst_id;
    _brpc_request.set_node_id(_dest_node_id);
    _brpc_request.set_sender_id(_parent->_sender_id);
    _brpc_request.set_be_number(_parent->_be_number);

    *_chunk_request.mutable_finst_id() = _finst_id;
    _chunk_request.set_node_id(_dest_node_id);
    _chunk_request.set_sender_id(_parent->_sender_id);
    _chunk_request.set_be_number(_parent->_be_number);

    _chunk_closure = new RefCountClosure<PTransmitChunkResult>();
    _chunk_closure->ref();

    _brpc_timeout_ms = std::min(3600, state->query_options().query_timeout) * 1000;
    // For bucket shuffle, the dest is unreachable, there is no need to establish a connection
    if (_fragment_instance_id.lo == -1) {
        _is_inited = true;
        return Status::OK();
    }

    _need_close = true;
    _is_inited = true;

    return Status::OK();
}

Status DataStreamSender::Channel::send_one_chunk(const vectorized::Chunk* chunk, bool eos, bool* is_real_sent) {
    *is_real_sent = false;

    // If chunk is not null, append it to request
    if (chunk != nullptr) {
        auto pchunk = _chunk_request.add_chunks();
        RETURN_IF_ERROR(_parent->serialize_chunk(chunk, pchunk, &_is_first_chunk));
        _current_request_bytes += pchunk->data().size();
    }

    // Try to accumulate enough bytes before sending a RPC. When eos is true we should send
    // last packet
    if (_current_request_bytes > _parent->_request_bytes_threshold || eos) {
        // NOTE: Before we send current request, we must wait last RPC's result to make sure
        // it have finished. Because in some cases, receiver depend the order of sender data.
        // We can add KeepOrder flag in Frontend to tell sender if it can send packet before
        // last RPC return. Then we can have a better pipeline. Before that we make wait last
        // RPC first.
        RETURN_IF_ERROR(_wait_prev_request());
        _chunk_request.set_eos(eos);
        // we will send the current request now
        butil::IOBuf attachment;
        _parent->construct_brpc_attachment(&_chunk_request, &attachment);
        RETURN_IF_ERROR(_do_send_chunk_rpc(&_chunk_request, attachment));
        // lets request sequence increment
        _chunk_request.clear_chunks();
        _current_request_bytes = 0;
        *is_real_sent = true;
    }

    return Status::OK();
}

Status DataStreamSender::Channel::send_chunk_request(PTransmitChunkParams* params, const butil::IOBuf& attachment) {
    RETURN_IF_ERROR(_wait_prev_request());
    *params->mutable_finst_id() = _finst_id;
    params->set_node_id(_dest_node_id);
    params->set_sender_id(_parent->_sender_id);
    params->set_be_number(_parent->_be_number);
    auto status = _do_send_chunk_rpc(params, attachment);
    return status;
}

Status DataStreamSender::Channel::_do_send_chunk_rpc(PTransmitChunkParams* request, const butil::IOBuf& attachment) {
    SCOPED_TIMER(_parent->_send_request_timer);

    request->set_sequence(_request_seq);
    if (_is_transfer_chain && (_send_query_statistics_with_every_batch || request->eos())) {
        auto statistic = request->mutable_query_statistics();
        _parent->_query_statistics->to_pb(statistic);
    }
    _chunk_closure->ref();
    _chunk_closure->cntl.Reset();
    _chunk_closure->cntl.set_timeout_ms(_brpc_timeout_ms);
    _chunk_closure->cntl.request_attachment().append(attachment);
    _brpc_stub->transmit_chunk(&_chunk_closure->cntl, request, &_chunk_closure->result, _chunk_closure);
    _request_seq++;
    return Status::OK();
}

Status DataStreamSender::Channel::add_rows_selective(RuntimeState* state, vectorized::Chunk* chunk,
                                                     const uint32_t* indexes, uint32_t from, uint32_t size) {
    // TODO(kks): find a way to remove this if condition
    if (UNLIKELY(_chunk == nullptr)) {
        _chunk = chunk->clone_empty_with_tuple();
    }

    if (_chunk->num_rows() + size > state->chunk_size()) {
        // _chunk is full, let's send it; but first wait for an ongoing
        // transmission to finish before modifying _pb_chunk
        RETURN_IF_ERROR(_send_current_chunk(false));
        DCHECK_EQ(0, _chunk->num_rows());
    }

    _chunk->append_selective(*chunk, indexes, from, size);
    return Status::OK();
}

Status DataStreamSender::Channel::_send_current_chunk(bool eos) {
    bool is_real_sent = false;
    RETURN_IF_ERROR(send_one_chunk(_chunk.get(), eos, &is_real_sent));

    // we only clear column data, because we need to reuse column schema
    for (ColumnPtr& column : _chunk->columns()) {
        column->resize(0);
    }
    return Status::OK();
}

Status DataStreamSender::Channel::close_internal() {
    if (!_need_close) {
        return Status::OK();
    }

    VLOG_RPC << "_chunk Channel::close() instance_id=" << _fragment_instance_id << " dest_node=" << _dest_node_id
             << " #rows= " << ((_chunk == nullptr) ? 0 : _chunk->num_rows());
    if (_chunk != nullptr && _chunk->num_rows() > 0) {
        RETURN_IF_ERROR(_send_current_chunk(true));
    } else {
        bool is_real_sent = false;
        RETURN_IF_ERROR(send_one_chunk(nullptr, true, &is_real_sent));
    }

    // Don't wait for the last packet to finish, left it to close_wait.
    return Status::OK();
}

void DataStreamSender::Channel::close(RuntimeState* state) {
    state->log_error(close_internal().get_error_msg());
}

void DataStreamSender::Channel::close_wait(RuntimeState* state) {
    if (_need_close) {
        auto st = _wait_prev_request();
        if (!st.ok()) {
            LOG(WARNING) << "fail to close channel, st=" << st.to_string()
                         << ", instance_id=" << print_id(_fragment_instance_id) << ", dest=" << _brpc_dest_addr.hostname
                         << ":" << _brpc_dest_addr.port;
            if (_parent->_close_status.ok()) {
                _parent->_close_status = st;
            }
        }
        _need_close = false;
    }
    _chunk.reset();
}

DataStreamSender::DataStreamSender(RuntimeState* state, int sender_id, const RowDescriptor& row_desc,
                                   const TDataStreamSink& sink,
                                   const std::vector<TPlanFragmentDestination>& destinations,
                                   int per_channel_buffer_size, bool send_query_statistics_with_every_batch,
                                   bool enable_exchange_pass_through, bool enable_exchange_perf)
        : _sender_id(sender_id),
          _state(state),
          _pool(state->obj_pool()),
          _row_desc(row_desc),
          _current_channel_idx(0),
          _part_type(sink.output_partition.type),
          _profile(nullptr),
          _serialize_chunk_timer(nullptr),
          _bytes_sent_counter(nullptr),
          _dest_node_id(sink.dest_node_id),
          _destinations(destinations),
          _enable_exchange_pass_through(enable_exchange_pass_through),
          _enable_exchange_perf(enable_exchange_perf),
          _output_columns(sink.output_columns) {
    DCHECK_GT(destinations.size(), 0);
    DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED ||
           sink.output_partition.type == TPartitionType::HASH_PARTITIONED ||
           sink.output_partition.type == TPartitionType::RANDOM ||
           sink.output_partition.type == TPartitionType::RANGE_PARTITIONED ||
           sink.output_partition.type == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED);
    // TODO: use something like google3's linked_ptr here (std::unique_ptr isn't copyable

    std::map<int64_t, int64_t> fragment_id_to_channel_index;
    for (int i = 0; i < destinations.size(); ++i) {
        // Select first dest as transfer chain.
        bool is_transfer_chain = (i == 0);
        const auto& fragment_instance_id = destinations[i].fragment_instance_id;
        if (fragment_id_to_channel_index.find(fragment_instance_id.lo) == fragment_id_to_channel_index.end()) {
            _channel_shared_ptrs.emplace_back(new Channel(this, destinations[i].brpc_server, fragment_instance_id,
                                                          sink.dest_node_id, per_channel_buffer_size, is_transfer_chain,
                                                          send_query_statistics_with_every_batch));
            fragment_id_to_channel_index.insert({fragment_instance_id.lo, _channel_shared_ptrs.size() - 1});
            _channels.push_back(_channel_shared_ptrs.back().get());
        } else {
            _channel_shared_ptrs.emplace_back(
                    _channel_shared_ptrs[fragment_id_to_channel_index[fragment_instance_id.lo]]);
            _channels.push_back(_channel_shared_ptrs.back().get());
        }
    }
    _request_bytes_threshold = config::max_transmit_batched_bytes;
}

// We use the PartitionRange to compare here. It should not be a member function of PartitionInfo
// class because there are some other member in it.
static bool compare_part_use_range(const PartitionInfo* v1, const PartitionInfo* v2) {
    return v1->range() < v2->range();
}

Status DataStreamSender::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSink::init(tsink));
    const TDataStreamSink& t_stream_sink = tsink.stream_sink;
    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(
                Expr::create_expr_trees(_pool, t_stream_sink.output_partition.partition_exprs, &_partition_expr_ctxs));
    } else if (_part_type == TPartitionType::RANGE_PARTITIONED) {
        // Range partition
        // Partition Exprs
        RETURN_IF_ERROR(
                Expr::create_expr_trees(_pool, t_stream_sink.output_partition.partition_exprs, &_partition_expr_ctxs));
        // Partition infos
        int num_parts = t_stream_sink.output_partition.partition_infos.size();
        if (num_parts == 0) {
            return Status::InternalError("Empty partition info.");
        }
        for (int i = 0; i < num_parts; ++i) {
            PartitionInfo* info = _pool->add(new PartitionInfo());
            RETURN_IF_ERROR(PartitionInfo::from_thrift(_pool, t_stream_sink.output_partition.partition_infos[i], info,
                                                       _state->chunk_size()));
            _partition_infos.push_back(info);
        }
        // partitions should be in ascending order
        std::sort(_partition_infos.begin(), _partition_infos.end(), compare_part_use_range);
    } else {
    }

    _partitions_columns.resize(_partition_expr_ctxs.size());
    return Status::OK();
}

Status DataStreamSender::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    _state = state;
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
    std::stringstream title;
    title << "DataStreamSender (dst_id=" << _dest_node_id << ", dst_fragments=[" << instances << "])";
    _profile = _pool->add(new RuntimeProfile(title.str()));
    SCOPED_TIMER(_profile->total_time_counter());
    _profile->add_info_string("PartType", _TPartitionType_VALUES_TO_NAMES.at(_part_type));

    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state));
    } else {
        RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state));
        for (auto iter : _partition_infos) {
            RETURN_IF_ERROR(iter->prepare(state, _row_desc));
        }
    }

    // Randomize the order we open/transmit to channels to avoid thundering herd problems.
    _channel_indices.resize(_channels.size());
    std::iota(_channel_indices.begin(), _channel_indices.end(), 0);
    srand(reinterpret_cast<uint64_t>(this));
    std::shuffle(_channel_indices.begin(), _channel_indices.end(), std::mt19937(std::random_device()()));

    _bytes_sent_counter = ADD_COUNTER(profile(), "BytesSent", TUnit::BYTES);
    _uncompressed_bytes_counter = ADD_COUNTER(profile(), "UncompressedBytes", TUnit::BYTES);
    _ignore_rows = ADD_COUNTER(profile(), "IgnoreRows", TUnit::UNIT);
    _serialize_chunk_timer = ADD_TIMER(profile(), "SerializeChunkTime");
    _compress_timer = ADD_TIMER(profile(), "CompressTime");
    _send_request_timer = ADD_TIMER(profile(), "SendRequestTime");
    _wait_response_timer = ADD_TIMER(profile(), "WaitResponseTime");
    _shuffle_dispatch_timer = ADD_TIMER(profile(), "ShuffleDispatchTime");
    _shuffle_hash_timer = ADD_TIMER(profile(), "ShuffleHashTime");
    _overall_throughput = profile()->add_derived_counter(
            "OverallThroughput", TUnit::BYTES_PER_SECOND,
            [capture0 = _bytes_sent_counter, capture1 = profile()->total_time_counter()] {
                return RuntimeProfile::units_per_second(capture0, capture1);
            },
            "");
    for (auto& _channel : _channels) {
        RETURN_IF_ERROR(_channel->init(state));
    }

    // set eos for all channels.
    // It will be set to true when closing.
    _chunk_request.set_eos(false);

    _row_indexes.resize(state->chunk_size());

    return Status::OK();
}

DataStreamSender::~DataStreamSender() {
    // TODO: check that sender was either already closed() or there was an error
    // on some channel
    _channel_shared_ptrs.clear();
}

Status DataStreamSender::open(RuntimeState* state) {
    // RETURN_IF_ERROR(DataSink::open(state));
    DCHECK(state != nullptr);
    RETURN_IF_ERROR(Expr::open(_partition_expr_ctxs, state));
    for (auto iter : _partition_infos) {
        RETURN_IF_ERROR(iter->open(state));
    }
    return Status::OK();
}

Status DataStreamSender::send_chunk(RuntimeState* state, vectorized::Chunk* chunk) {
    SCOPED_TIMER(_profile->total_time_counter());
    uint16_t num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }
    // Unpartition or _channel size
    if (_part_type == TPartitionType::UNPARTITIONED || _channels.size() == 1) {
        // We use sender request to avoid serialize chunk many times.
        // 1. create a new chunk PB to serialize
        ChunkPB* pchunk = _chunk_request.add_chunks();
        // 2. serialize input chunk to pchunk
        RETURN_IF_ERROR(serialize_chunk(chunk, pchunk, &_is_first_chunk, _channels.size()));
        _current_request_bytes += pchunk->data().size();
        // 3. if request bytes exceed the threshold, send current request
        if (_current_request_bytes > _request_bytes_threshold) {
            butil::IOBuf attachment;
            construct_brpc_attachment(&_chunk_request, &attachment);
            for (auto idx : _channel_indices) {
                RETURN_IF_ERROR(_channels[idx]->send_chunk_request(&_chunk_request, attachment));
            }
            _current_request_bytes = 0;
            _chunk_request.clear_chunks();
        }
    } else if (_part_type == TPartitionType::RANDOM) {
        // Round-robin batches among channels. Wait for the current channel to finish its
        // rpc before overwriting its batch.
        // 1. Get request of that channel
        Channel* channel = _channels[_channel_indices[_current_channel_idx]];
        bool real_sent = false;
        RETURN_IF_ERROR(channel->send_one_chunk(chunk, false, &real_sent));
        if (real_sent) {
            _current_channel_idx = (_current_channel_idx + 1) % _channels.size();
        }
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED) {
        SCOPED_TIMER(_shuffle_dispatch_timer);
        // hash-partition batch's rows across channels
        int num_channels = _channels.size();

        {
            SCOPED_TIMER(_shuffle_hash_timer);
            for (size_t i = 0; i < _partitions_columns.size(); ++i) {
                ASSIGN_OR_RETURN(_partitions_columns[i], _partition_expr_ctxs[i]->evaluate(chunk));
                DCHECK(_partitions_columns[i] != nullptr);
            }

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

            // compute row indexes for each channel
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
            RETURN_IF_ERROR(_channels[i]->add_rows_selective(state, chunk, _row_indexes.data(), from, size));
        }
    } else {
        DCHECK(false) << "shouldn't go to here";
    }

    return Status::OK();
}

Status DataStreamSender::close(RuntimeState* state, Status exec_status) {
    RETURN_IF_ERROR(DataSink::close(state, exec_status));
    ScopedTimer<MonotonicStopWatch> close_timer(_profile != nullptr ? _profile->total_time_counter() : nullptr);
    // TODO: only close channels that didn't have any errors
    // make all channels close parallel

    // If broadcast is used, _chunk_request may contain some data which should
    // be sent to receiver.
    if (_current_request_bytes > 0) {
        _chunk_request.set_eos(true);
        butil::IOBuf attachment;
        construct_brpc_attachment(&_chunk_request, &attachment);
        for (auto& _channel : _channels) {
            _channel->send_chunk_request(&_chunk_request, attachment);
        }
    } else {
        for (auto& _channel : _channels) {
            _channel->close(state);
        }
    }

    // wait all channels to finish
    for (auto& _channel : _channels) {
        _channel->close_wait(state);
    }
    for (auto iter : _partition_infos) {
        auto st = iter->close(state);
        if (!st.ok()) {
            LOG(WARNING) << "fail to close sender partition, st=" << st.to_string();
            if (_close_status.ok()) {
                _close_status = st;
            }
        }
    }
    Expr::close(_partition_expr_ctxs, state);

    return _close_status;
}

Status DataStreamSender::serialize_chunk(const vectorized::Chunk* src, ChunkPB* dst, bool* is_first_chunk,
                                         int num_receivers) {
    VLOG_ROW << "serializing " << src->num_rows() << " rows";

    {
        SCOPED_TIMER(_serialize_chunk_timer);
        // We only serialize chunk meta for first chunk
        if (*is_first_chunk) {
            StatusOr<ChunkPB> res = Status::OK();
            TRY_CATCH_BAD_ALLOC(res = serde::ProtobufChunkSerde::serialize(*src));
            if (!res.ok()) return res.status();
            res->Swap(dst);
            *is_first_chunk = false;
        } else {
            StatusOr<ChunkPB> res = Status::OK();
            TRY_CATCH_BAD_ALLOC(res = serde::ProtobufChunkSerde::serialize_without_meta(*src));
            if (!res.ok()) return res.status();
            res->Swap(dst);
        }
    }
    DCHECK(dst->has_uncompressed_size());
    DCHECK_EQ(dst->uncompressed_size(), dst->data().size());

    size_t uncompressed_size = dst->uncompressed_size();

    if (_compress_codec != nullptr && _compress_codec->exceed_max_input_size(uncompressed_size)) {
        return Status::InternalError(fmt::format("The input size for compression should be less than {}",
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

    COUNTER_UPDATE(_bytes_sent_counter, chunk_size * num_receivers);
    COUNTER_UPDATE(_uncompressed_bytes_counter, uncompressed_size * num_receivers);
    return Status::OK();
}

void DataStreamSender::construct_brpc_attachment(PTransmitChunkParams* params, butil::IOBuf* attachment) {
    for (int i = 0; i < params->chunks().size(); ++i) {
        auto chunk = params->mutable_chunks(i);
        chunk->set_data_size(chunk->data().size());
        attachment->append(chunk->data());
        chunk->clear_data();
    }
}

int64_t DataStreamSender::get_num_data_bytes_sent() const {
    // TODO: do we need synchronization here or are reads & writes to 8-byte ints
    // atomic?
    int64_t result = 0;

    for (auto _channel : _channels) {
        result += _channel->num_data_bytes_sent();
    }

    return result;
}

} // namespace starrocks
