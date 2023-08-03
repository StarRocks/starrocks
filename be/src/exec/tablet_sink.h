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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/tablet_sink.h

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

#pragma once

#include <memory>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "common/tracer.h"
#include "exec/data_sink.h"
#include "exec/tablet_info.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/mem_tracker.h"
#include "util/bitmap.h"
#include "util/compression/block_compression.h"
#include "util/raw_container.h"
#include "util/ref_count_closure.h"
#include "util/reusable_closure.h"
#include "util/threadpool.h"

namespace starrocks {

class Bitmap;
class MemTracker;
class RuntimeProfile;
class RowDescriptor;
class TupleDescriptor;
class ExprContext;
class TExpr;

namespace stream_load {

class OlapTableSink;

// The counter of add_batch rpc of a single node
struct AddBatchCounter {
    // total execution time of a add_batch rpc
    int64_t add_batch_execution_time_us = 0;
    // lock waiting time in a add_batch rpc
    int64_t add_batch_wait_lock_time_us = 0;
    // number of add_batch call
    int64_t add_batch_num = 0;
    // total time of client rpc
    int64_t client_prc_time_us = 0;

    AddBatchCounter& operator+=(const AddBatchCounter& rhs) {
        add_batch_execution_time_us += rhs.add_batch_execution_time_us;
        add_batch_wait_lock_time_us += rhs.add_batch_wait_lock_time_us;
        add_batch_num += rhs.add_batch_num;
        return *this;
    }
    friend AddBatchCounter operator+(const AddBatchCounter& lhs, const AddBatchCounter& rhs) {
        AddBatchCounter sum = lhs;
        sum += rhs;
        return sum;
    }
};

class NodeChannel {
public:
    NodeChannel(OlapTableSink* parent, int64_t node_id, bool is_incremental);
    ~NodeChannel() noexcept;

    // called before open, used to add tablet loacted in this backend
    void add_tablet(const int64_t index_id, const PTabletWithPartition& tablet) {
        _index_tablets_map[index_id].emplace_back(tablet);
    }

    Status init(RuntimeState* state);

    // async open interface: try_open() -> [is_open_done()] -> open_wait()
    // if is_open_done() return true, open_wait() will not block
    // otherwise open_wait() will block
    void try_open();
    void try_incremental_open();
    bool is_open_done();
    Status open_wait();

    // async add chunk interface
    // if is_full() return false, add_chunk() will not block
    bool is_full();

    Status add_chunk(Chunk* input, const std::vector<int64_t>& tablet_ids, const std::vector<uint32_t>& indexes,
                     uint32_t from, uint32_t size);

    Status add_chunks(Chunk* input, const std::vector<std::vector<int64_t>>& tablet_ids,
                      const std::vector<uint32_t>& indexes, uint32_t from, uint32_t size);

    // async close interface: try_close() -> [is_close_done()] -> close_wait()
    // if is_close_done() return true, close_wait() will not block
    // otherwise close_wait() will block
    Status try_close(bool wait_all_sender_close = false);
    bool is_close_done();
    Status close_wait(RuntimeState* state);

    void cancel(const Status& err_st);

    void time_report(std::unordered_map<int64_t, AddBatchCounter>* add_batch_counter_map, int64_t* serialize_batch_ns,
                     int64_t* actual_consume_ns) {
        (*add_batch_counter_map)[_node_id] += _add_batch_counter;
        *serialize_batch_ns += _serialize_batch_ns;
        *actual_consume_ns += _actual_consume_ns;
    }

    int64_t node_id() const { return _node_id; }
    const NodeInfo* node_info() const { return _node_info; }
    std::string print_load_info() const { return _load_info; }
    std::string name() const { return _name; }
    bool enable_colocate_mv_index() const { return _enable_colocate_mv_index; }

    bool is_incremental() const { return _is_incremental; }

private:
    Status _wait_request(ReusableClosure<PTabletWriterAddBatchResult>* closure);
    Status _wait_all_prev_request();
    Status _wait_one_prev_request();
    bool _check_prev_request_done();
    bool _check_all_prev_request_done();
    Status _serialize_chunk(const Chunk* src, ChunkPB* dst);
    void _open(int64_t index_id, RefCountClosure<PTabletWriterOpenResult>* open_closure,
               std::vector<PTabletWithPartition>& tablets, bool incrmental_open);
    Status _open_wait(RefCountClosure<PTabletWriterOpenResult>* open_closure);
    Status _send_request(bool eos, bool wait_all_sender_close = false);
    void _cancel(int64_t index_id, const Status& err_st);

    std::unique_ptr<MemTracker> _mem_tracker = nullptr;

    OlapTableSink* _parent = nullptr;
    int64_t _node_id = -1;
    std::string _load_info;
    std::string _name;

    TupleDescriptor* _tuple_desc = nullptr;
    const NodeInfo* _node_info = nullptr;

    CompressionTypePB _compress_type = CompressionTypePB::NO_COMPRESSION;
    const BlockCompressionCodec* _compress_codec = nullptr;
    raw::RawString _compression_scratch;

    // this should be set in init() using config
    int _rpc_timeout_ms = 60000;
    int64_t _next_packet_seq = 0;

    // user cancel or get some errors
    bool _cancelled{false};

    // send finished means the consumer thread which send the rpc can exit
    bool _send_finished{false};

    std::unique_ptr<RowDescriptor> _row_desc;

    doris::PBackendService_Stub* _stub = nullptr;
    std::vector<RefCountClosure<PTabletWriterOpenResult>*> _open_closures;

    std::map<int64_t, std::vector<PTabletWithPartition>> _index_tablets_map;

    std::vector<TTabletCommitInfo> _tablet_commit_infos;
    std::vector<TTabletFailInfo> _tablet_fail_infos;
    struct {
        std::unordered_set<std::string> invalid_dict_cache_column_set;
        std::unordered_map<std::string, int64_t> valid_dict_cache_column_set;
    } _valid_dict_cache_info;

    AddBatchCounter _add_batch_counter;
    int64_t _serialize_batch_ns = 0;

    size_t _max_parallel_request_size = 1;
    std::vector<ReusableClosure<PTabletWriterAddBatchResult>*> _add_batch_closures;
    std::unique_ptr<Chunk> _cur_chunk;

    PTabletWriterAddChunksRequest _rpc_request;
    using AddMultiChunkReq = std::pair<std::unique_ptr<Chunk>, PTabletWriterAddChunksRequest>;
    std::deque<AddMultiChunkReq> _request_queue;

    size_t _current_request_index = 0;
    size_t _max_request_queue_size = 8;

    int64_t _actual_consume_ns = 0;
    Status _err_st = Status::OK();

    RuntimeState* _runtime_state = nullptr;

    bool _enable_colocate_mv_index = config::enable_load_colocate_mv;

    WriteQuorumTypePB _write_quorum_type = WriteQuorumTypePB::MAJORITY;

    bool _is_incremental;
};

class IndexChannel {
public:
    IndexChannel(OlapTableSink* parent, int64_t index_id) : _parent(parent), _index_id(index_id) {}
    ~IndexChannel();

    Status init(RuntimeState* state, const std::vector<PTabletWithPartition>& tablets, bool is_incremental);

    void for_each_node_channel(const std::function<void(NodeChannel*)>& func) {
        for (auto& it : _node_channels) {
            func(it.second.get());
        }
    }

    void for_each_initial_node_channel(const std::function<void(NodeChannel*)>& func) {
        for (auto& it : _node_channels) {
            if (!it.second->is_incremental()) {
                func(it.second.get());
            }
        }
    }

    void for_each_incremental_node_channel(const std::function<void(NodeChannel*)>& func) {
        for (auto& it : _node_channels) {
            if (it.second->is_incremental()) {
                func(it.second.get());
            }
        }
    }

    void mark_as_failed(const NodeChannel* ch) { _failed_channels.insert(ch->node_id()); }

    bool is_failed_channel(const NodeChannel* ch) { return _failed_channels.count(ch->node_id()) != 0; }

    bool has_intolerable_failure();

    bool has_incremental_node_channel() const { return _has_incremental_node_channel; }

private:
    friend class OlapTableSink;
    OlapTableSink* _parent;
    int64_t _index_id;

    // BeId -> channel
    std::unordered_map<int64_t, std::unique_ptr<NodeChannel>> _node_channels;
    // map tablet_id to backend id
    std::unordered_map<int64_t, std::vector<int64_t>> _tablet_to_be;
    // map be_id to tablet num
    std::unordered_map<int64_t, int64_t> _be_to_tablet_num;
    // BeId
    std::set<int64_t> _failed_channels;

    TWriteQuorumType::type _write_quorum_type = TWriteQuorumType::MAJORITY;

    bool _has_incremental_node_channel = false;
};

// Write data to Olap Table.
// When OlapTableSink::open() called, there will be a consumer thread running in the background.
// When you call OlapTableSink::send(), you will be the productor who products pending batches.
// Join the consumer thread in close().
class OlapTableSink : public DataSink {
public:
    // Construct from thrift struct which is generated by FE.
    OlapTableSink(ObjectPool* pool, const std::vector<TExpr>& texprs, Status* status, RuntimeState* state);
    ~OlapTableSink() override = default;

    Status init(const TDataSink& sink, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    void cancel() override;

    // sync open interface
    Status open(RuntimeState* state) override;

    // async open interface: try_open() -> [is_open_done()] -> open_wait()
    // if is_open_done() return true, open_wait() will not block
    // otherwise open_wait() will block
    Status try_open(RuntimeState* state);

    bool is_open_done();

    Status open_wait();

    // async add chunk interface
    // if is_full() return false, add_chunk() will not block
    Status send_chunk(RuntimeState* state, Chunk* chunk) override;

    bool is_full();

    // async close interface: try_close() -> [is_close_done()] -> close_wait()
    // if is_close_done() return true, close_wait() will not block
    // otherwise close_wait() will block
    Status try_close(RuntimeState* state);

    bool is_close_done();

    Status close_wait(RuntimeState* state, Status close_status);

    // sync close() interface
    Status close(RuntimeState* state, Status close_status) override;

    // Returns the runtime profile for the sink.
    RuntimeProfile* profile() override { return _profile; }

    ObjectPool* pool() { return _pool; }

    Status reset_epoch(RuntimeState* state);

    void set_nonblocking_send_chunk(bool nonblocking_send_chunk) { _nonblocking_send_chunk = nonblocking_send_chunk; }
    bool nonblocking_send_chunk() const { return _nonblocking_send_chunk; }

private:
    template <LogicalType LT>
    void _validate_decimal(RuntimeState* state, Column* column, const SlotDescriptor* desc,
                           std::vector<uint8_t>* validate_selection);
    // This method will change _validate_selection
    void _validate_data(RuntimeState* state, Chunk* chunk);

    Status _init_node_channels(RuntimeState* state);

    // When compute buckect hash, we should use real string for char column.
    // So we need to pad char column after compute buckect hash.
    void _padding_char_column(Chunk* chunk);

    void _print_varchar_error_msg(RuntimeState* state, const Slice& str, SlotDescriptor* desc);

    static void _print_decimal_error_msg(RuntimeState* state, const DecimalV2Value& decimal, SlotDescriptor* desc);

    Status _send_chunk(Chunk* chunk);

    Status _send_chunk_with_colocate_index(Chunk* chunk);

    Status _send_chunk_by_node(Chunk* chunk, IndexChannel* channel, std::vector<uint16_t>& selection_idx);

    Status _fill_auto_increment_id(Chunk* chunk);

    Status _fill_auto_increment_id_internal(Chunk* chunk, SlotDescriptor* slot, int64_t table_id);

    void mark_as_failed(const NodeChannel* ch) { _failed_channels.insert(ch->node_id()); }
    bool is_failed_channel(const NodeChannel* ch) { return _failed_channels.count(ch->node_id()) != 0; }
    bool has_intolerable_failure() {
        if (_write_quorum_type == TWriteQuorumType::ALL) {
            return _failed_channels.size() > 0;
        } else if (_write_quorum_type == TWriteQuorumType::ONE) {
            return _failed_channels.size() >= _num_repicas;
        } else {
            return _failed_channels.size() >= ((_num_repicas + 1) / 2);
        }
    }

    void for_each_node_channel(const std::function<void(NodeChannel*)>& func) {
        for (auto& it : _node_channels) {
            func(it.second.get());
        }
    }

    void for_each_index_channel(const std::function<void(NodeChannel*)>& func) {
        for (auto& index_channel : _channels) {
            index_channel->for_each_node_channel(func);
        }
    }

    Status _automatic_create_partition();

    Status _incremental_open_node_channel(const std::vector<TOlapTablePartition>& partitions);

    friend class NodeChannel;
    friend class IndexChannel;

    ObjectPool* _pool;
    int64_t _rpc_http_min_size = 0;

    // unique load id
    PUniqueId _load_id;
    int64_t _txn_id = -1;
    std::string _txn_trace_parent;
    Span _span;
    int _num_repicas = -1;
    bool _need_gen_rollup = false;
    int _tuple_desc_id = -1;
    std::string _merge_condition;

    // this is tuple descriptor of destination OLAP table
    TupleDescriptor* _output_tuple_desc = nullptr;
    std::vector<ExprContext*> _output_expr_ctxs;

    // number of senders used to insert into OlapTable, if we only support single node insert,
    // all data from select should collectted and then send to OlapTable.
    // To support multiple senders, we maintain a channel for each sender.
    int _sender_id = -1;
    int _num_senders = -1;
    bool _is_lake_table = false;

    TKeysType::type _keys_type;

    // TODO(zc): think about cache this data
    std::shared_ptr<OlapTableSchemaParam> _schema;
    OlapTableLocationParam* _location = nullptr;
    StarRocksNodesInfo* _nodes_info = nullptr;

    RuntimeProfile* _profile = nullptr;

    std::set<int64_t> _partition_ids;

    // index_channel
    std::vector<std::unique_ptr<IndexChannel>> _channels;

    std::vector<DecimalV2Value> _max_decimalv2_val;
    std::vector<DecimalV2Value> _min_decimalv2_val;

    std::vector<OlapTablePartition*> _partitions;
    std::vector<uint32_t> _tablet_indexes;
    // one chunk selection index for partition validation and data validation
    std::vector<uint16_t> _validate_select_idx;
    // one chunk selection for data validation
    std::vector<uint8_t> _validate_selection;
    // one chunk selection for BE node
    std::vector<uint32_t> _node_select_idx;
    std::vector<int64_t> _tablet_ids;
    OlapTablePartitionParam* _vectorized_partition = nullptr;
    std::vector<std::vector<int64_t>> _index_tablet_ids;
    // Store the output expr comput result column
    std::unique_ptr<Chunk> _output_chunk;

    // Stats for this
    int64_t _convert_batch_ns = 0;
    int64_t _validate_data_ns = 0;
    int64_t _number_input_rows = 0;
    int64_t _number_output_rows = 0;
    int64_t _number_filtered_rows = 0;

    RuntimeProfile::Counter* _input_rows_counter = nullptr;
    RuntimeProfile::Counter* _output_rows_counter = nullptr;
    RuntimeProfile::Counter* _filtered_rows_counter = nullptr;
    RuntimeProfile::Counter* _prepare_data_timer = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _convert_chunk_timer = nullptr;
    RuntimeProfile::Counter* _validate_data_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _serialize_chunk_timer = nullptr;
    RuntimeProfile::Counter* _wait_response_timer = nullptr;
    RuntimeProfile::Counter* _compress_timer = nullptr;
    RuntimeProfile::Counter* _pack_chunk_timer = nullptr;
    RuntimeProfile::Counter* _send_rpc_timer = nullptr;
    RuntimeProfile::Counter* _client_rpc_timer = nullptr;
    RuntimeProfile::Counter* _server_rpc_timer = nullptr;
    RuntimeProfile::Counter* _alloc_auto_increment_timer = nullptr;

    // load mem limit is for remote load channel
    int64_t _load_mem_limit = 0;

    // the timeout of load channels opened by this tablet sink. in second
    int64_t _load_channel_timeout_s = 0;

    bool _open_done = false;
    bool _close_done = false;

    // BeId -> channel
    std::unordered_map<int64_t, std::unique_ptr<NodeChannel>> _node_channels;
    // BeId
    std::set<int64_t> _failed_channels;
    // enable colocate index
    bool _colocate_mv_index = config::enable_load_colocate_mv;

    bool _enable_replicated_storage = false;

    TWriteQuorumType::type _write_quorum_type = TWriteQuorumType::MAJORITY;

    SlotId _auto_increment_slot_id = -1;

    bool _has_auto_increment = false;

    bool _null_expr_in_auto_increment = false;

    bool _miss_auto_increment_column = false;

    std::unique_ptr<ThreadPoolToken> _automatic_partition_token;

    std::vector<std::vector<std::string>> _partition_not_exist_row_values;

    bool _enable_automatic_partition = false;

    bool _nonblocking_send_chunk = false;

    bool _has_automatic_partition = false;

    std::atomic<bool> _is_automatic_partition_running = false;
    Status _automatic_partition_status;
};

} // namespace stream_load
} // namespace starrocks
