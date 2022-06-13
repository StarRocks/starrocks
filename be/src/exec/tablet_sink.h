// This file is made available under Elastic License 2.0.
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
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/tablet_info.h"
#include "exec/vectorized/tablet_info.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/global_dicts.h"
#include "util/bitmap.h"
#include "util/block_compression.h"
#include "util/ref_count_closure.h"

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

template <typename T>
class ReusableClosure : public google::protobuf::Closure {
public:
    ReusableClosure() : cid(INVALID_BTHREAD_ID), _refs(0) {}
    ~ReusableClosure() {}

    int count() { return _refs.load(); }

    void ref() { _refs.fetch_add(1); }

    // If unref() returns true, this object should be delete
    bool unref() { return _refs.fetch_sub(1) == 1; }

    void Run() override {
        if (unref()) {
            delete this;
        }
    }

    bool join() {
        if (cid != INVALID_BTHREAD_ID) {
            brpc::Join(cid);
            cid = INVALID_BTHREAD_ID;
            return true;
        } else {
            return false;
        }
    }

    void cancel() {
        if (cid != INVALID_BTHREAD_ID) {
            brpc::StartCancel(cid);
        }
    }

    void reset() {
        cntl.Reset();
        cid = cntl.call_id();
    }

    brpc::Controller cntl;
    T result;

private:
    brpc::CallId cid;
    std::atomic<int> _refs;
};

class NodeChannel {
public:
    NodeChannel(OlapTableSink* parent, int64_t index_id, int64_t node_id, int32_t schema_hash);
    ~NodeChannel() noexcept;

    // called before open, used to add tablet loacted in this backend
    void add_tablet(const TTabletWithPartition& tablet) { _all_tablets.emplace_back(tablet); }

    Status init(RuntimeState* state);

    // we use open/open_wait to parallel
    void open();
    Status open_wait();

    Status add_chunk(vectorized::Chunk* chunk, const int64_t* tablet_ids, const uint32_t* indexes, uint32_t from,
                     uint32_t size, bool eos);

    Status close_wait(RuntimeState* state);

    void cancel(const Status& err_st);

    void time_report(std::unordered_map<int64_t, AddBatchCounter>* add_batch_counter_map, int64_t* serialize_batch_ns,
                     int64_t* mem_exceeded_block_ns, int64_t* queue_push_lock_ns, int64_t* actual_consume_ns) {
        (*add_batch_counter_map)[_node_id] += _add_batch_counter;
        *serialize_batch_ns += _serialize_batch_ns;
        *mem_exceeded_block_ns += _mem_exceeded_block_ns;
        *queue_push_lock_ns += _queue_push_lock_ns;
        *actual_consume_ns += _actual_consume_ns;
    }

    int64_t node_id() const { return _node_id; }
    const NodeInfo* node_info() const { return _node_info; }
    std::string print_load_info() const { return _load_info; }
    std::string name() const { return _name; }

    Status none_of(std::initializer_list<bool> vars);

    void clear_all_batches();

private:
    Status _wait_request(ReusableClosure<PTabletWriterAddBatchResult>* closure);
    Status _wait_all_prev_request();
    Status _wait_one_prev_request();
    bool _check_prev_request_done();
    Status _serialize_chunk(const vectorized::Chunk* src, ChunkPB* dst);

    std::unique_ptr<MemTracker> _mem_tracker = nullptr;

    OlapTableSink* _parent = nullptr;
    int64_t _index_id = -1;
    int64_t _node_id = -1;
    int32_t _schema_hash = 0;
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
    RefCountClosure<PTabletWriterOpenResult>* _open_closure = nullptr;

    std::vector<TTabletWithPartition> _all_tablets;
    std::vector<TTabletCommitInfo> _tablet_commit_infos;

    AddBatchCounter _add_batch_counter;
    int64_t _serialize_batch_ns = 0;

    size_t _max_parallel_request_size = 1;
    std::vector<ReusableClosure<PTabletWriterAddBatchResult>*> _add_batch_closures;
    PTabletWriterAddChunkRequest _cur_request;
    std::unique_ptr<vectorized::Chunk> _cur_chunk;
    using AddChunkReq = std::pair<std::unique_ptr<vectorized::Chunk>, PTabletWriterAddChunkRequest>;
    std::deque<AddChunkReq> _chunk_queue;
    size_t _current_request_index = 0;
    size_t _max_chunk_queue_size = 8;

    int64_t _mem_exceeded_block_ns = 0;
    int64_t _queue_push_lock_ns = 0;
    int64_t _actual_consume_ns = 0;
    Status _err_st = Status::OK();

    RuntimeState* _runtime_state = nullptr;
};

class IndexChannel {
public:
    IndexChannel(OlapTableSink* parent, int64_t index_id, int32_t schema_hash)
            : _parent(parent), _index_id(index_id), _schema_hash(schema_hash) {}
    ~IndexChannel();

    Status init(RuntimeState* state, const std::vector<TTabletWithPartition>& tablets);

    void for_each_node_channel(const std::function<void(NodeChannel*)>& func) {
        for (auto& it : _node_channels) {
            func(it.second.get());
        }
    }

    void mark_as_failed(const NodeChannel* ch) { _failed_channels.insert(ch->node_id()); }
    bool has_intolerable_failure();

private:
    friend class OlapTableSink;
    OlapTableSink* _parent;
    int64_t _index_id;
    int32_t _schema_hash;

    // BeId -> channel
    std::unordered_map<int64_t, std::unique_ptr<NodeChannel>> _node_channels;
    // map tablet_id to backend channel
    std::unordered_map<int64_t, std::vector<NodeChannel*>> _channels_by_tablet;
    // map tablet_id to backend id
    std::unordered_map<int64_t, std::vector<int64_t>> _tablet_to_be;
    // BeId
    std::set<int64_t> _failed_channels;
};

// Write data to Olap Table.
// When OlapTableSink::open() called, there will be a consumer thread running in the background.
// When you call OlapTableSink::send(), you will be the productor who products pending batches.
// Join the consumer thread in close().
class OlapTableSink : public DataSink {
public:
    // Construct from thrift struct which is generated by FE.
    OlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc, const std::vector<TExpr>& texprs, Status* status);
    ~OlapTableSink() override;

    Status init(const TDataSink& sink) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status send_chunk(RuntimeState* state, vectorized::Chunk* chunk) override;

    // close() will send RPCs too. If RPCs failed, return error.
    Status close(RuntimeState* state, Status close_status) override;

    // Returns the runtime profile for the sink.
    RuntimeProfile* profile() override { return _profile; }

private:
    template <PrimitiveType PT>
    void _validate_decimal(RuntimeState* state, vectorized::Column* column, const SlotDescriptor* desc,
                           std::vector<uint8_t>* validate_selection);
    // This method will change _validate_selection
    void _validate_data(RuntimeState* state, vectorized::Chunk* chunk);

    // When compute buckect hash, we should use real string for char column.
    // So we need to pad char column after compute buckect hash.
    void _padding_char_column(vectorized::Chunk* chunk);

    void _print_varchar_error_msg(RuntimeState* state, const Slice& str, SlotDescriptor* desc);

    void _print_decimal_error_msg(RuntimeState* state, const DecimalV2Value& decimal, SlotDescriptor* desc);

    // send chunk data to specific BE channel
    Status _send_chunk_by_node(vectorized::Chunk* chunk, IndexChannel* channel, std::vector<uint16_t>& _selection_idx);

    friend class NodeChannel;
    friend class IndexChannel;

    ObjectPool* _pool;
    const RowDescriptor& _input_row_desc;

    // unique load id
    PUniqueId _load_id;
    int64_t _txn_id = -1;
    int _num_repicas = -1;
    bool _need_gen_rollup = false;
    int _tuple_desc_id = -1;

    // this is tuple descriptor of destination OLAP table
    TupleDescriptor* _output_tuple_desc = nullptr;
    std::vector<ExprContext*> _output_expr_ctxs;

    // number of senders used to insert into OlapTable, if we only support single node insert,
    // all data from select should collectted and then send to OlapTable.
    // To support multiple senders, we maintain a channel for each sender.
    int _sender_id = -1;
    int _num_senders = -1;

    // TODO(zc): think about cache this data
    std::shared_ptr<OlapTableSchemaParam> _schema;
    OlapTableLocationParam* _location = nullptr;
    StarRocksNodesInfo* _nodes_info = nullptr;

    RuntimeProfile* _profile = nullptr;

    std::set<int64_t> _partition_ids;

    Bitmap _filter_bitmap;

    // index_channel
    std::vector<std::unique_ptr<IndexChannel>> _channels;

    std::thread _sender_thread;

    std::vector<DecimalValue> _max_decimal_val;
    std::vector<DecimalValue> _min_decimal_val;

    std::vector<DecimalV2Value> _max_decimalv2_val;
    std::vector<DecimalV2Value> _min_decimalv2_val;

    std::vector<vectorized::OlapTablePartition*> _partitions;
    std::vector<uint32_t> _tablet_indexes;
    // one chunk selection index for partition validation and data validation
    std::vector<uint16_t> _validate_select_idx;
    // one chunk selection for data validation
    std::vector<uint8_t> _validate_selection;
    // one chunk selection for BE node
    std::vector<uint32_t> _node_select_idx;
    std::vector<int64_t> _tablet_ids;
    vectorized::OlapTablePartitionParam* _vectorized_partition;
    // Store the output expr comput result column
    std::unique_ptr<vectorized::Chunk> _output_chunk;

    // Stats for this
    int64_t _convert_batch_ns = 0;
    int64_t _validate_data_ns = 0;
    int64_t _send_data_ns = 0;
    int64_t _non_blocking_send_ns = 0;
    int64_t _number_input_rows = 0;
    int64_t _number_output_rows = 0;
    int64_t _number_filtered_rows = 0;

    RuntimeProfile::Counter* _input_rows_counter = nullptr;
    RuntimeProfile::Counter* _output_rows_counter = nullptr;
    RuntimeProfile::Counter* _filtered_rows_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _convert_chunk_timer = nullptr;
    RuntimeProfile::Counter* _validate_data_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _non_blocking_send_timer = nullptr;
    RuntimeProfile::Counter* _serialize_chunk_timer = nullptr;
    RuntimeProfile::Counter* _wait_response_timer = nullptr;
    RuntimeProfile::Counter* _compress_timer = nullptr;
    RuntimeProfile::Counter* _append_attachment_timer = nullptr;
    RuntimeProfile::Counter* _mark_tablet_timer = nullptr;
    RuntimeProfile::Counter* _pack_chunk_timer = nullptr;

    // load mem limit is for remote load channel
    int64_t _load_mem_limit = 0;

    // the timeout of load channels opened by this tablet sink. in second
    int64_t _load_channel_timeout_s = 0;
};

} // namespace stream_load
} // namespace starrocks
