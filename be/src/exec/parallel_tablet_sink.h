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
#include "util/ref_count_closure.h"

DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bthread/execution_queue.h>
DIAGNOSTIC_POP

namespace starrocks {

class Bitmap;
class MemTracker;
class RuntimeProfile;
class RowDescriptor;
class TupleDescriptor;
class ExprContext;
class TExpr;

namespace parallel_tablet_sink {

class NodeChannel;
class IndexChannel;
class ParallelOlapTableSink;

struct TabletSinkAddChunkTask {
    vectorized::Chunk* chunk = nullptr;
    NodeChannel* node = nullptr;
    const int64_t* tablet_ids = nullptr;
    std::atomic<int32_t>* pending_processing_num_channel;
    std::unordered_map<int64_t, std::vector<int64_t>>* tablet_to_be;
    std::vector<uint16_t>* selection_idx;
    int64_t be_id;
};

struct TabletSinkCachedElement {
    TabletSinkCachedElement(std::unique_ptr<vectorized::Chunk>&& chunk, int32_t num_channel, int tablet_id_reserve_size)
            : chunk(std::move(chunk)), pending_processing_num_channel(num_channel) {}

    std::unique_ptr<vectorized::Chunk> chunk;
    std::atomic<int32_t> pending_processing_num_channel;
    std::vector<std::vector<int64_t>> vec_tablet_ids;
    std::vector<uint16_t> validate_select_idx;
};

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

// It's very error-prone to guarantee the handler capture vars' & this closure's destruct sequence.
// So using create() to get the closure pointer is recommended. We can delete the closure ptr before the capture vars destruction.
// Delete this point is safe, don't worry about RPC callback will run after ReusableClosure deleted.
template <typename T>
class ReusableClosure : public google::protobuf::Closure {
public:
    ReusableClosure() : cid(INVALID_BTHREAD_ID) {}
    ~ReusableClosure() override {
        // shouldn't delete when Run() is calling or going to be called, wait for current Run() done.
        join();
    }

    static ReusableClosure<T>* create() { return new ReusableClosure<T>(); }

    void addFailedHandler(std::function<void()> fn) { failed_handler = std::move(fn); }
    void addSuccessHandler(std::function<void(const T&, bool)> fn) { success_handler = fn; }

    void join() {
        if (cid != INVALID_BTHREAD_ID) {
            brpc::Join(cid);
        }
    }

    // plz follow this order: reset() -> set_in_flight() -> send brpc batch
    void reset() {
        join();
        DCHECK(_packet_in_flight == false);
        cntl.Reset();
        cid = cntl.call_id();
    }

    void set_in_flight() {
        DCHECK(_packet_in_flight == false);
        _packet_in_flight = true;
    }

    bool is_packet_in_flight() { return _packet_in_flight; }

    void end_mark() {
        DCHECK(_is_last_rpc == false);
        _is_last_rpc = true;
    }

    void Run() override {
        DCHECK(_packet_in_flight);
        if (cntl.Failed()) {
            LOG(WARNING) << "failed to send brpc batch, error=" << berror(cntl.ErrorCode())
                         << ", error_text=" << cntl.ErrorText();
            failed_handler();
        } else {
            success_handler(result, _is_last_rpc);
        }
        _packet_in_flight = false;
    }

    brpc::Controller cntl;
    T result;

private:
    brpc::CallId cid;
    std::atomic<bool> _packet_in_flight{false};
    std::atomic<bool> _is_last_rpc{false};
    std::function<void()> failed_handler;
    std::function<void(const T&, bool)> success_handler;
};

class NodeChannel {
public:
    NodeChannel(ParallelOlapTableSink* parent, IndexChannel* index_channel, int64_t index_id, int64_t node_id);
    ~NodeChannel() noexcept;

    // called before open, used to add tablet loacted in this backend
    void add_tablet(const TTabletWithPartition& tablet) { _all_tablets.emplace_back(tablet); }

    Status init(RuntimeState* state);

    // we use open/open_wait to parallel
    void open();
    Status open_wait();

    // two ways to stop channel:
    // 1. mark_close()->close_wait() PS. close_wait() will block waiting for the last AddBatch rpc response.
    // 2. just cancel()
    Status mark_close();
    Status close_wait(RuntimeState* state);

    void cancel(const Status& err_st);

    int try_send_chunk_and_fetch_status();

    void time_report(std::unordered_map<int64_t, AddBatchCounter>* add_batch_counter_map, int64_t* serialize_batch_ns,
                     int64_t* mem_exceeded_block_ns, int64_t* queue_push_lock_ns, int64_t* actual_consume_ns) {
        (*add_batch_counter_map)[_node_id] += _add_batch_counter;
        *serialize_batch_ns += _serialize_batch_ns;
        *mem_exceeded_block_ns += _mem_exceeded_block_ns;
        *queue_push_lock_ns += _queue_push_lock_ns;
        *actual_consume_ns += _actual_consume_ns;
    }

    ParallelOlapTableSink* parent() const { return _parent; }
    IndexChannel* index_channel() const { return _index_channel; }
    std::mutex& pending_batches_lock() { return _pending_batches_lock; }
    Status err_st() const { return _err_st; }
    int64_t node_id() const { return _node_id; }
    int64_t index_id() const { return _index_id; }
    const NodeInfo* node_info() const { return _node_info; }
    std::string print_load_info() const { return _load_info; }
    std::string name() const { return _name; }
    void increase_pending_batches_num() { _pending_batches_num++; }
    void decrease_pending_batches_num() { _pending_batches_num--; }
    bool cancelled() { return _cancelled; }
    bool eos_is_produced() { return _eos_is_produced; }
    RuntimeState* runtime_state() { return _runtime_state; }
    int64_t& queue_push_lock_ns() { return _queue_push_lock_ns; }
    PTabletWriterAddChunkRequest cur_add_chunk_request() { return _cur_add_chunk_request; }

    Status none_of(std::initializer_list<bool> vars);

    void clear_all_batches();

    ParallelOlapTableSink* _parent = nullptr;
    IndexChannel* _index_channel = nullptr;
    int64_t _index_id = -1;
    int64_t _node_id = -1;
    std::string _load_info;
    std::string _name;

    TupleDescriptor* _tuple_desc = nullptr;
    const NodeInfo* _node_info = nullptr;

    // this should be set in init() using config
    int _rpc_timeout_ms = 60000;
    int64_t _next_packet_seq = 0;

    // user cancel or get some errors
    std::atomic<bool> _cancelled{false};

    // send finished means the consumer thread which send the rpc can exit
    std::atomic<bool> _send_finished{false};

    // add batches finished means the last rpc has be responsed, used to check whether this channel can be closed
    std::atomic<bool> _add_batches_finished{false};

    bool _eos_is_produced{false}; // only for restricting producer behaviors

    std::unique_ptr<RowDescriptor> _row_desc;

    std::mutex _pending_batches_lock;

    std::atomic<int> _pending_batches_num{0};

    doris::PBackendService_Stub* _stub = nullptr;
    RefCountClosure<PTabletWriterOpenResult>* _open_closure = nullptr;
    ReusableClosure<PTabletWriterAddBatchResult>* _add_batch_closure = nullptr;

    std::vector<TTabletWithPartition> _all_tablets;
    std::vector<TTabletCommitInfo> _tablet_commit_infos;

    AddBatchCounter _add_batch_counter;
    int64_t _serialize_batch_ns = 0;

    std::unique_ptr<vectorized::Chunk> _cur_chunk;
    using AddChunkReq = std::pair<std::unique_ptr<vectorized::Chunk>, PTabletWriterAddChunkRequest>;
    std::queue<AddChunkReq> _pending_chunks;
    PTabletWriterAddChunkRequest _cur_add_chunk_request;

    int64_t _mem_exceeded_block_ns = 0;
    int64_t _queue_push_lock_ns = 0;
    int64_t _actual_consume_ns = 0;
    Status _err_st = Status::OK();

    RuntimeState* _runtime_state = nullptr;
};

class IndexChannel {
public:
    IndexChannel(ParallelOlapTableSink* parent, int64_t index_id) : _parent(parent), _index_id(index_id) {}
    ~IndexChannel();

    Status init(RuntimeState* state, const std::vector<TTabletWithPartition>& tablets);

    void for_each_node_channel(const std::function<void(NodeChannel*)>& func) {
        for (auto& it : _node_channels) {
            func(it.second.get());
        }
    }

    void mark_as_failed(const NodeChannel* ch) { _failed_channels.insert(ch->node_id()); }
    bool has_intolerable_failure();

    int num_node_channels() { return _node_channels.size(); }

    void set_err_st(Status err_st) { _err_st = err_st; }
    Status err_st() const { return _err_st; }
    std::mutex& mark_failed_lock() { return _mark_failed_lock; }

private:
    friend class ParallelOlapTableSink;
    ParallelOlapTableSink* _parent;
    int64_t _index_id;

    // BeId -> channel
    std::unordered_map<int64_t, std::unique_ptr<NodeChannel>> _node_channels;
    // map tablet_id to backend channel
    std::unordered_map<int64_t, std::vector<NodeChannel*>> _channels_by_tablet;
    // map tablet_id to backend id
    std::unordered_map<int64_t, std::vector<int64_t>> _tablet_to_be;
    // BeId
    std::set<int64_t> _failed_channels;

    std::mutex _mark_failed_lock;

    Status _err_st;
};

// Write data to Olap Table.
// When ParallelOlapTableSink::open() called, there will be a consumer thread running in the background.
// When you call ParallelOlapTableSink::send(), you will be the productor who products pending batches.
// Join the consumer thread in close().
class ParallelOlapTableSink : public DataSink {
public:
    // Construct from thrift struct which is generated by FE.
    ParallelOlapTableSink(ObjectPool* pool, const std::vector<TExpr>& texprs, Status* status,
                          int tablet_sink_split_chunk_dop);
    ~ParallelOlapTableSink() override;

    Status init(const TDataSink& sink) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status send_chunk(RuntimeState* state, vectorized::Chunk* chunk) override;

    // close() will send RPCs too. If RPCs failed, return error.
    Status close(RuntimeState* state, Status close_status) override;

    // Returns the runtime profile for the sink.
    RuntimeProfile* profile() override { return _profile; }

    static int _execute_node_channel_add_chunk(void* meta, bthread::TaskIterator<TabletSinkAddChunkTask>& iter);

    void increase_pending_batches_num() { _pending_batches_num++; }
    void decrease_pending_batches_num() { _pending_batches_num--; }

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

    // the consumer func of sending pending chunks in every NodeChannel.
    // use polling & NodeChannel::try_send_chunk_and_fetch_status() to achieve nonblocking sending.
    // only focus on pending chunks and channel status, the internal errors of NodeChannels will be handled by the productor
    void _send_chunk_process();

    // send chunk data to specific BE channel
    Status _send_chunk_by_node(vectorized::Chunk* chunk, IndexChannel* channel, std::vector<uint16_t>& _selection_idx,
                               std::vector<int64_t>& tablet_ids);

    friend class NodeChannel;
    friend class IndexChannel;

    ObjectPool* _pool;

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
    vectorized::OlapTablePartitionParam* _vectorized_partition = nullptr;
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

    // load mem limit is for remote load channel
    int64_t _load_mem_limit = 0;

    // the timeout of load channels opened by this tablet sink. in second
    int64_t _load_channel_timeout_s = 0;

    constexpr static uint64_t kInvalidQueueId = (uint64_t)-1;

    int _tablet_sink_split_chunk_dop = 0;
    std::vector<bthread::ExecutionQueueId<TabletSinkAddChunkTask>> _queue_ids;
    int64_t _num_node_channel = 0;
    vector<int64_t> _acc_num_node_channel;
    std::queue<TabletSinkCachedElement> _cached_element_queue;
    std::mutex _lock;
    std::condition_variable _send_chunk_cond;
    std::atomic<int> _pending_batches_num{0};
    size_t _max_pending_batches_num;
};

} // namespace parallel_tablet_sink
} // namespace starrocks
