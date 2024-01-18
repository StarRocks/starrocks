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
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "common/tracer.h"
#include "exec/data_sink.h"
#include "exec/tablet_info.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/mem_tracker.h"
#include "util/compression/block_compression.h"
#include "util/raw_container.h"
#include "util/ref_count_closure.h"
#include "util/reusable_closure.h"
#include "util/threadpool.h"

namespace starrocks {

class MemTracker;
class TupleDescriptor;

namespace stream_load {

class OlapTableSink;    // forward declaration
class TabletSinkSender; // forward declaration

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
    // total time of wait memtable flush
    int64_t add_batch_wait_memtable_flush_time_us = 0;

    AddBatchCounter& operator+=(const AddBatchCounter& rhs) {
        add_batch_execution_time_us += rhs.add_batch_execution_time_us;
        add_batch_wait_lock_time_us += rhs.add_batch_wait_lock_time_us;
        add_batch_num += rhs.add_batch_num;
        add_batch_wait_memtable_flush_time_us += rhs.add_batch_wait_memtable_flush_time_us;
        return *this;
    }
    friend AddBatchCounter operator+(const AddBatchCounter& lhs, const AddBatchCounter& rhs) {
        AddBatchCounter sum = lhs;
        sum += rhs;
        return sum;
    }
};

struct TabletSinkProfile {
    RuntimeProfile* runtime_profile;
    RuntimeProfile::Counter* input_rows_counter = nullptr;
    RuntimeProfile::Counter* output_rows_counter = nullptr;
    RuntimeProfile::Counter* filtered_rows_counter = nullptr;
    RuntimeProfile::Counter* prepare_data_timer = nullptr;
    RuntimeProfile::Counter* send_data_timer = nullptr;
    RuntimeProfile::Counter* convert_chunk_timer = nullptr;
    RuntimeProfile::Counter* validate_data_timer = nullptr;
    RuntimeProfile::Counter* open_timer = nullptr;
    RuntimeProfile::Counter* close_timer = nullptr;
    RuntimeProfile::Counter* serialize_chunk_timer = nullptr;
    RuntimeProfile::Counter* wait_response_timer = nullptr;
    RuntimeProfile::Counter* compress_timer = nullptr;
    RuntimeProfile::Counter* pack_chunk_timer = nullptr;
    RuntimeProfile::Counter* send_rpc_timer = nullptr;
    RuntimeProfile::Counter* client_rpc_timer = nullptr;
    RuntimeProfile::Counter* server_rpc_timer = nullptr;
    RuntimeProfile::Counter* alloc_auto_increment_timer = nullptr;
    RuntimeProfile::Counter* server_wait_flush_timer = nullptr;
};

// map index_id to TabletBEMap(map tablet_id to backend id)
using IndexIdToTabletBEMap = std::unordered_map<int64_t, std::unordered_map<int64_t, std::vector<int64_t>>>;

class NodeChannel {
public:
    NodeChannel(OlapTableSink* parent, int64_t node_id, bool is_incremental, ExprContext* where_clause = nullptr);
    ~NodeChannel() noexcept;

    // called before open, used to add tablet loacted in this backend
    void add_tablet(const int64_t index_id, const PTabletWithPartition& tablet) {
        _index_tablets_map[index_id].emplace_back(tablet);
        _index_tablet_ids_map[index_id].emplace(tablet.tablet_id());
    }

    const std::unordered_set<int64_t>& tablet_ids_of_index(int64_t index_id) const {
        return _index_tablet_ids_map.at(index_id);
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

    std::set<int64_t> immutable_partition_ids() { return _immutable_partition_ids; }
    bool has_immutable_partition() { return !_immutable_partition_ids.empty(); }
    void reset_immutable_partition_ids() { _immutable_partition_ids.clear(); }

    bool has_primary_replica() const { return _has_primary_replica; }
    void set_has_primary_replica(bool has_primary_replica) { _has_primary_replica = has_primary_replica; }

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
    Status _filter_indexes_with_where_expr(Chunk* input, const std::vector<uint32_t>& indexes,
                                           std::vector<uint32_t>& filtered_indexes);

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

    PInternalService_Stub* _stub = nullptr;
    std::vector<RefCountClosure<PTabletWriterOpenResult>*> _open_closures;

    std::map<int64_t, std::vector<PTabletWithPartition>> _index_tablets_map;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> _index_tablet_ids_map;

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
    TabletSinkProfile* _ts_profile = nullptr;

    bool _enable_colocate_mv_index = config::enable_load_colocate_mv;

    WriteQuorumTypePB _write_quorum_type = WriteQuorumTypePB::MAJORITY;

    bool _is_incremental;

    std::set<int64_t> _immutable_partition_ids;

    ExprContext* _where_clause = nullptr;

    bool _has_primary_replica = false;
};

class IndexChannel {
public:
    IndexChannel(OlapTableSink* parent, int64_t index_id, ExprContext* where_clause)
            : _parent(parent), _index_id(index_id), _where_clause(where_clause) {}
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

    void mark_as_failed(const NodeChannel* ch);

    bool is_failed_channel(const NodeChannel* ch) { return _failed_channels.count(ch->node_id()) != 0; }

    bool has_intolerable_failure();

    bool has_incremental_node_channel() const { return _has_incremental_node_channel; }

    int64_t index_id() const { return _index_id; }

private:
    friend class OlapTableSink;
    friend class TabletSinkSender;
    OlapTableSink* _parent;
    int64_t _index_id;

    // BeId -> channel
    std::unordered_map<int64_t, std::unique_ptr<NodeChannel>> _node_channels;
    // map be_id to tablet num
    std::unordered_map<int64_t, int64_t> _be_to_tablet_num;
    // BeId
    std::set<int64_t> _failed_channels;

    TWriteQuorumType::type _write_quorum_type = TWriteQuorumType::MAJORITY;

    bool _has_incremental_node_channel = false;
    ExprContext* _where_clause = nullptr;

    bool _has_intolerable_failure = false;
};

} // namespace stream_load
} // namespace starrocks
