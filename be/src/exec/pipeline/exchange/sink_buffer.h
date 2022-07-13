// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <algorithm>
#include <future>
#include <list>
#include <mutex>
#include <queue>
#include <unordered_set>

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bthread/mutex.h>
DIAGNOSTIC_POP

#include "column/chunk.h"
#include "exec/pipeline/exchange/exchange_sink_buffer.h"
#include "exec/pipeline/fragment_context.h"
#include "gen_cpp/BackendService.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "util/brpc_stub_cache.h"
#include "util/defer_op.h"
#include "util/disposable_closure.h"
#include "util/phmap/phmap.h"

namespace starrocks::pipeline {

struct ClosureContext {
    TUniqueId instance_id;
    int64_t sequence;
    int64_t send_timestamp;
};

// TODO(hcf) how to export brpc error
class SinkBuffer : public ExchangeSinkBuffer {
public:
    SinkBuffer(FragmentContext* fragment_ctx, const std::vector<TPlanFragmentDestination>& destinations,
               bool is_dest_merge, size_t num_sinkers);
    ~SinkBuffer() override;

    void add_request(TransmitChunkInfo& request) override;
    bool is_full() const override;

    void set_finishing() override;
    bool is_finished() const override;

    // Add counters to the given profile
    void update_profile(RuntimeProfile* profile) override;

    // When all the ExchangeSinkOperator shared this SinkBuffer are cancelled,
    // the rest chunk request and EOS request needn't be sent anymore.
    void cancel_one_sinker() override;

private:
    using Mutex = bthread::Mutex;

    void _update_network_time(const TUniqueId& instance_id, const int64_t send_timestamp,
                              const int64_t receive_timestamp);
    // Update the discontinuous acked window, here are the invariants:
    // all acks received with sequence from [0, _max_continuous_acked_seqs[x]]
    // not all the acks received with sequence from [_max_continuous_acked_seqs[x]+1, _request_seqs[x]]
    // _discontinuous_acked_seqs[x] stored the received discontinuous acks
    void _process_send_window(const TUniqueId& instance_id, const int64_t sequence);

    // Try to send rpc if buffer is not empty and channel is not busy
    // And we need to put this function and other extra works(pre_works) together as an atomic operation
    void _try_to_send_rpc(const TUniqueId& instance_id, std::function<void()> pre_works);

    // Roughly estimate network time which is defined as the time between sending a and receiving a packet,
    // and the processing time of both sides are excluded
    // For each destination, we may send multiply packages at the same time, and the time is
    // related to the degree of concurrency, so the network_time will be calculated as
    // `accumulated_network_time / average_concurrency`
    // And we just pick the maximum accumulated_network_time among all destination
    int64_t _network_time();

    FragmentContext* _fragment_ctx;
    MemTracker* const _mem_tracker;
    const int32_t _brpc_timeout_ms;
    const bool _is_dest_merge;

    /// Taking into account of efficiency, all the following maps
    /// use int64_t as key, which is the field type of TUniqueId::lo
    /// because TUniqueId::hi is exactly the same in one query

    // num eos per instance
    phmap::flat_hash_map<int64_t, int64_t> _num_sinkers;
    phmap::flat_hash_map<int64_t, int64_t> _request_seqs;
    // Considering the following situation
    // Sending request 1, 2, 3 in order with one possible order of response 1, 3, 2,
    // and field transformation are as following
    //      a. receive response-1, _max_continuous_acked_seqs[x]->1, _discontinuous_acked_seqs[x]->()
    //      b. receive response-3, _max_continuous_acked_seqs[x]->1, _discontinuous_acked_seqs[x]->(3)
    //      c. receive response-2, _max_continuous_acked_seqs[x]->3, _discontinuous_acked_seqs[x]->()
    phmap::flat_hash_map<int64_t, int64_t> _max_continuous_acked_seqs;
    phmap::flat_hash_map<int64_t, std::unordered_set<int64_t>> _discontinuous_acked_seqs;
    std::atomic<int32_t> _total_in_flight_rpc = 0;
    std::atomic<int32_t> _num_uncancelled_sinkers;
    std::atomic<int32_t> _num_remaining_eos = 0;

    // The request needs the reference to the allocated finst id,
    // so cache finst id for each dest fragment instance.
    phmap::flat_hash_map<int64_t, PUniqueId> _instance_id2finst_id;
    phmap::flat_hash_map<int64_t, std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>>> _buffers;
    phmap::flat_hash_map<int64_t, int32_t> _num_finished_rpcs;
    phmap::flat_hash_map<int64_t, int32_t> _num_in_flight_rpcs;
    phmap::flat_hash_map<int64_t, TimeTrace> _network_times;
    phmap::flat_hash_map<int64_t, std::unique_ptr<Mutex>> _mutexes;

    // True means that SinkBuffer needn't input chunk and send chunk anymore,
    // but there may be still in-flight RPC running.
    // It becomes true, when all sinkers have sent EOS, or been set_finished/cancelled, or RPC has returned error.
    // Unfortunately, _is_finishing itself cannot guarantee that no brpc process will trigger if entering finishing stage,
    // considering the following situations(events order by time)
    //      time1(thread A): _try_to_send_rpc check _is_finishing which returns false
    //      time2(thread B): set _is_finishing to true
    //      time3(thread A): _try_to_send_rpc trigger brpc process
    // So _num_sending_rpc is introduced to solve this problem by providing extra information
    // of how many threads are calling _try_to_send_rpc
    std::atomic<bool> _is_finishing = false;
    std::atomic<int32_t> _num_sending_rpc = 0;

    // RuntimeProfile counters
    std::atomic_bool _is_profile_updated = false;
    std::atomic<int64_t> _bytes_enqueued = 0;
    std::atomic<int64_t> _request_enqueued = 0;
    std::atomic<int64_t> _bytes_sent = 0;
    std::atomic<int64_t> _request_sent = 0;

    int64_t _pending_timestamp = -1;
    mutable std::atomic<int64_t> _last_full_timestamp = -1;
    mutable std::atomic<int64_t> _full_time = 0;
}; // namespace starrocks::pipeline

} // namespace starrocks::pipeline
