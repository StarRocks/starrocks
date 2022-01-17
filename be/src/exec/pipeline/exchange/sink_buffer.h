// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <algorithm>
#include <future>
#include <list>
#include <mutex>
#include <queue>
#include <unordered_set>

#include "column/chunk.h"
#include "exec/pipeline/fragment_context.h"
#include "gen_cpp/BackendService.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "util/brpc_stub_cache.h"
#include "util/defer_op.h"
#include "util/disposable_closure.h"
#include "util/phmap/phmap.h"

namespace starrocks::pipeline {

using PTransmitChunkParamsPtr = std::shared_ptr<PTransmitChunkParams>;

struct TransmitChunkInfo {
    // For BUCKET_SHFFULE_HASH_PARTITIONED, multiple channels may be related to
    // a same exchange source fragment instance, so we should use fragment_instance_id
    // of the destination as the key of destination instead of channel_id.
    TUniqueId fragment_instance_id;
    doris::PBackendService_Stub* brpc_stub;
    PTransmitChunkParamsPtr params;
    butil::IOBuf attachment;
};

struct ClosureContext {
    TUniqueId instance_id;
    int64_t sequence;
};

// TODO(hcf) how to export brpc error
class SinkBuffer {
public:
    SinkBuffer(FragmentContext* fragment_ctx, const std::vector<TPlanFragmentDestination>& destinations,
               bool is_dest_merge, size_t num_sinkers);
    ~SinkBuffer();

    void add_request(const TransmitChunkInfo& request);
    bool is_full() const;
    bool is_finished() const;

    // When all the ExchangeSinkOperator shared this SinkBuffer are cancelled,
    // the rest chunk request and EOS request needn't be sent anymore.
    void cancel_one_sinker();

private:
    // Update the discontinuous acked window, here are the invariants:
    // all acks received with sequence from [0, _max_continuous_acked_seqs[x]]
    // not all the acks received with sequence from [_max_continuous_acked_seqs[x]+1, _request_seqs[x]]
    // _discontinuous_acked_seqs[x] stored the received discontinuous acks
    void _process_send_window(const TUniqueId& instance_id, const int64_t sequence);
    void _try_to_send_rpc(const TUniqueId& instance_id);

    FragmentContext* _fragment_ctx;
    const MemTracker* _mem_tracker;
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
    phmap::flat_hash_map<int64_t, std::unique_ptr<std::mutex>> _mutexes;

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

}; // namespace starrocks::pipeline

} // namespace starrocks::pipeline
