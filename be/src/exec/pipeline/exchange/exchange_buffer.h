// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <unordered_set>
#include <vector>

#include "column/chunk.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/fragment_context.h"
#include "gen_cpp/BackendService.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "util/brpc_stub_cache.h"

namespace starrocks {
namespace pipeline {

struct TransmitChunkResult {
    // client send time
    int64_t send_timestamp;
    // server received timestamp
    int64_t received_timestamp;
    // client receive response timep
    int64_t start_timestamp;
    int64_t finish_timestamp;
};

struct ExchangeBufferClosureContext {
    int64_t sequence;
    int64_t send_timestamp;
    int64_t start_timestamp;
    bool is_eos;
};

class MultiExchangeBuffer;
// an ExchangeBuffer per instance
// lock-free
class ScheduleTask;
class ExchangeBuffer {
public:
    ExchangeBuffer(MultiExchangeBuffer* parent, TUniqueId instance_id, int num_writers);

    void add_request(TransmitChunkInfo& request);

    bool is_full();

    bool is_finished();

    void cancel_one_sinker();

private:
    friend class ScheduleTask;
    friend class MultiExchangeBuffer;
    bool is_concurreny_exceed_limit();

    // return true if really sent request, otherwise return false
    bool try_to_send_rpc();

    // return true if _last_acked_seqs has moved, otherwise return false
    bool process_rpc_result();

    void update_network_time(const int64_t send_timestamp, const int64_t receive_timestamp);

    MultiExchangeBuffer* _parent;
    std::atomic_bool _is_finishing = false;
    TUniqueId _instance_id;
    PUniqueId _finst_id;

    int32_t _buffer_capacity = -1;
    // fixed size ring buffer
    std::vector<TransmitChunkInfo> _buffer;
    std::vector<std::atomic_bool> _available_flags;
    std::vector<TransmitChunkResult> _results;
    std::vector<std::atomic_bool> _finish_flags;

    std::atomic_int64_t _last_arrived_seqs = -1;
    std::atomic_int64_t _last_in_flight_seqs = -1;
    std::atomic_int64_t _last_acked_seqs = -1;

    int32_t _num_writers;
    std::atomic_int32_t _num_in_flight_rpcs = 0;
    std::atomic_int32_t _num_remaining_eos = 0;
    std::atomic_int32_t _num_uncancelled_sinkers;

    // RuntimeProfile counters
    std::atomic_int64_t _bytes_enqueued = 0;
    std::atomic_int64_t _requests_enqueued = 0;
    std::atomic_int64_t _bytes_sent = 0;
    std::atomic_int64_t _requests_sent = 0;
    int64_t _schedule_count = 0;
    int64_t _schedule_time = 0;
    int64_t _rewardless_schedule_count = 0;
    TimeTrace _network_time;

    static const uint32_t kBufferSize = 1024;
};

using ExchangeBufferPtr = std::shared_ptr<ExchangeBuffer>;

class MultiExchangeBuffer {
public:
    MultiExchangeBuffer(FragmentContext* fragment_ctx, const std::vector<TPlanFragmentDestination>& destinations,
                        bool is_dest_merge, size_t num_sinkers);
    ~MultiExchangeBuffer();

    Status prepare();

    void add_request(TransmitChunkInfo& request);

    bool is_full();

    void set_finishing();

    bool is_finished();

    void cancel_one_sinker();

    void update_profile(RuntimeProfile* profile);

private:
    friend class ExchangeBuffer;

    int64_t network_time();

    FragmentContext* _fragment_ctx;
    const MemTracker* _mem_tracker;
    int32_t _exchange_sink_dop;
    const bool _is_dest_merge;
    int64_t _brpc_timeout_ms;
    std::atomic_bool _is_profile_updated{false};
    // instance_id -> buffer
    phmap::flat_hash_map<int64_t, ExchangeBufferPtr> _buffers;

    // runtime profiles
    std::atomic_int64_t _full_time = 0;
    std::atomic_int64_t _last_full_timestamp = -1;
    std::atomic_int64_t _pending_timestamp = -1;
};

} // namespace pipeline
} // namespace starrocks