// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/pipeline_driver_queue.h"
#include "util/sys_futex.h"

namespace starrocks::pipeline {

class ParkingLot {
public:
    class State {
    public:
        explicit State(int32_t val) : _val(val) {}

        int32_t val() const { return _val; }

        bool closed() const { return _val & 1; }

    private:
        int32_t _val;
    };

    State get_state() { return State(_state.load(std::memory_order_acquire)); }

    void close() {
        _state.fetch_or(1, std::memory_order_release);
        notify_all();
    }

    void wait(State expected) { futex_wait_private(reinterpret_cast<int32_t*>(&_state), expected.val(), nullptr); }

    int notify_one() { return notify(1); }

    int notify_all() { return notify(std::numeric_limits<int32_t>::max()); }

    int notify(int nwake) {
        _state.fetch_add(1 << 1, std::memory_order_release);
        return futex_wake_private(reinterpret_cast<int32_t*>(&_state), nwake);
    }

private:
    // The lowest bit is denoted whether it is closed.
    std::atomic<int32_t> _state = 0;
};

class DriverQueueManager {
public:
    DriverQueueManager() = default;
    ~DriverQueueManager() = default;

    void initialize(int num_dispatchers);

    void close();

    // TODO: make it able to change the number of dispatchers.
    void change_num_dispatchers(int new_num_dispatchers) {}

    StatusOr<DriverRawPtr> take(int dispatcher_id, size_t* queue_index, bool* is_from_remote);
    // Put the driver back to _queue_per_dispatcher if dispatcher_id is not -1.
    // Otherwise, put the driver to a random _remote_queue_per_dispatcher.
    void put_back(const DriverRawPtr driver);
    void put_back(const std::vector<DriverRawPtr>& drivers);

    void notify(int dispatcher_id, int num_drivers);

    SubQuerySharedDriverQueue* get_sub_queue(int dispatcher_id, size_t queue_index, bool is_from_remote);

    // Generate dispatcher id when the dispatcher thread starts.
    int gen_dispatcher_id() { return _next_dispatcher_id.fetch_add(1, std::memory_order_relaxed); }

private:
    int _random_dispatcher_id();

    static constexpr int NUM_PL = 4;

    // Used to block and notify dispatcher threads.
    // Multiple dispatcher waits in one pl.
    ParkingLot _pls[NUM_PL];

    int _num_dispatchers = 0;
    // Dispatcher thread puts the driver back to its own local queue.
    std::vector<std::unique_ptr<QuerySharedDriverQueue>> _queue_per_dispatcher;
    // Non-dispatcher thread puts the driver to a remote queue.
    std::vector<std::unique_ptr<QuerySharedDriverQueue>> _remote_queue_per_dispatcher;

    // Generate dispatcher id when the dispatcher thread starts.
    std::atomic<int> _next_dispatcher_id = 0;

    // Each start pos is corresponding to a different step size coprime with _num_dispatchers.
    // In this way, make the steal order of the different start pos different.
    std::vector<int> _rand_step_sizes;
};

} // namespace starrocks::pipeline