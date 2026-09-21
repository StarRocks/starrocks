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

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <atomic>
#include <cstdint>
#include <set>
#include <string>

#include "base/concurrency/countdown_latch.h"

namespace starrocks {

// Records the TabletsChannel::add_chunk() calls that are in progress, so that one which never
// returns can be named from outside.
//
// A stuck add_chunk is invisible to every ordinary tool. It parks a bthread, which has no stack
// of its own for a thread dump to show, and the tablet_writer_add_chunks closure it holds keeps
// its connection from ever being recycled -- which is what later stops the BE from finishing
// brpc's Server::Join() on exit. All a dump can say is that the closure is outstanding; it
// cannot say how far the call got or what it is still waiting to be told.
//
// A probe lives on the stack of the call it describes and deregisters itself when that call
// returns, so whatever the scan still finds is by definition not making progress.
class AddChunkProbe {
public:
    using Latch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

    AddChunkProbe(int64_t txn_id, int64_t index_id, int32_t sender_id, bool eos);
    ~AddChunkProbe();

    AddChunkProbe(const AddChunkProbe&) = delete;
    AddChunkProbe& operator=(const AddChunkProbe&) = delete;

    // Where the call has got to. |phase| must outlive the probe: pass a literal.
    void set_phase(const char* phase) { _phase.store(phase, std::memory_order_relaxed); }

    // The latch the call will block on, once it exists. It must outlive this probe, so declare
    // the probe after it.
    void watch_latch(const Latch* latch, uint64_t expected);

    // A count the call is waiting for, and the tablet that owes it. Whatever is left when the
    // call stops making progress is the answer.
    void owed_by(int64_t tablet_id);
    void settled_by(int64_t tablet_id);

    // Logs every call that has been running for longer than |threshold_seconds|, at WARNING.
    static void log_stuck_calls(int64_t threshold_seconds);

private:
    std::string describe(int64_t now_ns) const;

    const int64_t _txn_id;
    const int64_t _index_id;
    const int32_t _sender_id;
    const bool _eos;
    const int64_t _start_ns;

    std::atomic<const char*> _phase;
    std::atomic<const Latch*> _latch{nullptr};
    std::atomic<uint64_t> _latch_expected{0};

    mutable bthread::Mutex _owed_lock;
    std::set<int64_t> _owed;
};

} // namespace starrocks
