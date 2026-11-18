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
#include "base/logging.h"

namespace starrocks {

// Lightweight spinlock.
//
// Keeps `bool _locked` (1 byte, trivially movable) rather than `std::atomic<bool>`
// so containers such as `ObjectPool` can default their move constructors while all
// accesses to `_locked` go through explicit C++11-model `__atomic_*` builtins.
class SpinLock {
public:
    SpinLock() = default;

    // Acquires the lock, spins until the lock becomes available.
    void lock() {
        if (!try_lock()) [[unlikely]] {
            slow_acquire();
        }
    }

    void unlock() {
        DCHECK(__atomic_load_n(&_locked, __ATOMIC_RELAXED));
        // Release store: ensures all prior reads and writes in the critical section
        // are visible before `_locked` becomes false (emits `stlrb` on ARM64 and
        // plain `movb` on x86_64 without a full `dmb ish` / `mfence`).
        __atomic_store_n(&_locked, false, __ATOMIC_RELEASE);
    }

    // Tries to acquire the lock using Test-and-Test-and-Set (TTAS):
    // 1. Relaxed load avoids taking exclusive cache-line ownership when already locked.
    // 2. Acquire exchange (`swpab` / `ldaxrb`+`stxrb` on ARM64, `xchgb` on x86_64)
    //    avoids the full `SEQ_CST` barrier of `__sync_bool_compare_and_swap`.
    inline bool try_lock() {
        return !__atomic_load_n(&_locked, __ATOMIC_RELAXED) && !__atomic_exchange_n(&_locked, true, __ATOMIC_ACQUIRE);
    }

    void dcheck_locked() const { DCHECK(__atomic_load_n(&_locked, __ATOMIC_RELAXED)); }

private:
    // Out-of-line definition of the actual spin loop to keep the inlined fast path compact.
    void slow_acquire();

    // Number of exponential-backoff TTAS probes per spin round before yielding the OS thread slice,
    // and maximum cpu_relax() count per probe (1 -> 2 -> 4 -> 8 -> 16 -> 32 -> 64).
    static constexpr int NUM_SPIN_PROBES = 4;
    static constexpr int MAX_SPIN_STEP = 64;

    bool _locked{false};
};

} // end namespace starrocks
