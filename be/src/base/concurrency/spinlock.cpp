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

#include "base/concurrency/spinlock.h"

#include <sched.h>

namespace starrocks {

namespace {

inline void cpu_relax() {
#if defined(__i386) || defined(__x86_64__)
    asm volatile("pause\n" : : : "memory");
#elif defined(__aarch64__)
    // A "yield" instruction in aarch64 is essentially a nop, and does
    // not cause enough delay to help backoff. "isb" is a barrier that,
    // especially inside a loop, creates a small delay without consuming
    // ALU resources.
    asm volatile("isb\n" : : : "memory");
#else
    asm volatile("" : : : "memory");
#endif
}

} // namespace

void SpinLock::slow_acquire() {
    int step = 1;
    while (true) {
        for (int probe = 0; probe < NUM_SPIN_PROBES; ++probe) {
            for (int i = 0; i < step; ++i) {
                cpu_relax();
            }
            if (try_lock()) {
                return;
            }
            if (step < MAX_SPIN_STEP) {
                step <<= 1;
            }
        }
        sched_yield();
        if (try_lock()) {
            return;
        }
    }
}

} // end namespace starrocks
