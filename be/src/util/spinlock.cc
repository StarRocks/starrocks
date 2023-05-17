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

#include "util/spinlock.h"

namespace starrocks {

void SpinLock::slow_acquire() {
    while (true) {
        if (try_lock()) {
            return;
        }
        for (int i = 0; i < NUM_SPIN_CYCLES; ++i) {
#if (defined(__i386) || defined(__x86_64__))
            asm volatile("pause\n" : : : "memory");
#elif defined(__aarch64__)
            // A "yield" instruction in aarch64 is essentially a nop, and does
            // not cause enough delay to help backoff. "isb" is a barrier that,
            // especially inside a loop, creates a small delay without consuming
            // ALU resources.  Experiments shown that adding the isb instruction
            // improves stability and reduces result jitter. Adding more delay
            // to the UT_RELAX_CPU than a single isb reduces performance.
            asm volatile("isb\n" ::: "memory");
#endif
        }
        if (try_lock()) {
            return;
        }
        sched_yield();
    }
}

} // end namespace starrocks
