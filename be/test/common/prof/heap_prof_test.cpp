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

#include "common/prof/heap_prof.h"

#include <gtest/gtest.h>

#include "jemalloc/jemalloc.h"
#include "util/defer_op.h"

namespace starrocks {

namespace {

#ifndef __APPLE__

// Reading this node only needs jemalloc to be built with profiling support; writing it is
// what additionally requires the process to have been started with `prof:true`.
bool read_thread_active_init(bool* value) {
    size_t size = sizeof(*value);
    return je_mallctl("prof.thread_active_init", value, &size, nullptr, 0) == 0;
}

bool prof_enabled_at_startup() {
    bool enabled = false;
    size_t size = sizeof(enabled);
    return je_mallctl("opt.prof", &enabled, &size, nullptr, 0) == 0 && enabled;
}

#endif

} // namespace

// Toggling the heap profile must leave `prof.thread_active_init` alone. It is not a second
// switch but the value copied into thread.prof.active once, when a thread is created, and a
// thread may only change its own -- so a thread started while the flag was down would stay
// unsampled for the rest of its life, and re-enabling could not repair it. It also belongs to
// the operator, who may have started the process with `prof_thread_active_init:false` to
// sample selected threads only.
TEST(HeapProfTest, toggling_the_profile_keeps_thread_active_init) {
#ifdef __APPLE__
    GTEST_SKIP() << "HeapProf is a no-op on macOS: enable_prof() and disable_prof() do nothing and "
                    "has_enable() always reports false, so there is no toggle to observe";
#else
    if (!prof_enabled_at_startup()) {
        GTEST_SKIP() << "the process was not started with prof:true, so prof.thread_active_init "
                        "cannot be written and the regression cannot be reproduced";
    }

    // Restore the global switch on the way out, including when an assertion returns early:
    // the test only runs on a process started with prof:true, which is exactly a process
    // whose remaining tests would then run with profiling silently turned off.
    const bool was_active = HeapProf::getInstance().has_enable();
    DeferOp restore([was_active] {
        if (was_active) {
            HeapProf::getInstance().enable_prof();
        } else {
            HeapProf::getInstance().disable_prof();
        }
    });

    bool before = false;
    ASSERT_TRUE(read_thread_active_init(&before));

    HeapProf::getInstance().enable_prof();
    ASSERT_TRUE(HeapProf::getInstance().has_enable());

    bool while_enabled = false;
    ASSERT_TRUE(read_thread_active_init(&while_enabled));
    EXPECT_EQ(before, while_enabled) << "enabling the heap profile must not touch prof.thread_active_init";

    HeapProf::getInstance().disable_prof();
    ASSERT_FALSE(HeapProf::getInstance().has_enable());

    bool after = false;
    ASSERT_TRUE(read_thread_active_init(&after));
    EXPECT_EQ(before, after) << "disabling the heap profile must not touch prof.thread_active_init";
#endif
}

} // namespace starrocks
