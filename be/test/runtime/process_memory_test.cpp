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

#include "runtime/memory/process_memory.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"

namespace starrocks {

class ProcessMemoryTest : public testing::Test {
protected:
    void SetUp() override {
        auto* env = RuntimeEnv::GetInstance();
        _was_initialized = RuntimeEnv::is_init();
        _previous_tracker = env->swap_process_mem_tracker_for_test(std::make_shared<MemTracker>());
        env->_is_init = true;
    }
    void TearDown() override {
        auto* env = RuntimeEnv::GetInstance();
        env->_is_init = _was_initialized;
        env->swap_process_mem_tracker_for_test(std::move(_previous_tracker));
    }

private:
    bool _was_initialized;
    std::shared_ptr<MemTracker> _previous_tracker;
};

// BE_TEST replaces the malloc hook. These tests verify scope/ownership contracts,
// not production hook byte accounting. The latter needs a non-BE_TEST binary.
TEST_F(ProcessMemoryTest, RestoresCallerOnSuccessErrorAndException) {
    auto* process = process_memory_tracker();
    ASSERT_NE(process, nullptr);
    MemTracker task(-1, "task", process);
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&task);

    ASSERT_OK(with_process_memory(true, [&] {
        EXPECT_EQ(CurrentThread::mem_tracker(), process);
        return Status::OK();
    }));
    EXPECT_EQ(CurrentThread::mem_tracker(), &task);

    auto status = with_process_memory(true, [&] {
        EXPECT_EQ(CurrentThread::mem_tracker(), process);
        return Status::InternalError("owned error message");
    });
    EXPECT_TRUE(status.is_internal_error());
    EXPECT_EQ(status.message(), "owned error message");
    EXPECT_EQ(CurrentThread::mem_tracker(), &task);

    EXPECT_THROW(with_process_memory(true, []() -> Status { throw std::bad_alloc(); }), std::bad_alloc);
    EXPECT_EQ(CurrentThread::mem_tracker(), &task);

    ASSERT_OK(with_process_memory(false, [&] {
        EXPECT_EQ(CurrentThread::mem_tracker(), &task);
        return Status::OK();
    }));
}

TEST_F(ProcessMemoryTest, BorrowedStreamScopeDoesNotChangeMetadataOwner) {
    auto* process = process_memory_tracker();
    ASSERT_NE(process, nullptr);
    MemTracker task(-1, "reader", process);
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&task);

    ASSERT_OK(with_process_memory(true, [&] {
        EXPECT_EQ(CurrentThread::mem_tracker(), process);
        auto status = with_memory_tracker(&task, [&] {
            EXPECT_EQ(CurrentThread::mem_tracker(), &task);
            return Status::IOError("borrowed stream failure");
        });
        EXPECT_TRUE(status.is_io_error());
        EXPECT_EQ(CurrentThread::mem_tracker(), process);
        return Status::OK();
    }));
    EXPECT_EQ(CurrentThread::mem_tracker(), &task);
}

TEST_F(ProcessMemoryTest, StorageAllocatorDoesNotChangePayloadDestructorContext) {
    auto* process = process_memory_tracker();
    ASSERT_NE(process, nullptr);
    MemTracker task(-1, "consumer", process);
    struct Payload {
        MemTracker** destroyed_under;
        ~Payload() { *destroyed_under = CurrentThread::mem_tracker(); }
    };
    MemTracker* destroyed_under = nullptr;
    std::weak_ptr<Payload> weak;
    {
        auto ptr = std::allocate_shared<Payload>(ProcessMemoryAllocator<Payload>());
        ptr->destroyed_under = &destroyed_under;
        weak = ptr;
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&task);
        ptr.reset();
        EXPECT_EQ(destroyed_under, &task);
        EXPECT_TRUE(weak.expired());
        EXPECT_EQ(CurrentThread::mem_tracker(), &task);
    }
    // Deallocating a weak-only control block must not depend on a task tracker
    // retained by the allocator, nor invoke the payload destructor twice.
    weak.reset();
    EXPECT_EQ(destroyed_under, &task);
}

} // namespace starrocks
