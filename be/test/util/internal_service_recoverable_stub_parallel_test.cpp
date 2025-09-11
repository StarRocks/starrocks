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

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <thread>

#include "util/internal_service_recoverable_stub.h"
#include "util/ref_count_closure.h"

namespace starrocks {

class PInternalService_RecoverableStub_ParallelTest : public testing::Test {
public:
    PInternalService_RecoverableStub_ParallelTest() = default;
    ~PInternalService_RecoverableStub_ParallelTest() override = default;
};

TEST_F(PInternalService_RecoverableStub_ParallelTest, test_parallel_reset_execute) {
    butil::EndPoint endpoint;
    auto res = butil::str2endpoint("127.0.0.1", 53343, &endpoint);
    EXPECT_EQ(res, 0);

    std::shared_ptr<starrocks::PInternalService_RecoverableStub> stub =
            std::make_shared<starrocks::PInternalService_RecoverableStub>(endpoint);
    EXPECT_TRUE(stub->reset_channel().ok());

    std::atomic<bool> running = true;
    std::thread reset_thread([&] {
        while (running.load()) {
            stub->reset_channel();
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
    });

    constexpr int num_threads = 20;
    constexpr int repeats_in_single_round = 1000;
    std::vector<std::thread> send_threads;
    for (int i = 0; i < num_threads; ++i) {
        send_threads.emplace_back([&] {
            std::vector<RefCountClosure<PTabletWriterAddBatchResult>*> closures;
            closures.reserve(repeats_in_single_round);
            while (running.load()) {
                while (closures.size() < repeats_in_single_round && running.load()) {
                    PTabletWriterAddChunkRequest request;
                    auto* closure = new starrocks::RefCountClosure<PTabletWriterAddBatchResult>();
                    closure->ref(); // retain itself to be managed in the vector
                    closures.push_back(closure);
                    closure->ref(); // retain it to be used when closure->Run() is invoked.
                    stub->tablet_writer_add_chunk(&closure->cntl, &request, &closure->result, closure);
                    std::this_thread::sleep_for(std::chrono::microseconds(1));
                }
                // wait for closures done and release the resources
                for (auto* closure : closures) {
                    // make sure the closure is done
                    closure->join();
                    // dereference it to release the memory
                    if (closure->unref()) {
                        delete closure;
                    }
                }
                closures.clear();
            }
            EXPECT_TRUE(closures.empty());
        });
    }

    std::this_thread::sleep_for(std::chrono::seconds(2));
    running = false;

    reset_thread.join();
    for (auto& t : send_threads) {
        t.join();
    }
}

TEST_F(PInternalService_RecoverableStub_ParallelTest, test_reset_channel_with_connection_group) {
    butil::EndPoint endpoint;
    auto res = butil::str2endpoint("127.0.0.1", 53344, &endpoint);
    EXPECT_EQ(res, 0);
    std::shared_ptr<starrocks::PInternalService_RecoverableStub> stub =
            std::make_shared<starrocks::PInternalService_RecoverableStub>(endpoint);

    // reset channel without the next_connection_group parameter, always allowed
    EXPECT_EQ(0, stub->connection_group());
    EXPECT_TRUE(stub->reset_channel().ok());
    EXPECT_EQ(1, stub->connection_group());
    EXPECT_TRUE(stub->reset_channel().ok());
    EXPECT_EQ(2, stub->connection_group());

    // reset channel with the correct next_connection_group
    EXPECT_TRUE(stub->reset_channel(3).ok());
    EXPECT_EQ(3, stub->connection_group());

    // reset with wrong next_connection_group, connection_group() won't increase
    EXPECT_TRUE(stub->reset_channel(5).ok());
    EXPECT_EQ(3, stub->connection_group());

    EXPECT_TRUE(stub->reset_channel(3).ok());
    EXPECT_EQ(3, stub->connection_group());

    // rest channel with correct next_connection_group
    EXPECT_TRUE(stub->reset_channel(4).ok());
    EXPECT_EQ(4, stub->connection_group());
}

TEST_F(PInternalService_RecoverableStub_ParallelTest, test_parallel_reset_channel_exclusive) {
    butil::EndPoint endpoint;
    auto res = butil::str2endpoint("127.0.0.1", 53345, &endpoint);
    EXPECT_EQ(res, 0);
    std::shared_ptr<starrocks::PInternalService_RecoverableStub> stub =
            std::make_shared<starrocks::PInternalService_RecoverableStub>(endpoint);
    stub->reset_channel();
    int next_connection_group = stub->connection_group() + 1;

    int num_threads = 100;
    std::vector<std::thread> reset_threads;
    std::promise<void> ready_promise;
    std::shared_future<void> ready_future(ready_promise.get_future());
    for (int i = 0; i < num_threads; ++i) {
        reset_threads.emplace_back([&] {
            ready_future.wait();
            stub->reset_channel(next_connection_group);
        });
    }

    // allow all the threads to reset_channel() immediately
    ready_promise.set_value();
    for (auto& t : reset_threads) {
        t.join();
    }
    // only one of the threads can reset the channel successfully.
    EXPECT_EQ(next_connection_group, stub->connection_group());
}

} // namespace starrocks
