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

#include "data_workflows/load/routine_load/data_consumer_pool.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <utility>

#include "common/status.h"

namespace starrocks {

namespace {

struct DestructionSignal {
    std::mutex mutex;
    std::condition_variable cv;
    int count = 0;
};

class IdleDataConsumer final : public DataConsumer {
public:
    explicit IdleDataConsumer(std::shared_ptr<DestructionSignal> destruction_signal)
            : _destruction_signal(std::move(destruction_signal)) {}

    ~IdleDataConsumer() override {
        std::lock_guard<std::mutex> l(_destruction_signal->mutex);
        ++_destruction_signal->count;
        _destruction_signal->cv.notify_all();
    }

    Status init(StreamLoadContext*) override { return Status::OK(); }
    Status consume(StreamLoadContext*) override { return Status::OK(); }
    Status cancel(StreamLoadContext*) override { return Status::OK(); }
    Status reset() override { return Status::OK(); }
    bool match(StreamLoadContext*) override { return false; }

    void mark_idle() { _last_visit_time = time(nullptr) - 601; }

private:
    std::shared_ptr<DestructionSignal> _destruction_signal;
};

} // namespace

TEST(DataConsumerPoolTest, background_worker_cleans_idle_consumer) {
    auto destruction_signal = std::make_shared<DestructionSignal>();
    DataConsumerPool pool(1);
    auto consumer = std::make_shared<IdleDataConsumer>(destruction_signal);
    pool.return_consumer(consumer);
    consumer->mark_idle();
    consumer.reset();

    pool.start_bg_worker();

    std::unique_lock<std::mutex> l(destruction_signal->mutex);
    EXPECT_TRUE(destruction_signal->cv.wait_for(l, std::chrono::seconds(5),
                                                [&] { return destruction_signal->count == 1; }));
    l.unlock();

    pool.stop();
}

TEST(DataConsumerPoolTest, returning_consumer_refreshes_idle_time) {
    auto destruction_signal = std::make_shared<DestructionSignal>();
    DataConsumerPool pool(1);
    auto consumer = std::make_shared<IdleDataConsumer>(destruction_signal);
    consumer->mark_idle();
    pool.return_consumer(consumer);
    consumer.reset();

    pool.start_bg_worker();

    std::unique_lock<std::mutex> l(destruction_signal->mutex);
    EXPECT_FALSE(destruction_signal->cv.wait_for(l, std::chrono::milliseconds(100),
                                                 [&] { return destruction_signal->count == 1; }));
    l.unlock();

    pool.stop();
}

} // namespace starrocks
