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

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <atomic>

#include "google/protobuf/stubs/callback.h"
#include "util/countdown_latch.h"

namespace starrocks {

class MockClosure : public ::google::protobuf::Closure {
public:
    MockClosure() = default;
    ~MockClosure() override = default;

    void Run() override { _run.store(true); }

    bool has_run() { return _run.load(); }

private:
    std::atomic_bool _run = false;
};

class MockCountDownClosure : public MockClosure {
public:
    using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;
    MockCountDownClosure() : _latch(1) {}
    ~MockCountDownClosure() override = default;

    void wait() { _latch.wait(); }

    void Run() override {
        MockClosure::Run();
        _latch.count_down();
    }

private:
    BThreadCountDownLatch _latch;
};

} // namespace starrocks
