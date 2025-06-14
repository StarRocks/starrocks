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

#include "util/stack_trace_mutex.h"

#include <gtest/gtest.h>

#include <thread>

namespace starrocks {

template <class T>
class StackTraceMutexTest : public testing::Test {
protected:
    StackTraceMutexTest() : _mutex() {}

    ~StackTraceMutexTest() override = default;

    StackTraceMutex<T> _mutex;
};

typedef testing::Types<std::timed_mutex, bthread::Mutex> Implmentations;

TYPED_TEST_SUITE(StackTraceMutexTest, Implmentations);

TYPED_TEST(StackTraceMutexTest, test_lock_unlock) {
    this->_mutex.lock();
    this->_mutex.unlock();
}

TYPED_TEST(StackTraceMutexTest, test_multi_thread) {
    int counter = 0;

    std::thread t1([&]() {
        for (int i = 0; i < 500; i++) {
            this->_mutex.lock();
            counter += 1;
            this->_mutex.unlock();
        }
    });

    std::thread t2([&]() {
        for (int i = 0; i < 600; i++) {
            this->_mutex.lock();
            counter += 1;
            this->_mutex.unlock();
        }
    });

    for (int i = 0; i < 1000; i++) {
        this->_mutex.lock();
        counter += 1;
        this->_mutex.unlock();
    }

    t1.join();
    t2.join();
    ASSERT_EQ(2100, counter);
}

TYPED_TEST(StackTraceMutexTest, test_timed_lock) {
    bool locked = this->_mutex.try_lock_for(std::chrono::seconds(1));
    ASSERT_TRUE(locked);
    this->_mutex.unlock();
}

TYPED_TEST(StackTraceMutexTest, test_timed_lock_fail) {
    this->_mutex.lock();
    bool locked = this->_mutex.try_lock_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(locked);
    this->_mutex.unlock();
}

} // namespace starrocks
