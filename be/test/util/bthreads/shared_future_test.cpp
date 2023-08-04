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
//
// Copyright (C) 2010-2023 Free Software Foundation, Inc.
//
// This file is part of the GNU ISO C++ Library.  This library is free
// software; you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the
// Free Software Foundation; either version 3, or (at your option)
// any later version.

// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License along
// with this library; see the file COPYING3.  If not see
// <http://www.gnu.org/licenses/>.
//
// original source location refer to:
//  https://github.com/gcc-mirror/gcc/tree/master/libstdc%2B%2B-v3/testsuite/30_threads/shared_future

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include "util/bthreads/future.h"

namespace starrocks::bthreads {

namespace {
class ClassType {
private:
    int x;
};

class AbstractClass {
public:
    virtual ~AbstractClass() = default;
    virtual void foo() = 0;
};
shared_future<int> get() {
    return promise<int>().get_future();
}
} // namespace

TEST(SharedFutureTest, test_default_ctor) {
    bool __attribute__((unused)) test = true;

    shared_future<int> p1;
    EXPECT_TRUE(!p1.valid());
    shared_future<int&> p2;
    EXPECT_TRUE(!p2.valid());
    shared_future<void> p3;
    EXPECT_TRUE(!p3.valid());
    shared_future<ClassType> p4;
    EXPECT_TRUE(!p4.valid());
    shared_future<AbstractClass&> p5;
    EXPECT_TRUE(!p5.valid());
}

TEST(SharedFutureTest, test_move_ctor) {
    promise<int> p1;
    shared_future<int> f1(p1.get_future());
    shared_future<int> f2(std::move(f1));
}

TEST(SharedFutureTest, test_move_assign) {
    shared_future<int> p1;
    shared_future<int> p2 = get();
    p1 = std::move(p2);
    EXPECT_TRUE(p1.valid());
    EXPECT_TRUE(!p2.valid());
}

TEST(SharedFutureTest, test_exception_01) {
    shared_future<int> f;
    EXPECT_THROW(
            {
                try {
                    f.get();
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

TEST(SharedFutureTest, test_exception02) {
    shared_future<int&> f;
    EXPECT_THROW(
            {
                try {
                    f.get();
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

TEST(SharedFutureTest, test_exception03) {
    shared_future<void> f;
    try {
        f.get();
        EXPECT_TRUE(false);
    } catch (std::future_error& e) {
        EXPECT_TRUE(e.code() == std::future_errc::no_state);
    }
}

static int value = 99;

TEST(SharedFutureTest, test_get01) {
    promise<int> p1;
    const shared_future<int> f1(p1.get_future());
    shared_future<int> f2(f1);

    p1.set_value(value);
    EXPECT_TRUE(f1.get() == value);
    EXPECT_TRUE(f2.get() == value);
}

TEST(SharedFutureTest, test_get02) {
    promise<int&> p1;
    const shared_future<int&> f1(p1.get_future());
    shared_future<int&> f2(f1);

    p1.set_value(value);
    EXPECT_TRUE(&f1.get() == &value);
    EXPECT_TRUE(&f2.get() == &value);
}

TEST(SharedFutureTest, test_get03) {
    promise<void> p1;
    const shared_future<void> f1(p1.get_future());
    shared_future<void> f2(f1);

    p1.set_value();
    f1.get();
    f2.get();
}

TEST(SharedFutureTest, test_get04) {
    promise<int> p1;
    shared_future<int> f1(p1.get_future());
    shared_future<int> f2(f1);

    p1.set_exception(std::make_exception_ptr(value));
    try {
        (void)f1.get();
        EXPECT_TRUE(false);
    } catch (int& e) {
        EXPECT_TRUE(e == value);
    }
    try {
        (void)f2.get();
        EXPECT_TRUE(false);
    } catch (int& e) {
        EXPECT_TRUE(e == value);
    }
}

TEST(SharedFutureTest, test_get05) {
    promise<int&> p1;
    shared_future<int&> f1(p1.get_future());
    shared_future<int&> f2(f1);

    p1.set_exception(std::make_exception_ptr(value));
    try {
        (void)f1.get();
        EXPECT_TRUE(false);
    } catch (int& e) {
        EXPECT_TRUE(e == value);
    }
    try {
        (void)f2.get();
        EXPECT_TRUE(false);
    } catch (int& e) {
        EXPECT_TRUE(e == value);
    }
}

TEST(SharedFutureTest, test_get06) {
    promise<void> p1;
    shared_future<void> f1(p1.get_future());
    shared_future<void> f2(f1);

    p1.set_exception(std::make_exception_ptr(value));
    try {
        f1.get();
        EXPECT_TRUE(false);
    } catch (int& e) {
        EXPECT_TRUE(e == value);
    }
    try {
        f2.get();
        EXPECT_TRUE(false);
    } catch (int& e) {
        EXPECT_TRUE(e == value);
    }
}

TEST(SharedFutureTest, test_valid01) {
    shared_future<int> f0;

    EXPECT_TRUE(!f0.valid());

    promise<int> p1;
    shared_future<int> f1(p1.get_future());
    shared_future<int> f2(f1);

    EXPECT_TRUE(f1.valid());
    EXPECT_TRUE(f2.valid());

    p1.set_value(1);

    EXPECT_TRUE(f1.valid());
    EXPECT_TRUE(f2.valid());

    f1 = std::move(f0);

    EXPECT_TRUE(!f0.valid());
    EXPECT_TRUE(!f1.valid());
    EXPECT_TRUE(f2.valid());
}

static void fut_wait(shared_future<void> f) {
    f.wait();
}

static void* fut_wait02(void* arg) {
    auto f = static_cast<shared_future<void>*>(arg);
    f->wait();
    return nullptr;
}

TEST(SharedFutureTest, test_wait01) {
    promise<void> p1;
    shared_future<void> f1(p1.get_future());

    std::thread t1(fut_wait, f1);
    std::thread t2(fut_wait, f1);
    std::thread t3(fut_wait, f1);

    p1.set_value();
    t1.join();
    t2.join();
    t3.join();
}

TEST(SharedFutureTest, test_wait02) {
    promise<void> p1;
    shared_future<void> f1(p1.get_future());

    bthread_t t1, t2, t3;
    EXPECT_EQ(0, bthread_start_background(&t1, nullptr, fut_wait02, &f1));
    EXPECT_EQ(0, bthread_start_background(&t2, nullptr, fut_wait02, &f1));
    EXPECT_EQ(0, bthread_start_background(&t3, nullptr, fut_wait02, &f1));

    p1.set_value();

    bthread_join(t1, nullptr);
    bthread_join(t2, nullptr);
    bthread_join(t3, nullptr);
}

TEST(SharedFutureTest, test_wait_for01) {
    promise<int> p1;
    shared_future<int> f1(p1.get_future());
    shared_future<int> f2(f1);

    std::chrono::milliseconds delay(100);

    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::timeout);
    EXPECT_TRUE(f2.wait_for(delay) == std::future_status::timeout);

    p1.set_value(1);

    auto before = std::chrono::system_clock::now();
    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::ready);
    EXPECT_TRUE(f2.wait_for(delay) == std::future_status::ready);
    EXPECT_TRUE(std::chrono::system_clock::now() < (before + 2 * delay));
}

static std::chrono::system_clock::time_point make_time(int i) {
    return std::chrono::system_clock::now() + std::chrono::milliseconds(i);
}

TEST(SharedFutureTest, test_wait_until01) {
    promise<int> p1;
    shared_future<int> f1(p1.get_future());
    shared_future<int> f2(f1);

    auto when = make_time(10);
    EXPECT_TRUE(f1.wait_until(make_time(10)) == std::future_status::timeout);
    EXPECT_TRUE(std::chrono::system_clock::now() >= when);

    when = make_time(10);
    EXPECT_TRUE(f2.wait_until(make_time(10)) == std::future_status::timeout);
    EXPECT_TRUE(std::chrono::system_clock::now() >= when);

    p1.set_value(1);

    when = make_time(100);
    EXPECT_TRUE(f1.wait_until(when) == std::future_status::ready);
    EXPECT_TRUE(f2.wait_until(when) == std::future_status::ready);
    EXPECT_TRUE(std::chrono::system_clock::now() < when);
}

} // namespace starrocks::bthreads
