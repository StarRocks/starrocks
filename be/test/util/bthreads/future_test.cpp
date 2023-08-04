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
//  https://github.com/gcc-mirror/gcc/tree/master/libstdc%2B%2B-v3/testsuite/30_threads/future

#include "util/bthreads/future.h"

#include <bthread/bthread.h>
#include <gtest/gtest.h>

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

future<int> get() {
    return promise<int>().get_future();
}

} // namespace

TEST(FutureTest, test_default_ctor) {
    future<int> p1;
    EXPECT_FALSE(p1.valid());
    future<int&> p2;
    EXPECT_FALSE(p2.valid());
    future<void> p3;
    EXPECT_FALSE(p3.valid());
    future<ClassType> p4;
    EXPECT_FALSE(p4.valid());
    future<AbstractClass&> p5;
    EXPECT_FALSE(p5.valid());
}

TEST(FutureTest, test_move) {
    promise<int> p1;
    future<int> f1(p1.get_future());
    future<int> f2(std::move(f1));
    EXPECT_TRUE(f2.valid());
    EXPECT_FALSE(f1.valid());
}

TEST(FutureTest, test_move_assign) {
    future<int> p1;
    future<int> p2 = get();
    p1 = std::move(p2);
    EXPECT_TRUE(p1.valid());
    EXPECT_FALSE(p2.valid());
}

TEST(FutureTest, test_exception_no_state01) {
    bthreads::promise<int> p;
    bthreads::future<int> f = p.get_future();
    p.set_value(10);
    EXPECT_EQ(10, f.get());
    EXPECT_THROW(
            {
                try {
                    f.get();
                    EXPECT_FALSE(true) << "unreachable";
                } catch (const std::future_error& e) {
                    EXPECT_EQ(std::future_errc::no_state, e.code());
                    throw;
                }
            },
            std::future_error);
}

TEST(FutureTest, test_exception_no_state02) {
    bthreads::promise<int&> p;
    bthreads::future<int&> f = p.get_future();
    int i = 1;
    p.set_value(i);
    EXPECT_EQ(&i, &f.get());
    EXPECT_THROW(
            {
                try {
                    f.get();
                    EXPECT_FALSE(true) << "unreachable";
                } catch (const std::future_error& e) {
                    EXPECT_EQ(std::future_errc::no_state, e.code());
                    throw;
                }
            },
            std::future_error);
}

TEST(FutureTest, test_exception_no_state03) {
    bthreads::promise<void> p;
    bthreads::future<void> f = p.get_future();
    p.set_value();
    f.get();
    EXPECT_THROW(
            {
                try {
                    f.get();
                    EXPECT_FALSE(true) << "unreachable";
                } catch (const std::future_error& e) {
                    EXPECT_EQ(std::future_errc::no_state, e.code());
                    throw;
                }
            },
            std::future_error);
}

static int value = 99;

TEST(FutureTest, test_get01) {
    promise<int> p1;
    future<int> f1(p1.get_future());

    p1.set_value(value);
    EXPECT_EQ(value, f1.get());
    EXPECT_FALSE(f1.valid());
}

TEST(FutureTest, test_get02) {
    promise<int&> p1;
    future<int&> f1(p1.get_future());

    p1.set_value(value);
    EXPECT_EQ(&f1.get(), &value);
    EXPECT_FALSE(f1.valid());
}

TEST(FutureTest, test_get03) {
    promise<void> p1;
    future<void> f1(p1.get_future());

    p1.set_value();
    f1.get();
    EXPECT_FALSE(f1.valid());
}

TEST(FutureTest, test_get04) {
    promise<int> p1;
    future<int> f1(p1.get_future());

    p1.set_exception(std::make_exception_ptr(value));
    try {
        (void)f1.get();
        EXPECT_TRUE(false);
    } catch (int& e) {
        EXPECT_EQ(e, value);
    }
    EXPECT_FALSE(f1.valid());
}

TEST(FutureTest, test_get05) {
    promise<int&> p1;
    future<int&> f1(p1.get_future());

    p1.set_exception(std::make_exception_ptr(value));
    try {
        (void)f1.get();
        EXPECT_TRUE(false);
    } catch (int& e) {
        EXPECT_EQ(e, value);
    }
    EXPECT_FALSE(f1.valid());
}

TEST(FutureTest, test_get06) {
    promise<void> p1;
    future<void> f1(p1.get_future());

    p1.set_exception(std::make_exception_ptr(value));
    try {
        f1.get();
        EXPECT_TRUE(false);
    } catch (int& e) {
        EXPECT_EQ(value, e);
    }
    EXPECT_FALSE(f1.valid());
}

static int iterations = 200;

template <typename Duration>
static double print(const char* desc, Duration dur) {
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(dur).count();
    double d = double(ns) / iterations;
    std::cout << desc << ": " << ns << "ns for " << iterations << " calls, avg " << d << "ns per call\n";
    return d;
}

TEST(FutureTest, test_poll) {
    promise<int> p;
    future<int> f = p.get_future();

start_over:
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) f.wait_for(std::chrono::seconds(0));
    auto stop = std::chrono::high_resolution_clock::now();

    /* We've run too few iterations for the clock resolution.
     Attempt to calibrate it.  */
    if (start == stop) {
        /* After set_value, wait_for is faster, so use that for the
	 calibration to avoid zero at low clock resultions.  */
        promise<int> pc;
        future<int> fc = pc.get_future();
        pc.set_value(1);

        /* Loop until the clock advances, so that start is right after a
	 time increment.  */
        do
            start = std::chrono::high_resolution_clock::now();
        while (start == stop);
        int i = 0;
        /* Now until the clock advances again, so that stop is right
	 after another time increment.  */
        do {
            fc.wait_for(std::chrono::seconds(0));
            stop = std::chrono::high_resolution_clock::now();
            i++;
        } while (start == stop);
        /* Go for some 10 cycles, but if we're already past that and
	 still get into the calibration loop, double the iteration
	 count and try again.  */
        if (iterations < i * 10)
            iterations = i * 10;
        else
            iterations *= 2;
        goto start_over;
    }

    double wait_for_0 = print("wait_for(0s)", stop - start);

    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) f.wait_until(std::chrono::system_clock::time_point::min());
    stop = std::chrono::high_resolution_clock::now();
    double wait_until_sys_min __attribute__((unused)) = print("wait_until(system_clock minimum)", stop - start);

    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) f.wait_until(std::chrono::steady_clock::time_point::min());
    stop = std::chrono::high_resolution_clock::now();
    double wait_until_steady_min __attribute__((unused)) = print("wait_until(steady_clock minimum)", stop - start);

    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) f.wait_until(std::chrono::system_clock::time_point());
    stop = std::chrono::high_resolution_clock::now();
    double wait_until_sys_epoch __attribute__((unused)) = print("wait_until(system_clock epoch)", stop - start);

    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) f.wait_until(std::chrono::steady_clock::time_point());
    stop = std::chrono::high_resolution_clock::now();
    double wait_until_steady_epoch __attribute__((unused)) = print("wait_until(steady_clock epoch", stop - start);

    p.set_value(1);

    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) f.wait_for(std::chrono::seconds(0));
    stop = std::chrono::high_resolution_clock::now();
    double ready = print("wait_for when ready", stop - start);

    // Polling before ready with wait_for(0s) should be almost as fast as
    // after the result is ready.
    EXPECT_TRUE(wait_for_0 < (ready * 30));

    // Polling before ready using wait_until(min) should not be terribly slow.
    EXPECT_TRUE(wait_until_sys_min < (ready * 100));
    EXPECT_TRUE(wait_until_steady_min < (ready * 100));
}

TEST(FutureTest, test_share01) {
    promise<int> p1;
    future<int> f1(p1.get_future());
    shared_future<int> f2 = f1.share();

    p1.set_value(value);
    EXPECT_TRUE(f2.get() == value);
}

TEST(FutureTest, test_share02) {
    promise<int&> p1;
    future<int&> f1(p1.get_future());
    shared_future<int&> f2 = f1.share();

    p1.set_value(value);
    EXPECT_TRUE(&f2.get() == &value);
}

TEST(FutureTest, test_share03) {
    promise<void> p1;
    future<void> f1(p1.get_future());
    shared_future<void> f2 = f1.share();

    p1.set_value();
    f2.get();
}

TEST(FutureTest, test_valid) {
    future<int> f0;
    EXPECT_TRUE(!f0.valid());

    promise<int> p1;
    future<int> f1(p1.get_future());

    EXPECT_TRUE(f1.valid());

    p1.set_value(1);

    EXPECT_TRUE(f1.valid());

    f1 = std::move(f0);

    EXPECT_TRUE(!f1.valid());
    EXPECT_TRUE(!f0.valid());
}

static void fut_wait(future<void>& f) {
    f.wait();
}

static void* fut_wait2(void* arg) {
    auto f = static_cast<future<void>*>(arg);
    f->wait();
    return nullptr;
}

TEST(FutureTest, test_wait01) {
    promise<void> p1;
    future<void> f1(p1.get_future());

    std::thread t1(fut_wait, std::ref(f1));

    p1.set_value();
    t1.join();
}

TEST(FutureTest, test_wait02) {
    promise<void> p1;
    future<void> f1(p1.get_future());

    bthread_t t1;
    ASSERT_EQ(0, bthread_start_background(&t1, nullptr, fut_wait2, &f1));

    p1.set_value();
    bthread_join(t1, nullptr);
}

TEST(FutureTest, test_wait_for01) {
    promise<int> p1;
    future<int> f1(p1.get_future());

    std::chrono::milliseconds delay(100);

    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::timeout);

    p1.set_value(1);

    auto before = std::chrono::system_clock::now();
    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::ready);
    EXPECT_TRUE(std::chrono::system_clock::now() < (before + delay));
}

static std::chrono::system_clock::time_point make_time(int i) {
    return std::chrono::system_clock::now() + std::chrono::milliseconds(i);
}

TEST(FutureTest, test_wait_until01) {
    promise<int> p1;
    future<int> f1(p1.get_future());

    auto when = make_time(10);
    EXPECT_TRUE(f1.wait_until(when) == std::future_status::timeout);
    EXPECT_TRUE(std::chrono::system_clock::now() >= when);

    p1.set_value(1);

    when = make_time(100);
    EXPECT_TRUE(f1.wait_until(when) == std::future_status::ready);
    EXPECT_TRUE(std::chrono::system_clock::now() < when);
}

} // namespace starrocks::bthreads