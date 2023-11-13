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

Future<int> get() {
    return Promise<int>().get_future();
}

} // namespace

TEST(FutureTest, test_default_ctor) {
    Future<int> p1;
    EXPECT_FALSE(p1.valid());
    Future<int&> p2;
    EXPECT_FALSE(p2.valid());
    Future<void> p3;
    EXPECT_FALSE(p3.valid());
    Future<ClassType> p4;
    EXPECT_FALSE(p4.valid());
    Future<AbstractClass&> p5;
    EXPECT_FALSE(p5.valid());
}

TEST(FutureTest, test_move) {
    Promise<int> p1;
    Future<int> f1(p1.get_future());
    Future<int> f2(std::move(f1));
    EXPECT_TRUE(f2.valid());
    EXPECT_FALSE(f1.valid());
}

TEST(FutureTest, test_move_assign) {
    Future<int> p1;
    Future<int> p2 = get();
    p1 = std::move(p2);
    EXPECT_TRUE(p1.valid());
    EXPECT_FALSE(p2.valid());
}

TEST(FutureTest, test_exception_no_state01) {
    bthreads::Promise<int> p;
    bthreads::Future<int> f = p.get_future();
    p.set_value(10);
    EXPECT_EQ(10, f.get());
    EXPECT_THROW(
            {
                try {
                    f.get();
                    EXPECT_FALSE(true) << "unreachable";
                } catch (const future_error& e) {
                    EXPECT_EQ(std::make_error_code(future_errc::no_state), e.code());
                    throw;
                }
            },
            future_error);
}

TEST(FutureTest, test_exception_no_state02) {
    bthreads::Promise<int&> p;
    bthreads::Future<int&> f = p.get_future();
    int i = 1;
    p.set_value(i);
    EXPECT_EQ(&i, &f.get());
    EXPECT_THROW(
            {
                try {
                    f.get();
                    EXPECT_FALSE(true) << "unreachable";
                } catch (const future_error& e) {
                    EXPECT_EQ(std::make_error_code(future_errc::no_state), e.code());
                    throw;
                }
            },
            future_error);
}

TEST(FutureTest, test_exception_no_state03) {
    bthreads::Promise<void> p;
    bthreads::Future<void> f = p.get_future();
    p.set_value();
    f.get();
    EXPECT_THROW(
            {
                try {
                    f.get();
                    EXPECT_FALSE(true) << "unreachable";
                } catch (const future_error& e) {
                    EXPECT_EQ(std::make_error_code(future_errc::no_state), e.code());
                    throw;
                }
            },
            future_error);
}

static int value = 99;

TEST(FutureTest, test_get01) {
    Promise<int> p1;
    Future<int> f1(p1.get_future());

    p1.set_value(value);
    EXPECT_EQ(value, f1.get());
    EXPECT_FALSE(f1.valid());
}

TEST(FutureTest, test_get02) {
    Promise<int&> p1;
    Future<int&> f1(p1.get_future());

    p1.set_value(value);
    EXPECT_EQ(&f1.get(), &value);
    EXPECT_FALSE(f1.valid());
}

TEST(FutureTest, test_get03) {
    Promise<void> p1;
    Future<void> f1(p1.get_future());

    p1.set_value();
    f1.get();
    EXPECT_FALSE(f1.valid());
}

TEST(FutureTest, test_get04) {
    Promise<int> p1;
    Future<int> f1(p1.get_future());

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
    Promise<int&> p1;
    Future<int&> f1(p1.get_future());

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
    Promise<void> p1;
    Future<void> f1(p1.get_future());

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

TEST(FutureTest, test_share01) {
    Promise<int> p1;
    Future<int> f1(p1.get_future());
    SharedFuture<int> f2 = f1.share();

    p1.set_value(value);
    EXPECT_TRUE(f2.get() == value);
}

TEST(FutureTest, test_share02) {
    Promise<int&> p1;
    Future<int&> f1(p1.get_future());
    SharedFuture<int&> f2 = f1.share();

    p1.set_value(value);
    EXPECT_TRUE(&f2.get() == &value);
}

TEST(FutureTest, test_share03) {
    Promise<void> p1;
    Future<void> f1(p1.get_future());
    SharedFuture<void> f2 = f1.share();

    p1.set_value();
    f2.get();
}

TEST(FutureTest, test_valid) {
    Future<int> f0;
    EXPECT_TRUE(!f0.valid());

    Promise<int> p1;
    Future<int> f1(p1.get_future());

    EXPECT_TRUE(f1.valid());

    p1.set_value(1);

    EXPECT_TRUE(f1.valid());

    f1 = std::move(f0);

    EXPECT_TRUE(!f1.valid());
    EXPECT_TRUE(!f0.valid());
}

static void fut_wait(Future<void>& f) {
    f.wait();
}

static void* fut_wait2(void* arg) {
    auto f = static_cast<Future<void>*>(arg);
    f->wait();
    return nullptr;
}

TEST(FutureTest, test_wait01) {
    Promise<void> p1;
    Future<void> f1(p1.get_future());

    std::thread t1(fut_wait, std::ref(f1));

    p1.set_value();
    t1.join();
}

TEST(FutureTest, test_wait02) {
    Promise<void> p1;
    Future<void> f1(p1.get_future());

    bthread_t t1;
    ASSERT_EQ(0, bthread_start_background(&t1, nullptr, fut_wait2, &f1));

    p1.set_value();
    bthread_join(t1, nullptr);
}

TEST(FutureTest, test_wait_for01) {
    Promise<int> p1;
    Future<int> f1(p1.get_future());

    std::chrono::milliseconds delay(100);

    auto t0 = std::chrono::steady_clock::now();
    EXPECT_TRUE(f1.wait_for(delay) == future_status::timeout);
    auto t1 = std::chrono::steady_clock::now();
    auto d = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    EXPECT_GE(d, delay.count());
    // Assuming that the delay due to scheduling does not exceed 50ms
    EXPECT_LT(d, delay.count() + 50);

    p1.set_value(1);

    auto before = std::chrono::system_clock::now();
    EXPECT_TRUE(f1.wait_for(delay) == future_status::ready);
    EXPECT_TRUE(std::chrono::system_clock::now() < (before + delay));
}

static std::chrono::system_clock::time_point make_time(int i) {
    return std::chrono::system_clock::now() + std::chrono::milliseconds(i);
}

TEST(FutureTest, test_wait_until01) {
    Promise<int> p1;
    Future<int> f1(p1.get_future());

    auto when = make_time(10);
    EXPECT_TRUE(f1.wait_until(when) == future_status::timeout);
    EXPECT_TRUE(std::chrono::system_clock::now() >= when);

    p1.set_value(1);

    when = make_time(100);
    EXPECT_TRUE(f1.wait_until(when) == future_status::ready);
    EXPECT_TRUE(std::chrono::system_clock::now() < when);
}

} // namespace starrocks::bthreads