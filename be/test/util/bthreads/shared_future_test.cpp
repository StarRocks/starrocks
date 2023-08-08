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
SharedFuture<int> get() {
    return Promise<int>().get_future();
}
} // namespace

TEST(SharedFutureTest, test_default_ctor) {
    bool __attribute__((unused)) test = true;

    SharedFuture<int> p1;
    EXPECT_TRUE(!p1.valid());
    SharedFuture<int&> p2;
    EXPECT_TRUE(!p2.valid());
    SharedFuture<void> p3;
    EXPECT_TRUE(!p3.valid());
    SharedFuture<ClassType> p4;
    EXPECT_TRUE(!p4.valid());
    SharedFuture<AbstractClass&> p5;
    EXPECT_TRUE(!p5.valid());
}

TEST(SharedFutureTest, test_move_ctor) {
    Promise<int> p1;
    SharedFuture<int> f1(p1.get_future());
    SharedFuture<int> f2(std::move(f1));
}

TEST(SharedFutureTest, test_move_assign) {
    SharedFuture<int> p1;
    SharedFuture<int> p2 = get();
    p1 = std::move(p2);
    EXPECT_TRUE(p1.valid());
    EXPECT_TRUE(!p2.valid());
}

TEST(SharedFutureTest, test_exception_01) {
    SharedFuture<int> f;
    EXPECT_THROW(
            {
                try {
                    f.get();
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::no_state));
                    throw;
                }
            },
            future_error);
}

TEST(SharedFutureTest, test_exception02) {
    SharedFuture<int&> f;
    EXPECT_THROW(
            {
                try {
                    f.get();
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::no_state));
                    throw;
                }
            },
            future_error);
}

TEST(SharedFutureTest, test_exception03) {
    SharedFuture<void> f;
    try {
        f.get();
        EXPECT_TRUE(false);
    } catch (future_error& e) {
        EXPECT_TRUE(e.code() == std::make_error_code(future_errc::no_state));
    }
}

static int value = 99;

TEST(SharedFutureTest, test_get01) {
    Promise<int> p1;
    const SharedFuture<int> f1(p1.get_future());
    SharedFuture<int> f2(f1);

    p1.set_value(value);
    EXPECT_TRUE(f1.get() == value);
    EXPECT_TRUE(f2.get() == value);
}

TEST(SharedFutureTest, test_get02) {
    Promise<int&> p1;
    const SharedFuture<int&> f1(p1.get_future());
    SharedFuture<int&> f2(f1);

    p1.set_value(value);
    EXPECT_TRUE(&f1.get() == &value);
    EXPECT_TRUE(&f2.get() == &value);
}

TEST(SharedFutureTest, test_get03) {
    Promise<void> p1;
    const SharedFuture<void> f1(p1.get_future());
    SharedFuture<void> f2(f1);

    p1.set_value();
    f1.get();
    f2.get();
}

TEST(SharedFutureTest, test_get04) {
    Promise<int> p1;
    SharedFuture<int> f1(p1.get_future());
    SharedFuture<int> f2(f1);

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
    Promise<int&> p1;
    SharedFuture<int&> f1(p1.get_future());
    SharedFuture<int&> f2(f1);

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
    Promise<void> p1;
    SharedFuture<void> f1(p1.get_future());
    SharedFuture<void> f2(f1);

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
    SharedFuture<int> f0;

    EXPECT_TRUE(!f0.valid());

    Promise<int> p1;
    SharedFuture<int> f1(p1.get_future());
    SharedFuture<int> f2(f1);

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

static void fut_wait(SharedFuture<void> f) {
    f.wait();
}

static void* fut_wait02(void* arg) {
    auto f = static_cast<SharedFuture<void>*>(arg);
    f->wait();
    return nullptr;
}

TEST(SharedFutureTest, test_wait01) {
    Promise<void> p1;
    SharedFuture<void> f1(p1.get_future());

    std::thread t1(fut_wait, f1);
    std::thread t2(fut_wait, f1);
    std::thread t3(fut_wait, f1);

    p1.set_value();
    t1.join();
    t2.join();
    t3.join();
}

TEST(SharedFutureTest, test_wait02) {
    Promise<void> p1;
    SharedFuture<void> f1(p1.get_future());

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
    Promise<int> p1;
    SharedFuture<int> f1(p1.get_future());
    SharedFuture<int> f2(f1);

    std::chrono::milliseconds delay(100);

    EXPECT_TRUE(f1.wait_for(delay) == future_status::timeout);
    EXPECT_TRUE(f2.wait_for(delay) == future_status::timeout);

    p1.set_value(1);

    auto before = std::chrono::system_clock::now();
    EXPECT_TRUE(f1.wait_for(delay) == future_status::ready);
    EXPECT_TRUE(f2.wait_for(delay) == future_status::ready);
    EXPECT_TRUE(std::chrono::system_clock::now() < (before + 2 * delay));
}

static std::chrono::system_clock::time_point make_time(int i) {
    return std::chrono::system_clock::now() + std::chrono::milliseconds(i);
}

TEST(SharedFutureTest, test_wait_until01) {
    Promise<int> p1;
    SharedFuture<int> f1(p1.get_future());
    SharedFuture<int> f2(f1);

    auto when = make_time(10);
    EXPECT_TRUE(f1.wait_until(make_time(10)) == future_status::timeout);
    EXPECT_TRUE(std::chrono::system_clock::now() >= when);

    when = make_time(10);
    EXPECT_TRUE(f2.wait_until(make_time(10)) == future_status::timeout);
    EXPECT_TRUE(std::chrono::system_clock::now() >= when);

    p1.set_value(1);

    when = make_time(100);
    EXPECT_TRUE(f1.wait_until(when) == future_status::ready);
    EXPECT_TRUE(f2.wait_until(when) == future_status::ready);
    EXPECT_TRUE(std::chrono::system_clock::now() < when);
}

} // namespace starrocks::bthreads
