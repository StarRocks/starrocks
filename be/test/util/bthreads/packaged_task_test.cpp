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
//  https://github.com/gcc-mirror/gcc/tree/master/libstdc%2B%2B-v3/testsuite/30_threads/packaged_task

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
    virtual void rotate(int) = 0;
};

int f1() {
    return 0;
}
int& f2() {
    static int i;
    return i;
}
void f3() {}
ClassType f4() {
    return ClassType();
}

struct Derived : AbstractClass {
    void rotate(int) {}
    Derived& operator()(int i) {
        rotate(i);
        return *this;
    }
} f5;

template <typename F>
future<typename std::result_of<F()>::type> spawn_task(F f) {
    typedef typename std::result_of<F()>::type result_type;
    packaged_task<result_type()> task(std::move(f));
    future<result_type> res(task.get_future());
    std::thread(std::move(task)).detach();
    return res;
}

int get_res() {
    return 42;
}

struct S {
    S() = default;
    S(S&&) = default;
    void operator()() {}
};

packaged_task<void()> pt{S{}};

template <typename T, typename U>
struct require_same;
template <typename T>
struct require_same<T, T> {
    using type = void;
};

template <typename T, typename U>
typename require_same<T, U>::type check_type(U&) {}

void f0v() {}
void f0vn() noexcept {}
int f0i() {
    return 0;
}
int f0in() noexcept {
    return 3;
}
long f1l(int&) {
    return 2;
}
long f1ln(double*) noexcept {
    return 1;
}
struct X {
    int operator()(const short&, void*) { return 0; }
};

struct Y {
    void operator()(int) const& noexcept {}
};
} // namespace

TEST(PackagedTaskTest, test01) {
    packaged_task<int()> p1;
    EXPECT_TRUE(!p1.valid());
    packaged_task<int&()> p2;
    EXPECT_TRUE(!p2.valid());
    packaged_task<void()> p3;
    EXPECT_TRUE(!p3.valid());
    packaged_task<ClassType()> p4;
    EXPECT_TRUE(!p4.valid());
    packaged_task<AbstractClass&(int)> p5;
    EXPECT_TRUE(!p5.valid());
}

TEST(PackagedTaskTest, test02) {
    packaged_task<int()> p1(f1);
    EXPECT_TRUE(p1.valid());
    packaged_task<int&()> p2(f2);
    EXPECT_TRUE(p2.valid());
    packaged_task<void()> p3(f3);
    EXPECT_TRUE(p3.valid());
    packaged_task<ClassType()> p4(f4);
    EXPECT_TRUE(p4.valid());
    packaged_task<AbstractClass&(int)> p5(f5);
    EXPECT_TRUE(p5.valid());
}

TEST(PackagedTaskTest, test03) {
    auto f = spawn_task(get_res);
    EXPECT_TRUE(f.get() == get_res());
}

void test_deduction01() {
    packaged_task task1{f0v};
    check_type<packaged_task<void()>>(task1);

    packaged_task task2{f0vn};
    check_type<packaged_task<void()>>(task2);

    packaged_task task3{f0i};
    check_type<packaged_task<int()>>(task3);

    packaged_task task4{f0in};
    check_type<packaged_task<int()>>(task4);

    packaged_task task5{f1l};
    check_type<packaged_task<long(int&)>>(task5);

    packaged_task task6{f1ln};
    check_type<packaged_task<long(double*)>>(task6);

    packaged_task task5a{std::move(task5)};
    check_type<packaged_task<long(int&)>>(task5a);

    packaged_task task6a{std::move(task6)};
    check_type<packaged_task<long(double*)>>(task6a);
}

void test_deduction02() {
    X x;
    packaged_task task1{x};
    check_type<packaged_task<int(const short&, void*)>>(task1);

    Y y;
    packaged_task task2{y};
    check_type<packaged_task<void(int)>>(task2);

    packaged_task task3{[&x](float) -> X& { return x; }};
    check_type<packaged_task<X&(float)>>(task3);
}

TEST(PackagedTaskTest, test_move) {
    // move
    packaged_task<int()> p1(f1);
    packaged_task<int()> p2(std::move(p1));
    EXPECT_TRUE(!p1.valid());
    EXPECT_TRUE(p2.valid());
}

TEST(PackagedTaskTest, test_move_assign) {
    // move assign
    packaged_task<int()> p1;
    packaged_task<int()> p2(f1);
    p1 = std::move(p2);
    EXPECT_TRUE(p1.valid());
    EXPECT_TRUE(!p2.valid());
}

namespace test_at_pthread_exit {
bool executed = false;

int execute(int i) {
    executed = true;
    return i + 1;
}

future<int> f1;

bool ready(future<int>& f) {
    return f.wait_for(std::chrono::milliseconds(1)) == std::future_status::ready;
}

void foo01() {
    packaged_task<int(int)> p1(execute);
    f1 = p1.get_future();

    p1.make_ready_at_pthread_exit(1);

    EXPECT_TRUE(executed);
    EXPECT_TRUE(p1.valid());
    EXPECT_TRUE(!ready(f1));
}

TEST(PackagedTaskTest, test_at_pthread_exit) {
    std::thread t{foo01};
    t.join();
    EXPECT_TRUE(ready(f1));
    EXPECT_TRUE(f1.get() == 2);
}
} // namespace test_at_pthread_exit

namespace test_get_future {
int& inc(int& i) {
    return ++i;
}

TEST(PackagedTaskTest, test_get_future01) {
    packaged_task<int&(int&)> p1(inc);
    future<int&> f1 = p1.get_future();

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.valid());
    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::timeout);

    int i1 = 0;

    p1(i1);

    int& i2 = f1.get();

    EXPECT_TRUE(&i1 == &i2);
    EXPECT_TRUE(i1 == 1);
}
TEST(PackagedTaskTest, test_get_future02) {
    packaged_task<int&(int&)> p1(inc);
    p1.get_future();

    EXPECT_THROW(
            {
                try {
                    p1.get_future();
                    EXPECT_TRUE(false);
                } catch (const std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::future_already_retrieved);
                    throw;
                }
            },
            std::future_error);
}
} // namespace test_get_future

namespace test_invoke {

int zero() {
    return 0;
}

TEST(PackagedTaskTest, test_invoke01) {
    packaged_task<int()> p1(zero);
    future<int> f1 = p1.get_future();

    p1();

    EXPECT_TRUE(p1.valid());
    EXPECT_TRUE(f1.get() == 0);
}
bool odd(unsigned i) {
    return i % 2;
}

TEST(PackagedTaskTest, test_invoke02) {
    packaged_task<bool(unsigned)> p1(odd);

    p1(5);

    EXPECT_THROW(
            {
                try {
                    p1(4);
                } catch (const std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);
}
int& inc(int& i) {
    ++i;
    return i;
}

TEST(PackagedTaskTest, test_invoke03) {
    packaged_task<void(int&)> p1(inc);

    int i1 = 0;
    p1(i1);

    EXPECT_TRUE(i1 == 1);

    EXPECT_THROW(
            {
                try {
                    p1(i1);
                } catch (const std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    EXPECT_TRUE(i1 == 1);
}

void thrower() {
    throw 0;
}

TEST(PackagedTaskTest, test_invoke04) {
    bool test = false;

    packaged_task<void()> p1(thrower);
    future<void> f1 = p1.get_future();

    p1();

    try {
        f1.get();
    } catch (int) {
        test = true;
    }
    EXPECT_TRUE(test);
}
void noop() {}
void waiter(shared_future<void> f) {
    f.wait();
}

TEST(PackagedTaskTest, test_invoke05) {
    packaged_task<void()> p1(noop);
    shared_future<void> f1(p1.get_future());
    std::thread t1(waiter, f1);

    p1();

    t1.join();
}
} // namespace test_invoke

namespace test_reset {

int zero() {
    return 0;
}

TEST(PackagedTaskTest, test_reset01) {
    packaged_task<int()> p1(zero);
    future<int> f1 = p1.get_future();

    p1.reset();
    EXPECT_TRUE(p1.valid());

    future<int> f2 = p1.get_future();

    EXPECT_THROW(
            {
                try {
                    f1.get();
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::broken_promise);
                    throw;
                }
            },
            std::future_error);
}
int iota() {
    static int i = 0;
    return i++;
}

TEST(PackagedTaskTest, test_reset02) {
    packaged_task<int()> p1(iota);
    future<int> f1 = p1.get_future();

    p1();
    p1.reset();

    EXPECT_TRUE(p1.valid());
    EXPECT_TRUE(f1.get() == 0);

    future<int> f2 = p1.get_future();
    p1();
    EXPECT_TRUE(f2.get() == 1);
}
} // namespace test_reset

namespace test_swap {
int zero() {
    return 0;
}

TEST(PackagedTaskTest, test_swap) {
    packaged_task<int()> p1(zero);
    packaged_task<int()> p2;
    EXPECT_TRUE(p1.valid());
    EXPECT_TRUE(!p2.valid());
    p1.swap(p2);
    EXPECT_TRUE(!p1.valid());
    EXPECT_TRUE(p2.valid());
}
} // namespace test_swap

namespace test_valid {
int zero() {
    return 0;
}

TEST(PackagedTaskTest, test_valid) {
    packaged_task<int()> p1;
    EXPECT_TRUE(!p1.valid());

    packaged_task<int()> p2(zero);
    EXPECT_TRUE(p2.valid());
}
} // namespace test_valid

} // namespace starrocks::bthreads
