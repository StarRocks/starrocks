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
//  https://github.com/gcc-mirror/gcc/tree/master/libstdc%2B%2B-v3/testsuite/30_threads/promise

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
} // namespace

TEST(PromiseTest, test_default_ctor01) {
    promise<int> p1;
    promise<int&> p2;
    promise<void> p3;
    promise<ClassType> p4;
    promise<AbstractClass&> p5;
}

TEST(PromiseTest, test_move_ctor) {
    promise<int> p1;
    p1.set_value(3);
    promise<int> p2(std::move(p1));
    EXPECT_TRUE(p2.get_future().get() == 3);
    EXPECT_THROW(
            {
                try {
                    p1.get_future();
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

TEST(PromiseTest, test_move_assign) {
    promise<int> p1;
    p1.set_value(3);
    promise<int> p2;
    p2 = std::move(p1);
    EXPECT_TRUE(p2.get_future().get() == 3);
    EXPECT_THROW(
            {
                try {
                    p1.get_future();
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

namespace {
static int copies;
static int copies_cmp;
struct Obj {
    Obj() = default;
    Obj(const Obj&) { ++copies; }
};
static future<Obj> g_f1;

} // namespace

static bool ready(future<Obj>& f) {
    return f.wait_for(std::chrono::milliseconds(1)) == std::future_status::ready;
}

static void test_at_pthread_exit01_func() {
    promise<Obj> p1;
    g_f1 = p1.get_future();

    p1.set_value_at_pthread_exit({});

    copies_cmp = copies;

    EXPECT_TRUE(!ready(g_f1));
}

TEST(PromiseTest, test_at_pthread_exit) {
    std::thread t{test_at_pthread_exit01_func};
    t.join();
    EXPECT_TRUE(ready(g_f1));
    EXPECT_TRUE(copies == copies_cmp);
}

TEST(PromiseTest, test_at_pthread_exit02) {
    promise<int> p1;
    p1.set_value(1);
    EXPECT_THROW(
            {
                try {
                    p1.set_value_at_pthread_exit(2);
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    EXPECT_THROW(
            {
                try {
                    p1.set_exception_at_pthread_exit(std::make_exception_ptr(3));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    promise<int> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    p1.set_value_at_pthread_exit(2);
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);

    EXPECT_THROW(
            {
                try {
                    p1.set_exception_at_pthread_exit(std::make_exception_ptr(3));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

TEST(PromiseTest, test_at_pthread_exit03) {
    promise<int&> p1;
    int i = 1;
    p1.set_value(i);
    EXPECT_THROW(
            {
                try {
                    p1.set_value_at_pthread_exit(i);
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);
    EXPECT_THROW(
            {
                try {
                    p1.set_exception_at_pthread_exit(std::make_exception_ptr(3));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    promise<int&> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    int i = 0;
                    p1.set_value_at_pthread_exit(i);
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);

    EXPECT_THROW(
            {
                try {
                    p1.set_exception_at_pthread_exit(std::make_exception_ptr(3));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

TEST(PromiseTest, test_at_pthread_exit04) {
    promise<void> p1;
    p1.set_value();
    EXPECT_THROW(
            {
                try {
                    p1.set_value_at_pthread_exit();
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);
    EXPECT_THROW(
            {
                try {
                    p1.set_exception_at_pthread_exit(std::make_exception_ptr(3));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    promise<void> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    p1.set_value_at_pthread_exit();
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
    EXPECT_THROW(
            {
                try {
                    p1.set_exception_at_pthread_exit(std::make_exception_ptr(3));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

TEST(PromiseTest, test_get_future01) {
    promise<int&> p1;
    future<int&> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    int i1 = 0;

    p1.set_value(i1);

    int& i2 = f1.get();

    EXPECT_TRUE(&i1 == &i2);
}

TEST(PromiseTest, test_get_future02) {
    promise<int&> p1;
    p1.get_future();

    EXPECT_THROW(
            {
                try {
                    p1.get_future();
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::future_already_retrieved);
                    throw;
                }
            },
            std::future_error);
}

TEST(PromiseTest, test_exception01) {
    promise<int> p1;
    future<int> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    p1.set_exception(std::make_exception_ptr(0));

    EXPECT_THROW({ f1.get(); }, int);
    EXPECT_TRUE(!f1.valid());
}

TEST(PromiseTest, test_exception02) {
    promise<int&> p1;
    future<int&> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    p1.set_exception(std::make_exception_ptr(0));

    EXPECT_THROW({ f1.get(); }, int);
    EXPECT_TRUE(!f1.valid());
}

TEST(PromiseTest, test_exception03) {
    promise<void> p1;
    future<void> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    p1.set_exception(std::make_exception_ptr(0));

    EXPECT_THROW({ f1.get(); }, int);
    EXPECT_TRUE(!f1.valid());
}

TEST(PromiseTest, test_exception04) {
    promise<int> p1;
    future<int> f1 = p1.get_future();

    p1.set_exception(std::make_exception_ptr(0));

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(1));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    EXPECT_THROW(
            {
                try {
                    f1.get();
                    EXPECT_TRUE(false);
                } catch (int i) {
                    EXPECT_TRUE(i == 0);
                    throw;
                }
            },
            int);
}

TEST(PromiseTest, test_exception05) {
    promise<int> p1;
    future<int> f1 = p1.get_future();

    p1.set_value(2);

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(0));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);
}

TEST(PromiseTest, test_exception06) {
    promise<int&> p1;
    future<int&> f1 = p1.get_future();

    p1.set_exception(std::make_exception_ptr(0));

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(1));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    EXPECT_THROW(
            {
                try {
                    f1.get();
                    EXPECT_TRUE(false);
                } catch (int i) {
                    EXPECT_TRUE(i == 0);
                    throw;
                }
            },
            int);
}

TEST(PromiseTest, test_exception07) {
    promise<int&> p1;
    future<int&> f1 = p1.get_future();

    int i = 2;
    p1.set_value(i);

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(0));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);
}

TEST(PromiseTest, test_exception08) {
    promise<void> p1;
    future<void> f1 = p1.get_future();

    p1.set_exception(std::make_exception_ptr(0));

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(1));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    EXPECT_THROW(
            {
                try {
                    f1.get();
                    EXPECT_TRUE(false);
                } catch (int i) {
                    EXPECT_TRUE(i == 0);
                    throw;
                }
            },
            int);
}

TEST(PromiseTest, test_exception09) {
    promise<void> p1;
    future<void> f1 = p1.get_future();

    p1.set_value();

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(0));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);
}

// Check for no_state error condition (PR libstdc++/80316)

TEST(PromiseTest, test_exception10) {
    promise<int> p1;
    promise<int> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(1));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

TEST(PromiseTest, test_exception11) {
    promise<int&> p1;
    promise<int&> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(1));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

TEST(PromiseTest, test_exception12) {
    promise<void> p1;
    promise<void> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(1));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

TEST(PromiseTest, test_set_value01) {
    promise<int> p1;
    future<int> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    p1.set_value(0);

    int&& i1 = f1.get();

    EXPECT_TRUE(i1 == 0);
}

namespace {
// This class is designed to test libstdc++'s template-based rvalue
// reference support. It should fail at compile-time if there is an
// attempt to copy it.
struct rvalstruct {
    int val;
    bool valid;

    rvalstruct() : val(0), valid(true) {}

    rvalstruct(int inval) : val(inval), valid(true) {}

    rvalstruct& operator=(int newval) {
        val = newval;
        valid = true;
        return *this;
    }

    rvalstruct(const rvalstruct&) = delete;

    rvalstruct(rvalstruct&& in) {
        CHECK(in.valid == true);
        val = in.val;
        in.valid = false;
        valid = true;
    }

    rvalstruct& operator=(const rvalstruct&) = delete;

    rvalstruct& operator=(rvalstruct&& in) {
        CHECK(this != &in);
        CHECK(in.valid == true);
        val = in.val;
        in.valid = false;
        valid = true;
        return *this;
    }
};
} // namespace

TEST(PromiseTest, test_set_value02) {
    promise<rvalstruct> p1;
    future<rvalstruct> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    p1.set_value(rvalstruct(1));

    rvalstruct r1(f1.get());

    EXPECT_TRUE(!f1.valid());
    EXPECT_TRUE(r1.val == 1);
}

TEST(PromiseTest, test_set_value03) {
    promise<int&> p1;
    future<int&> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    int i1 = 0;
    p1.set_value(i1);
    int& i2 = f1.get();

    EXPECT_TRUE(!f1.valid());
    EXPECT_TRUE(&i1 == &i2);
}

TEST(PromiseTest, test_set_value04) {
    promise<void> p1;
    future<void> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    p1.set_value();
    f1.get();

    EXPECT_TRUE(!f1.valid());
}

TEST(PromiseTest, test_set_value05) {
    promise<int> p1;
    future<int> f1 = p1.get_future();

    p1.set_value(1);

    EXPECT_THROW(
            {
                try {
                    p1.set_value(2);
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::ready);
    EXPECT_TRUE(f1.get() == 1);
}

TEST(PromiseTest, test_set_value06) {
    promise<int> p1;
    future<int> f1 = p1.get_future();

    p1.set_value(3);

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(4));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::ready);
    EXPECT_TRUE(f1.get() == 3);
}

TEST(PromiseTest, test_set_value07) {
    promise<int> p1;
    future<int> f1 = p1.get_future();

    p1.set_exception(std::make_exception_ptr(4));

    EXPECT_THROW(
            {
                try {
                    p1.set_value(3);
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::ready);
    EXPECT_THROW(
            {
                try {
                    f1.get();
                    EXPECT_TRUE(false);
                } catch (int e) {
                    EXPECT_TRUE(e == 4);
                    throw;
                }
            },
            int);
}

TEST(PromiseTest, test_set_value08) {
    promise<int&> p1;
    future<int&> f1 = p1.get_future();

    int i = 1;
    p1.set_value(i);

    EXPECT_THROW(
            {
                try {
                    p1.set_value(i);
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::ready);
    EXPECT_TRUE(f1.get() == 1);
}

TEST(PromiseTest, test_set_value09) {
    promise<int&> p1;
    future<int&> f1 = p1.get_future();

    int i = 3;
    p1.set_value(i);

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(4));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::ready);
    EXPECT_TRUE(f1.get() == 3);
}

TEST(PromiseTest, test_set_value10) {
    promise<int&> p1;
    future<int&> f1 = p1.get_future();

    p1.set_exception(std::make_exception_ptr(4));

    EXPECT_THROW(
            {
                try {
                    int i = 3;
                    p1.set_value(i);
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::ready);
    EXPECT_THROW(
            {
                try {
                    f1.get();
                    EXPECT_TRUE(false);
                } catch (int e) {
                    EXPECT_TRUE(e == 4);
                    throw;
                }
            },
            int);
}

TEST(PromiseTest, test_set_value11) {
    promise<void> p1;
    future<void> f1 = p1.get_future();

    p1.set_value();

    EXPECT_THROW(
            {
                try {
                    p1.set_value();
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::ready);
    f1.get();
}

TEST(PromiseTest, test_set_value12) {
    promise<void> p1;
    future<void> f1 = p1.get_future();

    p1.set_value();

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(4));
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::ready);
    f1.get();
}

TEST(PromiseTest, test_set_value13) {
    promise<void> p1;
    future<void> f1 = p1.get_future();

    p1.set_exception(std::make_exception_ptr(4));

    EXPECT_THROW(
            {
                try {
                    p1.set_value();
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::promise_already_satisfied);
                    throw;
                }
            },
            std::future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == std::future_status::ready);
    EXPECT_THROW(
            {
                try {
                    f1.get();
                    EXPECT_TRUE(false);
                } catch (int e) {
                    EXPECT_TRUE(e == 4);
                    throw;
                }
            },
            int);
}

// Check for no_state error condition (PR libstdc++/80316)

TEST(PromiseTest, test_set_value14) {
    promise<int> p1;
    promise<int> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    p1.set_value(1);
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

TEST(PromiseTest, test_set_value15) {
    promise<int&> p1;
    promise<int&> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    int i = 0;
                    p1.set_value(i);
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

TEST(PromiseTest, test_set_value16) {
    promise<void> p1;
    promise<void> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    p1.set_value();
                    EXPECT_TRUE(false);
                } catch (std::future_error& e) {
                    EXPECT_TRUE(e.code() == std::future_errc::no_state);
                    throw;
                }
            },
            std::future_error);
}

namespace {
struct tester {
    tester(int);
    tester(const tester&);
    tester() = delete;
    tester& operator=(const tester&);
};

promise<tester> pglobal;
future<tester> fglobal = pglobal.get_future();

auto delay = std::chrono::milliseconds(1);

tester::tester(int) {
    EXPECT_TRUE(fglobal.wait_for(delay) == std::future_status::timeout);
}

tester::tester(const tester&) {
    // if this copy happens while a mutex is locked next line could deadlock:
    EXPECT_TRUE(fglobal.wait_for(delay) == std::future_status::timeout);
}
} // namespace

TEST(PromiseTest, test_set_value17) {
    pglobal.set_value(tester(1));

    EXPECT_TRUE(fglobal.wait_for(delay) == std::future_status::ready);
}

TEST(PromiseTest, test_swap) {
    promise<int> p1;
    promise<int> p2;
    p1.set_value(1);
    p1.swap(p2);
    auto delay = std::chrono::milliseconds(1);
    EXPECT_TRUE(p1.get_future().wait_for(delay) == std::future_status::timeout);
    EXPECT_TRUE(p2.get_future().wait_for(delay) == std::future_status::ready);
}

} // namespace starrocks::bthreads
