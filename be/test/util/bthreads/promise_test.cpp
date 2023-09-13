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
class AbstractClass {
public:
    virtual ~AbstractClass() = default;
    virtual void foo() = 0;
};
} // namespace

TEST(PromiseTest, test_default_ctor01) {
    Promise<int> p1;
    Promise<int&> p2;
    Promise<void> p3;
    Promise<Future<int>> p4;
    Promise<AbstractClass&> p5;
}

TEST(PromiseTest, test_move_ctor) {
    Promise<int> p1;
    p1.set_value(3);
    Promise<int> p2(std::move(p1));
    EXPECT_TRUE(p2.get_future().get() == 3);
    EXPECT_THROW(
            {
                try {
                    p1.get_future();
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::no_state));
                    throw;
                }
            },
            future_error);
}

TEST(PromiseTest, test_move_assign) {
    Promise<int> p1;
    p1.set_value(3);
    Promise<int> p2;
    p2 = std::move(p1);
    EXPECT_TRUE(p2.get_future().get() == 3);
    EXPECT_THROW(
            {
                try {
                    p1.get_future();
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::no_state));
                    throw;
                }
            },
            future_error);
}

TEST(PromiseTest, test_get_future01) {
    Promise<int&> p1;
    Future<int&> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    int i1 = 0;

    p1.set_value(i1);

    int& i2 = f1.get();

    EXPECT_TRUE(&i1 == &i2);
}

TEST(PromiseTest, test_get_future02) {
    Promise<int&> p1;
    p1.get_future();

    EXPECT_THROW(
            {
                try {
                    p1.get_future();
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::future_already_retrieved));
                    throw;
                }
            },
            future_error);
}

TEST(PromiseTest, test_get_future03) {
    Promise<int> p1;
    p1.get_future();

    EXPECT_THROW(
            {
                try {
                    p1.get_future();
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::future_already_retrieved));
                    throw;
                }
            },
            future_error);
}
TEST(PromiseTest, test_get_future04) {
    Promise<void> p1;
    p1.get_future();

    EXPECT_THROW(
            {
                try {
                    p1.get_future();
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::future_already_retrieved));
                    throw;
                }
            },
            future_error);
}

TEST(PromiseTest, test_exception01) {
    Promise<int> p1;
    Future<int> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    p1.set_exception(std::make_exception_ptr(0));

    EXPECT_THROW({ f1.get(); }, int);
    EXPECT_TRUE(!f1.valid());
}

TEST(PromiseTest, test_exception02) {
    Promise<int&> p1;
    Future<int&> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    p1.set_exception(std::make_exception_ptr(0));

    EXPECT_THROW({ f1.get(); }, int);
    EXPECT_TRUE(!f1.valid());
}

TEST(PromiseTest, test_exception03) {
    Promise<void> p1;
    Future<void> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    p1.set_exception(std::make_exception_ptr(0));

    EXPECT_THROW({ f1.get(); }, int);
    EXPECT_TRUE(!f1.valid());
}

TEST(PromiseTest, test_exception04) {
    Promise<int> p1;
    Future<int> f1 = p1.get_future();

    p1.set_exception(std::make_exception_ptr(0));

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(1));
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);

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
    Promise<int> p1;
    Future<int> f1 = p1.get_future();

    p1.set_value(2);

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(0));
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);
}

TEST(PromiseTest, test_exception06) {
    Promise<int&> p1;
    Future<int&> f1 = p1.get_future();

    p1.set_exception(std::make_exception_ptr(0));

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(1));
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);

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
    Promise<int&> p1;
    Future<int&> f1 = p1.get_future();

    int i = 2;
    p1.set_value(i);

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(0));
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);
}

TEST(PromiseTest, test_exception08) {
    Promise<void> p1;
    Future<void> f1 = p1.get_future();

    p1.set_exception(std::make_exception_ptr(0));

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(1));
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);

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
    Promise<void> p1;
    Future<void> f1 = p1.get_future();

    p1.set_value();

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(0));
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);
}

TEST(PromiseTest, test_exception10) {
    Promise<int> p1;
    Promise<int> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(1));
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::no_state));
                    throw;
                }
            },
            future_error);
}

TEST(PromiseTest, test_exception11) {
    Promise<int&> p1;
    Promise<int&> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(1));
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::no_state));
                    throw;
                }
            },
            future_error);
}

TEST(PromiseTest, test_exception12) {
    Promise<void> p1;
    Promise<void> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(1));
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::no_state));
                    throw;
                }
            },
            future_error);
}

TEST(PromiseTest, test_set_value01) {
    Promise<int> p1;
    Future<int> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    p1.set_value(0);

    int&& i1 = f1.get();

    EXPECT_TRUE(i1 == 0);
}

namespace {
struct NonCopyableStruct {
    int val;

    NonCopyableStruct() : val(0) {}

    NonCopyableStruct(int inval) : val(inval) {}

    NonCopyableStruct& operator=(int newval) {
        val = newval;
        return *this;
    }

    NonCopyableStruct(const NonCopyableStruct&) = delete;
    NonCopyableStruct& operator=(const NonCopyableStruct&) = delete;

    NonCopyableStruct(NonCopyableStruct&& in) noexcept { val = in.val; }

    NonCopyableStruct& operator=(NonCopyableStruct&& in) {
        CHECK(this != &in);
        val = in.val;
        return *this;
    }
};
} // namespace

TEST(PromiseTest, test_set_value02) {
    Promise<NonCopyableStruct> p1;
    Future<NonCopyableStruct> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    p1.set_value(NonCopyableStruct(1));

    NonCopyableStruct r1(f1.get());

    EXPECT_TRUE(!f1.valid());
    EXPECT_TRUE(r1.val == 1);
}

TEST(PromiseTest, test_set_value03) {
    Promise<int&> p1;
    Future<int&> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    int i1 = 0;
    p1.set_value(i1);
    int& i2 = f1.get();

    EXPECT_TRUE(!f1.valid());
    EXPECT_TRUE(&i1 == &i2);
}

TEST(PromiseTest, test_set_value04) {
    Promise<void> p1;
    Future<void> f1 = p1.get_future();

    EXPECT_TRUE(f1.valid());

    p1.set_value();
    f1.get();

    EXPECT_TRUE(!f1.valid());
}

TEST(PromiseTest, test_set_value05) {
    Promise<int> p1;
    Future<int> f1 = p1.get_future();

    p1.set_value(1);

    EXPECT_THROW(
            {
                try {
                    p1.set_value(2);
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == future_status::ready);
    EXPECT_TRUE(f1.get() == 1);
}

TEST(PromiseTest, test_set_value06) {
    Promise<int> p1;
    Future<int> f1 = p1.get_future();

    p1.set_value(3);

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(4));
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == future_status::ready);
    EXPECT_TRUE(f1.get() == 3);
}

TEST(PromiseTest, test_set_value07) {
    Promise<int> p1;
    Future<int> f1 = p1.get_future();

    p1.set_exception(std::make_exception_ptr(4));

    EXPECT_THROW(
            {
                try {
                    p1.set_value(3);
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == future_status::ready);
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
    Promise<int&> p1;
    Future<int&> f1 = p1.get_future();

    int i = 1;
    p1.set_value(i);

    EXPECT_THROW(
            {
                try {
                    p1.set_value(i);
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == future_status::ready);
    EXPECT_TRUE(f1.get() == 1);
}

TEST(PromiseTest, test_set_value09) {
    Promise<int&> p1;
    Future<int&> f1 = p1.get_future();

    int i = 3;
    p1.set_value(i);

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(4));
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == future_status::ready);
    EXPECT_TRUE(f1.get() == 3);
}

TEST(PromiseTest, test_set_value10) {
    Promise<int&> p1;
    Future<int&> f1 = p1.get_future();

    p1.set_exception(std::make_exception_ptr(4));

    EXPECT_THROW(
            {
                try {
                    int i = 3;
                    p1.set_value(i);
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == future_status::ready);
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
    Promise<void> p1;
    Future<void> f1 = p1.get_future();

    p1.set_value();

    EXPECT_THROW(
            {
                try {
                    p1.set_value();
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == future_status::ready);
    f1.get();
}

TEST(PromiseTest, test_set_value12) {
    Promise<void> p1;
    Future<void> f1 = p1.get_future();

    p1.set_value();

    EXPECT_THROW(
            {
                try {
                    p1.set_exception(std::make_exception_ptr(4));
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == future_status::ready);
    f1.get();
}

TEST(PromiseTest, test_set_value13) {
    Promise<void> p1;
    Future<void> f1 = p1.get_future();

    p1.set_exception(std::make_exception_ptr(4));

    EXPECT_THROW(
            {
                try {
                    p1.set_value();
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::promise_already_satisfied));
                    throw;
                }
            },
            future_error);

    std::chrono::milliseconds delay(1);
    EXPECT_TRUE(f1.wait_for(delay) == future_status::ready);
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
    Promise<int> p1;
    Promise<int> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    p1.set_value(1);
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::no_state));
                    throw;
                }
            },
            future_error);
}

TEST(PromiseTest, test_set_value15) {
    Promise<int&> p1;
    Promise<int&> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    int i = 0;
                    p1.set_value(i);
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::no_state));
                    throw;
                }
            },
            future_error);
}

TEST(PromiseTest, test_set_value16) {
    Promise<void> p1;
    Promise<void> p2(std::move(p1));
    EXPECT_THROW(
            {
                try {
                    p1.set_value();
                    EXPECT_TRUE(false);
                } catch (future_error& e) {
                    EXPECT_TRUE(e.code() == std::make_error_code(future_errc::no_state));
                    throw;
                }
            },
            future_error);
}

namespace {
struct tester {
    tester(int);
    tester(const tester&);
    tester() = delete;
    // tester& operator=(const tester&);
};

Promise<tester> pglobal;
Future<tester> fglobal = pglobal.get_future();

auto delay = std::chrono::milliseconds(1);

tester::tester(int) {
    EXPECT_TRUE(fglobal.wait_for(delay) == future_status::timeout);
}

tester::tester(const tester&) {
    // if this copy happens while a mutex is locked next line could deadlock:
    EXPECT_TRUE(fglobal.wait_for(delay) == future_status::timeout);
}
} // namespace

TEST(PromiseTest, test_set_value17) {
    pglobal.set_value(tester(1));

    EXPECT_TRUE(fglobal.wait_for(delay) == future_status::ready);
}

TEST(PromiseTest, test_swap) {
    Promise<int> p1;
    Promise<int> p2;
    p1.set_value(1);
    p1.swap(p2);
    auto delay = std::chrono::milliseconds(1);
    EXPECT_TRUE(p1.get_future().wait_for(delay) == future_status::timeout);
    EXPECT_TRUE(p2.get_future().wait_for(delay) == future_status::ready);
}

} // namespace starrocks::bthreads
