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

#include <execinfo.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/config_diagnostic_fwd.h"
#include "common/logging.h"

namespace starrocks {

class ExceptionStackTest : public testing::Test {
public:
    ExceptionStackTest() = default;
    ~ExceptionStackTest() override = default;
    void SetUp() override {
        _old_exception_stack_level = config::exception_stack_level;
        _old_exception_stack_white_list = config::exception_stack_white_list;
        config::exception_stack_level = 1;
        config::exception_stack_white_list = "std::";
        _old = std::cerr.rdbuf(_buffer.rdbuf());
    }
    void TearDown() override {
        std::cerr.rdbuf(_old);
        config::exception_stack_level = _old_exception_stack_level;
        config::exception_stack_white_list = _old_exception_stack_white_list;
    }

private:
    std::streambuf* _old = nullptr;
    std::stringstream _buffer;
    int32_t _old_exception_stack_level = 0;
    std::string _old_exception_stack_white_list;
};

TEST_F(ExceptionStackTest, print_exception_stack) {
#ifdef __APPLE__
    GTEST_SKIP() << "__cxa_throw wrapping is disabled on macOS test binaries";
#endif

    std::string exception = "runtime_error";
    std::string res;
    try {
        throw std::runtime_error("test_print_exception_stack.");
    } catch (std::exception& e) {
        res = e.what();
    } catch (...) {
        res = "unknown";
    }
    std::string text = _buffer.str();
    ASSERT_TRUE(text.find(exception) != text.npos);
}
} // namespace starrocks
