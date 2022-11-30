// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <execinfo.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/logging.h"

namespace starrocks {

class ExceptionStackTest : public testing::Test {
public:
    ExceptionStackTest() = default;
    ~ExceptionStackTest() override = default;
    void SetUp() override { _old = std::cerr.rdbuf(_buffer.rdbuf()); }
    void TearDown() override { std::cerr.rdbuf(_old); }

private:
    std::streambuf* _old = nullptr;
    std::stringstream _buffer;
};

TEST_F(ExceptionStackTest, print_exception_stack) {
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
