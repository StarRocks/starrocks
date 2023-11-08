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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/common/status_test.cpp

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

#include "common/status.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "gen_cpp/Types_types.h"
#include "util/logging.h"

namespace starrocks {

class StatusTest : public testing::Test {};

TEST_F(StatusTest, OK) {
    // default
    Status st;
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("", st.get_error_msg());
    ASSERT_EQ("OK", st.to_string());
    // copy
    {
        const Status& other = st;
        ASSERT_TRUE(other.ok());
    }
    // move assign
    st = Status();
    ASSERT_TRUE(st.ok());
    // move construct
    {
        Status other = std::move(st);
        ASSERT_TRUE(other.ok());
    }
}

TEST_F(StatusTest, Error) {
    // default
    Status st = Status::InternalError("123");
    ASSERT_FALSE(st.ok());
    ASSERT_EQ("123", st.get_error_msg());
    ASSERT_EQ("123", st.message());
    ASSERT_EQ("123", st.detailed_message());
    ASSERT_EQ("Internal error: 123", st.to_string());
    // copy
    {
        const Status& other = st;
        ASSERT_FALSE(other.ok());
        ASSERT_EQ("123", st.get_error_msg());
        ASSERT_EQ("123", st.message());
        ASSERT_EQ("123", st.detailed_message());
    }
    // move assign
    st = Status::InternalError("456");
    ASSERT_FALSE(st.ok());
    ASSERT_EQ("456", st.get_error_msg());
    ASSERT_EQ("456", st.message());
    // move construct
    {
        Status other = std::move(st);
        ASSERT_FALSE(other.ok());
        ASSERT_EQ("456", other.get_error_msg());
        ASSERT_EQ("456", other.message());
        ASSERT_EQ("456", other.detailed_message());
        ASSERT_EQ("Internal error: 456", other.to_string());
        ASSERT_FALSE(st.ok());
        ASSERT_EQ(TStatusCode::INTERNAL_ERROR, st.code());
    }
}

TEST_F(StatusTest, ErrorWithContext) {
    // default
    Status st = Status::InternalError("123");
    ASSERT_FALSE(st.ok());
    ASSERT_EQ("123", st.get_error_msg());
    ASSERT_EQ("123", st.message());
    ASSERT_EQ("123", st.detailed_message());
    ASSERT_EQ("Internal error: 123", st.to_string());
    ASSERT_EQ("Internal error: 123", st.to_string(true));
    ASSERT_EQ("Internal error: 123", st.to_string(false));

    Status st1 = st.clone_and_append_context("a.cpp", 10, "expr1");
    ASSERT_EQ("123", st1.get_error_msg());
    ASSERT_EQ("123", st1.message());
    ASSERT_EQ("123\na.cpp:10 expr1", st1.detailed_message());
    ASSERT_EQ("Internal error: 123\na.cpp:10 expr1", st1.to_string());
    ASSERT_EQ("Internal error: 123\na.cpp:10 expr1", st1.to_string(true));
    ASSERT_EQ("Internal error: 123", st1.to_string(false));

    Status st2 = st1.clone_and_append_context("b.cpp", 11, "expr2");
    ASSERT_EQ("123", st2.get_error_msg());
    ASSERT_EQ("123", st2.message());
    ASSERT_EQ("123\na.cpp:10 expr1\nb.cpp:11 expr2", st2.detailed_message());
    ASSERT_EQ("Internal error: 123\na.cpp:10 expr1\nb.cpp:11 expr2", st2.to_string());
    ASSERT_EQ("Internal error: 123\na.cpp:10 expr1\nb.cpp:11 expr2", st2.to_string(true));
    ASSERT_EQ("Internal error: 123", st2.to_string(false));

    Status st3 = st2.clone_and_append_context("c.cpp", 13, "expr3");
    ASSERT_EQ("123", st3.get_error_msg());
    ASSERT_EQ("123", st3.message());
    ASSERT_EQ("123\na.cpp:10 expr1\nb.cpp:11 expr2\nc.cpp:13 expr3", st3.detailed_message());
    ASSERT_EQ("Internal error: 123\na.cpp:10 expr1\nb.cpp:11 expr2\nc.cpp:13 expr3", st3.to_string());
    ASSERT_EQ("Internal error: 123\na.cpp:10 expr1\nb.cpp:11 expr2\nc.cpp:13 expr3", st3.to_string(true));
    ASSERT_EQ("Internal error: 123", st3.to_string(false));
}

TEST_F(StatusTest, LongContext) {
    std::string message(std::numeric_limits<uint16_t>::max(), 'x');
    std::string context(std::numeric_limits<uint16_t>::max() - 10, 'y');
    // default
    Status st = Status::InternalError(message);
    ASSERT_FALSE(st.ok());
    ASSERT_EQ(message, st.get_error_msg());
    ASSERT_EQ(message, st.message());
    ASSERT_EQ(message, st.detailed_message());
    ASSERT_EQ(fmt::format("Internal error: {}", message), st.to_string());

    Status st1 = st.clone_and_append_context("a.cpp", 10, context.data());
    ASSERT_EQ(message, st1.get_error_msg());
    ASSERT_EQ(message, st1.message());
    ASSERT_EQ(fmt::format("{}\na.cpp:10 {}", message, context), st1.detailed_message());
    ASSERT_EQ(fmt::format("Internal error: {}\na.cpp:10 {}", message, context), st1.to_string());
}

TEST_F(StatusTest, update) {
    Status st;
    st.update(Status::NotFound(""));
    ASSERT_TRUE(st.is_not_found());

    st.update(Status::InternalError(""));
    ASSERT_TRUE(st.is_not_found());

    Status st1 = Status::InvalidArgument("");
    st.update(st1);
    ASSERT_TRUE(st.is_not_found());

    st.update(std::move(st1));
    ASSERT_TRUE(st.is_not_found());
}

} // namespace starrocks
