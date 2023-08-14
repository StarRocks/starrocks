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

#pragma once

#include "gutil/macros.h"

#define CHECK_OK(stmt)            \
    do {                          \
        auto&& st__ = (stmt);     \
        CHECK(st__.ok()) << st__; \
    } while (0)

#define ASSERT_OK(stmt)                 \
    do {                                \
        auto&& st__ = (stmt);           \
        ASSERT_TRUE(st__.ok()) << st__; \
    } while (0)

#define ASSERT_ERROR(stmt)       \
    do {                         \
        auto&& st__ = (stmt);    \
        ASSERT_FALSE(st__.ok()); \
    } while (0)

#define EXPECT_OK(stmt)                 \
    do {                                \
        Status st__ = (stmt);           \
        EXPECT_TRUE(st__.ok()) << st__; \
    } while (0)

#define EXPECT_ERROR(stmt)       \
    do {                         \
        auto&& st__ = (stmt);    \
        EXPECT_TRUE(!st__.ok()); \
    } while (0)

#define EXPECT_STATUS(expect, stmt)                                  \
    do {                                                             \
        Status exp = (expect);                                       \
        Status real = (stmt);                                        \
        EXPECT_EQ(exp.code(), real.code()) << exp << " vs " << real; \
    } while (0)

#define ASSIGN_OR_ABORT_IMPL(varname, lhs, rhs) \
    auto&& varname = (rhs);                     \
    CHECK(varname.ok()) << varname.status();    \
    lhs = std::move(varname).value();

#define ASSIGN_OR_ABORT(lhs, rhs) ASSIGN_OR_ABORT_IMPL(VARNAME_LINENUM(value_or_err), lhs, rhs)

namespace starrocks {
class Chunk;
void assert_chunk_equals(const Chunk& chunk1, const Chunk& chunk2);
} // namespace starrocks
