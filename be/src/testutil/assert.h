// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "gutil/macros.h"

#define CHECK_OK(stmt)        \
    do {                      \
        auto&& st = (stmt);   \
        CHECK(st.ok()) << st; \
    } while (0)

#define ASSERT_OK(stmt)             \
    do {                            \
        auto&& st = (stmt);         \
        ASSERT_TRUE(st.ok()) << st; \
    } while (0)

#define ASSERT_ERROR(stmt)     \
    do {                       \
        auto&& st = (stmt);    \
        ASSERT_FALSE(st.ok()); \
    } while (0)

#define EXPECT_OK(stmt)             \
    do {                            \
        Status st = (stmt);         \
        EXPECT_TRUE(st.ok()) << st; \
    } while (0)

#define EXPECT_ERROR(stmt)     \
    do {                       \
        auto&& st = (stmt);    \
        EXPECT_TRUE(!st.ok()); \
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
