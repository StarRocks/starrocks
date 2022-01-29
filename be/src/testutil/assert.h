// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#define CHECK_OK(stmt)        \
    do {                      \
        Status st = (stmt);   \
        CHECK(st.ok()) << st; \
    } while (0)

#define ASSERT_OK(stmt)             \
    do {                            \
        Status st = (stmt);         \
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

#define EXPECT_STATUS(expect, stmt)                                  \
    do {                                                             \
        Status exp = (expect);                                       \
        Status real = (stmt);                                        \
        EXPECT_EQ(exp.code(), real.code()) << exp << " vs " << real; \
    } while (0)
