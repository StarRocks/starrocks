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

#include "io/io_profiler.h"

#include <gtest/gtest.h>

#include "testutil/assert.h"

namespace starrocks {

#define ADD_READ_IO_STAT(stat, bytes, time_ns) \
    stat.read_ops += 1;                        \
    stat.read_bytes += bytes;                  \
    stat.read_time_ns += time_ns

#define ADD_WRITE_IO_STAT(stat, bytes, time_ns) \
    stat.write_ops += 1;                        \
    stat.write_bytes += bytes;                  \
    stat.write_time_ns += time_ns

#define ADD_SYNC_IO_STAT(stat, time_ns) \
    stat.sync_ops += 1;                 \
    stat.sync_time_ns += time_ns

#define ASSERT_IO_STAT_EQ(expect, actual)                  \
    ASSERT_EQ(expect.read_ops, actual.read_ops);           \
    ASSERT_EQ(expect.read_bytes, actual.read_bytes);       \
    ASSERT_EQ(expect.read_time_ns, actual.read_time_ns);   \
    ASSERT_EQ(expect.write_ops, actual.write_ops);         \
    ASSERT_EQ(expect.write_bytes, actual.write_bytes);     \
    ASSERT_EQ(expect.write_time_ns, actual.write_time_ns); \
    ASSERT_EQ(expect.sync_ops, actual.sync_ops);           \
    ASSERT_EQ(expect.sync_time_ns, actual.sync_time_ns)

TEST(IOProfilerTest, test_tls_io) {
    ASSERT_OK(IOProfiler::start(IOProfiler::IOMode::IOMODE_ALL));
    auto scope = IOProfiler::scope(IOProfiler::TAG_LOAD, 6);
    // the init io stat may be not 0, because other test cases maybe have updated the stat
    auto expect = scope.current_scoped_tls_io();

    IOProfiler::add_read(1, 1000);
    ADD_READ_IO_STAT(expect, 1, 1000);
    auto actual1 = scope.current_scoped_tls_io();
    ASSERT_IO_STAT_EQ(expect, actual1);

    IOProfiler::add_write(2, 200000);
    ADD_WRITE_IO_STAT(expect, 2, 200000);
    auto actual2 = scope.current_scoped_tls_io();
    ASSERT_IO_STAT_EQ(expect, actual2);

    IOProfiler::add_write(1024, 500000);
    ADD_WRITE_IO_STAT(expect, 1024, 500000);
    auto actual3 = scope.current_scoped_tls_io();
    ASSERT_IO_STAT_EQ(expect, actual3);

    IOProfiler::add_read(1048576, 1000000);
    ADD_READ_IO_STAT(expect, 1048576, 1000000);
    auto actual4 = scope.current_scoped_tls_io();
    ASSERT_IO_STAT_EQ(expect, actual4);

    IOProfiler::add_sync(9883);
    ADD_SYNC_IO_STAT(expect, 9883);
    auto actual5 = scope.current_scoped_tls_io();
    ASSERT_IO_STAT_EQ(expect, actual5);
    IOProfiler::stop();
}

TEST(IOProfilerTest, test_context_io) {
    {
        auto expect = IOProfiler::IOStat{0, 0, 0, 0, 0, 0};
        // io mode is IOMODE_NONE
        auto scope = IOProfiler::scope(IOProfiler::TAG_LOAD, 1);
        ASSERT_EQ(IOProfiler::IOMode::IOMODE_NONE, IOProfiler::get_context_io_mode());
        IOProfiler::add_read(1, 1000);
        IOProfiler::add_write(1, 1000);
        ASSERT_IO_STAT_EQ(expect, scope.current_context_io());
    }
    {
        auto expect = IOProfiler::IOStat{0, 0, 0, 0, 0, 0};
        // io mode is IOMODE_READ
        ASSERT_OK(IOProfiler::start(IOProfiler::IOMode::IOMODE_READ));
        auto scope = IOProfiler::scope(IOProfiler::TAG_LOAD, 2);

        ASSERT_EQ(IOProfiler::IOMode::IOMODE_READ, IOProfiler::get_context_io_mode());
        IOProfiler::add_read(2, 2000);
        ADD_READ_IO_STAT(expect, 2, 0);
        IOProfiler::add_write(2, 2000);
        ASSERT_IO_STAT_EQ(expect, scope.current_context_io());
        IOProfiler::stop();
        ASSERT_EQ(IOProfiler::IOMode::IOMODE_NONE, IOProfiler::get_context_io_mode());
    }
    {
        auto expect = IOProfiler::IOStat{0, 0, 0, 0, 0, 0};
        // io mode is IOMODE_WRITE
        ASSERT_OK(IOProfiler::start(IOProfiler::IOMode::IOMODE_WRITE));
        auto scope = IOProfiler::scope(IOProfiler::TAG_LOAD, 3);

        ASSERT_EQ(IOProfiler::IOMode::IOMODE_WRITE, IOProfiler::get_context_io_mode());
        IOProfiler::add_read(3, 3000);
        IOProfiler::add_write(3, 3000);
        ADD_WRITE_IO_STAT(expect, 3, 0);
        ASSERT_IO_STAT_EQ(expect, scope.current_context_io());
        IOProfiler::stop();
        ASSERT_EQ(IOProfiler::IOMode::IOMODE_NONE, IOProfiler::get_context_io_mode());
    }
    {
        auto expect = IOProfiler::IOStat{0, 0, 0, 0, 0, 0};
        // io mode is IOMODE_ALL
        ASSERT_OK(IOProfiler::start(IOProfiler::IOMode::IOMODE_ALL));
        auto scope = IOProfiler::scope(IOProfiler::TAG_LOAD, 4);

        ASSERT_EQ(IOProfiler::IOMode::IOMODE_ALL, IOProfiler::get_context_io_mode());
        IOProfiler::add_read(4, 4000);
        ADD_READ_IO_STAT(expect, 4, 0);
        IOProfiler::add_write(4, 4000);
        ADD_WRITE_IO_STAT(expect, 4, 0);
        ASSERT_IO_STAT_EQ(expect, scope.current_context_io());
        IOProfiler::stop();
        ASSERT_EQ(IOProfiler::IOMode::IOMODE_NONE, IOProfiler::get_context_io_mode());
    }
}

TEST(IOProfilerTest, test_profile_and_get_topn_stats) {
    ASSERT_OK(IOProfiler::start(IOProfiler::IOMode::IOMODE_ALL));
    auto scope = IOProfiler::scope(IOProfiler::TAG_LOAD, 5);
    auto expect = IOProfiler::IOStat{0, 0, 0, 0, 0, 0};
    IOProfiler::add_read(4, 4000);
    ADD_READ_IO_STAT(expect, 4, 0);
    IOProfiler::add_write(4, 4000);
    ADD_WRITE_IO_STAT(expect, 4, 0);
    IOProfiler::stop();
    ASSERT_FALSE(IOProfiler::is_empty());
    auto ret = IOProfiler::profile_and_get_topn_stats_str("all", 1, 1);
    ASSERT_TRUE(IOProfiler::is_empty());
}

} // namespace starrocks