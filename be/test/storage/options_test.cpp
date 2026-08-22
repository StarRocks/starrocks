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

#include "storage/options.h"

#include <gtest/gtest.h>

#include "common/config.h"

namespace starrocks {

class LakeScanBufferSizeTest : public testing::Test {
public:
    void SetUp() override {
        _saved = config::lake_scan_min_remote_read_bytes;
        config::lake_scan_min_remote_read_bytes = 131072;
    }
    void TearDown() override { config::lake_scan_min_remote_read_bytes = _saved; }

private:
    int64_t _saved = 0;
};

// A caller that picked its own size keeps it. Compaction and segment rewrite do this, and
// they must not be pulled down to the scan path's much smaller bound.
TEST_F(LakeScanBufferSizeTest, caller_choice_wins) {
    LakeIOOptions opts;
    opts.buffer_size = 8 * 1024 * 1024;
    EXPECT_EQ(8 * 1024 * 1024, lake_scan_buffer_size(opts));
    // Even where the bound would otherwise apply.
    opts.skip_disk_cache = true;
    EXPECT_EQ(8 * 1024 * 1024, lake_scan_buffer_size(opts));
}

// Zero is a choice, not "unset": it asks for unbuffered, exactly-sized reads.
TEST_F(LakeScanBufferSizeTest, zero_is_a_caller_choice) {
    LakeIOOptions opts;
    opts.buffer_size = 0;
    EXPECT_EQ(0, lake_scan_buffer_size(opts));
}

// Bypassing the disk cache means the request size is ours to choose, so the bound applies.
TEST_F(LakeScanBufferSizeTest, skipping_the_disk_cache_takes_the_bound) {
    LakeIOOptions opts;
    opts.skip_disk_cache = true;
    opts.fill_data_cache = true;
    EXPECT_EQ(131072, lake_scan_buffer_size(opts));
}

// Not filling the cache also escapes block alignment: cachefs reads just the bytes asked
// for rather than rounding out to a whole block, so the bound applies here too.
TEST_F(LakeScanBufferSizeTest, not_filling_the_cache_takes_the_bound) {
    LakeIOOptions opts;
    ASSERT_EQ(-1, opts.buffer_size);
    ASSERT_FALSE(opts.fill_data_cache);
    ASSERT_FALSE(opts.skip_disk_cache);
    EXPECT_EQ(131072, lake_scan_buffer_size(opts));
}

// Filling the cache without skipping it: cachefs fetches a whole block whatever we ask for,
// so shrinking the bound would buy nothing. Defer to starlet, as before this config existed.
TEST_F(LakeScanBufferSizeTest, block_aligned_reads_defer_to_starlet) {
    LakeIOOptions opts;
    opts.fill_data_cache = true;
    opts.skip_disk_cache = false;
    EXPECT_LT(lake_scan_buffer_size(opts), 0);
}

// A negative config is the escape hatch back to starlet's own default, so it has to survive
// as a negative value rather than being clamped to zero, which would disable buffering.
TEST_F(LakeScanBufferSizeTest, negative_config_defers_to_starlet) {
    config::lake_scan_min_remote_read_bytes = -1;
    LakeIOOptions opts;
    EXPECT_LT(lake_scan_buffer_size(opts), 0);
}

class ShouldEnableIoCoalesceLakeReadTest : public testing::Test {
public:
    void SetUp() override {
        _saved = config::io_coalesce_lake_read_enable;
        config::io_coalesce_lake_read_enable = false;
    }
    void TearDown() override { config::io_coalesce_lake_read_enable = _saved; }

private:
    bool _saved = false;
};

// The reads whose size is ours to choose are the ones worth merging, and they are the same
// reads lake_scan_min_remote_read_bytes shrinks -- so coalescing is on for them by default.
TEST_F(ShouldEnableIoCoalesceLakeReadTest, on_where_the_read_size_is_ours) {
    LakeIOOptions skip_cache;
    skip_cache.skip_disk_cache = true;
    skip_cache.fill_data_cache = true;
    EXPECT_TRUE(should_enable_io_coalesce_lake_read(skip_cache));

    LakeIOOptions no_fill;
    ASSERT_FALSE(no_fill.fill_data_cache);
    ASSERT_FALSE(no_fill.skip_disk_cache);
    EXPECT_TRUE(should_enable_io_coalesce_lake_read(no_fill));
}

// A read cachefs serves from whole blocks has its size dictated by the block layout, so
// merging page ranges cannot change what reaches the object store. Leave it off.
TEST_F(ShouldEnableIoCoalesceLakeReadTest, off_for_block_aligned_reads) {
    LakeIOOptions opts;
    opts.fill_data_cache = true;
    opts.skip_disk_cache = false;
    EXPECT_FALSE(should_enable_io_coalesce_lake_read(opts));
}

// The config still forces it on everywhere, including the block-aligned case.
TEST_F(ShouldEnableIoCoalesceLakeReadTest, config_forces_it_on_everywhere) {
    config::io_coalesce_lake_read_enable = true;
    LakeIOOptions opts;
    opts.fill_data_cache = true;
    opts.skip_disk_cache = false;
    EXPECT_TRUE(should_enable_io_coalesce_lake_read(opts));
}

} // namespace starrocks
