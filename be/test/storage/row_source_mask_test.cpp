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

#include "storage/row_source_mask.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "fs/fs_util.h"
#include "storage/olap_define.h"

namespace starrocks {

class RowSourceMaskTest : public testing::Test {
protected:
    void SetUp() override {
        _max_row_source_mask_memory_bytes = config::max_row_source_mask_memory_bytes;

        // create tmp dir
        std::stringstream tmp_dir_s;
        tmp_dir_s << config::storage_root_path << TMP_PREFIX;
        _tmp_dir = tmp_dir_s.str();
        fs::create_directories(_tmp_dir);
    }

    void TearDown() override {
        config::max_row_source_mask_memory_bytes = _max_row_source_mask_memory_bytes;

        // remove tmp dir
        if (!_tmp_dir.empty()) {
            fs::remove(_tmp_dir);
        }
    }

    int64_t _max_row_source_mask_memory_bytes;
    std::string _tmp_dir;
};

// NOLINTNEXTLINE
TEST_F(RowSourceMaskTest, mask) {
    RowSourceMask mask(0);
    ASSERT_EQ(0, mask.get_source_num());
    ASSERT_EQ(false, mask.get_agg_flag());

    mask.data = 0x8000;
    ASSERT_EQ(0, mask.get_source_num());
    ASSERT_EQ(true, mask.get_agg_flag());

    mask.set_source_num(0x7FFF);
    mask.set_agg_flag(false);
    ASSERT_EQ(0x7FFF, mask.data);

    mask.set_source_num(0x7FFF);
    mask.set_agg_flag(true);
    ASSERT_EQ(0xFFFF, mask.data);
}

// NOLINTNEXTLINE
TEST_F(RowSourceMaskTest, memory_masks) {
    RowSourceMaskBuffer buffer(0, config::storage_root_path);
    std::vector<RowSourceMask> source_masks;
    config::max_row_source_mask_memory_bytes = 1024;

    source_masks.emplace_back(RowSourceMask(0, false));
    source_masks.emplace_back(RowSourceMask(1, true));
    source_masks.emplace_back(RowSourceMask(1, false));
    buffer.write(source_masks);
    source_masks.clear();
    source_masks.emplace_back(RowSourceMask(1, true));
    source_masks.emplace_back(RowSourceMask(3, true));
    source_masks.emplace_back(RowSourceMask(2, true));
    buffer.write(source_masks);
    buffer.flush();

    // --- read ---
    buffer.flip_to_read();
    auto st = buffer.has_remaining();
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(st.value());
    RowSourceMask mask = buffer.current();
    ASSERT_EQ(0, mask.get_source_num());
    ASSERT_FALSE(mask.get_agg_flag());
    buffer.advance();

    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(1, mask.get_source_num());
    ASSERT_TRUE(mask.get_agg_flag());
    buffer.advance();

    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(1, mask.get_source_num());
    ASSERT_FALSE(mask.get_agg_flag());
    buffer.advance();

    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(1, mask.get_source_num());
    ASSERT_TRUE(mask.get_agg_flag());
    buffer.advance();

    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(3, mask.get_source_num());
    ASSERT_TRUE(mask.get_agg_flag());
    buffer.advance();

    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(2, mask.get_source_num());
    ASSERT_TRUE(mask.get_agg_flag());
    buffer.advance();

    // end
    st = buffer.has_remaining();
    ASSERT_TRUE(st.ok());
    ASSERT_FALSE(st.value());

    // --- read again and check has same source ---
    buffer.flip_to_read();
    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(0, mask.get_source_num());
    ASSERT_FALSE(mask.get_agg_flag());
    buffer.advance();

    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(1, mask.get_source_num());
    ASSERT_TRUE(mask.get_agg_flag());

    // check has same source
    ASSERT_TRUE(buffer.has_same_source(mask.get_source_num(), 2));
    ASSERT_TRUE(buffer.has_same_source(mask.get_source_num(), 3));
    ASSERT_FALSE(buffer.has_same_source(mask.get_source_num(), 4));
}

// NOLINTNEXTLINE
TEST_F(RowSourceMaskTest, memory_masks_with_persistence) {
    RowSourceMaskBuffer buffer(1, config::storage_root_path);
    std::vector<RowSourceMask> source_masks;
    config::max_row_source_mask_memory_bytes = 1;

    source_masks.emplace_back(RowSourceMask(0, false));
    source_masks.emplace_back(RowSourceMask(1, true));
    source_masks.emplace_back(RowSourceMask(1, false));
    buffer.write(source_masks);
    source_masks.clear();
    source_masks.emplace_back(RowSourceMask(1, true));
    source_masks.emplace_back(RowSourceMask(3, true));
    source_masks.emplace_back(RowSourceMask(2, true));
    buffer.write(source_masks);
    buffer.flush();

    // --- read ---
    buffer.flip_to_read();
    auto st = buffer.has_remaining();
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(st.value());
    RowSourceMask mask = buffer.current();
    ASSERT_EQ(0, mask.get_source_num());
    ASSERT_FALSE(mask.get_agg_flag());
    buffer.advance();

    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(1, mask.get_source_num());
    ASSERT_TRUE(mask.get_agg_flag());
    buffer.advance();

    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(1, mask.get_source_num());
    ASSERT_FALSE(mask.get_agg_flag());
    buffer.advance();

    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(1, mask.get_source_num());
    ASSERT_TRUE(mask.get_agg_flag());
    buffer.advance();

    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(3, mask.get_source_num());
    ASSERT_TRUE(mask.get_agg_flag());
    buffer.advance();

    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(2, mask.get_source_num());
    ASSERT_TRUE(mask.get_agg_flag());
    buffer.advance();

    // end
    st = buffer.has_remaining();
    ASSERT_TRUE(st.ok());
    ASSERT_FALSE(st.value());

    // --- read again and check has same source ---
    buffer.flip_to_read();
    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(0, mask.get_source_num());
    ASSERT_FALSE(mask.get_agg_flag());
    buffer.advance();

    ASSERT_TRUE(buffer.has_remaining().value());
    mask = buffer.current();
    ASSERT_EQ(1, mask.get_source_num());
    ASSERT_TRUE(mask.get_agg_flag());

    // check has same source
    ASSERT_TRUE(buffer.has_same_source(mask.get_source_num(), 2));
    // new masks are not deserialized, so there is only 2 same sources
    ASSERT_FALSE(buffer.has_same_source(mask.get_source_num(), 3));
    ASSERT_FALSE(buffer.has_same_source(mask.get_source_num(), 4));
}

} // namespace starrocks
