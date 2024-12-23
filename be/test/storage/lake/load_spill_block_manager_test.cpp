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

#include "storage/lake/load_spill_block_manager.h"

#include <gtest/gtest.h>

#include "fs/fs.h"
#include "testutil/assert.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"

namespace starrocks::lake {

class LoadSpillBlockManagerTest : public ::testing::Test {
public:
    void SetUp() { (void)FileSystem::Default()->create_dir_recursive(kTestDir); }

    void TearDown() { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

protected:
    constexpr static const char* const kTestDir = "./lake_load_spill_block_manager_test";
};

TEST_F(LoadSpillBlockManagerTest, test_basic) {
    std::unique_ptr<LoadSpillBlockManager> block_manager =
            std::make_unique<LoadSpillBlockManager>(TUniqueId(), 1, 1, kTestDir);
    ASSERT_OK(block_manager->init());
    ASSIGN_OR_ABORT(auto block, block_manager->acquire_block(1024));
    ASSERT_OK(block_manager->release_block(block));
}

TEST_F(LoadSpillBlockManagerTest, test_write_read) {
    std::unique_ptr<LoadSpillBlockManager> block_manager =
            std::make_unique<LoadSpillBlockManager>(TUniqueId(), 1, 1, kTestDir);
    ASSERT_OK(block_manager->init());
    ASSIGN_OR_ABORT(auto block, block_manager->acquire_block(1024));
    ASSERT_OK(block->append({Slice("hello"), Slice("world")}));
    ASSERT_OK(block->flush());
    ASSIGN_OR_ABORT(auto input_stream, block->get_readable());
    ASSERT_TRUE(input_stream != nullptr);
    raw::RawString buffer;
    buffer.resize(10);
    ASSERT_OK(input_stream->read_fully(buffer.data(), 10));
    ASSERT_EQ(buffer, "helloworld");
    ASSERT_OK(block_manager->release_block(block));
}

} // namespace starrocks::lake