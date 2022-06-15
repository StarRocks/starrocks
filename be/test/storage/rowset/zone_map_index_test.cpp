// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/zone_map_index_test.cpp

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

#include "storage/rowset/zone_map_index.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/statusor.h"
#include "env/env_memory.h"
#include "storage/fs/file_block_manager.h"
#include "storage/page_cache.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/assert.h"
#include "util/file_utils.h"

namespace starrocks {

class ColumnZoneMapTest : public testing::Test {
protected:
    const std::string kTestDir = "/zone_map_index_test";

    void SetUp() override {
        _mem_tracker = std::make_unique<MemTracker>();
        StoragePageCache::create_global_cache(_mem_tracker.get(), 1000000000);
        _env = new EnvMemory();
        _block_mgr = new fs::FileBlockManager(_env, fs::BlockManagerOptions());
        ASSERT_TRUE(_env->create_dir(kTestDir).ok());
    }

    void TearDown() override {
        StoragePageCache::release_global_cache();
        delete _block_mgr;
        delete _env;
    }

    void test_string(const std::string& testname, Field* field) {
        std::string filename = kTestDir + "/" + testname;

        std::unique_ptr<ZoneMapIndexWriter> builder = ZoneMapIndexWriter::create(field);
        std::vector<std::string> values1 = {"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"};
        for (auto& value : values1) {
            Slice slice(value);
            builder->add_values((const uint8_t*)&slice, 1);
        }
        builder->flush();
        std::vector<std::string> values2 = {"aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee", "fffff"};
        for (auto& value : values2) {
            Slice slice(value);
            builder->add_values((const uint8_t*)&slice, 1);
        }
        builder->add_nulls(1);
        builder->flush();
        for (int i = 0; i < 6; ++i) {
            builder->add_nulls(1);
        }
        builder->flush();
        // write out zone map index
        ColumnIndexMetaPB index_meta;
        {
            std::unique_ptr<fs::WritableBlock> wblock;
            fs::CreateBlockOptions opts({filename});
            ASSERT_TRUE(_block_mgr->create_block(opts, &wblock).ok());
            ASSERT_TRUE(builder->finish(wblock.get(), &index_meta).ok());
            ASSERT_EQ(ZONE_MAP_INDEX, index_meta.type());
            ASSERT_TRUE(wblock->close().ok());
        }

        ZoneMapIndexReader column_zone_map;
        ASSIGN_OR_ABORT(auto r, column_zone_map.load(_block_mgr, filename, index_meta.zone_map_index(), true, false));
        ASSERT_TRUE(r);
        ASSERT_EQ(3, column_zone_map.num_pages());
        const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
        ASSERT_EQ(3, zone_maps.size());
        ASSERT_EQ("aaaa", zone_maps[0].min());
        ASSERT_EQ("ffff", zone_maps[0].max());
        ASSERT_EQ(false, zone_maps[0].has_null());
        ASSERT_EQ(true, zone_maps[0].has_not_null());

        ASSERT_EQ("aaaaa", zone_maps[1].min());
        ASSERT_EQ("fffff", zone_maps[1].max());
        ASSERT_EQ(true, zone_maps[1].has_null());
        ASSERT_EQ(true, zone_maps[1].has_not_null());

        ASSERT_EQ(true, zone_maps[2].has_null());
        ASSERT_EQ(false, zone_maps[2].has_not_null());
    }

    EnvMemory* _env = nullptr;
    fs::FileBlockManager* _block_mgr = nullptr;
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
};

// Test for int
TEST_F(ColumnZoneMapTest, NormalTestIntPage) {
    std::string filename = kTestDir + "/NormalTestIntPage";

    TabletColumn int_column = create_int_key(0);
    Field* field = FieldFactory::create(int_column);

    std::unique_ptr<ZoneMapIndexWriter> builder = ZoneMapIndexWriter::create(field);
    std::vector<int> values1 = {1, 10, 11, 20, 21, 22};
    for (auto value : values1) {
        builder->add_values((const uint8_t*)&value, 1);
    }
    builder->flush();
    std::vector<int> values2 = {2, 12, 31, 23, 21, 22};
    for (auto value : values2) {
        builder->add_values((const uint8_t*)&value, 1);
    }
    builder->add_nulls(1);
    builder->flush();
    builder->add_nulls(6);
    builder->flush();
    // write out zone map index
    ColumnIndexMetaPB index_meta;
    {
        std::unique_ptr<fs::WritableBlock> wblock;
        fs::CreateBlockOptions opts({filename});
        ASSERT_TRUE(_block_mgr->create_block(opts, &wblock).ok());
        ASSERT_TRUE(builder->finish(wblock.get(), &index_meta).ok());
        ASSERT_EQ(ZONE_MAP_INDEX, index_meta.type());
        ASSERT_TRUE(wblock->close().ok());
    }

    ZoneMapIndexReader column_zone_map;
    ASSIGN_OR_ABORT(auto r, column_zone_map.load(_block_mgr, filename, index_meta.zone_map_index(), true, false));
    ASSERT_TRUE(r);
    ASSERT_EQ(3, column_zone_map.num_pages());
    const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
    ASSERT_EQ(3, zone_maps.size());

    ASSERT_EQ(std::to_string(1), zone_maps[0].min());
    ASSERT_EQ(std::to_string(22), zone_maps[0].max());
    ASSERT_EQ(false, zone_maps[0].has_null());
    ASSERT_EQ(true, zone_maps[0].has_not_null());

    ASSERT_EQ(std::to_string(2), zone_maps[1].min());
    ASSERT_EQ(std::to_string(31), zone_maps[1].max());
    ASSERT_EQ(true, zone_maps[1].has_null());
    ASSERT_EQ(true, zone_maps[1].has_not_null());

    ASSERT_EQ(true, zone_maps[2].has_null());
    ASSERT_EQ(false, zone_maps[2].has_not_null());
    delete field;
}

// Test for string
TEST_F(ColumnZoneMapTest, NormalTestVarcharPage) {
    TabletColumn varchar_column = create_varchar_key(0);
    Field* field = FieldFactory::create(varchar_column);
    test_string("NormalTestVarcharPage", field);
    delete field;
}

// Test for string
TEST_F(ColumnZoneMapTest, NormalTestCharPage) {
    TabletColumn char_column = create_char_key(0);
    Field* field = FieldFactory::create(char_column);
    test_string("NormalTestCharPage", field);
    delete field;
}

} // namespace starrocks
