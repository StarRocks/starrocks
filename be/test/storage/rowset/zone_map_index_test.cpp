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

#include "cache/object_cache/page_cache.h"
#include "common/config.h"
#include "fs/fs_memory.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/assert.h"

namespace starrocks {

class ColumnZoneMapTest : public testing::Test {
protected:
    const std::string kTestDir = "/zone_map_index_test";

    void SetUp() override {
        _mem_tracker = std::make_unique<MemTracker>();
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kTestDir).ok());
    }

    void TearDown() override {}

    void test_string(const std::string& testname, TypeInfoPtr type_info) {
        std::string filename = kTestDir + "/" + testname;

        auto builder = ZoneMapIndexWriter::create(type_info.get());
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
            ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(filename))
            ASSERT_TRUE(builder->finish(wfile.get(), &index_meta).ok());
            ASSERT_EQ(ZONE_MAP_INDEX, index_meta.type());
            ASSERT_OK(wfile->close());
        }

        IndexReadOptions opts;
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(filename))
        opts.read_file = rfile.get();
        opts.use_page_cache = false;
        OlapReaderStatistics stats;
        opts.stats = &stats;
        ZoneMapIndexReader column_zone_map;
        ASSIGN_OR_ABORT(auto r, column_zone_map.load(opts, index_meta.zone_map_index()))
        ASSERT_TRUE(r);
        ASSERT_EQ(3, column_zone_map.num_pages());
        const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
        ASSERT_EQ(3, zone_maps.size());
        size_t pfx = config::enable_string_prefix_zonemap ? (size_t)config::string_prefix_zonemap_prefix_len : 64;
        check_result_prefix(zone_maps[0], true, true, "aaaa", "ffff", false, true, pfx);
        ASSERT_EQ(false, zone_maps[0].has_null());
        ASSERT_EQ(true, zone_maps[0].has_not_null());

        check_result_prefix(zone_maps[1], true, true, "aaaaa", "fffff", true, true, pfx);
        ASSERT_EQ(true, zone_maps[1].has_null());
        ASSERT_EQ(true, zone_maps[1].has_not_null());

        ASSERT_EQ(true, zone_maps[2].has_null());
        ASSERT_EQ(false, zone_maps[2].has_not_null());
    }

    void write_file(ZoneMapIndexWriter& builder, ColumnIndexMetaPB& meta, std::string filename);
    void load_zone_map(ZoneMapIndexReader& reader, ColumnIndexMetaPB& meta, std::string filename);
    void check_result(const ZoneMapPB& zone_map, bool has_min, bool has_max, const std::string& min,
                      const std::string& max, bool has_null, bool has_not_null);

    // Check with prefix truncation semantics for string zonemap entries: min is prefix; max is prefix possibly with 0xFF.
    void check_result_prefix(const ZoneMapPB& zone_map, bool has_min, bool has_max, const std::string& min,
                             const std::string& max, bool has_null, bool has_not_null, size_t prefix_len = 64) {
        ASSERT_EQ(has_min, zone_map.has_min());
        ASSERT_EQ(has_max, zone_map.has_max());
        if (has_min) {
            const auto& zmin = zone_map.min();
            ASSERT_TRUE(min.rfind(zmin, 0) == 0 || zmin == min.substr(0, std::min(prefix_len, min.size())));
            ASSERT_TRUE(zmin <= min);
        }
        if (has_max) {
            ASSERT_TRUE(zone_map.max() >= max);
        }
        ASSERT_EQ(has_null, zone_map.has_null());
        ASSERT_EQ(has_not_null, zone_map.has_not_null());
    }

    std::shared_ptr<MemoryFileSystem> _fs = nullptr;
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
};

void ColumnZoneMapTest::check_result(const ZoneMapPB& zone_map, bool has_min, bool has_max, const std::string& min,
                                     const std::string& max, bool has_null, bool has_not_null) {
    ASSERT_EQ(has_min, zone_map.has_min());
    ASSERT_EQ(has_max, zone_map.has_max());
    ASSERT_EQ(min, zone_map.min());
    ASSERT_EQ(max, zone_map.max());
    ASSERT_EQ(has_null, zone_map.has_null());
    ASSERT_EQ(has_not_null, zone_map.has_not_null());
}

void ColumnZoneMapTest::write_file(ZoneMapIndexWriter& builder, ColumnIndexMetaPB& meta, std::string filename) {
    ASSIGN_OR_ABORT(auto file, _fs->new_writable_file(filename))
    ASSERT_TRUE(builder.finish(file.get(), &meta).ok());
    ASSERT_EQ(ZONE_MAP_INDEX, meta.type());
    ASSERT_OK(file->close());
}

void ColumnZoneMapTest::load_zone_map(ZoneMapIndexReader& reader, ColumnIndexMetaPB& meta, std::string filename) {
    IndexReadOptions opts;
    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(filename))
    opts.read_file = rfile.get();
    opts.use_page_cache = false;
    OlapReaderStatistics stats;
    opts.stats = &stats;
    ASSERT_TRUE(reader.load(opts, meta.zone_map_index()).value());
    if (!meta.zone_map_index().segment_zone_map().has_not_null()) {
        delete meta.mutable_zone_map_index()->mutable_segment_zone_map()->release_min();
        delete meta.mutable_zone_map_index()->mutable_segment_zone_map()->release_max();
    }
}

TEST_F(ColumnZoneMapTest, PartialNullPage1) {
    std::string filename = kTestDir + "/PartialNullPage1";

    TabletColumn int_column = create_int_key(0);
    TypeInfoPtr type_info = get_type_info(int_column);

    auto writer = ZoneMapIndexWriter::create(type_info.get());

    // add two page
    writer->add_nulls(10);
    writer->flush();

    std::vector<int> values = {1, 2, 3, 4, 5};
    writer->add_values(values.data(), 5);
    writer->flush();

    // write
    ColumnIndexMetaPB index_meta;
    write_file(*writer, index_meta, filename);

    // read
    ZoneMapIndexReader reader;
    load_zone_map(reader, index_meta, filename);

    // page zone map
    ASSERT_EQ(2, reader.num_pages());
    const auto& zone_maps = reader.page_zone_maps();
    ASSERT_EQ(2, zone_maps.size());

    check_result(zone_maps[0], false, false, "", "", true, false);
    check_result(zone_maps[1], true, true, "1", "5", false, true);

    // segment zonemap
    const auto& segment_zonemap = index_meta.zone_map_index().segment_zone_map();
    check_result(segment_zonemap, true, true, "1", "5", true, true);
}

TEST_F(ColumnZoneMapTest, PartialNullPage2) {
    std::string filename = kTestDir + "/PartialNullPage2";

    TabletColumn int_column = create_int_key(0);
    TypeInfoPtr type_info = get_type_info(int_column);

    auto writer = ZoneMapIndexWriter::create(type_info.get());

    // add two page
    std::vector<int> values = {1, 2, 3, 4, 5};
    writer->add_values(values.data(), 5);
    writer->flush();

    writer->add_nulls(10);
    writer->flush();

    // write
    ColumnIndexMetaPB index_meta;
    write_file(*writer, index_meta, filename);

    // read
    ZoneMapIndexReader reader;
    load_zone_map(reader, index_meta, filename);

    // page zone map
    ASSERT_EQ(2, reader.num_pages());
    const auto& zone_maps = reader.page_zone_maps();
    ASSERT_EQ(2, zone_maps.size());

    check_result(zone_maps[0], true, true, "1", "5", false, true);
    check_result(zone_maps[1], false, false, "", "", true, false);

    // segment zonemap
    const auto& segment_zonemap = index_meta.zone_map_index().segment_zone_map();
    check_result(segment_zonemap, true, true, "1", "5", true, true);
}

TEST_F(ColumnZoneMapTest, StringResize) {
    std::string filename = kTestDir + "/StringResize";

    TabletColumn varchar_column = create_varchar_key(0);
    TypeInfoPtr type_info = get_type_info(varchar_column);

    auto writer = ZoneMapIndexWriter::create(type_info.get());

    // add two page
    std::string str1 = std::string("01234");
    std::string str2 = std::string("0123456789");
    std::string str3;
    std::string str4;
    for (size_t i = 0; i < 10; i++) {
        str3.append(str2);
    }
    for (size_t i = 0; i < 20; i++) {
        str4.append(str2);
    }
    std::vector<Slice> values1 = {{str1.data(), str1.size()}, {str2.data(), str2.size()}};
    writer->add_values(values1.data(), 2);
    writer->flush();

    std::vector<Slice> values2 = {{str3.data(), str3.size()}, {str4.data(), str4.size()}};
    writer->add_values(values2.data(), 2);
    writer->flush();

    // write
    ColumnIndexMetaPB index_meta;
    write_file(*writer, index_meta, filename);

    // read
    ZoneMapIndexReader reader;
    load_zone_map(reader, index_meta, filename);

    // page zone map
    ASSERT_EQ(2, reader.num_pages());
    const auto& zone_maps = reader.page_zone_maps();
    ASSERT_EQ(2, zone_maps.size());

    size_t pfx = config::enable_string_prefix_zonemap ? (size_t)config::string_prefix_zonemap_prefix_len : 64;
    check_result_prefix(zone_maps[0], true, true, str1, str2, false, true, pfx);
    check_result_prefix(zone_maps[1], true, true, str3, str4, false, true, pfx);

    // segment zonemap
    const auto& segment_zonemap = index_meta.zone_map_index().segment_zone_map();
    check_result_prefix(segment_zonemap, true, true, str1, str4, false, true, pfx);
}

TEST_F(ColumnZoneMapTest, AllNullPage) {
    std::string filename = kTestDir + "/AllNullPage";

    TabletColumn int_column = create_int_key(0);
    TypeInfoPtr type_info = get_type_info(int_column);

    auto writer = ZoneMapIndexWriter::create(type_info.get());

    // add one page
    writer->add_nulls(10);
    writer->flush();

    // write
    ColumnIndexMetaPB index_meta;
    write_file(*writer, index_meta, filename);

    // read
    ZoneMapIndexReader reader;
    load_zone_map(reader, index_meta, filename);

    // page zone map
    ASSERT_EQ(1, reader.num_pages());
    const auto& zone_maps = reader.page_zone_maps();
    ASSERT_EQ(1, zone_maps.size());

    check_result(zone_maps[0], false, false, "", "", true, false);

    // segment zonemap
    const auto& segment_zonemap = index_meta.zone_map_index().segment_zone_map();
    check_result(segment_zonemap, false, false, "", "", true, false);
}

// Test for int
TEST_F(ColumnZoneMapTest, NormalTestIntPage) {
    std::string filename = kTestDir + "/NormalTestIntPage";

    TabletColumn int_column = create_int_key(0);
    TypeInfoPtr type_info = get_type_info(int_column);

    std::unique_ptr<ZoneMapIndexWriter> builder = ZoneMapIndexWriter::create(type_info.get());
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
    write_file(*builder, index_meta, filename);

    ZoneMapIndexReader column_zone_map;
    load_zone_map(column_zone_map, index_meta, filename);

    ASSERT_EQ(3, column_zone_map.num_pages());
    const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
    ASSERT_EQ(3, zone_maps.size());

    check_result(zone_maps[0], true, true, "1", "22", false, true);
    check_result(zone_maps[1], true, true, "2", "31", true, true);
    check_result(zone_maps[2], false, false, "", "", true, false);
}

// Test for string
TEST_F(ColumnZoneMapTest, NormalTestVarcharPage) {
    TabletColumn varchar_column = create_varchar_key(0);
    TypeInfoPtr type_info = get_type_info(varchar_column);
    // Use prefix check inside test_string by reading page checks
    test_string("NormalTestVarcharPage", type_info);
}

// Test for string
TEST_F(ColumnZoneMapTest, NormalTestCharPage) {
    TabletColumn char_column = create_char_key(0);
    TypeInfoPtr type_info = get_type_info(char_column);
    test_string("NormalTestCharPage", type_info);
}

// Test for varbinary
TEST_F(ColumnZoneMapTest, NormalTestVarbinaryPage) {
    TabletColumn varbinary_column = create_varbinary_key(0);
    TypeInfoPtr type_info = get_type_info(varbinary_column);
    test_string("NormalTestVarbinaryPage", type_info);
}

// Test for varbinary with binary data
TEST_F(ColumnZoneMapTest, VarbinaryWithBinaryData) {
    std::string filename = kTestDir + "/VarbinaryWithBinaryData";

    TabletColumn varbinary_column = create_varbinary_key(0);
    TypeInfoPtr type_info = get_type_info(varbinary_column);

    auto writer = ZoneMapIndexWriter::create(type_info.get());

    // Add binary data with various patterns
    std::vector<std::string> binary_values1 = {
            std::string("\x00\x01\x02\x03", 4), // Binary data starting with null bytes
            std::string("\xFF\xFE\xFD\xFC", 4), // Binary data with high bytes
            std::string("ABCD", 4),             // ASCII data
            std::string("\x00\x00\x00\x00", 4), // All null bytes
    };

    for (auto& value : binary_values1) {
        Slice slice(value);
        writer->add_values((const uint8_t*)&slice, 1);
    }
    writer->flush();

    // Add more binary data with different patterns
    std::vector<std::string> binary_values2 = {
            std::string("\x01\x02\x03\x04", 4), std::string("\xFE\xFD\xFC\xFB", 4), std::string("EFGH", 4),
            std::string("\xFF\xFF\xFF\xFF", 4), // All high bytes
    };

    for (auto& value : binary_values2) {
        Slice slice(value);
        writer->add_values((const uint8_t*)&slice, 1);
    }
    writer->add_nulls(1);
    writer->flush();

    // Add null values
    writer->add_nulls(3);
    writer->flush();

    // Write out zone map index
    ColumnIndexMetaPB index_meta;
    write_file(*writer, index_meta, filename);

    // Read and verify
    ZoneMapIndexReader column_zone_map;
    load_zone_map(column_zone_map, index_meta, filename);

    ASSERT_EQ(3, column_zone_map.num_pages());
    const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
    ASSERT_EQ(3, zone_maps.size());

    // Check first page - should have min/max from binary_values1
    // For binary data, comparison is byte-by-byte, so "\x00\x00\x00\x00" is min and "\xFF\xFE\xFD\xFC" is max
    check_result(zone_maps[0], true, true, std::string("\x00\x00\x00\x00", 4), std::string("\xFF\xFE\xFD\xFC", 4),
                 false, true);

    // Check second page - should have min/max from binary_values2 plus null
    // "\x01\x02\x03\x04" is min and "\xFF\xFF\xFF\xFF" is max
    check_result(zone_maps[1], true, true, std::string("\x01\x02\x03\x04", 4), std::string("\xFF\xFF\xFF\xFF", 4), true,
                 true);

    // Check third page - should be all nulls
    check_result(zone_maps[2], false, false, "", "", true, false);

    // Check segment zonemap - should cover all data
    // The segment zonemap should have the overall min/max across all pages
    const auto& segment_zonemap = index_meta.zone_map_index().segment_zone_map();
    check_result(segment_zonemap, true, true, std::string("\x00\x00\x00\x00", 4), std::string("\xFF\xFF\xFF\xFF", 4),
                 true, true);
}

TEST_F(ColumnZoneMapTest, StringPrefixZonemapVariants) {
    // Enable string prefix zonemap for this test context
    bool old_switch = config::enable_string_prefix_zonemap;
    int old_len = config::string_prefix_zonemap_prefix_len;
    config::enable_string_prefix_zonemap = true;
    config::string_prefix_zonemap_prefix_len = 16;

    // Build a segment with various string lengths and patterns
    std::string filename = kTestDir + "/StringPrefixZonemapVariants";

    TabletColumn varchar_column = create_varchar_key(0);
    TypeInfoPtr type_info = get_type_info(varchar_column);

    auto writer = ZoneMapIndexWriter::create(type_info.get());

    // Short strings
    std::vector<Slice> shorts = {{"a", 1}, {"b", 1}, {"c", 1}};
    writer->add_values(shorts.data(), shorts.size());
    writer->flush();

    // Common prefix strings
    std::vector<std::string> cp = {"prefix_0001", "prefix_0002", "prefix_9999"};
    std::vector<Slice> cp_slices;
    for (auto& s : cp) cp_slices.push_back({s.data(), s.size()});
    writer->add_values(cp_slices.data(), cp_slices.size());
    writer->flush();

    // Random long strings (> 64 to ensure truncation even if config changes)
    std::string long1(80, 'X');
    std::string long2(120, 'Y');
    std::vector<Slice> longs = {{long1.data(), long1.size()}, {long2.data(), long2.size()}};
    writer->add_values(longs.data(), longs.size());
    writer->flush();

    // Write index out
    ColumnIndexMetaPB index_meta;
    write_file(*writer, index_meta, filename);

    // Read back
    ZoneMapIndexReader reader;
    load_zone_map(reader, index_meta, filename);

    ASSERT_EQ(3, reader.num_pages());
    const auto& zone_maps = reader.page_zone_maps();
    size_t pfx = (size_t)config::string_prefix_zonemap_prefix_len;

    // Page 0: shorts
    check_result_prefix(zone_maps[0], true, true, "a", "c", false, true, pfx);
    // Page 1: common prefix
    check_result_prefix(zone_maps[1], true, true, cp.front(), cp.back(), false, true, pfx);
    // Page 2: long strings
    check_result_prefix(zone_maps[2], true, true, long1, long2, false, true, pfx);

    // Restore config
    config::enable_string_prefix_zonemap = old_switch;
    config::string_prefix_zonemap_prefix_len = old_len;
}

} // namespace starrocks
