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

#include "storage/rows_mapper.h"

#include "fs/fs.h"
#include "fs/fs_util.h"
#include "testutil/assert.h"

namespace starrocks {

class RowsMapperTest : public testing::Test {
public:
    RowsMapperTest() {}

protected:
    constexpr static const char* kTestDirectory = "./test_rows_mapper/";

    void SetUp() override { ASSERT_OK(fs::create_directories(kTestDirectory)); }

    void TearDown() override { (void)fs::remove_all(kTestDirectory); }

    // generate id between [start, end)
    void generate_rssid_rowids(std::vector<uint64_t>* rssid_rowids, uint64_t start, size_t end, uint64_t rssid) {
        for (uint64_t i = start; i < end; i++) {
            rssid_rowids->push_back((rssid << 32) | i);
        }
    }
};

TEST_F(RowsMapperTest, test_write_read) {
    const std::string filename = std::string(kTestDirectory) + "test_write_read.crm";
    RowsMapperBuilder builder(filename);
    std::vector<uint64_t> rssid_rowids;
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_FALSE(fs::path_exist(filename));
    generate_rssid_rowids(&rssid_rowids, 0, 1000, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    rssid_rowids.clear();
    generate_rssid_rowids(&rssid_rowids, 1000, 3000, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    // read from file
    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));
    for (uint32_t i = 0; i < 3000; i += 100) {
        std::vector<uint64_t> rows_mapper;
        ASSERT_OK(iterator.next_values(100, &rows_mapper));
        ASSERT_TRUE(rows_mapper.size() == 100);
        for (uint32_t j = 0; j < rows_mapper.size(); j++) {
            ASSERT_TRUE((rows_mapper[j] >> 32) == 11);
            ASSERT_TRUE((rows_mapper[j] & 0xFFFFFFFF) == i + j);
        }
    }
    ASSERT_OK(iterator.status());
    // should eof
    std::vector<uint64_t> rows_mapper;
    ASSERT_TRUE(iterator.next_values(1, &rows_mapper).is_end_of_file());
}

TEST_F(RowsMapperTest, test_write_read_multi_segment) {
    const std::string filename = std::string(kTestDirectory) + "test_write_read_multi_segment.crm";
    RowsMapperBuilder builder(filename);
    std::vector<uint64_t> rssid_rowids;
    // rssid = 11
    generate_rssid_rowids(&rssid_rowids, 0, 1000, 11);
    ASSERT_OK(builder.append(rssid_rowids));
    rssid_rowids.clear();
    // rssid = 43
    generate_rssid_rowids(&rssid_rowids, 1000, 3000, 43);
    ASSERT_OK(builder.append(rssid_rowids));
    ASSERT_OK(builder.finalize());

    // read from file
    RowsMapperIterator iterator;
    ASSERT_OK(iterator.open(filename));
    for (uint32_t i = 0; i < 3000; i += 100) {
        std::vector<uint64_t> rows_mapper;
        ASSERT_OK(iterator.next_values(100, &rows_mapper));
        ASSERT_TRUE(rows_mapper.size() == 100);
        for (uint32_t j = 0; j < rows_mapper.size(); j++) {
            if (i + j < 1000) {
                ASSERT_TRUE((rows_mapper[j] >> 32) == 11);
                ASSERT_TRUE((rows_mapper[j] & 0xFFFFFFFF) == i + j);
            } else {
                ASSERT_TRUE((rows_mapper[j] >> 32) == 43);
                ASSERT_TRUE((rows_mapper[j] & 0xFFFFFFFF) == i + j);
            }
        }
    }
    ASSERT_OK(iterator.status());
    // should eof
    std::vector<uint64_t> rows_mapper;
    ASSERT_TRUE(iterator.next_values(1, &rows_mapper).is_end_of_file());
}

} // namespace starrocks