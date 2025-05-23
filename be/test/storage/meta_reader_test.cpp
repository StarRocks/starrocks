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

#include "storage/meta_reader.h"

#include <gtest/gtest.h>

#include <memory>

#include "column/fixed_length_column.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "storage/rowset/segment_writer.h"
#include "testutil/assert.h"

namespace starrocks {

class SegmentMetaCollecterTest : public ::testing::Test {
public:
    SegmentMetaCollecterTest() {
        TabletSchemaPB schema_pb;
        auto col = schema_pb.add_column();
        col->set_name("c0");
        col->set_type("INT");
        col->set_is_key(true);
        col->set_is_nullable(false);
        _tablet_schema = TabletSchema::create(schema_pb);

        _segment_name = "segment_meta_collector_test.dat";
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(_segment_name));
        auto encryption_pair = KeyCache::instance().create_plain_random_encryption_meta_pair().value();
        WritableFileOptions options{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                    .encryption_info = encryption_pair.info};
        ASSIGN_OR_ABORT(auto wf, fs->new_writable_file(options, _segment_name));
        SegmentWriter writer(std::move(wf), 0, _tablet_schema, SegmentWriterOptions());
        EXPECT_OK(writer.init());
        uint64_t file_size, index_size, footer_pos;
        EXPECT_OK(writer.finalize(&file_size, &index_size, &footer_pos));

        FileInfo file_info{.path = _segment_name, .encryption_meta = encryption_pair.encryption_meta};
        ASSIGN_OR_ABORT(_segment, Segment::open(fs, file_info, 0, _tablet_schema));
    }

    ~SegmentMetaCollecterTest() { fs::delete_file(_segment_name); }

protected:
    std::string _segment_name;
    TabletSchemaCSPtr _tablet_schema;
    SegmentSharedPtr _segment;
    std::string _segment_encryption_meta;
};

TEST_F(SegmentMetaCollecterTest, test_init) {
    SegmentMetaCollecter collecter(_segment);
    EXPECT_FALSE(collecter.init(nullptr).ok());

    SegmentMetaCollecterParams params;
    EXPECT_FALSE(collecter.init(&params).ok());

    params.fields.emplace_back("rows");
    EXPECT_FALSE(collecter.init(&params).ok());

    params.field_type.emplace_back(LogicalType::TYPE_INT);
    EXPECT_FALSE(collecter.init(&params).ok());

    params.cids.emplace_back(0);
    EXPECT_FALSE(collecter.init(&params).ok());

    params.read_page.emplace_back(false);
    EXPECT_FALSE(collecter.init(&params).ok());

    params.tablet_schema = _tablet_schema;
    EXPECT_OK(collecter.init(&params));
}

TEST_F(SegmentMetaCollecterTest, test_open_and_collect) {
    SegmentMetaCollecter collecter(_segment);

    // init() has not been called
    EXPECT_FALSE(collecter.open().ok());

    SegmentMetaCollecterParams params;
    params.fields.emplace_back("rows");
    params.field_type.emplace_back(LogicalType::TYPE_INT);
    params.cids.emplace_back(0);
    params.read_page.emplace_back(false);
    params.tablet_schema = _tablet_schema;
    EXPECT_OK(collecter.init(&params));
    EXPECT_OK(collecter.open());

    std::vector<Column*> columns;
    EXPECT_FALSE(collecter.collect(&columns).ok());

    auto col = Int64Column::create();
    columns.emplace_back(col.get());

    EXPECT_OK(collecter.collect(&columns));
    EXPECT_EQ(1, col->size());
    EXPECT_EQ(0, col->get(0).get_int64());
}

} // namespace starrocks
