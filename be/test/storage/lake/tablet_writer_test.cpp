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

#include "storage/lake/tablet_writer.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

using namespace starrocks;

using VSchema = starrocks::Schema;
using VChunk = starrocks::Chunk;

class LakeTabletWriterTest : public testing::Test, public testing::WithParamInterface<KeysType> {
public:
    LakeTabletWriterTest() {
        _location_provider = std::make_unique<FixedLocationProvider>(kTestGroupPath);
        _tablet_manager = std::make_unique<TabletManager>(_location_provider.get(), nullptr, 0);
        _tablet_metadata = std::make_unique<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(GetParam());
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            c1->set_aggregation("REPLACE");
        }

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_tablet_schema));
    }

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { (void)fs::remove_all(kTestGroupPath); }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_tablet_writer";

    std::unique_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<VSchema> _schema;
};

TEST_P(LakeTabletWriterTest, test_write_success) {
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    std::vector<int> k1{30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41};
    std::vector<int> v1{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    auto c2 = Int32Column::create();
    auto c3 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    c2->append_numbers(k1.data(), k1.size() * sizeof(int));
    c3->append_numbers(v1.data(), v1.size() * sizeof(int));

    VChunk chunk0({c0, c1}, _schema);
    VChunk chunk1({c2, c3}, _schema);

    const int segment_rows = chunk0.num_rows() + chunk1.num_rows();

    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer());
    ASSERT_OK(writer->open());

    // segment #1
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->write(chunk1));
    ASSERT_OK(writer->finish());

    // segment #2
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->write(chunk1));
    ASSERT_OK(writer->finish());

    auto files = writer->files();
    ASSERT_EQ(2, files.size());
    ASSERT_NE(files[0], files[1]);
    ASSERT_EQ(2 * segment_rows, writer->num_rows());
    ASSERT_GT(writer->data_size(), 0);

    writer->close();

    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestGroupPath));
    ASSIGN_OR_ABORT(auto seg0, Segment::open(fs, _location_provider->segment_location(_tablet_metadata->id(), files[0]),
                                             0, _tablet_schema.get()));
    ASSIGN_OR_ABORT(auto seg1, Segment::open(fs, _location_provider->segment_location(_tablet_metadata->id(), files[1]),
                                             1, _tablet_schema.get()));

    OlapReaderStatistics statistics;
    SegmentReadOptions opts;
    opts.fs = fs;
    opts.tablet_id = _tablet_metadata->id();
    opts.stats = &statistics;
    opts.chunk_size = 1024;

    auto check_segment = [&](const SegmentSharedPtr& segment) {
        ASSIGN_OR_ABORT(auto seg_iter, segment->new_iterator(*_schema, opts));
        auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
        ASSERT_OK(seg_iter->get_next(read_chunk_ptr.get()));
        ASSERT_EQ(segment_rows, read_chunk_ptr->num_rows());
        for (int i = 0, sz = k0.size(); i < sz; i++) {
            EXPECT_EQ(k0[i], read_chunk_ptr->get(i)[0].get_int32());
            EXPECT_EQ(v0[i], read_chunk_ptr->get(i)[1].get_int32());
        }
        for (int i = 0, sz = k1.size(); i < sz; i++) {
            EXPECT_EQ(k1[i], read_chunk_ptr->get(k0.size() + i)[0].get_int32());
            EXPECT_EQ(v1[i], read_chunk_ptr->get(k0.size() + i)[1].get_int32());
        }
        read_chunk_ptr->reset();
        ASSERT_TRUE(seg_iter->get_next(read_chunk_ptr.get()).is_end_of_file());
        seg_iter->close();
    };

    check_segment(seg0);
    check_segment(seg1);
}

TEST_P(LakeTabletWriterTest, test_write_fail) {
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestGroupPath));
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));

    VChunk chunk0({c0, c1}, _schema);

    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer());
    ASSERT_OK(writer->open());
    ASSERT_OK(fs::remove_all(kTestGroupPath));
    ASSERT_ERROR(writer->write(chunk0));
}

TEST_P(LakeTabletWriterTest, test_close_without_finish) {
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestGroupPath));
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));

    VChunk chunk0({c0, c1}, _schema);

    ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer());
    ASSERT_OK(writer->open());

    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->flush());

    ASSERT_EQ(1, writer->files().size());
    auto seg_path = _location_provider->segment_location(_tablet_metadata->id(), writer->files()[0]);
    ASSERT_OK(fs->path_exists(seg_path));

    // `close()` directly without calling `finish()`
    writer->close();

    // segment file should be deleted
    ASSERT_TRUE(fs->path_exists(seg_path).is_not_found());
}

INSTANTIATE_TEST_SUITE_P(LakeTabletWriterTest, LakeTabletWriterTest,
                         ::testing::Values(DUP_KEYS, AGG_KEYS, UNIQUE_KEYS, PRIMARY_KEYS));

} // namespace starrocks::lake
