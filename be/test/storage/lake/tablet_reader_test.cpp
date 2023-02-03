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

#include "storage/lake/tablet_reader.h"

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
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/lru_cache.h"

namespace starrocks::lake {

using namespace starrocks;

using VSchema = starrocks::Schema;
using VChunk = starrocks::Chunk;

class DuplicateTabletReaderTest : public testing::Test {
public:
    DuplicateTabletReaderTest() {
        _location_provider = std::make_unique<FixedLocationProvider>(kTestGroupPath);
        _update_manager = std::make_unique<UpdateManager>(_location_provider.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider.get(), _update_manager.get(), 0);
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
        schema->set_keys_type(DUP_KEYS);
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
        }

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_tablet_schema));
    }

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(join_path(kTestGroupPath, kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestGroupPath, kTxnLogDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestGroupPath, kSegmentDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { (void)fs::remove_all(kTestGroupPath); }

protected:
    constexpr static const char* const kTestGroupPath = "test_duplicate_lake_tablet_reader";

    std::unique_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<VSchema> _schema;
};

TEST_F(DuplicateTabletReaderTest, test_read_success) {
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

    {
        // write rowset 1 with 2 segments
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer());
        ASSERT_OK(writer->open());

        // write rowset data
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

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        auto* segs = rowset->mutable_segments();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file));
        }

        writer->close();
    }

    // write tablet metadata
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_manager->put_tablet_metadata(*_tablet_metadata));

    // test reader
    ASSIGN_OR_ABORT(auto reader, tablet.new_reader(2, *_schema));
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
    for (int j = 0; j < 2; ++j) {
        read_chunk_ptr->reset();
        ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
        ASSERT_EQ(segment_rows, read_chunk_ptr->num_rows());
        for (int i = 0, sz = k0.size(); i < sz; i++) {
            EXPECT_EQ(k0[i], read_chunk_ptr->get(i)[0].get_int32());
            EXPECT_EQ(v0[i], read_chunk_ptr->get(i)[1].get_int32());
        }
        for (int i = 0, sz = k1.size(); i < sz; i++) {
            EXPECT_EQ(k1[i], read_chunk_ptr->get(k0.size() + i)[0].get_int32());
            EXPECT_EQ(v1[i], read_chunk_ptr->get(k0.size() + i)[1].get_int32());
        }
    }

    read_chunk_ptr->reset();
    ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());

    reader->close();
}

class AggregateTabletReaderTest : public testing::Test {
public:
    AggregateTabletReaderTest() {
        _location_provider = std::make_unique<FixedLocationProvider>(kTestGroupPath);
        _update_manager = std::make_unique<UpdateManager>(_location_provider.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider.get(), _update_manager.get(), 0);
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
        schema->set_keys_type(AGG_KEYS);
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
            c1->set_aggregation("SUM");
        }

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_tablet_schema));
    }

    void SetUp() override {
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(join_path(kTestGroupPath, kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestGroupPath, kTxnLogDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(kTestGroupPath, kSegmentDirectoryName)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { (void)fs::remove_all(kTestGroupPath); }

protected:
    constexpr static const char* const kTestGroupPath = "test_aggregate_lake_tablet_reader";

    std::unique_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<VSchema> _schema;
};

TEST_F(AggregateTabletReaderTest, test_read_success) {
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

    {
        // write rowset 1 with 2 segments
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer());
        ASSERT_OK(writer->open());

        // write rowset data
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

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        auto* segs = rowset->mutable_segments();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file));
        }

        writer->close();
    }

    {
        // write rowset 2 with 1 segment
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer());
        ASSERT_OK(writer->open());

        // write rowset data
        // segment #1
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->write(chunk1));
        ASSERT_OK(writer->finish());

        auto files = writer->files();
        ASSERT_EQ(1, files.size());

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_id(2);
        auto* segs = rowset->mutable_segments();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file));
        }

        writer->close();
    }

    // write tablet metadata
    _tablet_metadata->set_version(3);
    CHECK_OK(_tablet_manager->put_tablet_metadata(*_tablet_metadata));

    // test reader
    ASSIGN_OR_ABORT(auto reader, tablet.new_reader(3, *_schema));
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
    ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
    ASSERT_EQ(segment_rows, read_chunk_ptr->num_rows());
    for (int i = 0, sz = k0.size(); i < sz; i++) {
        EXPECT_EQ(k0[i], read_chunk_ptr->get(i)[0].get_int32());
        EXPECT_EQ(v0[i] * 3, read_chunk_ptr->get(i)[1].get_int32());
    }
    for (int i = 0, sz = k1.size(); i < sz; i++) {
        EXPECT_EQ(k1[i], read_chunk_ptr->get(k0.size() + i)[0].get_int32());
        EXPECT_EQ(v1[i] * 3, read_chunk_ptr->get(k0.size() + i)[1].get_int32());
    }

    read_chunk_ptr->reset();
    ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());

    reader->close();
}

class DuplicateTabletReaderWithDeleteTest : public testing::Test {
public:
    DuplicateTabletReaderWithDeleteTest() {
        _location_provider = std::make_unique<FixedLocationProvider>(kTestGroupPath);
        _update_manager = std::make_unique<UpdateManager>(_location_provider.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider.get(), _update_manager.get(), 0);
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
        schema->set_keys_type(DUP_KEYS);
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
    constexpr static const char* const kTestGroupPath = "test_duplicate_lake_tablet_reader_with_delete";

    std::unique_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<VSchema> _schema;
};

TEST_F(DuplicateTabletReaderWithDeleteTest, test_read_success) {
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

    {
        // write rowset 1 with 2 segments
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer());
        ASSERT_OK(writer->open());

        // write rowset data
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

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        auto* segs = rowset->mutable_segments();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file));
        }

        writer->close();
    }

    {
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(false);
        rowset->set_num_rows(0);
        rowset->set_data_size(0);

        auto* delete_predicate = rowset->mutable_delete_predicate();
        delete_predicate->set_version(-1);
        // delete c0 (21, 22, 30)
        auto* binary_predicate = delete_predicate->add_binary_predicates();
        binary_predicate->set_column_name("c0");
        binary_predicate->set_op("<=");
        binary_predicate->set_value("30");
        auto* in_predicate = delete_predicate->add_in_predicates();
        in_predicate->set_column_name("c1");
        in_predicate->set_is_not_in(false);
        in_predicate->add_values("41");
        in_predicate->add_values("44");
        in_predicate->add_values("0");
        in_predicate->add_values("1");
    }

    // write tablet metadata
    _tablet_metadata->set_version(3);
    CHECK_OK(_tablet_manager->put_tablet_metadata(*_tablet_metadata));

    // test reader
    ASSIGN_OR_ABORT(auto reader, tablet.new_reader(3, *_schema));
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
    for (int j = 0; j < 2; ++j) {
        read_chunk_ptr->reset();
        ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
        ASSERT_EQ(segment_rows - 3, read_chunk_ptr->num_rows());
        int chunk_index = 0;
        int sz0 = k0.size() - 2;
        for (int i = 0; i < sz0; i++) {
            EXPECT_EQ(k0[i], read_chunk_ptr->get(chunk_index)[0].get_int32());
            EXPECT_EQ(v0[i], read_chunk_ptr->get(chunk_index)[1].get_int32());
            ++chunk_index;
        }
        int sz1 = k1.size() - 1;
        for (int i = 0; i < sz1; i++) {
            EXPECT_EQ(k1[i + 1], read_chunk_ptr->get(chunk_index)[0].get_int32());
            EXPECT_EQ(v1[i + 1], read_chunk_ptr->get(chunk_index)[1].get_int32());
            ++chunk_index;
        }
    }

    read_chunk_ptr->reset();
    ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());

    reader->close();
}

} // namespace starrocks::lake
