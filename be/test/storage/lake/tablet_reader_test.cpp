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
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/rowset/common.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeDuplicateTabletReaderTest : public TestBase {
public:
    LakeDuplicateTabletReaderTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_duplicate_lake_tablet_reader";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeDuplicateTabletReaderTest, test_read_success) {
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

    Chunk chunk0({c0, c1}, _schema);
    Chunk chunk1({c2, c3}, _schema);

    const int segment_rows = chunk0.num_rows() + chunk1.num_rows();

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    {
        int64_t txn_id = next_id();
        // write rowset 1 with 2 segments
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
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
        auto* segs_size = rowset->mutable_segment_size();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file.path));
            segs_size->Add(std::move(file.size.value()));
        }

        writer->close();
    }

    // write tablet metadata
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    // test reader
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
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

class LakeAggregateTabletReaderTest : public TestBase {
public:
    LakeAggregateTabletReaderTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(AGG_KEYS);
        _tablet_metadata->mutable_schema()->mutable_column(1)->set_aggregation("SUM");
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_aggregate_lake_tablet_reader";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeAggregateTabletReaderTest, test_read_success) {
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

    Chunk chunk0({c0, c1}, _schema);
    Chunk chunk1({c2, c3}, _schema);

    const int segment_rows = chunk0.num_rows() + chunk1.num_rows();

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);

    {
        // write rowset 1 with 2 segments
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
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
        auto* segs_size = rowset->mutable_segment_size();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file.path));
            segs_size->Add(std::move(file.size.value()));
        }

        writer->close();
    }

    {
        // write rowset 2 with 1 segment
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
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
        auto* segs_size = rowset->mutable_segment_size();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file.path));
            segs_size->Add(std::move(file.size.value()));
        }

        writer->close();
    }

    // write tablet metadata
    _tablet_metadata->set_version(3);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    // test reader
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
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

class LakeDuplicateTabletReaderWithDeleteTest : public TestBase {
public:
    LakeDuplicateTabletReaderWithDeleteTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_duplicate_lake_tablet_reader_with_delete";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeDuplicateTabletReaderWithDeleteTest, test_read_success) {
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

    Chunk chunk0({c0, c1}, _schema);
    Chunk chunk1({c2, c3}, _schema);

    const int segment_rows = chunk0.num_rows() + chunk1.num_rows();

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);

    {
        // write rowset 1 with 2 segments
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
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
        auto* segs_size = rowset->mutable_segment_size();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file.path));
            segs_size->Add(std::move(file.size.value()));
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
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    // test reader
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
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

class LakeDuplicateTabletReaderWithDeleteNotInOneValueTest : public TestBase {
public:
    LakeDuplicateTabletReaderWithDeleteNotInOneValueTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_duplicate_lake_tablet_reader_with_delete_not_in_one";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeDuplicateTabletReaderWithDeleteNotInOneValueTest, test_read_success) {
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));

    Chunk chunk0({c0, c1}, _schema);

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    {
        // write rowset 1 with 1 segments
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        // write rowset data
        ASSERT_OK(writer->write(chunk0));
        ASSERT_OK(writer->finish());

        auto files = writer->files();
        ASSERT_EQ(1, files.size());

        // add rowset metadata
        auto* rowset = _tablet_metadata->add_rowsets();
        rowset->set_overlapped(true);
        rowset->set_id(1);
        auto* segs = rowset->mutable_segments();
        auto* segs_size = rowset->mutable_segment_size();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file.path));
            segs_size->Add(std::move(file.size.value()));
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
        // delete c0 not in (10)
        auto* in_predicate = delete_predicate->add_in_predicates();
        in_predicate->set_column_name("c0");
        in_predicate->set_is_not_in(true);
        in_predicate->add_values("10");
    }

    // write tablet metadata
    _tablet_metadata->set_version(3);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    // test reader
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
    ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
    ASSERT_EQ(1, read_chunk_ptr->num_rows());
    EXPECT_EQ(10, read_chunk_ptr->get(0)[0].get_int32());
    EXPECT_EQ(20, read_chunk_ptr->get(0)[1].get_int32());

    read_chunk_ptr->reset();
    ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());

    reader->close();
}

class LakeTabletReaderSpit : public TestBase {
public:
    LakeTabletReaderSpit() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(DUP_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    TabletReaderParams generate_tablet_reader_params(TScanRange* scan_range) {
        TabletReaderParams params;
        params.splitted_scan_rows = 4;
        params.scan_dop = 4;
        params.plan_node_id = 1;
        params.start_key = std::vector<OlapTuple>();
        params.end_key = std::vector<OlapTuple>();
        params.scan_range = scan_range;
        return params;
    }

protected:
    constexpr static const char* const kTestDirectory = "test_tablet_reader_split";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    std::shared_ptr<TScanRange> _scan_range;
};

TEST_F(LakeTabletReaderSpit, test_reader_split) {
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

    Chunk chunk0({c0, c1}, _schema);
    Chunk chunk1({c2, c3}, _schema);

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);

    {
        // write rowset 1 with 2 segments
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
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
        rowset->set_num_rows(2 * (chunk0.num_rows() + chunk1.num_rows()));
        auto* segs = rowset->mutable_segments();
        auto* segs_size = rowset->mutable_segment_size();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file.path));
            segs_size->Add(std::move(file.size.value()));
        }

        writer->close();
    }

    {
        // write rowset 2 with 1 segment
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
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
        rowset->set_num_rows(chunk0.num_rows() + chunk1.num_rows());
        auto* segs = rowset->mutable_segments();
        auto* segs_size = rowset->mutable_segment_size();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file.path));
            segs_size->Add(std::move(file.size.value()));
        }

        writer->close();
    }

    // write tablet metadata
    _tablet_metadata->set_version(3);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    {
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema, true, true);

        // construct scan_range
        TInternalScanRange internal_scan_range;
        internal_scan_range.__set_tablet_id(_tablet_metadata->id());
        internal_scan_range.__set_version(std::to_string(_tablet_metadata->version()));
        TScanRange scan_range;
        scan_range.__set_internal_scan_range(internal_scan_range);
        auto params = generate_tablet_reader_params(&scan_range);

        ASSERT_OK(reader->prepare());
        ASSERT_OK(reader->open(params));

        std::vector<pipeline::ScanSplitContextPtr> split_tasks;
        reader->get_split_tasks(&split_tasks);
        ASSERT_GT(split_tasks.size(), 0);

        reader->close();
    }

    {
        // test read data
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema, false, false);

        // construct scan_range
        TInternalScanRange internal_scan_range;
        internal_scan_range.__set_tablet_id(_tablet_metadata->id());
        internal_scan_range.__set_version(std::to_string(_tablet_metadata->version()));
        TScanRange scan_range;
        scan_range.__set_internal_scan_range(internal_scan_range);
        auto params = generate_tablet_reader_params(&scan_range);

        // construct rowid_range_option
        auto rowid_range_option = std::make_shared<RowidRangeOption>();
        Rowset rowset(_tablet_mgr.get(), _tablet_metadata, 1, 0 /* compaction_segment_limit */);
        auto segment = rowset.get_segments().back();
        auto sparse_range = std::make_shared<SparseRange<rowid_t>>(1, 21);
        rowid_range_option->add(&rowset, segment.get(), sparse_range, true);
        params.rowid_range_option = rowid_range_option;

        ASSERT_OK(reader->prepare());
        ASSERT_OK(reader->open(params));

        auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
        read_chunk_ptr->reset();
        ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
        ASSERT_EQ(20, read_chunk_ptr->num_rows());

        read_chunk_ptr->reset();
        ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());

        reader->close();
    }
}

class DISABLED_LakeLoadSegmentParallelTest : public TestBase {
public:
    DISABLED_LakeLoadSegmentParallelTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
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
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_load_segment_parallel_test";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(DISABLED_LakeLoadSegmentParallelTest, test_normal) {
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

    Chunk chunk0({c0, c1}, _schema);
    Chunk chunk1({c2, c3}, _schema);

    const int segment_rows = chunk0.num_rows() + chunk1.num_rows();

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    {
        int64_t txn_id = next_id();
        // write rowset 1 with 2 segments
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
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
        auto* segs_size = rowset->mutable_segment_size();
        for (auto& file : writer->files()) {
            segs->Add(std::move(file.path));
            segs_size->Add(std::move(file.size.value()));
        }

        writer->close();
    }

    // write tablet metadata
    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    // test reader
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), _tablet_metadata, *_schema);
    config::enable_load_segment_parallel = true;
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

} // namespace starrocks::lake
