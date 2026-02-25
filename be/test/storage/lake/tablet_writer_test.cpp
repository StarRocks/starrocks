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

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/starlet_location_provider.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "test_util.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeTabletWriterTest : public TestBase, testing::WithParamInterface<KeysType> {
public:
    LakeTabletWriterTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_simple_tablet_metadata(GetParam());
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_tablet_writer";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
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

    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
    Chunk chunk1({std::move(c2), std::move(c3)}, _schema);

    const int segment_rows = chunk0.num_rows() + chunk1.num_rows();

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
    ASSERT_OK(writer->open());

    // segment #1
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->write(chunk1));
    ASSERT_OK(writer->finish());

    // segment #2
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->write(chunk1));
    ASSERT_OK(writer->finish());

    auto files = writer->segments();
    ASSERT_EQ(2, files.size());
    ASSERT_NE(files[0].path, files[1].path);
    ASSERT_GT(files[0].size.value(), 0);
    ASSERT_GT(files[1].size.value(), 0);
    ASSERT_EQ(2 * segment_rows, writer->num_rows());
    ASSERT_GT(writer->data_size(), 0);
    ASSERT_EQ(files[1].size.value() + files[1].size.value(), writer->data_size());

    writer->close();

    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    ASSIGN_OR_ABORT(auto seg0,
                    Segment::open(fs, FileInfo{_tablet_mgr->segment_location(_tablet_metadata->id(), files[0].path)}, 0,
                                  _tablet_schema));
    ASSIGN_OR_ABORT(auto seg1,
                    Segment::open(fs, FileInfo{_tablet_mgr->segment_location(_tablet_metadata->id(), files[1].path)}, 1,
                                  _tablet_schema));

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

TEST_P(LakeTabletWriterTest, test_vertical_write_success) {
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

    auto schema0 = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema, {0}));
    auto schema1 = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema, {1}));

    Chunk c0_chunk({std::move(c0)}, schema0);
    Chunk c1_chunk({std::move(c1)}, schema1);
    Chunk c2_chunk({std::move(c2)}, schema0);
    Chunk c3_chunk({std::move(c3)}, schema1);

    const int segment_rows = c0_chunk.num_rows() + c2_chunk.num_rows();

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kVertical, next_id(), segment_rows + 1));

    // generate 2 segments automatically
    ASSERT_OK(writer->open());
    ASSERT_OK(writer->write_columns(c0_chunk, {0}, true));
    ASSERT_OK(writer->write_columns(c2_chunk, {0}, true));
    ASSERT_OK(writer->write_columns(c0_chunk, {0}, true));
    ASSERT_OK(writer->write_columns(c2_chunk, {0}, true));
    ASSERT_OK(writer->flush_columns());
    ASSERT_OK(writer->write_columns(c1_chunk, {1}, false));
    ASSERT_OK(writer->write_columns(c3_chunk, {1}, false));
    ASSERT_OK(writer->write_columns(c1_chunk, {1}, false));
    ASSERT_OK(writer->write_columns(c3_chunk, {1}, false));
    ASSERT_OK(writer->flush_columns());
    ASSERT_OK(writer->finish());

    auto files = writer->segments();
    ASSERT_EQ(2, files.size());
    ASSERT_NE(files[0].path, files[1].path);
    ASSERT_GT(files[0].size.value(), 0);
    ASSERT_GT(files[1].size.value(), 0);
    ASSERT_EQ(files[1].size.value() + files[1].size.value(), writer->data_size());
    ASSERT_EQ(2 * segment_rows, writer->num_rows());
    ASSERT_GT(writer->data_size(), 0);

    writer->close();

    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    ASSIGN_OR_ABORT(auto seg0,
                    Segment::open(fs, FileInfo{_tablet_mgr->segment_location(_tablet_metadata->id(), files[0].path)}, 0,
                                  _tablet_schema));
    ASSIGN_OR_ABORT(auto seg1,
                    Segment::open(fs, FileInfo{_tablet_mgr->segment_location(_tablet_metadata->id(), files[1].path)}, 1,
                                  _tablet_schema));

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
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));

    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
    ASSERT_OK(writer->open());
    ASSERT_OK(fs::remove_all(kTestDirectory));
    ASSERT_ERROR(writer->write(chunk0));

    writer->close();
}

TEST_P(LakeTabletWriterTest, test_close_without_finish) {
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));

    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
    ASSERT_OK(writer->open());

    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->flush());

    ASSERT_EQ(1, writer->segments().size());
    auto seg_path = _tablet_mgr->segment_location(_tablet_metadata->id(), writer->segments()[0].path);
    ASSERT_OK(fs->path_exists(seg_path));

    // `close()` directly without calling `finish()`
    writer->close();

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();

    // segment file should be deleted
    ASSERT_TRUE(fs->path_exists(seg_path).is_not_found());
}

TEST_P(LakeTabletWriterTest, test_vertical_write_close_without_finish) {
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));

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

    auto schema0 = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema, {0}));
    auto schema1 = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema, {1}));

    Chunk c0_chunk({std::move(c0)}, schema0);
    Chunk c1_chunk({std::move(c1)}, schema1);
    Chunk c2_chunk({std::move(c2)}, schema0);
    Chunk c3_chunk({std::move(c3)}, schema1);

    const int segment_rows = c0_chunk.num_rows() + c2_chunk.num_rows();

    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kVertical, next_id(), segment_rows + 1));

    ASSERT_OK(writer->open());
    ASSERT_OK(writer->write_columns(c0_chunk, {0}, true));
    ASSERT_OK(writer->write_columns(c2_chunk, {0}, true));
    ASSERT_OK(writer->write_columns(c0_chunk, {0}, true));
    ASSERT_OK(writer->write_columns(c2_chunk, {0}, true));
    ASSERT_OK(writer->flush_columns());
    ASSERT_OK(writer->write_columns(c1_chunk, {1}, false));
    ASSERT_OK(writer->write_columns(c3_chunk, {1}, false));
    ASSERT_OK(writer->write_columns(c1_chunk, {1}, false));
    ASSERT_OK(writer->write_columns(c3_chunk, {1}, false));
    ASSERT_OK(writer->flush_columns());

    const auto& files = writer->segments();
    ASSERT_EQ(0, files.size());

    // `close()` directly without calling `finish()`
    writer->close();
}

#ifdef USE_STAROS

TEST_P(LakeTabletWriterTest, test_write_sdk) {
    auto provider = std::make_shared<starrocks::lake::StarletLocationProvider>();
    auto location = provider->root_location(12345);

    Tablet tablet(_tablet_mgr.get(), next_id(), provider, _tablet_metadata);
    auto meta_location = tablet.metadata_location(0);
    auto txn_log_location = tablet.txn_log_location(0);
    auto txn_vlog_location = tablet.txn_vlog_location(0);
    auto test_segment_location = tablet.segment_location("test_segment");
    auto root_location = tablet.root_location();
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kVertical, next_id(), 1));
    writer->close();
}

#endif // USE_STAROS

// Helper class for testing check_global_dict by exposing SegmentWriter interface
class TestSegmentWriterWrapper : public SegmentWriter {
public:
    TestSegmentWriterWrapper(std::unique_ptr<WritableFile> wfile, uint32_t segment_id, TabletSchemaCSPtr tablet_schema,
                             SegmentWriterOptions opts)
            : SegmentWriter(std::move(wfile), segment_id, std::move(tablet_schema), opts) {}

    // Expose method to set global_dict_columns_valid_info for testing
    void set_global_dict_info(const std::string& column_name, bool is_valid) {
        _global_dict_columns_valid_info[column_name] = is_valid;
    }

    // Access the protected member through public method
    DictColumnsValidMap& get_global_dict_info() { return _global_dict_columns_valid_info; }
};

TEST_P(LakeTabletWriterTest, test_check_global_dict_all_valid) {
    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
    ASSERT_OK(writer->open());

    // Create a segment writer wrapper with all columns marked as valid
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    std::string segment_path = _tablet_mgr->segment_location(_tablet_metadata->id(), "test_segment");
    ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(segment_path));

    SegmentWriterOptions opts;
    auto seg_writer = std::make_unique<TestSegmentWriterWrapper>(std::move(wfile), 0, _tablet_schema, opts);
    seg_writer->set_global_dict_info("col1", true);
    seg_writer->set_global_dict_info("col2", true);

    // Call check_global_dict
    writer->check_global_dict(seg_writer.get());

    // Verify that all columns are marked as valid
    const auto& dict_info = writer->global_dict_columns_valid_info();
    ASSERT_EQ(2, dict_info.size());
    ASSERT_TRUE(dict_info.at("col1"));
    ASSERT_TRUE(dict_info.at("col2"));

    writer->close();
}

TEST_P(LakeTabletWriterTest, test_check_global_dict_some_invalid) {
    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
    ASSERT_OK(writer->open());

    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    SegmentWriterOptions opts;

    // First segment: all valid
    {
        std::string segment_path = _tablet_mgr->segment_location(_tablet_metadata->id(), "test_segment1");
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(segment_path));
        auto seg_writer1 = std::make_unique<TestSegmentWriterWrapper>(std::move(wfile), 0, _tablet_schema, opts);
        seg_writer1->set_global_dict_info("col1", true);
        seg_writer1->set_global_dict_info("col2", true);
        writer->check_global_dict(seg_writer1.get());
    }

    // Second segment: col1 becomes invalid
    {
        std::string segment_path = _tablet_mgr->segment_location(_tablet_metadata->id(), "test_segment2");
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(segment_path));
        auto seg_writer2 = std::make_unique<TestSegmentWriterWrapper>(std::move(wfile), 1, _tablet_schema, opts);
        seg_writer2->set_global_dict_info("col1", false);
        seg_writer2->set_global_dict_info("col2", true);
        writer->check_global_dict(seg_writer2.get());
    }

    // Verify that col1 is invalid, col2 is still valid
    const auto& dict_info = writer->global_dict_columns_valid_info();
    ASSERT_EQ(2, dict_info.size());
    ASSERT_FALSE(dict_info.at("col1")); // Should be false due to second segment
    ASSERT_TRUE(dict_info.at("col2"));

    writer->close();
}

TEST_P(LakeTabletWriterTest, test_check_global_dict_once_invalid_always_invalid) {
    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
    ASSERT_OK(writer->open());

    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    SegmentWriterOptions opts;

    // First segment: col1 is invalid
    {
        std::string segment_path = _tablet_mgr->segment_location(_tablet_metadata->id(), "test_segment3");
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(segment_path));
        auto seg_writer1 = std::make_unique<TestSegmentWriterWrapper>(std::move(wfile), 0, _tablet_schema, opts);
        seg_writer1->set_global_dict_info("col1", false);
        writer->check_global_dict(seg_writer1.get());
    }

    // Second segment: col1 becomes valid again
    {
        std::string segment_path = _tablet_mgr->segment_location(_tablet_metadata->id(), "test_segment4");
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(segment_path));
        auto seg_writer2 = std::make_unique<TestSegmentWriterWrapper>(std::move(wfile), 1, _tablet_schema, opts);
        seg_writer2->set_global_dict_info("col1", true);
        writer->check_global_dict(seg_writer2.get());
    }

    // Verify that col1 remains invalid (once invalid, always invalid)
    const auto& dict_info = writer->global_dict_columns_valid_info();
    ASSERT_EQ(1, dict_info.size());
    ASSERT_FALSE(dict_info.at("col1"));

    writer->close();
}

TEST_P(LakeTabletWriterTest, test_check_global_dict_new_columns_in_later_segments) {
    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
    ASSERT_OK(writer->open());

    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    SegmentWriterOptions opts;

    // First segment: only col1
    {
        std::string segment_path = _tablet_mgr->segment_location(_tablet_metadata->id(), "test_segment5");
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(segment_path));
        auto seg_writer1 = std::make_unique<TestSegmentWriterWrapper>(std::move(wfile), 0, _tablet_schema, opts);
        seg_writer1->set_global_dict_info("col1", true);
        writer->check_global_dict(seg_writer1.get());
    }

    // Second segment: col1 and col2
    {
        std::string segment_path = _tablet_mgr->segment_location(_tablet_metadata->id(), "test_segment6");
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(segment_path));
        auto seg_writer2 = std::make_unique<TestSegmentWriterWrapper>(std::move(wfile), 1, _tablet_schema, opts);
        seg_writer2->set_global_dict_info("col1", true);
        seg_writer2->set_global_dict_info("col2", true);
        writer->check_global_dict(seg_writer2.get());
    }

    // Verify both columns are valid
    const auto& dict_info = writer->global_dict_columns_valid_info();
    ASSERT_EQ(2, dict_info.size());
    ASSERT_TRUE(dict_info.at("col1"));
    ASSERT_TRUE(dict_info.at("col2"));

    writer->close();
}

TEST_P(LakeTabletWriterTest, test_clone_writer) {
    // Test cloning a writer for parallel execution
    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
    ASSERT_OK(writer->open());

    // Clone the writer
    ASSIGN_OR_ABORT(auto cloned_writer, writer->clone());
    ASSERT_NE(cloned_writer, nullptr);

    // Write data to the original writer
    std::vector<int> k0{1, 2, 3, 4, 5};
    std::vector<int> v0{2, 4, 6, 8, 10};
    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->finish());

    // Write different data to the cloned writer
    std::vector<int> k1{10, 20, 30};
    std::vector<int> v1{11, 22, 33};
    auto c2 = Int32Column::create();
    auto c3 = Int32Column::create();
    c2->append_numbers(k1.data(), k1.size() * sizeof(int));
    c3->append_numbers(v1.data(), v1.size() * sizeof(int));
    Chunk chunk1({std::move(c2), std::move(c3)}, _schema);
    ASSERT_OK(cloned_writer->write(chunk1));
    ASSERT_OK(cloned_writer->finish());

    // Verify both writers have their own files
    ASSERT_EQ(1, writer->segments().size());
    ASSERT_EQ(1, cloned_writer->segments().size());
    ASSERT_NE(writer->segments()[0].path, cloned_writer->segments()[0].path);

    // Verify row counts
    ASSERT_EQ(k0.size(), writer->num_rows());
    ASSERT_EQ(k1.size(), cloned_writer->num_rows());

    writer->close();
    cloned_writer->close();
}

TEST_P(LakeTabletWriterTest, test_merge_other_writers) {
    // Test merging multiple writers into one
    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);

    // Create main writer
    ASSIGN_OR_ABORT(auto main_writer, tablet.new_writer(kHorizontal, next_id()));
    ASSERT_OK(main_writer->open());

    // Write data to main writer
    std::vector<int> k0{1, 2, 3};
    std::vector<int> v0{10, 20, 30};
    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
    ASSERT_OK(main_writer->write(chunk0));
    ASSERT_OK(main_writer->finish());

    size_t main_initial_rows = main_writer->num_rows();
    size_t main_initial_data_size = main_writer->data_size();

    // Create and write to other writers
    std::vector<std::unique_ptr<TabletWriter>> other_writers;
    for (int i = 0; i < 3; i++) {
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
        ASSERT_OK(writer->open());

        std::vector<int> k{100 + i, 200 + i};
        std::vector<int> v{1000 + i, 2000 + i};
        auto col0 = Int32Column::create();
        auto col1 = Int32Column::create();
        col0->append_numbers(k.data(), k.size() * sizeof(int));
        col1->append_numbers(v.data(), v.size() * sizeof(int));
        Chunk chunk({std::move(col0), std::move(col1)}, _schema);

        ASSERT_OK(writer->write(chunk));
        ASSERT_OK(writer->finish());

        other_writers.push_back(std::move(writer));
    }

    // Calculate expected totals
    size_t expected_rows = main_initial_rows;
    size_t expected_data_size = main_initial_data_size;
    for (const auto& writer : other_writers) {
        expected_rows += writer->num_rows();
        expected_data_size += writer->data_size();
    }

    // Merge other writers into main writer
    ASSERT_OK(main_writer->merge_other_writers(other_writers));

    // Verify merged results
    ASSERT_EQ(expected_rows, main_writer->num_rows());
    ASSERT_EQ(expected_data_size, main_writer->data_size());

    // Verify file count (1 from main + 3 from other writers)
    ASSERT_EQ(4, main_writer->segments().size());

    main_writer->close();
    for (auto& writer : other_writers) {
        writer->close();
    }
}

TEST_P(LakeTabletWriterTest, test_merge_writers_with_global_dict) {
    // Test that merge_other_writers correctly handles global dict info
    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);

    // Create main writer
    ASSIGN_OR_ABORT(auto main_writer, tablet.new_writer(kHorizontal, next_id()));
    ASSERT_OK(main_writer->open());

    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestDirectory));
    SegmentWriterOptions opts;

    // Set up main writer's global dict info
    {
        std::string segment_path = _tablet_mgr->segment_location(_tablet_metadata->id(), "main_segment");
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(segment_path));
        auto seg_writer = std::make_unique<TestSegmentWriterWrapper>(std::move(wfile), 0, _tablet_schema, opts);
        seg_writer->set_global_dict_info("col1", true);
        seg_writer->set_global_dict_info("col2", true);
        main_writer->check_global_dict(seg_writer.get());
    }

    // Create other writers with different global dict info
    std::vector<std::unique_ptr<TabletWriter>> other_writers;

    // Writer 1: col1 valid, col2 valid
    {
        ASSIGN_OR_ABORT(auto writer1, tablet.new_writer(kHorizontal, next_id()));
        ASSERT_OK(writer1->open());
        std::string segment_path = _tablet_mgr->segment_location(_tablet_metadata->id(), "other_segment1");
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(segment_path));
        auto seg_writer = std::make_unique<TestSegmentWriterWrapper>(std::move(wfile), 0, _tablet_schema, opts);
        seg_writer->set_global_dict_info("col1", true);
        seg_writer->set_global_dict_info("col2", true);
        writer1->check_global_dict(seg_writer.get());
        other_writers.push_back(std::move(writer1));
    }

    // Writer 2: col1 invalid, col3 valid (new column)
    {
        ASSIGN_OR_ABORT(auto writer2, tablet.new_writer(kHorizontal, next_id()));
        ASSERT_OK(writer2->open());
        std::string segment_path = _tablet_mgr->segment_location(_tablet_metadata->id(), "other_segment2");
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(segment_path));
        auto seg_writer = std::make_unique<TestSegmentWriterWrapper>(std::move(wfile), 0, _tablet_schema, opts);
        seg_writer->set_global_dict_info("col1", false);
        seg_writer->set_global_dict_info("col3", true);
        writer2->check_global_dict(seg_writer.get());
        other_writers.push_back(std::move(writer2));
    }

    // Merge other writers
    ASSERT_OK(main_writer->merge_other_writers(other_writers));

    // Verify merged global dict info
    const auto& dict_info = main_writer->global_dict_columns_valid_info();
    ASSERT_EQ(3, dict_info.size());
    ASSERT_FALSE(dict_info.at("col1")); // Should be false due to writer2
    ASSERT_TRUE(dict_info.at("col2"));  // Still valid
    ASSERT_TRUE(dict_info.at("col3"));  // New column from writer2

    main_writer->close();
    for (auto& writer : other_writers) {
        writer->close();
    }
}

TEST_P(LakeTabletWriterTest, test_merge_empty_writers) {
    // Test merging when other_writers is empty
    VersionedTablet tablet(_tablet_mgr.get(), _tablet_metadata);
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
    ASSERT_OK(writer->open());

    std::vector<int> k0{1, 2, 3};
    std::vector<int> v0{10, 20, 30};
    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));
    Chunk chunk0({std::move(c0), std::move(c1)}, _schema);
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->finish());

    size_t original_rows = writer->num_rows();
    size_t original_data_size = writer->data_size();

    // Merge with empty vector
    std::vector<std::unique_ptr<TabletWriter>> empty_writers;
    ASSERT_OK(writer->merge_other_writers(empty_writers));

    // Verify nothing changed
    ASSERT_EQ(original_rows, writer->num_rows());
    ASSERT_EQ(original_data_size, writer->data_size());

    writer->close();
}

INSTANTIATE_TEST_SUITE_P(LakeTabletWriterTest, LakeTabletWriterTest,
                         ::testing::Values(DUP_KEYS, AGG_KEYS, UNIQUE_KEYS, PRIMARY_KEYS));

} // namespace starrocks::lake
