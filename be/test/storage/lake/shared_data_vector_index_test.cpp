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

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/array_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config_vector_index_fwd.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/vector/vector_index_writer.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/test_util.h"

namespace starrocks::lake {

// Unit tests for vector index support on shared-data write path.
// Shared-data mode uses LocationProvider + FileSystem to write segments and vector index files
// to remote storage (S3/HDFS). These tests verify VectorIndexWriter and path generation work
// correctly with the path layout used by LocationProvider::segment_location().
class SharedDataVectorIndexTest : public ::testing::Test {
public:
    SharedDataVectorIndexTest() = default;

protected:
    void SetUp() override {
        _test_dir = "shared_data_vector_index_test_" + std::to_string(GetCurrentTimeMicros());
        CHECK_OK(fs::remove_all(_test_dir));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kSegmentDirectoryName)));
        ASSIGN_OR_ABORT(_fs, FileSystemFactory::CreateSharedFromString(_test_dir));
        _location_provider = std::make_shared<FixedLocationProvider>(_test_dir);
    }

    void TearDown() override { (void)fs::remove_all(_test_dir); }

    std::shared_ptr<TabletIndex> create_tablet_index(int64_t index_id = 0, int32_t col_unique_id = 1) {
        std::shared_ptr<TabletIndex> tablet_index = std::make_shared<TabletIndex>();
        TabletIndexPB index_pb;
        index_pb.set_index_id(index_id);
        index_pb.set_index_name("vector_index");
        index_pb.set_index_type(IndexType::VECTOR);
        index_pb.add_col_unique_id(col_unique_id);
        tablet_index->init_from_pb(index_pb);
        tablet_index->add_common_properties("index_type", "hnsw");
        tablet_index->add_common_properties("dim", "3");
        tablet_index->add_common_properties("is_vector_normed", "false");
        tablet_index->add_common_properties("metric_type", "l2_distance");
        tablet_index->add_index_properties("efconstruction", "40");
        tablet_index->add_index_properties("M", "16");
        return tablet_index;
    }

    ColumnPtr create_array_column_with_vectors(size_t num_rows) {
        auto element = FixedLengthColumn<float>::create();
        auto null_column = NullColumn::create();
        auto offsets = UInt32Column::create();
        offsets->append(0);

        for (size_t i = 0; i < num_rows; ++i) {
            element->append(static_cast<float>(i) + 1.1f);
            element->append(static_cast<float>(i) + 2.2f);
            element->append(static_cast<float>(i) + 3.3f);
            null_column->append(0);
            null_column->append(0);
            null_column->append(0);
            offsets->append((i + 1) * 3);
        }

        auto nullable_column = NullableColumn::create(std::move(element), std::move(null_column));
        return ArrayColumn::create(std::move(nullable_column), std::move(offsets));
    }

    std::string _test_dir;
    std::shared_ptr<FileSystem> _fs;
    std::shared_ptr<LocationProvider> _location_provider;
};

// Test VectorIndexWriter writing to a path that follows shared-data layout.
// The path is generated using LocationProvider::segment_location(tablet_id, vi_filename),
// which is the same path format used by fill_vector_index_file_paths in general_tablet_writer.
TEST_F(SharedDataVectorIndexTest, test_vector_index_write_shared_data_path) {
    // Set threshold low enough so that 5 rows triggers index build
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 1);

    const int64_t tablet_id = 12345;
    const int64_t index_id = 0;
    const std::string segment_name = "0000000000000001_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.dat";

    std::string vi_filename = gen_vector_index_filename(segment_name, index_id);
    std::string vector_index_path = _location_provider->segment_location(tablet_id, vi_filename);

    auto tablet_index = create_tablet_index(index_id);
    std::unique_ptr<VectorIndexWriter> vector_index_writer;
    VectorIndexWriter::create(tablet_index, vector_index_path, true, &vector_index_writer);
    ASSERT_OK(vector_index_writer->init());

    auto array_column = create_array_column_with_vectors(5);
    ASSERT_OK(vector_index_writer->append(*array_column));
    ASSERT_EQ(vector_index_writer->size(), 5);

    uint64_t index_size = 0;
    ASSERT_OK(vector_index_writer->finish(&index_size));
    ASSERT_GT(index_size, 0);

    // Verify the vector index file exists at the shared-data path
    ASSERT_OK(_fs->path_exists(vector_index_path));

#ifndef WITH_TENANN
    // Without tenann, vector index writes empty mark
    ASSIGN_OR_ABORT(auto index_file, _fs->new_random_access_file(vector_index_path));
    ASSIGN_OR_ABORT(auto data, index_file->read_all());
    ASSERT_EQ(data, IndexDescriptor::mark_word);
#endif
}

// Test that gen_vector_index_filename produces correct filename for shared-data segment names.
TEST_F(SharedDataVectorIndexTest, test_vector_index_filename_for_shared_data_segment) {
    std::string segment_name = "0000000000000001_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.dat";
    std::string vi_name = gen_vector_index_filename(segment_name, 0);
    ASSERT_EQ(vi_name, "0000000000000001_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b_0.vi");

    vi_name = gen_vector_index_filename(segment_name, 123);
    ASSERT_EQ(vi_name, "0000000000000001_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b_123.vi");

    // Verify full path from LocationProvider matches expected layout
    const int64_t tablet_id = 1;
    std::string full_path = _location_provider->segment_location(tablet_id, vi_name);
    std::string expected = lake::join_path(lake::join_path(_test_dir, lake::kSegmentDirectoryName), vi_name);
    ASSERT_EQ(full_path, expected);
}

// Test VectorIndexWriter writes an empty-mark file when the build threshold is not met
// (shared-data path). finish() goes through VectorIndexBuilder::flush_empty(), which
// writes IndexDescriptor::mark_word so the reader can distinguish "threshold not met"
// from a missing segment.
TEST_F(SharedDataVectorIndexTest, test_vector_index_empty_mark_shared_data_path) {
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 100);

    const int64_t tablet_id = 999;
    const int64_t index_id = 0;
    std::string segment_name = "0000000000000002_abc12345-6789-0def-1234-567890abcdef.dat";
    std::string vi_filename = gen_vector_index_filename(segment_name, index_id);
    std::string vector_index_path = _location_provider->segment_location(tablet_id, vi_filename);

    auto tablet_index = create_tablet_index(index_id);
    tablet_index->add_common_properties("index_type", "ivfpq");

    std::unique_ptr<VectorIndexWriter> vector_index_writer;
    VectorIndexWriter::create(tablet_index, vector_index_path, true, &vector_index_writer);
    ASSERT_OK(vector_index_writer->init());

    auto array_column = create_array_column_with_vectors(3);
    ASSERT_OK(vector_index_writer->append(*array_column));

    uint64_t index_size = 0;
    ASSERT_OK(vector_index_writer->finish(&index_size));

    // Threshold not met -> flush_empty() writes IndexDescriptor::mark_word.
    ASSERT_OK(_fs->path_exists(vector_index_path));
    ASSERT_EQ(index_size, IndexDescriptor::mark_word.size());

    ASSIGN_OR_ABORT(auto index_file, _fs->new_random_access_file(vector_index_path));
    ASSIGN_OR_ABORT(auto data, index_file->read_all());
    ASSERT_EQ(data, IndexDescriptor::mark_word);
}

} // namespace starrocks::lake
