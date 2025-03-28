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

#ifdef WITH_TENANN
#include <tenann/factory/ann_searcher_factory.h>
#include <tenann/factory/index_factory.h>
#endif

#include "column/column_helper.h"
#include "runtime/mem_pool.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "storage/index/vector/vector_index_writer.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/bitmap_index_writer.h"
#include "testutil/assert.h"

namespace starrocks {

class VectorIndexWriterTest : public testing::Test {
public:
    VectorIndexWriterTest() = default;

protected:
    void SetUp() override {
        srand(GetCurrentTimeMicros());
        CHECK_OK(fs::remove_all(test_vector_index_dir));
        CHECK_OK(fs::create_directories(test_vector_index_dir));
        ASSIGN_OR_ABORT(_fs, FileSystem::CreateSharedFromString(test_vector_index_dir));
    }

    void TearDown() override { fs::remove_all(test_vector_index_dir); }

    std::shared_ptr<FileSystem> _fs;
    const std::string test_vector_index_dir = "vector_tenann_builder_test";
    const std::string vector_index_name = "vector_index.vi";
    const std::string empty_index_name = "empty_index.vi";

    std::shared_ptr<TabletIndex> prepare_tablet_index() {
        std::shared_ptr<TabletIndex> tablet_index = std::make_shared<TabletIndex>();
        TabletIndexPB index_pb;
        index_pb.set_index_id(0);
        index_pb.set_index_name("test_index");
        index_pb.set_index_type(IndexType::VECTOR);
        index_pb.add_col_unique_id(1);
        tablet_index->init_from_pb(index_pb);
        return tablet_index;
    }

    void write_vector_index(const std::string& path, const std::shared_ptr<TabletIndex>& tablet_index) {
        DeferOp op([&] { ASSERT_TRUE(fs::path_exist(path)); });

        std::unique_ptr<VectorIndexWriter> vector_index_writer;
        VectorIndexWriter::create(tablet_index, path, true, &vector_index_writer);
        CHECK_OK(vector_index_writer->init());

        // construct columns
        auto element = FixedLengthColumn<float>::create();
        element->append(1);
        element->append(2);
        element->append(3);
        NullColumnPtr null_column = NullColumn::create(element->size(), 0);
        auto nullable_column = NullableColumn::create(std::move(element), std::move(null_column));
        auto offsets = UInt32Column::create();
        offsets->append(0);
        offsets->append(3);
        for (int i = 0; i < 10; i++) {
            auto e = FixedLengthColumn<float>::create();
            e->append(i + 1.1);
            e->append(i + 2.2);
            e->append(i + 3.3);
            nullable_column->append(*e, 0, e->size());
            offsets->append((i + 2) * 3);
        }

        ArrayColumn::Ptr array_column = ArrayColumn::create(std::move(nullable_column), std::move(offsets));

        CHECK_OK(vector_index_writer->append(*array_column));

        ASSERT_EQ(vector_index_writer->size(), 11);

        uint64_t size = 0;
        CHECK_OK(vector_index_writer->finish(&size));

        ASSERT_GT(size, 0);
    }

    void check_empty(const std::string& index_path) {
        auto res = _fs->new_random_access_file(index_path);
        CHECK_OK(res);
        const auto& index_file = res.value();
        auto data_res = index_file->read_all();
        CHECK_OK(data_res);
        ASSERT_EQ(data_res.value(), IndexDescriptor::mark_word);
    }
};

TEST_F(VectorIndexWriterTest, test_write_vector_index) {
    auto tablet_index = prepare_tablet_index();
    tablet_index->add_common_properties("index_type", "hnsw");
    tablet_index->add_common_properties("dim", "3");
    tablet_index->add_common_properties("is_vector_normed", "false");
    tablet_index->add_common_properties("metric_type", "l2_distance");
    tablet_index->add_index_properties("efconstruction", "40");
    tablet_index->add_index_properties("M", "16");
    tablet_index->add_search_properties("efsearch", "40");

    auto index_path = test_vector_index_dir + "/" + vector_index_name;
    write_vector_index(index_path, tablet_index);

#ifdef WITH_TENANN
    try {
        const auto& empty_meta = std::map<std::string, std::string>{};
        auto status = get_vector_meta(tablet_index, empty_meta);

        CHECK_OK(status);
        auto meta = status.value();

        // read and search index
        tenann::IndexReaderRef index_reader = tenann::IndexFactory::CreateReaderFromMeta(meta);
        auto ann_searcher = tenann::AnnSearcherFactory::CreateSearcherFromMeta(meta);
        ann_searcher->ReadIndex(index_path);
        ASSERT_TRUE(ann_searcher->is_index_loaded());
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
    }
#else
    check_empty(index_path);
#endif
}

TEST_F(VectorIndexWriterTest, testwrite_with_empty_mark) {
    config::config_vector_index_default_build_threshold = 100;
    auto tablet_index = prepare_tablet_index();

    tablet_index->add_common_properties("index_type", "ivfpq");
    tablet_index->add_common_properties("dim", "3");
    tablet_index->add_common_properties("is_vector_normed", "false");
    tablet_index->add_common_properties("metric_type", "l2_distance");

    auto index_path = test_vector_index_dir + "/" + empty_index_name;
    write_vector_index(index_path, tablet_index);

    check_empty(index_path);
}

} // namespace starrocks