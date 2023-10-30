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

#include "storage/lake/metacache.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeMetacacheTest : public TestBase {
public:
    LakeMetacacheTest() : TestBase(kTestDirectory) {
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
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_metadata_cache";

    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
};

TEST_F(LakeMetacacheTest, test_tablet_metadata_cache) {
    auto* metacache = _tablet_mgr->metacache();

    auto m1 = std::make_shared<TabletMetadataPB>();
    metacache->cache_tablet_metadata("metadata1", m1);

    auto m2 = metacache->lookup_tablet_metadata("metadata1");
    EXPECT_EQ(m1.get(), m2.get());

    auto m3 = metacache->lookup_tablet_metadata("metadata2");
    ASSERT_TRUE(m3 == nullptr);

    auto log = metacache->lookup_txn_log("metadata1");
    ASSERT_TRUE(log == nullptr);
}

TEST_F(LakeMetacacheTest, test_txn_log_cache) {
    auto* metacache = _tablet_mgr->metacache();

    auto log1 = std::make_shared<TxnLogPB>();
    metacache->cache_txn_log("log1", log1);

    auto log2 = metacache->lookup_txn_log("log1");
    EXPECT_EQ(log1.get(), log2.get());

    auto log3 = metacache->lookup_txn_log("log2");
    ASSERT_TRUE(log3 == nullptr);

    auto schema = metacache->lookup_tablet_schema("log1");
    ASSERT_TRUE(schema == nullptr);
}

TEST_F(LakeMetacacheTest, test_tablet_schema_cache) {
    auto* metacache = _tablet_mgr->metacache();

    auto schema1 = std::make_shared<TabletSchema>();
    metacache->cache_tablet_schema("schema1", schema1, 0);

    auto schema2 = metacache->lookup_tablet_schema("schema1");
    EXPECT_EQ(schema1.get(), schema2.get());

    auto schema3 = metacache->lookup_tablet_schema("schema2");
    ASSERT_TRUE(schema3 == nullptr);

    auto delvec = metacache->lookup_delvec("schema1");
    ASSERT_TRUE(delvec == nullptr);
}

TEST_F(LakeMetacacheTest, test_deletion_vector_cache) {
    auto* metacache = _tablet_mgr->metacache();

    auto dv1 = std::make_shared<DelVector>();
    metacache->cache_delvec("dv1", dv1);

    auto dv2 = metacache->lookup_delvec("dv1");
    EXPECT_EQ(dv1.get(), dv2.get());

    auto dv3 = metacache->lookup_delvec("dv2");
    ASSERT_TRUE(dv3 == nullptr);

    auto seg = metacache->lookup_segment("dv1");
    ASSERT_TRUE(seg == nullptr);

    auto meta = metacache->lookup_tablet_metadata("dv1");
    ASSERT_TRUE(meta == nullptr);
}

TEST_F(LakeMetacacheTest, test_segment_cache) {
    auto* metacache = _tablet_mgr->metacache();

    // write data and metadata
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

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));

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
        for (auto& file : writer->files()) {
            segs->Add(std::move(file));
        }

        writer->close();
    }

    _tablet_metadata->set_version(2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));

    // no segment
    auto sz0 = metacache->memory_usage();

    ASSIGN_OR_ABORT(auto reader, tablet.new_reader(2, *_schema));
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    // load segment without indexes
    auto sz1 = metacache->memory_usage();

    auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
    for (int j = 0; j < 2; ++j) {
        read_chunk_ptr->reset();
        ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
        ASSERT_EQ(segment_rows, read_chunk_ptr->num_rows());
    }
    read_chunk_ptr->reset();
    ASSERT_TRUE(reader->get_next(read_chunk_ptr.get()).is_end_of_file());
    reader->close();

    // load segment with indexes, and remove index meta (index meta is larger than index)
    auto sz2 = metacache->memory_usage();
    std::cout << "metadata cache memory usage: " << sz0 << "-" << sz1 << "-" << sz2;
    ASSERT_GT(sz1, sz0);
    ASSERT_LT(sz2, sz1);
}

TEST_F(LakeMetacacheTest, test_update_capacity) {
    auto* metacache = _tablet_mgr->metacache();

    auto old_cap = metacache->capacity();
    metacache->update_capacity(old_cap + 1024 * 1024);
    auto new_cap = metacache->capacity();
    ASSERT_EQ(old_cap + 1024 * 1024, new_cap);
    metacache->update_capacity(new_cap - 1024);
    auto new_cap2 = metacache->capacity();
    ASSERT_EQ(new_cap - 1024, new_cap2);
}

TEST_F(LakeMetacacheTest, test_prune) {
    auto* metacache = _tablet_mgr->metacache();

    auto meta = std::make_shared<TabletMetadataPB>();
    metacache->cache_tablet_metadata("meta1", meta);
    auto meta2 = metacache->lookup_tablet_metadata("meta1");
    ASSERT_TRUE(meta2 != nullptr);

    metacache->prune();

    auto meta3 = metacache->lookup_tablet_metadata("meta1");
    ASSERT_TRUE(meta3 == nullptr);
}

} // namespace starrocks::lake
