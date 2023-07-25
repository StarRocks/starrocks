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

#include <random>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

using VSchema = starrocks::Schema;
using VChunk = starrocks::Chunk;

class TestLocationProvider : public LocationProvider {
public:
    explicit TestLocationProvider(std::string dir) : _dir(dir) {}

    std::string root_location(int64_t tablet_id) const override { return _dir; }

    void set_failed(bool f) { _set_failed = f; }

    std::set<int64_t> _owned_shards;
    std::string _dir;
    bool _set_failed = false;
};

class ConditionUpdateTest : public testing::Test {
public:
    ConditionUpdateTest() {
        _location_provider = std::make_unique<TestLocationProvider>(kTestGroupPath);
        _update_manager = std::make_unique<UpdateManager>(_location_provider.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider.get(), _update_manager.get(), 1024 * 1024);

        _tablet_metadata = std::make_unique<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);
        _location_provider->_owned_shards.insert(_tablet_metadata->id());

        _backup_location_provider = _tablet_manager->TEST_set_location_provider(_location_provider.get());

        _parent_mem_tracker = std::make_unique<MemTracker>(-1);
        _mem_tracker = std::make_unique<MemTracker>(-1, "", _parent_mem_tracker.get());
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(PRIMARY_KEYS);
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
        _referenced_column_ids.push_back(0);
        _referenced_column_ids.push_back(1);
        _partial_tablet_schema = TabletSchema::create(*schema);
        _partial_schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_partial_tablet_schema));

        auto c2 = schema->add_column();
        {
            c2->set_unique_id(next_id());
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            c2->set_aggregation("REPLACE");
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

    void TearDown() override {
        // check primary index cache's ref
        EXPECT_TRUE(_update_manager->TEST_check_primary_index_cache_ref(_tablet_metadata->id(), 1));
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
        ASSERT_OK(tablet.delete_txn_log(_txn_id));
        _txn_id++;
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_location_provider(_backup_location_provider);
        (void)fs::remove_all(kTestGroupPath);
    }

    VChunk generate_data(int64_t chunk_size, int shift, int a, int b) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        std::vector<int> v2(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i + shift * chunk_size;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * a;
        }

        for (int i = 0; i < chunk_size; i++) {
            v2[i] = v0[i] * b;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        c2->append_numbers(v2.data(), v2.size() * sizeof(int));

        return VChunk({c0, c1, c2}, _schema);
    }

    int64_t check(int64_t version, std::function<bool(int c0, int c1, int c2)> check_fn) {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(_tablet_metadata->id()));
        ASSIGN_OR_ABORT(auto reader, tablet.new_reader(version, *_schema));
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkHelper::new_chunk(*_schema, 128);
        int64_t ret = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            ret += chunk->num_rows();
            auto cols = chunk->columns();
            for (int i = 0; i < chunk->num_rows(); i++) {
                EXPECT_TRUE(check_fn(cols[0]->get(i).get_int32(), cols[1]->get(i).get_int32(),
                                     cols[2]->get(i).get_int32()));
            }
            chunk->reset();
        }
        return ret;
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_condition_update";
    constexpr static const int kChunkSize = 12;

    std::unique_ptr<TestLocationProvider> _location_provider;
    LocationProvider* _backup_location_provider;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<TabletSchema> _partial_tablet_schema;
    std::unique_ptr<MemTracker> _parent_mem_tracker;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::shared_ptr<VSchema> _schema;
    std::shared_ptr<VSchema> _partial_schema;
    std::vector<int32_t> _referenced_column_ids;
    int64_t _txn_id = 1231;
    int64_t _partition_id = 4561;
};

TEST_F(ConditionUpdateTest, test_condition_update) {
    auto chunk0 = generate_data(kChunkSize, 0, 3, 4);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    for (int i = 0; i < 3; i++) {
        _txn_id++;
        auto delta_writer = DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr,
                                                _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(_tablet_manager->publish_version(tablet_id, version, version + 1, &_txn_id, 1).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_manager->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    // condition update
    VChunk chunks[4];
    chunks[0] = generate_data(kChunkSize, 0, 4, 5);
    chunks[1] = generate_data(kChunkSize, 0, 3, 6);
    chunks[2] = generate_data(kChunkSize, 0, 4, 6);
    chunks[3] = generate_data(kChunkSize, 0, 5, 6);
    std::pair<int, int> result[4];
    result[0] = std::make_pair(4, 5);
    result[1] = std::make_pair(4, 5);
    result[2] = std::make_pair(4, 6);
    result[3] = std::make_pair(5, 6);
    for (int i = 0; i < 4; i++) {
        _txn_id++;
        auto delta_writer = DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr, "c1",
                                                _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(_tablet_manager->publish_version(tablet_id, version, version + 1, &_txn_id, 1).status());
        version++;
        ASSERT_EQ(kChunkSize, check(version, [&](int c0, int c1, int c2) {
                      return (c0 * result[i].first == c1) && (c0 * result[i].second == c2);
                  }));
        ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_manager->get_tablet_metadata(tablet_id, version));
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 4 + i);
    }
}

TEST_F(ConditionUpdateTest, test_condition_update_multi_segment) {
    auto chunk0 = generate_data(kChunkSize, 0, 3, 4);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    for (int i = 0; i < 3; i++) {
        _txn_id++;
        auto delta_writer = DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr,
                                                _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(_tablet_manager->publish_version(tablet_id, version, version + 1, &_txn_id, 1).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_manager->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    // condition update
    auto chunk1 = generate_data(kChunkSize, 0, 4, 5);
    auto chunk2 = generate_data(kChunkSize, 0, 3, 6);
    const int64_t old_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    for (int i = 0; i < 2; i++) {
        _txn_id++;
        auto delta_writer = DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr, "c1",
                                                _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(i == 0 ? chunk1 : chunk2, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(_tablet_manager->publish_version(tablet_id, version, version + 1, &_txn_id, 1).status());
        version++;
    }
    config::write_buffer_size = old_size;
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 4 == c1) && (c0 * 5 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_manager->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 5);
}

TEST_F(ConditionUpdateTest, test_condition_update_in_memtable) {
    // condition update
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }
    VChunk chunks[2];
    chunks[0] = generate_data(kChunkSize, 0, 4, 5);
    chunks[1] = generate_data(kChunkSize, 0, 3, 6);
    std::pair<int, int> result = std::make_pair(4, 5);

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    _txn_id++;
    auto delta_writer = DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr, "c1",
                                            _mem_tracker.get());
    ASSERT_OK(delta_writer->open());
    // finish condition merge in one memtable
    ASSERT_OK(delta_writer->write(chunks[0], indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->write(chunks[1], indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->finish());
    delta_writer->close();
    // Publish version
    ASSERT_OK(_tablet_manager->publish_version(tablet_id, version, version + 1, &_txn_id, 1).status());
    version++;
    ASSERT_EQ(kChunkSize, check(version, [&](int c0, int c1, int c2) {
                  return (c0 * result.first == c1) && (c0 * result.second == c2);
              }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_manager->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
}

} // namespace starrocks::lake
