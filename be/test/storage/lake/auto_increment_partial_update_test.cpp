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

class AutoIncrementPartialUpdateTest : public testing::Test {
public:
    AutoIncrementPartialUpdateTest() {
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
        //  | column | type | KEY | NULL | isAutoIncrement |
        //  +--------+------+-----+------+-----------------+
        //  |   c0   |  INT | YES |  NO  |      FALSE      |
        //  |   c1   |BIGINT| NO  |  NO  |      TRUE       |
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
            c1->set_type("BIGINT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            c1->set_is_auto_increment(true);
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

    VChunk generate_data(std::vector<int64_t>& auto_increment_ids, bool partial) {
        auto chunk_size = auto_increment_ids.size();
        std::vector<int> v0(chunk_size);
        std::vector<int64_t> v1(chunk_size);
        std::vector<int> v2(chunk_size);

        v1.assign(auto_increment_ids.begin(), auto_increment_ids.end());

        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int64Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int64_t));

        if (!partial) {
            for (int i = 0; i < chunk_size; i++) {
                v2[i] = i;
            }
            auto c2 = Int32Column::create();
            c2->append_numbers(v2.data(), v2.size() * sizeof(int));
            return VChunk({c0, c1, c2}, _schema);
        } else {
            return VChunk({c0, c1}, _partial_schema);
        }
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
                EXPECT_TRUE(check_fn(cols[0]->get(i).get_int32(), cols[1]->get(i).get_int64(),
                                     cols[2]->get(i).get_int32()));
            }
            chunk->reset();
        }
        return ret;
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_auto_increment_partial_update";
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
    int64_t _txn_id = 2231;
    int64_t _partition_id = 7561;
};

TEST_F(AutoIncrementPartialUpdateTest, test_write) {
    std::vector<int64_t> auto_increment_ids;
    auto_increment_ids.resize(kChunkSize);
    std::iota(auto_increment_ids.begin(), auto_increment_ids.end(), 1);

    auto chunk0 = generate_data(auto_increment_ids, false);
    auto chunk1 = generate_data(auto_increment_ids, true);
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
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c1 - 1 == c0) && (c1 - 1 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_manager->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    // partial update with normal column and auto increment column
    for (int i = 0; i < 3; i++) {
        _txn_id++;
        auto delta_writer = DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr,
                                                _mem_tracker.get());
        delta_writer->TEST_set_partial_update(_partial_tablet_schema, _referenced_column_ids);
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        delta_writer->TEST_set_miss_auto_increment_column();
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(_tablet_manager->publish_version(tablet_id, version, version + 1, &_txn_id, 1).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c1 - 1 == c0) && (c1 - 1 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_manager->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
}

TEST_F(AutoIncrementPartialUpdateTest, test_resolve_conflict) {
    std::vector<int64_t> auto_increment_ids;
    auto_increment_ids.resize(kChunkSize);
    std::iota(auto_increment_ids.begin(), auto_increment_ids.end(), 1);

    auto chunk0 = generate_data(auto_increment_ids, false);
    auto chunk1 = generate_data(auto_increment_ids, true);
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
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c1 - 1 == c0) && (c1 - 1 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_manager->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    // concurrent partial update
    for (int i = 0; i < 3; i++) {
        _txn_id++;
        auto delta_writer = DeltaWriter::create(_tablet_manager.get(), tablet_id, _txn_id, _partition_id, nullptr,
                                                _mem_tracker.get());
        delta_writer->TEST_set_partial_update(_partial_tablet_schema, _referenced_column_ids);
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        delta_writer->TEST_set_miss_auto_increment_column();
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
    }
    // publish in order
    for (int i = _txn_id - 2; i <= _txn_id; i++) {
        // Publish version
        const int64_t ctxnid = i;
        ASSERT_OK(_tablet_manager->publish_version(tablet_id, version, version + 1, &ctxnid, 1).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c1 - 1 == c0) && (c1 - 1 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_manager->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
}

} // namespace starrocks::lake
