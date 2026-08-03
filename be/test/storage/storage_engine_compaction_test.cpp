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

#include <filesystem>
#include <memory>

#include "common/config.h"
#include "fs/fs_util.h"
#include "gen_cpp/AgentService_types.h"
#include "http/action/compaction_action.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/compaction.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "testutil/assert.h"
#include "util/defer_op.h"

namespace starrocks {

// Exercises StorageEngine::_perform_cumulative_compaction / _perform_base_compaction, the legacy
// (non event-based) compaction entry points. Unlike CumulativeCompactionTest / BaseCompactionTest,
// which construct the Compaction objects directly, this drives the full path through
// TabletManager::find_best_tablet_to_compaction so the engine-owned MemTracker creation is covered.
class StorageEngineCompactionTest : public testing::Test {
public:
    ~StorageEngineCompactionTest() override {
        if (_engine != nullptr) {
            _engine->stop();
            delete _engine;
            _engine = nullptr;
        }
    }

    TabletSharedPtr create_registered_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::DUP_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "k1";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn v1;
        v1.column_name = "v1";
        v1.__set_is_key(false);
        v1.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(v1);

        Status st = StorageEngine::instance()->create_tablet(request);
        if (!st.ok()) {
            return nullptr;
        }
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    RowsetSharedPtr create_visible_rowset(const TabletSharedPtr& tablet, int64_t version) {
        RowsetWriterContext writer_context;
        writer_context.rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = VISIBLE;
        writer_context.tablet_schema = tablet->thread_safe_get_tablet_schema();
        writer_context.version = Version(version, version);
        writer_context.segments_overlap = NONOVERLAPPING;

        std::unique_ptr<RowsetWriter> writer;
        CHECK_OK(RowsetFactory::create_rowset_writer(writer_context, &writer));

        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, 128);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < 128; ++i) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int64_t>(version * 1000 + i)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
        }
        CHECK_OK(writer->flush_chunk(*chunk));
        return *writer->build();
    }

    void append_rowsets(const TabletSharedPtr& tablet, int num_rowsets) {
        int64_t base_version = tablet->max_version().second;
        for (int i = 1; i <= num_rowsets; ++i) {
            RowsetSharedPtr rowset = create_visible_rowset(tablet, base_version + i);
            ASSERT_TRUE(rowset != nullptr);
            ASSERT_OK(tablet->add_rowset(rowset));
        }
    }

    void SetUp() override {
        config::min_cumulative_compaction_num_singleton_deltas = 2;
        config::max_cumulative_compaction_num_singleton_deltas = 5;
        config::max_compaction_concurrency = 1;
        config::enable_event_based_compaction_framework = false;
        Compaction::init(config::max_compaction_concurrency);

        _compaction_mem_tracker = std::make_unique<MemTracker>(-1);

        _default_storage_root_path = config::storage_root_path;
        config::storage_root_path = std::filesystem::current_path().string() + "/storage_engine_compaction_test";
        fs::remove_all(config::storage_root_path);
        ASSERT_TRUE(fs::create_directories(config::storage_root_path).ok());

        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path);

        EngineOptions options;
        options.store_paths = paths;
        options.compaction_mem_tracker = _compaction_mem_tracker.get();
        if (_engine == nullptr) {
            Status s = StorageEngine::open(options, &_engine);
            ASSERT_TRUE(s.ok()) << s.to_string();
        }
    }

    void TearDown() override {
        if (_engine != nullptr) {
            _engine->stop();
            delete _engine;
            _engine = nullptr;
        }
        if (fs::path_exist(config::storage_root_path)) {
            ASSERT_TRUE(fs::remove_all(config::storage_root_path).ok());
        }
        config::storage_root_path = _default_storage_root_path;
    }

protected:
    // _perform_cumulative_compaction / _perform_base_compaction are private on StorageEngine and only this
    // fixture is befriended. friendship is not inherited, so the TEST_F-generated subclasses cannot call them
    // directly; route the calls through these helpers that live on the befriended fixture.
    Status perform_cumulative_compaction(DataDir* data_dir, std::pair<int32_t, int32_t> tablet_shards_range) {
        return _engine->_perform_cumulative_compaction(data_dir, tablet_shards_range);
    }
    Status perform_base_compaction(DataDir* data_dir, std::pair<int32_t, int32_t> tablet_shards_range) {
        return _engine->_perform_base_compaction(data_dir, tablet_shards_range);
    }

    StorageEngine* _engine = nullptr;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
    std::string _default_storage_root_path;
};

TEST_F(StorageEngineCompactionTest, test_perform_cumulative_compaction) {
    TabletSharedPtr tablet = create_registered_tablet(987001, 9001);
    ASSERT_TRUE(tablet != nullptr);
    ASSERT_NE(PRIMARY_KEYS, tablet->keys_type());

    append_rowsets(tablet, 5);
    tablet->calculate_cumulative_point();
    ASSERT_GE(tablet->calc_cumulative_compaction_score(), 2);

    int64_t versions_before = tablet->version_count();
    Status st = perform_cumulative_compaction(_engine->get_stores()[0], {0, config::tablet_map_shard_size});
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_LT(tablet->version_count(), versions_before);
}

TEST_F(StorageEngineCompactionTest, test_perform_base_compaction) {
    TabletSharedPtr tablet = create_registered_tablet(987002, 9002);
    ASSERT_TRUE(tablet != nullptr);
    ASSERT_NE(PRIMARY_KEYS, tablet->keys_type());

    append_rowsets(tablet, 5);
    // Push the cumulative point above every rowset so they all become base-compaction inputs.
    tablet->set_cumulative_layer_point(tablet->max_version().second + 1);
    ASSERT_GE(tablet->calc_base_compaction_score(), 2);

    int64_t versions_before = tablet->version_count();
    Status st = perform_base_compaction(_engine->get_stores()[0], {0, config::tablet_map_shard_size});
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_LT(tablet->version_count(), versions_before);
}

TEST_F(StorageEngineCompactionTest, test_perform_cumulative_compaction_no_suitable_tablet) {
    // No registered tablet: find_best_tablet_to_compaction returns null and the engine reports NotFound.
    Status st = perform_cumulative_compaction(_engine->get_stores()[0], {0, config::tablet_map_shard_size});
    ASSERT_TRUE(st.is_not_found()) << st.to_string();
}

// run_manual_compaction on a non primary-key tablet with the size-tiered strategy disabled drives the
// legacy CumulativeCompaction path, which now receives a Compaction-<tablet_id> labelled child tracker.
TEST_F(StorageEngineCompactionTest, test_run_manual_cumulative_compaction) {
    bool saved = config::enable_size_tiered_compaction_strategy;
    config::enable_size_tiered_compaction_strategy = false;
    DeferOp reset([&] { config::enable_size_tiered_compaction_strategy = saved; });

    TabletSharedPtr tablet = create_registered_tablet(987003, 9003);
    ASSERT_TRUE(tablet != nullptr);
    ASSERT_NE(PRIMARY_KEYS, tablet->keys_type());

    append_rowsets(tablet, 5);
    tablet->calculate_cumulative_point();
    ASSERT_GE(tablet->calc_cumulative_compaction_score(), 2);

    int64_t versions_before = tablet->version_count();
    const std::string compaction_type = to_string(CompactionType::CUMULATIVE_COMPACTION);
    ASSERT_OK(CompactionAction::do_compaction(tablet->tablet_id(), compaction_type, ""));
    ASSERT_LT(tablet->version_count(), versions_before);
}

// Same as above for the legacy BaseCompaction path.
TEST_F(StorageEngineCompactionTest, test_run_manual_base_compaction) {
    bool saved = config::enable_size_tiered_compaction_strategy;
    config::enable_size_tiered_compaction_strategy = false;
    DeferOp reset([&] { config::enable_size_tiered_compaction_strategy = saved; });

    TabletSharedPtr tablet = create_registered_tablet(987004, 9004);
    ASSERT_TRUE(tablet != nullptr);
    ASSERT_NE(PRIMARY_KEYS, tablet->keys_type());

    append_rowsets(tablet, 5);
    // Push the cumulative point above every rowset so they all become base-compaction inputs.
    tablet->set_cumulative_layer_point(tablet->max_version().second + 1);
    ASSERT_GE(tablet->calc_base_compaction_score(), 2);

    int64_t versions_before = tablet->version_count();
    ASSERT_OK(CompactionAction::do_compaction(tablet->tablet_id(), to_string(CompactionType::BASE_COMPACTION), ""));
    ASSERT_LT(tablet->version_count(), versions_before);
}

} // namespace starrocks
