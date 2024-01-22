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

#include <functional>
#include <iostream>
#include <memory>

#include "column/datum_tuple.h"
#include "fs/fs_memory.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/empty_iterator.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset_column_update_state.h"
#include "storage/schema_change_utils.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"
#include "storage/tablet_schema.h"
#include "storage/union_iterator.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"

namespace starrocks {

class RowsetColumnPartialUpdateTest : public ::testing::Test, testing::WithParamInterface<int64_t> {
public:
    void SetUp() override {
        _compaction_mem_tracker = std::make_unique<MemTracker>(-1);
        _update_mem_tracker = std::make_unique<MemTracker>();
        config::primary_key_batch_get_index_memory_limit = GetParam();
    }

    void TearDown() override {
        for (auto& tablet : _tablets) {
            if (tablet != nullptr) {
                StorageEngine::instance()->tablet_manager()->drop_tablet(tablet->tablet_id());
                tablet.reset();
            }
        }
    }

    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys, bool add_v3 = false) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (long key : keys) {
            cols[0]->append_datum(Datum(key));
            cols[1]->append_datum(Datum((int16_t)(key % 100 + 1)));
            cols[2]->append_datum(Datum((int32_t)(key % 1000 + 2)));
            if (add_v3) {
                cols[3]->append_datum(Datum((int32_t)(key % 1000 + 3)));
            }
        }
        if (!keys.empty()) {
            CHECK_OK(writer->flush_chunk(*chunk));
        } else {
            CHECK_OK(writer->flush());
        }
        return *writer->build();
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash, bool add_v3 = false) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k3);

        if (add_v3) {
            TColumn k4;
            k4.column_name = "v3";
            k4.__set_default_value("1");
            k4.__set_is_key(false);
            k4.column_type.type = TPrimitiveType::INT;
            request.tablet_schema.columns.push_back(k4);
        }

        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        auto tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
        _tablets.push_back(tablet);
        return tablet;
    }

    DataDir* get_stores() {
        TCreateTabletReq request;
        return StorageEngine::instance()->get_stores_for_create_tablet(request.storage_medium)[0];
    }

    RowsetSharedPtr create_partial_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                          std::vector<int32_t>& column_indexes, std::function<int16_t(int64_t)> v1_func,
                                          std::function<int32_t(int64_t)> v2_func,
                                          const std::shared_ptr<TabletSchema>& partial_schema, int segment_num,
                                          PartialUpdateMode mode = PartialUpdateMode::COLUMN_UPDATE_MODE) {
        // create partial rowset
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;

        writer_context.tablet_schema = partial_schema;
        writer_context.referenced_column_ids = column_indexes;
        writer_context.full_tablet_schema = tablet->tablet_schema();
        writer_context.is_partial_update = true;
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        writer_context.partial_update_mode = mode;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(partial_schema);

        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (long key : keys) {
            int idx = 0;
            for (int colid : column_indexes) {
                if (colid == 0) {
                    cols[idx]->append_datum(Datum(key));
                } else if (colid == 1) {
                    cols[idx]->append_datum(Datum(v1_func(key)));
                } else {
                    cols[idx]->append_datum(Datum(v2_func(key)));
                }
                idx++;
            }
        }
        for (int i = 0; i < segment_num; i++) {
            CHECK_OK(writer->flush_chunk(*chunk));
        }
        RowsetSharedPtr partial_rowset = *writer->build();
        partial_rowset->set_schema(tablet->tablet_schema());

        return partial_rowset;
    }

protected:
    std::vector<TabletSharedPtr> _tablets;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
    std::unique_ptr<MemTracker> _update_mem_tracker;
};

static ChunkIteratorPtr create_tablet_iterator(TabletReader& reader, Schema& schema) {
    TabletReaderParams params;
    if (!reader.prepare().ok()) {
        LOG(ERROR) << "reader prepare failed";
        return nullptr;
    }
    std::vector<ChunkIteratorPtr> seg_iters;
    if (!reader.get_segment_iterators(params, &seg_iters).ok()) {
        LOG(ERROR) << "reader get segment iterators fail";
        return nullptr;
    }
    if (seg_iters.empty()) {
        return new_empty_iterator(schema, DEFAULT_CHUNK_SIZE);
    }
    return new_union_iterator(seg_iters);
}

static bool check_until_eof(const ChunkIteratorPtr& iter, int64_t check_rows_cnt,
                            std::function<bool(int64_t, int16_t, int32_t)> check_fn) {
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
    int64_t rows_cnt = 0;
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            rows_cnt += chunk->num_rows();
            for (int r = 0; r < chunk->num_rows(); r++) {
                if (!check_fn(chunk->columns()[0]->get(r).get_int64(), chunk->columns()[1]->get(r).get_int16(),
                              chunk->columns()[2]->get(r).get_int32())) {
                    return false;
                }
            }
            chunk->reset();
        } else {
            LOG(WARNING) << "read error: " << st.to_string();
            return false;
        }
    }
    return rows_cnt == check_rows_cnt;
}

static bool check_tablet(const TabletSharedPtr& tablet, int64_t version, int64_t check_rows_cnt,
                         std::function<bool(int64_t, int16_t, int32_t)> check_fn) {
    Schema schema = ChunkHelper::convert_schema(tablet->tablet_schema());
    TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return false;
    }
    return check_until_eof(iter, check_rows_cnt, check_fn);
}

static void commit_rowsets(const TabletSharedPtr& tablet, std::vector<RowsetSharedPtr>& rowsets, int64_t& version) {
    for (int i = 0; i < rowsets.size(); i++) {
        auto st = tablet->rowset_commit(++version, rowsets[i], 10000);
        CHECK(st.ok()) << st.to_string();
        ASSERT_EQ(version, tablet->updates()->max_version());
        ASSERT_EQ(version, tablet->updates()->version_history_count());
    }
}

static void compact(const TabletSharedPtr& tablet, int64_t& version, int64_t expected_num_rowsets,
                    MemTracker* compaction_mem_tracker) {
    const auto& best_tablet =
            StorageEngine::instance()->tablet_manager()->find_best_tablet_to_do_update_compaction(tablet->data_dir());
    ASSERT_EQ(best_tablet->tablet_id(), tablet->tablet_id());
    ASSERT_TRUE(best_tablet->updates()->get_compaction_score() > 0);
    ASSERT_TRUE(best_tablet->updates()->compaction(compaction_mem_tracker).ok());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(best_tablet->updates()->num_rowsets(), expected_num_rowsets);
    ASSERT_EQ(best_tablet->updates()->version_history_count(), version + 1);
    // the time interval is not enough after last compaction
    ASSERT_EQ(best_tablet->updates()->get_compaction_score(), -1);
}

static Status full_clone(const TabletSharedPtr& sourcetablet, int64_t clone_version,
                         const TabletSharedPtr& dest_tablet) {
    auto snapshot_dir = SnapshotManager::instance()->snapshot_full(sourcetablet, clone_version, 3600);
    CHECK(snapshot_dir.ok()) << snapshot_dir.status();

    DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(sourcetablet, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    CHECK(snapshot_meta.ok()) << snapshot_meta.status();

    RETURN_IF_ERROR(SnapshotManager::instance()->assign_new_rowset_id(&(*snapshot_meta), meta_dir,
                                                                      sourcetablet->tablet_schema()));

    std::set<std::string> files;
    auto st = fs::list_dirs_files(meta_dir, nullptr, &files);
    CHECK(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = dest_tablet->schema_hash_path() + "/" + f;
        st = FileSystem::Default()->link_file(src, dst);
        if (st.ok()) {
            LOG(INFO) << "Linked " << src << " to " << dst;
        } else if (st.is_already_exist()) {
            LOG(INFO) << dst << " already exist";
        } else {
            return st;
        }
    }
    // Pretend that source_tablet is a peer replica of dest_tablet
    snapshot_meta->tablet_meta().set_tablet_id(dest_tablet->tablet_id());
    snapshot_meta->tablet_meta().set_schema_hash(dest_tablet->schema_hash());
    for (auto& rm : snapshot_meta->rowset_metas()) {
        rm.set_tablet_id(dest_tablet->tablet_id());
    }

    st = dest_tablet->updates()->load_snapshot(*snapshot_meta);
    dest_tablet->updates()->remove_expired_versions(time(nullptr));
    return st;
}

static Status increment_clone(const TabletSharedPtr& sourcetablet, const std::vector<int64_t>& delta_versions,
                              const TabletSharedPtr& dest_tablet) {
    auto snapshot_dir = SnapshotManager::instance()->snapshot_incremental(sourcetablet, delta_versions, 3600);
    CHECK(snapshot_dir.ok()) << snapshot_dir.status();

    DeferOp defer1([&]() { (void)fs::remove_all(*snapshot_dir); });

    auto meta_dir = SnapshotManager::instance()->get_schema_hash_full_path(sourcetablet, *snapshot_dir);
    auto snapshot_meta = SnapshotManager::instance()->parse_snapshot_meta(meta_dir + "/meta");
    CHECK(snapshot_meta.ok()) << snapshot_meta.status();

    std::set<std::string> files;
    auto st = fs::list_dirs_files(meta_dir, nullptr, &files);
    CHECK(st.ok()) << st;
    files.erase("meta");

    for (const auto& f : files) {
        std::string src = meta_dir + "/" + f;
        std::string dst = dest_tablet->schema_hash_path() + "/" + f;
        st = FileSystem::Default()->link_file(src, dst);
        CHECK(st.ok()) << st;
        LOG(INFO) << "Linked " << src << " to " << dst;
    }
    // Pretend that tablet0 is a peer replica of tablet1
    snapshot_meta->tablet_meta().set_tablet_id(dest_tablet->tablet_id());
    snapshot_meta->tablet_meta().set_schema_hash(dest_tablet->schema_hash());
    for (auto& rm : snapshot_meta->rowset_metas()) {
        rm.set_tablet_id(dest_tablet->tablet_id());
    }

    return dest_tablet->updates()->load_snapshot(*snapshot_meta);
}

static void prepare_tablet(RowsetColumnPartialUpdateTest* self, const TabletSharedPtr& tablet, int64_t& version,
                           int64_t& version_before_partial_update, int N) {
    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    auto v1_func = [](int64_t k1) { return (int16_t)(k1 % 100 + 3); };
    auto v2_func = [](int64_t k1) { return (int32_t)(k1 % 1000 + 4); };
    {
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.reserve(10);
        // write full rowset first
        for (int i = 0; i < 10; i++) {
            rowsets.emplace_back(self->create_rowset(tablet, keys));
        }
        commit_rowsets(tablet, rowsets, version);
        // check data
        ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
            return (int16_t)(k1 % 100 + 1) == v1 && (int32_t)(k1 % 1000 + 2) == v2;
        }));
        // check refcnt
        for (const auto& rs_ptr : rowsets) {
            ASSERT_FALSE(
                    StorageEngine::instance()->update_manager()->TEST_update_state_exist(tablet.get(), rs_ptr.get()));
        }
        ASSERT_TRUE(StorageEngine::instance()->update_manager()->TEST_primary_index_refcnt(tablet->tablet_id(), 1));
        version_before_partial_update = version;
    }

    {
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.reserve(10);
        std::vector<std::shared_ptr<TabletSchema>> partial_schemas;
        // partial update v1 and v2 one by one
        for (int i = 0; i < 10; i++) {
            std::vector<int32_t> column_indexes = {0, (i % 2) + 1};
            partial_schemas.push_back(TabletSchema::create(tablet->tablet_schema(), column_indexes));
            rowsets.emplace_back(
                    self->create_partial_rowset(tablet, keys, column_indexes, v1_func, v2_func, partial_schemas[i], 5));
            ASSERT_EQ(rowsets[i]->num_update_files(), 5);
            // preload rowset update state
            ASSERT_OK(StorageEngine::instance()->update_manager()->on_rowset_finished(tablet.get(), rowsets[i].get()));
        }
        commit_rowsets(tablet, rowsets, version);
        // check data
        ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
            return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
        }));
        // check refcnt
        for (const auto& rs_ptr : rowsets) {
            ASSERT_FALSE(
                    StorageEngine::instance()->update_manager()->TEST_update_state_exist(tablet.get(), rs_ptr.get()));
        }
        ASSERT_TRUE(StorageEngine::instance()->update_manager()->TEST_primary_index_refcnt(tablet->tablet_id(), 1));
    }
}

TEST_P(RowsetColumnPartialUpdateTest, partial_update_and_check) {
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());

    // create full rowsets first
    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(10);
    for (int i = 0; i < 10; i++) {
        rowsets.emplace_back(create_rowset(tablet, keys));
    }
    int64_t version = 1;
    commit_rowsets(tablet, rowsets, version);
    // check data
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 1) == v1 && (int32_t)(k1 % 1000 + 2) == v2;
    }));

    std::vector<int32_t> column_indexes = {0, 1};
    auto v1_func = [](int64_t k1) { return (int16_t)(k1 % 100 + 3); };
    auto v2_func = [](int64_t k1) { return (int32_t)(k1 % 1000 + 4); };
    std::shared_ptr<TabletSchema> partial_schema = TabletSchema::create(tablet->tablet_schema(), column_indexes);
    RowsetSharedPtr partial_rowset =
            create_partial_rowset(tablet, keys, column_indexes, v1_func, v2_func, partial_schema, 1);
    // check data of write column
    RowsetColumnUpdateState state;
    state.load(tablet.get(), partial_rowset.get(), _update_mem_tracker.get());
    const std::vector<ColumnPartialUpdateState>& parital_update_states = state.parital_update_states();
    ASSERT_EQ(parital_update_states.size(), 1);
    ASSERT_EQ(parital_update_states[0].src_rss_rowids.size(), N);
    ASSERT_EQ(parital_update_states[0].rss_rowid_to_update_rowid.size(), N);
    for (int upt_id = 0; upt_id < parital_update_states[0].src_rss_rowids.size(); upt_id++) {
        uint64_t src_rss_rowid = parital_update_states[0].src_rss_rowids[upt_id];
        ASSERT_EQ(parital_update_states[0].rss_rowid_to_update_rowid.find(src_rss_rowid)->second, upt_id);
    }
    // commit partial update
    auto st = tablet->rowset_commit(++version, partial_rowset, 10000);
    ASSERT_TRUE(st.ok()) << st.to_string();
    // check data
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 2) == v2;
    }));
}

TEST_P(RowsetColumnPartialUpdateTest, normal_partial_update_and_check) {
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());

    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    std::vector<int32_t> column_indexes = {0, 1};
    auto v1_func = [](int64_t k1) { return (int16_t)(k1 % 100 + 3); };
    auto v2_func = [](int64_t k1) { return (int32_t)(k1 % 1000 + 4); };
    std::shared_ptr<TabletSchema> partial_schema = TabletSchema::create(tablet->tablet_schema(), column_indexes);
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(20);
    // write full rowset and partial rowset one by one
    for (int i = 0; i < 20; i++) {
        if (i % 2 == 0) {
            rowsets.emplace_back(create_rowset(tablet, keys));
        } else {
            rowsets.emplace_back(
                    create_partial_rowset(tablet, keys, column_indexes, v1_func, v2_func, partial_schema, 1));
        }
    }
    int64_t version = 1;
    commit_rowsets(tablet, rowsets, version);
    // check data
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 2) == v2;
    }));
}

TEST_P(RowsetColumnPartialUpdateTest, partial_update_diff_column_and_check) {
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());

    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    auto v1_func = [](int64_t k1) { return (int16_t)(k1 % 100 + 3); };
    auto v2_func = [](int64_t k1) { return (int32_t)(k1 % 1000 + 4); };
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(20);
    // write full rowset first
    for (int i = 0; i < 10; i++) {
        rowsets.emplace_back(create_rowset(tablet, keys));
    }
    std::vector<std::shared_ptr<TabletSchema>> partial_schemas;
    // partial update v1 and v2 one by one
    for (int i = 0; i < 10; i++) {
        std::vector<int32_t> column_indexes = {0, (i % 2) + 1};
        partial_schemas.push_back(TabletSchema::create(tablet->tablet_schema(), column_indexes));
        rowsets.emplace_back(
                create_partial_rowset(tablet, keys, column_indexes, v1_func, v2_func, partial_schemas[i], 1));
    }
    int64_t version = 1;
    commit_rowsets(tablet, rowsets, version);
    // check data
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
    }));
    // check refcnt
    for (const auto& rs_ptr : rowsets) {
        ASSERT_FALSE(StorageEngine::instance()->update_manager()->TEST_update_state_exist(tablet.get(), rs_ptr.get()));
    }
    ASSERT_TRUE(StorageEngine::instance()->update_manager()->TEST_primary_index_refcnt(tablet->tablet_id(), 1));
}

TEST_P(RowsetColumnPartialUpdateTest, partial_update_multi_segment_and_check) {
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());

    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    auto v1_func = [](int64_t k1) { return (int16_t)(k1 % 100 + 3); };
    auto v2_func = [](int64_t k1) { return (int32_t)(k1 % 1000 + 4); };
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(20);
    // write full rowset first
    for (int i = 0; i < 10; i++) {
        rowsets.emplace_back(create_rowset(tablet, keys));
    }
    std::vector<std::shared_ptr<TabletSchema>> partial_schemas;
    // partial update v1 and v2 one by one
    for (int i = 0; i < 10; i++) {
        std::vector<int32_t> column_indexes = {0, (i % 2) + 1};
        partial_schemas.push_back(TabletSchema::create(tablet->tablet_schema(), column_indexes));
        rowsets.emplace_back(
                create_partial_rowset(tablet, keys, column_indexes, v1_func, v2_func, partial_schemas[i], 5));
        ASSERT_EQ(rowsets.back()->num_update_files(), 5);
    }
    int64_t version = 1;
    commit_rowsets(tablet, rowsets, version);
    // check data
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
    }));
    // check refcnt
    for (const auto& rs_ptr : rowsets) {
        ASSERT_FALSE(StorageEngine::instance()->update_manager()->TEST_update_state_exist(tablet.get(), rs_ptr.get()));
    }
    ASSERT_TRUE(StorageEngine::instance()->update_manager()->TEST_primary_index_refcnt(tablet->tablet_id(), 1));
}

TEST_P(RowsetColumnPartialUpdateTest, partial_update_multi_segment_preload_and_check) {
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());
    int64_t version = 1;
    int64_t version_before_partial_update = 1;
    prepare_tablet(this, tablet, version, version_before_partial_update, N);
}

TEST_P(RowsetColumnPartialUpdateTest, partial_update_compaction_and_check) {
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());
    int64_t version = 1;
    int64_t version_before_partial_update = 1;
    prepare_tablet(this, tablet, version, version_before_partial_update, N);

    {
        // compaction, only merge empty rowsets
        compact(tablet, version, 2, _compaction_mem_tracker.get());
        // check data
        ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
            return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
        }));
    }
}

TEST_P(RowsetColumnPartialUpdateTest, TEST_Pull_clone) {
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());
    int64_t version = 1;
    int64_t version_before_partial_update = 1;
    prepare_tablet(this, tablet, version, version_before_partial_update, N);

    {
        // clone from _tablet to new_tablet
        auto new_tablet = create_tablet(rand(), rand());
        ASSERT_EQ(1, new_tablet->updates()->version_history_count());
        ASSERT_OK(full_clone(tablet, version, new_tablet));
        ASSERT_TRUE(check_tablet(new_tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
            return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
        }));
    }

    {
        // clone from _tablet to new_tablet with version before partial update
        auto new_tablet = create_tablet(rand(), rand());
        ASSERT_EQ(1, new_tablet->updates()->version_history_count());
        ASSERT_OK(full_clone(tablet, version_before_partial_update, new_tablet));
        ASSERT_TRUE(check_tablet(new_tablet, version_before_partial_update, N, [](int64_t k1, int64_t v1, int32_t v2) {
            return (int16_t)(k1 % 100 + 1) == v1 && (int32_t)(k1 % 1000 + 2) == v2;
        }));
    }

    {
        // clone from _tablet to new_tablet with version just after first partial update
        auto new_tablet = create_tablet(rand(), rand());
        ASSERT_EQ(1, new_tablet->updates()->version_history_count());
        ASSERT_OK(full_clone(tablet, version_before_partial_update + 1, new_tablet));
        ASSERT_TRUE(
                check_tablet(new_tablet, version_before_partial_update + 1, N, [](int64_t k1, int64_t v1, int32_t v2) {
                    return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 2) == v2;
                }));
    }
}

TEST_P(RowsetColumnPartialUpdateTest, test_increment_clone) {
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());
    int64_t version = 1;
    int64_t version_before_partial_update = 1;
    prepare_tablet(this, tablet, version, version_before_partial_update, N);

    {
        // 1. full clone with version before partial update
        auto new_tablet = create_tablet(rand(), rand());
        ASSERT_EQ(1, new_tablet->updates()->version_history_count());
        ASSERT_OK(full_clone(tablet, version_before_partial_update, new_tablet));
        ASSERT_TRUE(check_tablet(new_tablet, version_before_partial_update, N, [](int64_t k1, int64_t v1, int32_t v2) {
            return (int16_t)(k1 % 100 + 1) == v1 && (int32_t)(k1 % 1000 + 2) == v2;
        }));
        // 2. increment clone, update v1 = k1 % 100 + 3
        ASSERT_OK(increment_clone(tablet, {version_before_partial_update + 1}, new_tablet));
        ASSERT_TRUE(
                check_tablet(new_tablet, version_before_partial_update + 1, N, [](int64_t k1, int64_t v1, int32_t v2) {
                    return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 2) == v2;
                }));
        // 3. increment clone, update v2 = k1 % 100 + 4
        ASSERT_OK(increment_clone(tablet, {version_before_partial_update + 2}, new_tablet));
        ASSERT_TRUE(
                check_tablet(new_tablet, version_before_partial_update + 2, N, [](int64_t k1, int64_t v1, int32_t v2) {
                    return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
                }));
    }
}

TEST_P(RowsetColumnPartialUpdateTest, test_schema_change) {
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());
    int64_t version = 1;
    int64_t version_before_partial_update = 1;
    prepare_tablet(this, tablet, version, version_before_partial_update, N);

    {
        // create table with add column, test link_from
        auto new_tablet = create_tablet(rand(), rand(), true);
        new_tablet->set_tablet_state(TABLET_NOTREADY);
        auto chunk_changer = std::make_unique<ChunkChanger>(new_tablet->tablet_schema());
        ASSERT_TRUE(new_tablet->updates()
                            ->link_from(tablet.get(), version, chunk_changer.get(), tablet->tablet_schema())
                            .ok());
        // check data
        ASSERT_TRUE(check_tablet(new_tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
            return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
        }));
    }
}

TEST_P(RowsetColumnPartialUpdateTest, TEST_Pull_clone2) {
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());
    int64_t version = 1;
    int64_t version_before_partial_update = 1;
    prepare_tablet(this, tablet, version, version_before_partial_update, N);

    {
        // clone from _tablet to new_tablet
        auto new_tablet = create_tablet(rand(), rand());
        ASSERT_EQ(1, new_tablet->updates()->version_history_count());
        ASSERT_OK(full_clone(tablet, version, new_tablet));
        // delete old tablet
        fs::remove_all(tablet->schema_hash_path());
        ASSERT_TRUE(check_tablet(new_tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
            return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
        }));
    }
}

TEST_P(RowsetColumnPartialUpdateTest, test_dcg_gc) {
    // Only run one parameter here
    if (GetParam() != 104857600) return;
    fs::remove_all(get_stores()->path());
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());
    int64_t version = 1;
    int64_t version_before_partial_update = 1;
    auto meta = tablet->data_dir()->get_meta();
    prepare_tablet(this, tablet, version, version_before_partial_update, N);
    ASSERT_EQ(version_before_partial_update, 11);
    // clear dcg before version 13, expect clear 0 dcg
    StatusOr<size_t> clear_size = StorageEngine::instance()->update_manager()->clear_delta_column_group_before_version(
            meta, tablet->schema_hash_path(), tablet->tablet_id(), version_before_partial_update + 2);
    ASSERT_TRUE(clear_size.ok());
    ASSERT_EQ(*clear_size, 0);
    // clear dcg before version 14, expect clear 1 dcg
    clear_size = StorageEngine::instance()->update_manager()->clear_delta_column_group_before_version(
            meta, tablet->schema_hash_path(), tablet->tablet_id(), version_before_partial_update + 3);
    ASSERT_TRUE(clear_size.ok());
    ASSERT_EQ(*clear_size, 1);
    // clear dcg before version 15, expect clear 1 dcg
    clear_size = StorageEngine::instance()->update_manager()->clear_delta_column_group_before_version(
            meta, tablet->schema_hash_path(), tablet->tablet_id(), version_before_partial_update + 4);
    ASSERT_TRUE(clear_size.ok());
    ASSERT_EQ(*clear_size, 1);
    // clear dcg with newest version, expect clear 6 dcg
    clear_size = StorageEngine::instance()->update_manager()->clear_delta_column_group_before_version(
            meta, tablet->schema_hash_path(), tablet->tablet_id(), version + 1);
    ASSERT_TRUE(clear_size.ok());
    ASSERT_EQ(*clear_size, 6);
    // check data
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
    }));
    // check delta column files gc
    tablet->data_dir()->perform_path_scan();
    ASSERT_EQ(tablet->data_dir()->get_all_check_dcg_files_cnt(), 2);
    // gc and check again
    tablet->data_dir()->perform_delta_column_files_gc();
    tablet->data_dir()->perform_path_gc_by_rowsetid();
    tablet->data_dir()->perform_path_scan();
    ASSERT_EQ(tablet->data_dir()->get_all_check_dcg_files_cnt(), 2);
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
    }));
}

TEST_P(RowsetColumnPartialUpdateTest, test_get_column_values) {
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());
    int64_t version = 1;
    int64_t version_before_partial_update = 1;
    prepare_tablet(this, tablet, version, version_before_partial_update, N);

    {
        // get columns by `get_column_values`
        std::vector<uint32_t> column_ids = {0, 1, 2};
        std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
        for (int rowid = 0; rowid < N; rowid++) {
            rowids_by_rssid[9].push_back(rowid);
        }
        auto read_column_schema = ChunkHelper::convert_schema(tablet->tablet_schema(), column_ids);
        vector<std::unique_ptr<Column>> columns(column_ids.size());
        for (int colid = 0; colid < column_ids.size(); colid++) {
            auto column = ChunkHelper::column_from_field(*read_column_schema.field(colid).get());
            columns[colid] = column->clone_empty();
        }
        ASSERT_OK(tablet->updates()->get_column_values(column_ids, version, false, rowids_by_rssid, &columns, nullptr,
                                                       tablet->tablet_schema()));
        // check column values
        for (int i = 0; i < N; i++) {
            ASSERT_EQ(columns[0]->get(i).get_int64(), i);
            ASSERT_EQ(columns[1]->get(i).get_int16(), (int16_t)(i % 100 + 3));
            ASSERT_EQ(columns[2]->get(i).get_int32(), (int32_t)(i % 1000 + 4));
        }
    }
}

TEST_P(RowsetColumnPartialUpdateTest, test_upsert) {
    const int N = 100;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());
    int64_t version = 1;
    int64_t version_before_partial_update = 1;
    prepare_tablet(this, tablet, version, version_before_partial_update, N);
    int64_t version_after_partial_update = version;
    auto v1_func = [](int64_t k1) { return (int16_t)(k1 % 100 + 3); };
    auto v2_func = [](int64_t k1) { return (int32_t)(k1 % 1000 + 4); };

    {
        // upsert keys, because keys aren't exist, so they will be inserted.
        std::vector<int64_t> keys(N);
        for (int i = 0; i < N; i++) {
            keys[i] = i + N;
        }
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.reserve(10);
        std::vector<std::shared_ptr<TabletSchema>> partial_schemas;
        // upsert v1 and v2 one by one
        for (int i = 0; i < 10; i++) {
            std::vector<int32_t> column_indexes = {0, (i % 2) + 1};
            partial_schemas.push_back(TabletSchema::create(tablet->tablet_schema(), column_indexes));
            rowsets.emplace_back(create_partial_rowset(tablet, keys, column_indexes, v1_func, v2_func,
                                                       partial_schemas[i], 5, PartialUpdateMode::COLUMN_UPSERT_MODE));
            ASSERT_EQ(rowsets[i]->num_update_files(), 5);
            // preload rowset update state
            ASSERT_OK(StorageEngine::instance()->update_manager()->on_rowset_finished(tablet.get(), rowsets[i].get()));
        }
        commit_rowsets(tablet, rowsets, version);
        // check data
        ASSERT_TRUE(check_tablet(tablet, version, 2 * N, [](int64_t k1, int64_t v1, int32_t v2) {
            return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
        }));
        // check refcnt
        for (const auto& rs_ptr : rowsets) {
            ASSERT_FALSE(
                    StorageEngine::instance()->update_manager()->TEST_update_state_exist(tablet.get(), rs_ptr.get()));
        }
        ASSERT_TRUE(StorageEngine::instance()->update_manager()->TEST_primary_index_refcnt(tablet->tablet_id(), 1));
    }

    {
        // test clone after upsert
        // 1. full clone with version after partial update
        auto new_tablet = create_tablet(rand(), rand());
        ASSERT_EQ(1, new_tablet->updates()->version_history_count());
        ASSERT_OK(full_clone(tablet, version_after_partial_update, new_tablet));
        ASSERT_TRUE(check_tablet(new_tablet, version_after_partial_update, N, [](int64_t k1, int64_t v1, int32_t v2) {
            return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
        }));
        // 2. increment clone, upsert v1 = k1 % 100 + 3
        ASSERT_OK(increment_clone(tablet, {version_after_partial_update + 1}, new_tablet));
        ASSERT_TRUE(check_tablet(new_tablet, version_after_partial_update + 1, 2 * N,
                                 [](int64_t k1, int64_t v1, int32_t v2) {
                                     if (k1 < N) {
                                         return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
                                     } else {
                                         return (int16_t)((k1 - N) % 100 + 3) == v1 && (int32_t)0 == v2;
                                     }
                                 }));
        // 3. increment clone, update v2 = k1 % 100 + 4
        ASSERT_OK(increment_clone(tablet, {version_after_partial_update + 2}, new_tablet));
        ASSERT_TRUE(check_tablet(new_tablet, version_after_partial_update + 2, 2 * N,
                                 [](int64_t k1, int64_t v1, int32_t v2) {
                                     return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
                                 }));
    }
}

TEST_P(RowsetColumnPartialUpdateTest, partial_update_two_rowset_and_check) {
    const int N = 100;
    const int M = N / 2;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());

    // create full rowsets first
    std::vector<int64_t> keys(N);
    std::vector<int64_t> keys1(M);
    std::vector<int64_t> keys2(M);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
        if (i < M) {
            keys1[i] = i;
        } else {
            keys2[i - M] = i;
        }
    }
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(2);
    rowsets.emplace_back(create_rowset(tablet, keys1));
    rowsets.emplace_back(create_rowset(tablet, keys2));
    int64_t version = 1;
    commit_rowsets(tablet, rowsets, version);
    // check data
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 1) == v1 && (int32_t)(k1 % 1000 + 2) == v2;
    }));

    std::vector<int32_t> column_indexes = {0, 1};
    auto v1_func = [](int64_t k1) { return (int16_t)(k1 % 100 + 3); };
    auto v2_func = [](int64_t k1) { return (int32_t)(k1 % 1000 + 4); };
    std::shared_ptr<TabletSchema> partial_schema = TabletSchema::create(tablet->tablet_schema(), column_indexes);
    RowsetSharedPtr partial_rowset =
            create_partial_rowset(tablet, keys, column_indexes, v1_func, v2_func, partial_schema, 1);
    // check data of write column
    RowsetColumnUpdateState state;
    state.load(tablet.get(), partial_rowset.get(), _update_mem_tracker.get());
    const std::vector<ColumnPartialUpdateState>& parital_update_states = state.parital_update_states();
    ASSERT_EQ(parital_update_states.size(), 1);
    ASSERT_EQ(parital_update_states[0].src_rss_rowids.size(), N);
    ASSERT_EQ(parital_update_states[0].rss_rowid_to_update_rowid.size(), N);
    for (int upt_id = 0; upt_id < parital_update_states[0].src_rss_rowids.size(); upt_id++) {
        uint64_t src_rss_rowid = parital_update_states[0].src_rss_rowids[upt_id];
        ASSERT_EQ(parital_update_states[0].rss_rowid_to_update_rowid.find(src_rss_rowid)->second, upt_id);
    }
    // commit partial update
    auto st = tablet->rowset_commit(++version, partial_rowset, 10000);
    ASSERT_TRUE(st.ok()) << st.to_string();
    // check data
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 2) == v2;
    }));
}

TEST_P(RowsetColumnPartialUpdateTest, partial_update_too_many_segment_and_check) {
    const int N = 10;
    // generate M upt files in each partial rowset
    const int M = 1000;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());

    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    auto v1_func = [](int64_t k1) { return (int16_t)(k1 % 100 + 3); };
    auto v2_func = [](int64_t k1) { return (int32_t)(k1 % 1000 + 4); };
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(20);
    // write full rowset first
    for (int i = 0; i < 10; i++) {
        rowsets.emplace_back(create_rowset(tablet, keys));
    }
    std::vector<std::shared_ptr<TabletSchema>> partial_schemas;
    // partial update v1 and v2 one by one
    for (int i = 0; i < 10; i++) {
        std::vector<int32_t> column_indexes = {0, (i % 2) + 1};
        partial_schemas.push_back(TabletSchema::create(tablet->tablet_schema(), column_indexes));
        rowsets.emplace_back(
                create_partial_rowset(tablet, keys, column_indexes, v1_func, v2_func, partial_schemas[i], M));
        ASSERT_EQ(rowsets.back()->num_update_files(), M);
    }
    int64_t version = 1;
    commit_rowsets(tablet, rowsets, version);
    // check data
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
    }));
    // check refcnt
    for (const auto& rs_ptr : rowsets) {
        ASSERT_FALSE(StorageEngine::instance()->update_manager()->TEST_update_state_exist(tablet.get(), rs_ptr.get()));
    }
    ASSERT_TRUE(StorageEngine::instance()->update_manager()->TEST_primary_index_refcnt(tablet->tablet_id(), 1));
}

TEST_P(RowsetColumnPartialUpdateTest, partial_update_too_many_segment_and_limit_mem_tracker) {
    const int N = 10;
    // generate M upt files in each partial rowset
    const int M = 1000;
    auto tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());

    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    auto v1_func = [](int64_t k1) { return (int16_t)(k1 % 100 + 3); };
    auto v2_func = [](int64_t k1) { return (int32_t)(k1 % 1000 + 4); };
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(20);
    // write full rowset first
    for (int i = 0; i < 10; i++) {
        rowsets.emplace_back(create_rowset(tablet, keys));
    }
    std::vector<std::shared_ptr<TabletSchema>> partial_schemas;
    // partial update v1 and v2 one by one
    for (int i = 0; i < 10; i++) {
        std::vector<int32_t> column_indexes = {0, (i % 2) + 1};
        partial_schemas.push_back(TabletSchema::create(tablet->tablet_schema(), column_indexes));
        rowsets.emplace_back(
                create_partial_rowset(tablet, keys, column_indexes, v1_func, v2_func, partial_schemas[i], M));
        ASSERT_EQ(rowsets.back()->num_update_files(), M);
    }

    MemTracker* tracker = StorageEngine::instance()->update_manager()->mem_tracker();
    const int64_t old_limit = tracker->limit();
    tracker->set_limit(1);
    int64_t version = 1;
    commit_rowsets(tablet, rowsets, version);
    // check data
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
    }));
    tracker->set_limit(old_limit);
    // check refcnt
    for (const auto& rs_ptr : rowsets) {
        ASSERT_FALSE(StorageEngine::instance()->update_manager()->TEST_update_state_exist(tablet.get(), rs_ptr.get()));
    }
    ASSERT_TRUE(StorageEngine::instance()->update_manager()->TEST_primary_index_refcnt(tablet->tablet_id(), 1));
}

TEST_P(RowsetColumnPartialUpdateTest, partial_update_multi_column_batch) {
    const int N = 10;
    // generate M upt files in each partial rowset
    const int M = 100;
    auto tablet = create_tablet(rand(), rand(), true);
    ASSERT_EQ(1, tablet->updates()->version_history_count());

    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    auto v1_func = [](int64_t k1) { return (int16_t)(k1 % 100 + 3); };
    auto v2_func = [](int64_t k1) { return (int32_t)(k1 % 1000 + 4); };
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(20);
    // write full rowset first
    for (int i = 0; i < 10; i++) {
        rowsets.emplace_back(create_rowset(tablet, keys, true));
    }
    std::vector<std::shared_ptr<TabletSchema>> partial_schemas;
    // partial update v1 and v2 at once
    for (int i = 0; i < 10; i++) {
        std::vector<int32_t> column_indexes = {0, 1, 2};
        partial_schemas.push_back(TabletSchema::create(tablet->tablet_schema(), column_indexes));
        rowsets.emplace_back(
                create_partial_rowset(tablet, keys, column_indexes, v1_func, v2_func, partial_schemas[i], M));
        ASSERT_EQ(rowsets.back()->num_update_files(), M);
    }

    int32_t old_val = config::vertical_compaction_max_columns_per_group;
    config::vertical_compaction_max_columns_per_group = 1;
    int64_t version = 1;
    commit_rowsets(tablet, rowsets, version);
    // check data
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
    }));
    config::vertical_compaction_max_columns_per_group = old_val;
    // check refcnt
    for (const auto& rs_ptr : rowsets) {
        ASSERT_FALSE(StorageEngine::instance()->update_manager()->TEST_update_state_exist(tablet.get(), rs_ptr.get()));
    }
    ASSERT_TRUE(StorageEngine::instance()->update_manager()->TEST_primary_index_refcnt(tablet->tablet_id(), 1));
}

TEST_P(RowsetColumnPartialUpdateTest, partial_update_multi_segment_and_column_batch) {
    const int N = 10;
    // generate M upt files in each partial rowset
    const int M = 100;
    auto tablet = create_tablet(rand(), rand(), true);
    ASSERT_EQ(1, tablet->updates()->version_history_count());

    std::vector<int64_t> keys(N);
    std::vector<int64_t> partial_keys1;
    std::vector<int64_t> partial_keys2;
    for (int i = 0; i < N; i++) {
        keys[i] = i;
        if (i % 2 == 0) {
            partial_keys1.push_back(i);
        } else {
            partial_keys2.push_back(i);
        }
    }
    auto v1_func = [](int64_t k1) { return (int16_t)(k1 % 100 + 3); };
    auto v2_func = [](int64_t k1) { return (int32_t)(k1 % 1000 + 4); };
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(20);
    // write full rowset first
    for (int i = 0; i < 5; i++) {
        rowsets.emplace_back(create_rowset(tablet, partial_keys1, true));
    }
    for (int i = 0; i < 5; i++) {
        rowsets.emplace_back(create_rowset(tablet, partial_keys2, true));
    }
    std::vector<std::shared_ptr<TabletSchema>> partial_schemas;
    // partial update v1 and v2 at once
    for (int i = 0; i < 10; i++) {
        std::vector<int32_t> column_indexes = {0, 1, 2};
        partial_schemas.push_back(TabletSchema::create(tablet->tablet_schema(), column_indexes));
        rowsets.emplace_back(
                create_partial_rowset(tablet, keys, column_indexes, v1_func, v2_func, partial_schemas[i], M));
        ASSERT_EQ(rowsets.back()->num_update_files(), M);
    }

    int32_t old_val = config::vertical_compaction_max_columns_per_group;
    config::vertical_compaction_max_columns_per_group = 1;
    int64_t version = 1;
    commit_rowsets(tablet, rowsets, version);
    // check data
    ASSERT_TRUE(check_tablet(tablet, version, N, [](int64_t k1, int64_t v1, int32_t v2) {
        return (int16_t)(k1 % 100 + 3) == v1 && (int32_t)(k1 % 1000 + 4) == v2;
    }));
    config::vertical_compaction_max_columns_per_group = old_val;
    // check refcnt
    for (const auto& rs_ptr : rowsets) {
        ASSERT_FALSE(StorageEngine::instance()->update_manager()->TEST_update_state_exist(tablet.get(), rs_ptr.get()));
    }
    ASSERT_TRUE(StorageEngine::instance()->update_manager()->TEST_primary_index_refcnt(tablet->tablet_id(), 1));
}

INSTANTIATE_TEST_SUITE_P(RowsetColumnPartialUpdateTest, RowsetColumnPartialUpdateTest,
                         ::testing::Values(1, 1024, 104857600));

} // namespace starrocks