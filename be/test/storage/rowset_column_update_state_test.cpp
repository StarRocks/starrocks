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

#include "storage/rowset_column_update_state.h"

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
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"
#include "storage/tablet_schema.h"
#include "storage/union_iterator.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"

namespace starrocks {

class RowsetColumnUpdateStateTest : public ::testing::Test {
public:
    void SetUp() override {
        _compaction_mem_tracker = std::make_unique<MemTracker>(-1);
        _update_tracker = std::make_unique<MemTracker>();
    }

    void TearDown() override {
        if (_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
    }

    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                  Column* one_delete = nullptr) {
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
        }
        if (one_delete == nullptr && !keys.empty()) {
            CHECK_OK(writer->flush_chunk(*chunk));
        } else if (one_delete == nullptr) {
            CHECK_OK(writer->flush());
        } else if (one_delete != nullptr) {
            CHECK_OK(writer->flush_chunk_with_deletes(*chunk, *one_delete));
        }
        return *writer->build();
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
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
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    RowsetSharedPtr create_partial_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                          std::vector<int32_t>& column_indexes,
                                          const std::shared_ptr<TabletSchema>& partial_schema, int segment_num) {
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
        writer_context.partial_update_mode = PartialUpdateMode::COLUMN_UPDATE_MODE;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(partial_schema);

        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        EXPECT_TRUE(2 == chunk->num_columns());
        auto& cols = chunk->columns();
        for (long key : keys) {
            cols[0]->append_datum(Datum(key));
            cols[1]->append_datum(Datum((int16_t)(key % 100 + 3)));
        }
        for (int i = 0; i < segment_num; i++) {
            CHECK_OK(writer->flush_chunk(*chunk));
        }
        RowsetSharedPtr partial_rowset = *writer->build();

        return partial_rowset;
    }

protected:
    TabletSharedPtr _tablet;
    std::unique_ptr<MemTracker> _compaction_mem_tracker;
    std::unique_ptr<MemTracker> _update_tracker;
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

static ssize_t read_until_eof(const ChunkIteratorPtr& iter) {
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
    size_t count = 0;
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            count += chunk->num_rows();
            chunk->reset();
        } else {
            LOG(WARNING) << "read error: " << st.to_string();
            return -1;
        }
    }
    return count;
}

static ssize_t read_tablet(const TabletSharedPtr& tablet, int64_t version) {
    Schema schema = ChunkHelper::convert_schema(tablet->tablet_schema());
    TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    return read_until_eof(iter);
}

TEST_F(RowsetColumnUpdateStateTest, prepare_partial_update_states) {
    const int N = 100;
    _tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, _tablet->updates()->version_history_count());

    // create full rowsets first
    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(10);
    for (int i = 0; i < 10; i++) {
        rowsets.emplace_back(create_rowset(_tablet, keys));
    }
    auto pool = StorageEngine::instance()->update_manager()->apply_thread_pool();
    for (int i = 0; i < rowsets.size(); i++) {
        auto version = i + 2;
        auto st = _tablet->rowset_commit(version, rowsets[i], 0);
        ASSERT_TRUE(st.ok()) << st.to_string();
        // Ensure that there is at most one thread doing the version apply job.
        ASSERT_LE(pool->num_threads(), 1);
        ASSERT_EQ(version, _tablet->updates()->max_version());
        ASSERT_EQ(version, _tablet->updates()->version_history_count());
    }
    ASSERT_EQ(N, read_tablet(_tablet, rowsets.size()));

    std::vector<int32_t> column_indexes = {0, 1};
    {
        std::shared_ptr<TabletSchema> partial_schema = TabletSchema::create(_tablet->tablet_schema(), column_indexes);
        RowsetSharedPtr partial_rowset = create_partial_rowset(_tablet, keys, column_indexes, partial_schema, 1);
        // check data of write column
        RowsetColumnUpdateState state;
        state.load(_tablet.get(), partial_rowset.get(), _update_tracker.get());
        const std::vector<ColumnPartialUpdateState>& parital_update_states = state.parital_update_states();
        ASSERT_EQ(parital_update_states.size(), 1);
        ASSERT_EQ(parital_update_states[0].src_rss_rowids.size(), N);
        ASSERT_EQ(parital_update_states[0].rss_rowid_to_update_rowid.size(), N);
        for (int upt_id = 0; upt_id < parital_update_states[0].src_rss_rowids.size(); upt_id++) {
            uint64_t src_rss_rowid = parital_update_states[0].src_rss_rowids[upt_id];
            ASSERT_EQ(parital_update_states[0].rss_rowid_to_update_rowid.find(src_rss_rowid)->second, upt_id);
        }
    }
    {
        // partial update keys that not exist
        std::shared_ptr<TabletSchema> partial_schema = TabletSchema::create(_tablet->tablet_schema(), column_indexes);
        std::vector<int64_t> keys_no_exist(N);
        for (int i = 0; i < N; i++) {
            keys_no_exist[i] = i + N;
        }
        RowsetSharedPtr partial_rowset =
                create_partial_rowset(_tablet, keys_no_exist, column_indexes, partial_schema, 1);
        // check data of write column
        RowsetColumnUpdateState state;
        state.load(_tablet.get(), partial_rowset.get(), _update_tracker.get());
        const std::vector<ColumnPartialUpdateState>& parital_update_states = state.parital_update_states();
        ASSERT_EQ(parital_update_states.size(), 1);
        ASSERT_EQ(parital_update_states[0].src_rss_rowids.size(), N);
        ASSERT_EQ(parital_update_states[0].rss_rowid_to_update_rowid.size(), 0);
    }
}

TEST_F(RowsetColumnUpdateStateTest, partial_update_states_batch_get_index) {
    const int N = 100;
    _tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, _tablet->updates()->version_history_count());

    // create full rowsets first
    std::vector<int64_t> keys(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
    }
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(10);
    for (int i = 0; i < 10; i++) {
        rowsets.emplace_back(create_rowset(_tablet, keys));
    }
    auto pool = StorageEngine::instance()->update_manager()->apply_thread_pool();
    for (int i = 0; i < rowsets.size(); i++) {
        auto version = i + 2;
        auto st = _tablet->rowset_commit(version, rowsets[i], 0);
        ASSERT_TRUE(st.ok()) << st.to_string();
        // Ensure that there is at most one thread doing the version apply job.
        ASSERT_LE(pool->num_threads(), 1);
        ASSERT_EQ(version, _tablet->updates()->max_version());
        ASSERT_EQ(version, _tablet->updates()->version_history_count());
    }
    ASSERT_EQ(N, read_tablet(_tablet, rowsets.size()));

    std::vector<int32_t> column_indexes = {0, 1};
    for (size_t seg_cnt = 2; seg_cnt <= 10; seg_cnt++) {
        // Rowset contains many segments, and memory is enough
        std::shared_ptr<TabletSchema> partial_schema = TabletSchema::create(_tablet->tablet_schema(), column_indexes);
        RowsetSharedPtr partial_rowset = create_partial_rowset(_tablet, keys, column_indexes, partial_schema, seg_cnt);
        // check data of write column
        RowsetColumnUpdateState state;
        // assume that state will load all segments at once
        state.load(_tablet.get(), partial_rowset.get(), _update_tracker.get());
        const std::vector<ColumnPartialUpdateState>& parital_update_states = state.parital_update_states();
        ASSERT_EQ(parital_update_states.size(), seg_cnt);
        ASSERT_EQ(parital_update_states[0].src_rss_rowids.size(), N);
        ASSERT_EQ(parital_update_states[0].rss_rowid_to_update_rowid.size(), N);
        for (int upt_id = 0; upt_id < parital_update_states[0].src_rss_rowids.size(); upt_id++) {
            uint64_t src_rss_rowid = parital_update_states[0].src_rss_rowids[upt_id];
            ASSERT_EQ(parital_update_states[0].rss_rowid_to_update_rowid.find(src_rss_rowid)->second, upt_id);
        }
        // check upserts
        std::vector<BatchPKsPtr> upserts = state.upserts();
        ASSERT_EQ(upserts.size(), seg_cnt);
        auto ptr = upserts[0].get();
        for (size_t i = 0; i < seg_cnt; i++) {
            ASSERT_EQ(upserts[i].get(), ptr);
        }
        for (size_t i = 0; i < seg_cnt - 1; i++) {
            ASSERT_FALSE(upserts[i]->is_last(i));
        }
        ASSERT_TRUE(upserts[seg_cnt - 1]->is_last(seg_cnt - 1));
        ASSERT_EQ(upserts[0]->upserts->size(), seg_cnt * N);
        ASSERT_EQ(upserts[0]->start_idx, 0);
        ASSERT_EQ(upserts[0]->end_idx, seg_cnt);
        for (size_t i = 0; i < seg_cnt; i++) {
            ASSERT_EQ(upserts[0]->offsets[i], i * N);
        }
        for (size_t i = 0; i < seg_cnt; i++) {
            std::vector<uint64_t> target_src_rss_rowids;
            upserts[0]->split_src_rss_rowids(i, target_src_rss_rowids);
            uint32_t sid = 0;
            for (const uint64_t id : target_src_rss_rowids) {
                auto rssid = (uint32_t)(id >> 32);
                auto rowid = (uint32_t)(id & ROWID_MASK);
                ASSERT_EQ(rssid, 9);
                ASSERT_EQ(rowid, sid++);
            }
        }
    }
}

} // namespace starrocks
