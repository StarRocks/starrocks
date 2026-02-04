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

#include "storage/persistent_index_load_executor.h"

#include <gtest/gtest.h>

#include <utility>

#include "base/testutil/assert.h"
#include "column/datum.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment_options.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_updates.h"
#include "storage/update_manager.h"
#include "util/threadpool.h"

namespace starrocks {

class PersistentIndexLoadExecutorTest : public ::testing::Test {
public:
    using Row = std::vector<Datum>;

    void SetUp() override {
        srand(GetCurrentTimeMicros());
        _partition_id = 1;
        _index_id = 1;
        _tablet = create_tablet(rand(), rand());
    }

    void TearDown() override {
        if (_tablet) {
            auto st = StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            CHECK(st.ok()) << st.to_string();
            _tablet.reset();
        }
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
        k2.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k2);

        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    void generate_data(int64_t key_start, size_t num_row, size_t num_segment, std::vector<std::vector<Row>>& segments) {
        size_t num_row_per_segment = (num_row + num_segment - 1) / num_segment;
        std::vector<int64_t> keys(num_row, 0);
        for (size_t i = 0; i < num_row; i++) {
            keys[i] = key_start + i;
        }
        std::random_shuffle(keys.begin(), keys.end());
        for (size_t i = 0; i < num_segment; i++) {
            auto& segment = segments.emplace_back();
            size_t start = i * num_row_per_segment;
            size_t end = std::min((i + 1) * num_row_per_segment, num_row);
            std::sort(keys.begin() + start, keys.begin() + end);
            for (size_t j = start; j < end; j++) {
                auto& row = segment.emplace_back();
                auto key = keys[j];
                row.push_back(Datum(key));
                row.push_back(Datum(static_cast<int64_t>((key * 7919) % 7883)));
            }
        }
    }

    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const std::vector<std::vector<Row>>& segments,
                                  SegmentsOverlapPB overlap) {
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
        writer_context.segments_overlap = overlap;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        for (size_t i = 0; i < segments.size(); i++) {
            auto& segment = segments[i];
            auto chunk = ChunkHelper::new_chunk(schema, segment.size());
            auto cols = chunk->mutable_columns();
            for (auto& row : segment) {
                CHECK(cols.size() == row.size());
                for (size_t j = 0; j < row.size(); j++) {
                    cols[j]->append_datum(row[j]);
                }
            }
            CHECK_OK(writer->flush_chunk(*chunk));
        }
        return *writer->build();
    }

    void wait_for_version(int64_t version, int64_t timeout_ms = 10000) {
        while (true) {
            timeout_ms -= 200;
            if (timeout_ms <= 0) {
                ASSERT_TRUE(false) << "timeout";
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            std::vector<RowsetSharedPtr> rowsets;
            EditVersion full_version;
            ASSERT_TRUE(_tablet->updates()->get_applied_rowsets(version, &rowsets, &full_version).ok());
            if (full_version.major_number() >= version) {
                break;
            }
            std::cerr << "waiting for version " << version << std::endl;
        }
    }

protected:
    int64_t _partition_id;
    int64_t _index_id;
    TabletSharedPtr _tablet;
};

TEST_F(PersistentIndexLoadExecutorTest, test_submit_task) {
    const int64_t key_start = 0;
    const int num_row = 10000;
    const size_t num_segment = 1;
    LOG(INFO) << "rowset=1, segment=" << num_segment << ", num_row=" << num_row;
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    std::vector<std::vector<Row>> segments;
    std::srand(0);
    generate_data(key_start, num_row, num_segment, segments);
    auto rs = create_rowset(_tablet, segments, SegmentsOverlapPB::OVERLAPPING);
    ASSERT_TRUE(_tablet->rowset_commit(2, rs).ok());
    wait_for_version(2);
    ASSERT_EQ(2, _tablet->updates()->max_version());

    auto tablet_id = _tablet->tablet_id();
    auto manager = StorageEngine::instance()->update_manager();
    auto& index_cache = manager->index_cache();

    auto index_entry = index_cache.get_or_create(tablet_id);
    ASSERT_TRUE(index_entry->value().is_loaded());
    index_entry->value().unload();
    ASSERT_FALSE(index_entry->value().is_loaded());
    index_cache.remove(index_entry);

    // test submit task and wait to finish
    auto* pindex_load_executor = manager->get_pindex_load_executor();
    auto st = pindex_load_executor->submit_task_and_wait_for(_tablet, 60);
    CHECK_OK(st);
    index_entry = index_cache.get_or_create(tablet_id);
    ASSERT_TRUE(index_entry->value().is_loaded());
}

TEST_F(PersistentIndexLoadExecutorTest, test_submit_task_many_times) {
    const int64_t key_start = 0;
    const int num_row = 100000;
    const size_t num_segment = 10;
    LOG(INFO) << "rowset=1, segment=" << num_segment << ", num_row=" << num_row;
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    std::vector<std::vector<Row>> segments;
    std::srand(0);
    generate_data(key_start, num_row, num_segment, segments);
    auto rs = create_rowset(_tablet, segments, SegmentsOverlapPB::OVERLAPPING);
    ASSERT_TRUE(_tablet->rowset_commit(2, rs).ok());
    wait_for_version(2);
    ASSERT_EQ(2, _tablet->updates()->max_version());

    auto tablet_id = _tablet->tablet_id();
    auto manager = StorageEngine::instance()->update_manager();
    auto& index_cache = manager->index_cache();

    auto index_entry = index_cache.get_or_create(tablet_id);
    ASSERT_TRUE(index_entry->value().is_loaded());
    index_entry->value().unload();
    ASSERT_FALSE(index_entry->value().is_loaded());
    index_cache.remove(index_entry);

    // delete persistent index
    std::string key = "tpi_";
    starrocks::put_fixed64_le(&key, BigEndian::FromHost64(tablet_id));
    Status st = _tablet->data_dir()->get_meta()->remove(starrocks::META_COLUMN_FAMILY_INDEX, key);
    CHECK_OK(st);

    // submit task to rebuild persistent index and not wait, many times
    auto* pindex_load_executor = manager->get_pindex_load_executor();
    st = pindex_load_executor->submit_task_and_wait_for(_tablet, 0);
    ASSERT_TRUE(st.is_time_out());

    st = pindex_load_executor->submit_task_and_wait_for(_tablet, 0);
    ASSERT_TRUE(st.is_time_out());

    st = pindex_load_executor->submit_task_and_wait_for(_tablet, 0);
    ASSERT_TRUE(st.is_time_out());

    // submit task and wait to finish
    st = pindex_load_executor->submit_task_and_wait_for(_tablet, 60);
    CHECK_OK(st);
    index_entry = index_cache.get_or_create(tablet_id);
    ASSERT_TRUE(index_entry->value().is_loaded());
}

TEST_F(PersistentIndexLoadExecutorTest, test_non_pk_tablet) {
    // non pk tablet
    auto tablet = std::make_shared<Tablet>();
    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->set_tablet_id(10000);
    tablet->set_tablet_meta(tablet_meta);

    auto manager = StorageEngine::instance()->update_manager();
    auto* pindex_load_executor = manager->get_pindex_load_executor();
    auto st = pindex_load_executor->submit_task_and_wait_for(tablet, 60);
    ASSERT_TRUE(st.is_invalid_argument());
}

TEST_F(PersistentIndexLoadExecutorTest, test_submit_task_fail) {
    const int64_t key_start = 0;
    const int num_row = 10000;
    const size_t num_segment = 1;
    LOG(INFO) << "rowset=1, segment=" << num_segment << ", num_row=" << num_row;
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());
    std::vector<std::vector<Row>> segments;
    std::srand(0);
    generate_data(key_start, num_row, num_segment, segments);
    auto rs = create_rowset(_tablet, segments, SegmentsOverlapPB::OVERLAPPING);
    ASSERT_TRUE(_tablet->rowset_commit(2, rs).ok());
    wait_for_version(2);
    ASSERT_EQ(2, _tablet->updates()->max_version());

    // load pool is not initialized and test submit task
    auto* pindex_load_executor = StorageEngine::instance()->update_manager()->get_pindex_load_executor();
    pindex_load_executor->TEST_reset_load_pool();
    auto st = pindex_load_executor->submit_task_and_wait_for(_tablet, 60);
    ASSERT_TRUE(st.is_internal_error());

    // reset load pool
    CHECK_OK(pindex_load_executor->init());
}

} // namespace starrocks
