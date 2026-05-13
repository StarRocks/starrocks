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

#define private public
#include "storage/rowset/metadata_cache.h"
#undef private

#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"

namespace starrocks {

class RecordingCache final : public Cache {
public:
    Handle* insert(const CacheKey& /*key*/, void* /*value*/, size_t /*value_size*/,
                   void (*/*deleter*/)(const CacheKey& key, void* value),
                   CachePriority /*priority*/ = CachePriority::NORMAL) override {
        return nullptr;
    }

    Handle* lookup(const CacheKey& key) override {
        ++lookup_calls;
        last_lookup_key = key.to_string();
        return &_handle;
    }

    void release(Handle* /*handle*/) override { ++release_calls; }

    void touch(const CacheKey& key) override {
        ++touch_calls;
        last_touch_key = key.to_string();
    }

    void* value(Handle* /*handle*/) override { return nullptr; }

    Slice value_slice(Handle* /*handle*/) override { return {}; }

    void erase(const CacheKey& /*key*/) override {}

    uint64_t new_id() override { return 0; }

    void get_cache_status(rapidjson::Document* /*document*/) override {}

    void set_capacity(size_t capacity) override { _capacity = capacity; }

    size_t get_capacity() const override { return _capacity; }

    size_t get_memory_usage() const override { return 0; }

    size_t get_lookup_count() const override { return lookup_calls; }

    size_t get_hit_count() const override { return 0; }

    size_t get_insert_count() const override { return 0; }

    size_t get_insert_evict_count() const override { return 0; }

    size_t get_release_evict_count() const override { return 0; }

    bool adjust_capacity(int64_t delta, size_t min_capacity = 0) override {
        int64_t new_capacity = static_cast<int64_t>(_capacity) + delta;
        if (new_capacity < static_cast<int64_t>(min_capacity)) {
            return false;
        }
        _capacity = static_cast<size_t>(new_capacity);
        return true;
    }

    size_t lookup_calls = 0;
    size_t release_calls = 0;
    size_t touch_calls = 0;
    std::string last_lookup_key;
    std::string last_touch_key;

private:
    size_t _capacity = 0;
    Handle _handle;
};

class MetadataCacheTest : public ::testing::Test {
public:
    void SetUp() override {}

    void TearDown() override {}

    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys) {
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
        auto cols = chunk->mutable_columns();
        for (int64_t key : keys) {
            cols[0]->append_datum(Datum(key));
            cols[1]->append_datum(Datum((int16_t)(key % 100 + 1)));
            cols[2]->append_datum(Datum((int32_t)(key % 1000 + 2)));
        }
        EXPECT_TRUE(writer->flush_chunk(*chunk).ok());
        return *writer->build();
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::DUP_KEYS;
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
};

TEST_F(MetadataCacheTest, test_auto_evcit) {
    const size_t N = 1000;
    vector<int64_t> keys;
    for (size_t i = 0; i < N; i++) {
        keys.push_back(i);
    }
    vector<RowsetSharedPtr> rowsets;
    auto tablet_ptr = create_tablet(1001, 10002);
    auto metadata_cache_ptr = std::make_unique<MetadataCache>(10);
    for (int i = 0; i < 10; i++) {
        auto rowset_ptr = create_rowset(tablet_ptr, keys);
        ASSERT_TRUE(rowset_ptr->load().ok());
        metadata_cache_ptr->cache_rowset(rowset_ptr.get());
        rowsets.push_back(rowset_ptr);
    }
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(rowsets[i]->segment_memory_usage() == 0);
    }
}

TEST_F(MetadataCacheTest, test_manual_evcit) {
    const size_t N = 100;
    vector<int64_t> keys;
    for (size_t i = 0; i < N; i++) {
        keys.push_back(i);
    }
    vector<RowsetSharedPtr> rowsets;
    auto tablet_ptr = create_tablet(1002, 10003);
    auto metadata_cache_ptr = std::make_unique<MetadataCache>(10000000);
    for (int i = 0; i < 10; i++) {
        auto rowset_ptr = create_rowset(tablet_ptr, keys);
        ASSERT_TRUE(rowset_ptr->load().ok());
        ASSERT_TRUE(rowset_ptr->segment_memory_usage() > 0);
        metadata_cache_ptr->cache_rowset(rowset_ptr.get());
        rowsets.push_back(rowset_ptr);
    }
    for (int i = 0; i < 10; i++) {
        metadata_cache_ptr->refresh_rowset(rowsets[i].get());
        ASSERT_TRUE(rowsets[i]->segment_memory_usage() > 0);
        metadata_cache_ptr->evict_rowset(rowsets[i].get());
        ASSERT_TRUE(rowsets[i]->segment_memory_usage() == 0);
        metadata_cache_ptr->refresh_rowset(rowsets[i].get());
    }
}

TEST_F(MetadataCacheTest, test_warmup) {
    const size_t N = 100;
    vector<int64_t> keys;
    for (size_t i = 0; i < N; i++) {
        keys.push_back(i);
    }
    {
        vector<RowsetSharedPtr> rowsets;
        auto tablet_ptr = create_tablet(1002, 10004);
        auto metadata_cache_ptr = std::make_unique<MetadataCache>(10000000);
        for (int i = 0; i < 10 * 32; i++) {
            auto rowset_ptr = create_rowset(tablet_ptr, keys);
            ASSERT_TRUE(rowset_ptr->load().ok());
            ASSERT_TRUE(rowset_ptr->segment_memory_usage() > 0);
            metadata_cache_ptr->cache_rowset(rowset_ptr.get());
            rowsets.push_back(rowset_ptr);
        }
        metadata_cache_ptr->set_capacity(rowsets[0]->segment_memory_usage() * 32);
        ASSERT_TRUE(rowsets[0]->segment_memory_usage() == 0);
    }
    {
        vector<RowsetSharedPtr> rowsets;
        auto tablet_ptr = create_tablet(1002, 10004);
        auto metadata_cache_ptr = std::make_unique<MetadataCache>(10000000);
        for (int i = 0; i < 10 * 32; i++) {
            auto rowset_ptr = create_rowset(tablet_ptr, keys);
            ASSERT_TRUE(rowset_ptr->load().ok());
            ASSERT_TRUE(rowset_ptr->segment_memory_usage() > 0);
            metadata_cache_ptr->cache_rowset(rowset_ptr.get());
            rowsets.push_back(rowset_ptr);
        }
        // warmup first rowset
        metadata_cache_ptr->refresh_rowset(rowsets[0].get());
        metadata_cache_ptr->set_capacity(rowsets[0]->segment_memory_usage() * 64);
        ASSERT_TRUE(rowsets[0]->segment_memory_usage() > 0);
    }
}

TEST_F(MetadataCacheTest, test_warmup_uses_touch_without_releasing_handle) {
    MetadataCache metadata_cache(1);
    auto* recording_cache = new RecordingCache();
    metadata_cache._cache.reset(recording_cache);

    metadata_cache._warmup("rowset_warmup_key");

    ASSERT_EQ(1, recording_cache->touch_calls);
    ASSERT_EQ("rowset_warmup_key", recording_cache->last_touch_key);
    ASSERT_EQ(0, recording_cache->lookup_calls);
    ASSERT_EQ(0, recording_cache->release_calls);
}

TEST_F(MetadataCacheTest, test_concurrency_issue) {
    const size_t N = 100;
    vector<int64_t> keys;
    for (size_t i = 0; i < N; i++) {
        keys.push_back(i);
    }
    vector<RowsetSharedPtr> rowsets;
    auto tablet_ptr = create_tablet(1002, 10005);
    auto metadata_cache_ptr = std::make_unique<MetadataCache>(1);
    std::vector<std::thread> threads;
    threads.emplace_back([&]() {
        for (int i = 0; i < 100; i++) {
            auto rowset_ptr = create_rowset(tablet_ptr, keys);
            ASSERT_TRUE(rowset_ptr->load().ok());
            ASSERT_TRUE(rowset_ptr->segment_memory_usage() > 0);
            metadata_cache_ptr->cache_rowset(rowset_ptr.get());
        }
    });
    threads.emplace_back([&]() {
        for (int i = 0; i < 100; i++) {
            auto rowset_ptr = create_rowset(tablet_ptr, keys);
            ASSERT_TRUE(rowset_ptr->load().ok());
            ASSERT_TRUE(rowset_ptr->segment_memory_usage() > 0);
            metadata_cache_ptr->cache_rowset(rowset_ptr.get());
        }
    });
    for (auto& t : threads) {
        t.join();
    }
}

} // namespace starrocks
