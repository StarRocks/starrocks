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

#include "storage/rowset/metadata_cache.h"

#include <gtest/gtest.h>

#include "common/config.h"
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
#include "testutil/sync_point.h"
#include "util/defer_op.h"
#include "util/starrocks_metrics.h"

namespace starrocks {
class MetadataCacheTest : public ::testing::Test {
public:
    void SetUp() override {}

    void TearDown() override {}

    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys, size_t num_segments = 1) {
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
        for (size_t segment_id = 0; segment_id < num_segments; ++segment_id) {
            const size_t begin = keys.size() * segment_id / num_segments;
            const size_t end = keys.size() * (segment_id + 1) / num_segments;
            auto chunk = ChunkHelper::new_chunk(schema, end - begin);
            auto cols = chunk->columns();
            for (size_t i = begin; i < end; ++i) {
                int64_t key = keys[i];
                cols[0]->as_mutable_ptr()->append_datum(Datum(key));
                cols[1]->as_mutable_ptr()->append_datum(Datum((int16_t)(key % 100 + 1)));
                cols[2]->as_mutable_ptr()->append_datum(Datum((int32_t)(key % 1000 + 2)));
            }
            EXPECT_TRUE(writer->flush_chunk(*chunk).ok());
        }
        return *writer->build();
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash,
                                  TKeysType::type keys_type = TKeysType::DUP_KEYS) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = keys_type;
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

TEST_F(MetadataCacheTest, update_charge_on_last_reader_release) {
    constexpr size_t kMetadataCacheCapacity = 10 * 1024 * 1024;
    const size_t num_rows = 1000;
    vector<int64_t> keys;
    keys.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        keys.push_back(i);
    }

    const int32_t old_metadata_cache_memory_limit_percent = config::metadata_cache_memory_limit_percent;
    config::metadata_cache_memory_limit_percent = 30;
    DeferOp restore_config([old_metadata_cache_memory_limit_percent] {
        config::metadata_cache_memory_limit_percent = old_metadata_cache_memory_limit_percent;
    });

    auto metadata_cache = std::make_unique<MetadataCache>(kMetadataCacheCapacity);
    auto tablet = create_tablet(1003, 10006);
    auto rowset = create_rowset(tablet, keys);
    ASSERT_TRUE(rowset->load().ok());
    metadata_cache->cache_rowset(rowset.get());

    const size_t initial_rowset_size = rowset->segment_memory_usage();
    const size_t initial_cache_usage = metadata_cache->get_memory_usage();

    // Segment::open() marks a share-nothing segment dirty. Verify that the
    // dirty bit is consumed exactly once, then restore it for the Rowset-level
    // last-reader test below.
    ASSERT_FALSE(rowset->segments().empty());
    auto& segment = rowset->segments().front();
    ASSERT_TRUE(segment->consume_lazy_mem_update());
    ASSERT_FALSE(segment->consume_lazy_mem_update());
    segment->update_cache_size();

    int update_charge_calls = 0;
    size_t captured_charge = 0;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("Rowset::_update_metadata_cache_charge", [&](void* arg) {
        ++update_charge_calls;
        captured_charge = *static_cast<size_t*>(arg);
        metadata_cache->update_rowset_charge(rowset.get(), captured_charge);
    });
    DeferOp defer([] {
        SyncPoint::GetInstance()->ClearCallBack("Rowset::_update_metadata_cache_charge");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    // Consume the dirty flag restored above. This intentionally pays for one redundant refresh
    // per Rowset load in exchange for keeping update_cache_size() uniform.
    rowset->acquire();
    rowset->release();
    ASSERT_EQ(1, update_charge_calls);
    ASSERT_EQ(initial_rowset_size, captured_charge);
    ASSERT_EQ(initial_cache_usage, metadata_cache->get_memory_usage());

    rowset->acquire();
    rowset->acquire();

    std::vector<std::string> short_keys;
    ASSERT_TRUE(rowset->get_segment_sk_index(&short_keys).ok());
    const size_t loaded_rowset_size = rowset->segment_memory_usage();
    ASSERT_GT(loaded_rowset_size, initial_rowset_size);
    ASSERT_EQ(initial_cache_usage, metadata_cache->get_memory_usage());

    rowset->release();
    ASSERT_EQ(1, update_charge_calls);
    ASSERT_EQ(initial_cache_usage, metadata_cache->get_memory_usage());

    rowset->release();
    ASSERT_EQ(2, update_charge_calls);
    ASSERT_EQ(loaded_rowset_size, captured_charge);
    ASSERT_EQ(loaded_rowset_size - initial_rowset_size, metadata_cache->get_memory_usage() - initial_cache_usage);
    ASSERT_GT(rowset->segment_memory_usage(), 0);

    const size_t updated_cache_usage = metadata_cache->get_memory_usage();
    rowset->acquire();
    rowset->release();
    ASSERT_EQ(2, update_charge_calls);
    ASSERT_EQ(updated_cache_usage, metadata_cache->get_memory_usage());

    // A charge refresh after the entry has been removed is a no-op.
    metadata_cache->evict_rowset(rowset.get());
    const size_t usage_after_evict = metadata_cache->get_memory_usage();
    ASSERT_EQ(0, usage_after_evict);
    metadata_cache->update_rowset_charge(rowset.get(), loaded_rowset_size);
    ASSERT_EQ(usage_after_evict, metadata_cache->get_memory_usage());
}

TEST_F(MetadataCacheTest, update_charge_ignores_replaced_rowset) {
    const std::vector<int64_t> keys{1, 2, 3};
    auto tablet = create_tablet(1007, 10010);
    auto original = create_rowset(tablet, keys);
    ASSERT_TRUE(original->load().ok());
    MetadataCache metadata_cache(10 * 1024 * 1024);
    metadata_cache.cache_rowset(original.get());

    std::vector<std::string> short_keys;
    ASSERT_TRUE(original->get_segment_sk_index(&short_keys).ok());
    const size_t original_charge = original->segment_memory_usage();

    // Migration can create another Rowset object with the same ID. Its cache
    // entry must not receive a delayed charge update from the original object.
    RowsetSharedPtr replacement;
    ASSERT_TRUE(RowsetFactory::create_rowset(tablet->tablet_schema(), original->rowset_path(), original->rowset_meta(),
                                             &replacement, nullptr)
                        .ok());
    ASSERT_TRUE(replacement->load().ok());
    metadata_cache.cache_rowset(replacement.get());
    ASSERT_EQ(0, original->segment_memory_usage());
    const size_t replacement_charge = replacement->segment_memory_usage();
    ASSERT_GT(original_charge, replacement_charge);
    const size_t cache_usage = metadata_cache.get_memory_usage();

    metadata_cache.update_rowset_charge(original.get(), original_charge);
    ASSERT_EQ(cache_usage, metadata_cache.get_memory_usage());
    ASSERT_EQ(replacement_charge, replacement->segment_memory_usage());

    // The replacement's own lazy metadata growth must still be accounted for.
    ASSERT_TRUE(replacement->get_segment_sk_index(&short_keys).ok());
    const size_t loaded_charge = replacement->segment_memory_usage();
    ASSERT_GT(loaded_charge, replacement_charge);
    metadata_cache.update_rowset_charge(replacement.get(), loaded_charge);
    ASSERT_EQ(cache_usage + loaded_charge - replacement_charge, metadata_cache.get_memory_usage());
    ASSERT_EQ(0, metadata_cache._cache->get_lookup_count());

    metadata_cache.evict_rowset(replacement.get());
    ASSERT_EQ(0, metadata_cache.get_memory_usage());
    ASSERT_EQ(0, replacement->segment_memory_usage());
}

TEST_F(MetadataCacheTest, last_reader_release_closes_unloading_rowset) {
    const std::vector<int64_t> keys{1, 2, 3};
    auto tablet = create_tablet(1006, 10009);
    auto rowset = create_rowset(tablet, keys);
    ASSERT_TRUE(rowset->load().ok());
    ASSERT_GT(rowset->segment_memory_usage(), 0);

    rowset->acquire();
    rowset->close();
    // close() moves a referenced rowset to ROWSET_UNLOADING without clearing
    // its segments. The last reader release performs the deferred close.
    ASSERT_GT(rowset->segment_memory_usage(), 0);
    rowset->release();
    ASSERT_EQ(0, rowset->segment_memory_usage());
}

TEST_F(MetadataCacheTest, skip_charge_calculation_for_ineligible_rowsets) {
    const std::vector<int64_t> keys{1, 2, 3};
    auto dup_tablet = create_tablet(1004, 10007);
    auto pk_tablet = create_tablet(1005, 10008, TKeysType::PRIMARY_KEYS);
    auto dup_rowset = create_rowset(dup_tablet, keys);
    auto pk_rowset = create_rowset(pk_tablet, keys);
    ASSERT_TRUE(dup_rowset->load().ok());
    ASSERT_TRUE(pk_rowset->load().ok());

    int segment_memory_usage_calls = 0;
    const int32_t old_metadata_cache_memory_limit_percent = config::metadata_cache_memory_limit_percent;
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("Rowset::segment_memory_usage", [&](void*) { ++segment_memory_usage_calls; });
    DeferOp defer([old_metadata_cache_memory_limit_percent] {
        config::metadata_cache_memory_limit_percent = old_metadata_cache_memory_limit_percent;
        SyncPoint::GetInstance()->ClearCallBack("Rowset::segment_memory_usage");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    config::metadata_cache_memory_limit_percent = 30;
    pk_rowset->acquire();
    pk_rowset->release();
    ASSERT_EQ(0, segment_memory_usage_calls);

    config::metadata_cache_memory_limit_percent = 0;
    dup_rowset->acquire();
    dup_rowset->release();
    ASSERT_EQ(0, segment_memory_usage_calls);

    config::metadata_cache_memory_limit_percent = 30;
    dup_rowset->acquire();
    dup_rowset->release();
    ASSERT_EQ(1, segment_memory_usage_calls);

    dup_rowset->acquire();
    dup_rowset->release();
    ASSERT_EQ(1, segment_memory_usage_calls);
}

class MetadataCacheReaderTest : public MetadataCacheTest {
public:
    void SetUp() override {
        MetadataCacheTest::SetUp();
        _previous_cache = MetadataCache::_s_instance;
        _previous_cache_percent = config::metadata_cache_memory_limit_percent;
        _metadata_cache = std::make_unique<MetadataCache>(10 * 1024 * 1024);
        MetadataCache::_s_instance = _metadata_cache.get();
        config::metadata_cache_memory_limit_percent = 30;
    }

    void TearDown() override {
        if (_observing_charge_refreshes) {
            SyncPoint::GetInstance()->DisableProcessing();
            SyncPoint::GetInstance()->ClearCallBack("Rowset::segment_memory_usage");
            SyncPoint::GetInstance()->ClearCallBack("Rowset::_update_metadata_cache_charge");
        }
        MetadataCache::_s_instance = _previous_cache;
        config::metadata_cache_memory_limit_percent = _previous_cache_percent;
        _metadata_cache.reset();
        MetadataCacheTest::TearDown();
    }

protected:
    void observe_charge_refreshes() {
        _observing_charge_refreshes = true;
        SyncPoint::GetInstance()->SetCallBack("Rowset::segment_memory_usage", [&](void*) { ++_snapshot_calls; });
        SyncPoint::GetInstance()->SetCallBack("Rowset::_update_metadata_cache_charge", [&](void* arg) {
            ++_charge_update_calls;
            _last_charge = *static_cast<size_t*>(arg);
        });
        SyncPoint::GetInstance()->EnableProcessing();
    }

    std::unique_ptr<MetadataCache> _metadata_cache;
    MetadataCache* _previous_cache = nullptr;
    int32_t _previous_cache_percent = 0;
    bool _observing_charge_refreshes = false;
    int _snapshot_calls = 0;
    int _charge_update_calls = 0;
    size_t _last_charge = 0;
};

TEST_F(MetadataCacheReaderTest, segment_dirty_flags_are_consumed_independently_and_can_be_rearmed) {
    auto tablet = create_tablet(1012, 10015);
    auto rowset = create_rowset(tablet, {1, 2, 3, 4, 5, 6}, 2);
    ASSERT_TRUE(rowset->load().ok());
    ASSERT_EQ(2, rowset->segments().size());
    const auto& first = rowset->segments()[0];
    const auto& second = rowset->segments()[1];
    first->consume_lazy_mem_update();
    second->consume_lazy_mem_update();
    ASSERT_FALSE(first->consume_lazy_mem_update());
    ASSERT_FALSE(second->consume_lazy_mem_update());

    // Multiple notifications coalesce until consumed and do not dirty a sibling.
    first->update_cache_size();
    first->update_cache_size();
    ASSERT_TRUE(first->consume_lazy_mem_update());
    ASSERT_FALSE(first->consume_lazy_mem_update());
    ASSERT_FALSE(second->consume_lazy_mem_update());

    first->update_cache_size();
    second->update_cache_size();
    ASSERT_TRUE(first->consume_lazy_mem_update());
    ASSERT_TRUE(second->consume_lazy_mem_update());
    ASSERT_FALSE(first->consume_lazy_mem_update());
    ASSERT_FALSE(second->consume_lazy_mem_update());
}

TEST_F(MetadataCacheReaderTest, release_refreshes_all_dirty_segments_once) {
    auto tablet = create_tablet(1013, 10016);
    auto rowset = create_rowset(tablet, {1, 2, 3, 4, 5, 6, 7, 8, 9}, 3);
    ASSERT_TRUE(rowset->load().ok());
    ASSERT_EQ(3, rowset->segments().size());
    _metadata_cache->cache_rowset(rowset.get());
    const size_t initial_rowset_size = rowset->segment_memory_usage();
    const size_t initial_cache_usage = _metadata_cache->get_memory_usage();
    const auto& segments = rowset->segments();
    for (const auto& segment : segments) {
        segment->consume_lazy_mem_update();
    }
    const size_t first_size = segments[0]->mem_usage();
    const size_t last_size = segments[2]->mem_usage();
    observe_charge_refreshes();

    rowset->acquire();
    rowset->acquire();
    ASSERT_TRUE(segments[0]->load_index().ok());
    ASSERT_TRUE(segments[2]->load_index().ok());
    const size_t loaded_rowset_size =
            initial_rowset_size + segments[0]->mem_usage() - first_size + segments[2]->mem_usage() - last_size;
    ASSERT_GT(loaded_rowset_size, initial_rowset_size);

    rowset->release();
    ASSERT_EQ(1, rowset->refs_by_reader());
    ASSERT_EQ(0, _snapshot_calls);
    ASSERT_EQ(0, _charge_update_calls);
    ASSERT_EQ(initial_cache_usage, _metadata_cache->get_memory_usage());

    rowset->release();
    ASSERT_EQ(0, rowset->refs_by_reader());
    ASSERT_EQ(1, _snapshot_calls);
    ASSERT_EQ(1, _charge_update_calls);
    ASSERT_EQ(loaded_rowset_size, _last_charge);
    ASSERT_EQ(initial_cache_usage + loaded_rowset_size - initial_rowset_size, _metadata_cache->get_memory_usage());
    // Finding the first dirty segment must not skip consumption of later flags.
    for (const auto& segment : segments) {
        ASSERT_FALSE(segment->consume_lazy_mem_update());
    }

    rowset->acquire();
    rowset->release();
    ASSERT_EQ(1, _snapshot_calls);
    ASSERT_EQ(1, _charge_update_calls);

    // A subsequent change in the previously clean middle segment triggers one
    // more snapshot and publishes the complete rowset charge.
    const size_t middle_size = segments[1]->mem_usage();
    rowset->acquire();
    ASSERT_TRUE(segments[1]->load_index().ok());
    const size_t final_rowset_size = loaded_rowset_size + segments[1]->mem_usage() - middle_size;
    ASSERT_GT(final_rowset_size, loaded_rowset_size);
    rowset->release();
    ASSERT_EQ(2, _snapshot_calls);
    ASSERT_EQ(2, _charge_update_calls);
    ASSERT_EQ(final_rowset_size, _last_charge);
    ASSERT_EQ(initial_cache_usage + final_rowset_size - initial_rowset_size, _metadata_cache->get_memory_usage());
    ASSERT_FALSE(segments[1]->consume_lazy_mem_update());
}

TEST_F(MetadataCacheReaderTest, unloading_release_destroys_segments_without_refreshing_dirty_charge) {
    auto tablet = create_tablet(1014, 10017);
    auto rowset = create_rowset(tablet, {1, 2, 3});
    ASSERT_TRUE(rowset->load().ok());
    _metadata_cache->cache_rowset(rowset.get());
    std::weak_ptr<Segment> segment = rowset->segments().front();
    rowset->acquire();
    rowset->acquire();
    ASSERT_TRUE(rowset->segments().front()->load_index().ok());
    observe_charge_refreshes();

    rowset->close();
    ASSERT_EQ(2, rowset->refs_by_reader());
    ASSERT_FALSE(segment.expired());
    rowset->release();
    ASSERT_EQ(1, rowset->refs_by_reader());
    ASSERT_FALSE(segment.expired());
    ASSERT_EQ(0, _snapshot_calls);
    ASSERT_EQ(0, _charge_update_calls);

    rowset->release();
    ASSERT_EQ(0, rowset->refs_by_reader());
    ASSERT_TRUE(rowset->segments().empty());
    ASSERT_TRUE(segment.expired());
    ASSERT_EQ(0, _snapshot_calls);
    ASSERT_EQ(0, _charge_update_calls);

    // Successful reload verifies that on_release() completed the UNLOADED transition.
    ASSERT_TRUE(rowset->load().ok());
    ASSERT_FALSE(rowset->segments().empty());
}

TEST_F(MetadataCacheReaderTest, loaded_empty_rowset_release_skips_charge_calculation) {
    auto tablet = create_tablet(1015, 10018);
    auto rowset = create_rowset(tablet, {}, 0);
    ASSERT_EQ(0, rowset->num_segments());
    ASSERT_TRUE(rowset->load().ok());
    ASSERT_TRUE(rowset->segments().empty());
    _metadata_cache->cache_rowset(rowset.get());
    const size_t initial_cache_usage = _metadata_cache->get_memory_usage();
    observe_charge_refreshes();

    rowset->acquire();
    rowset->release();
    ASSERT_EQ(0, rowset->refs_by_reader());
    ASSERT_EQ(0, _snapshot_calls);
    ASSERT_EQ(0, _charge_update_calls);
    ASSERT_EQ(initial_cache_usage, _metadata_cache->get_memory_usage());
}

TEST_F(MetadataCacheReaderTest, close_refreshes_charge_only_after_last_reader) {
    const std::vector<int64_t> keys{1, 2, 3};
    auto tablet = create_tablet(1008, 10011);
    auto rowset = create_rowset(tablet, keys);
    ASSERT_TRUE(rowset->load().ok());
    _metadata_cache->cache_rowset(rowset.get());
    const size_t initial_rowset_size = rowset->segment_memory_usage();
    const size_t initial_cache_usage = _metadata_cache->get_memory_usage();
    auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());

    // Exercise release through TabletReader's production implementation, with
    // two overlapping readers retaining the same rowset.
    TabletReader first_reader(tablet, Version(0, 0), schema, std::vector<RowsetSharedPtr>{rowset});
    ASSERT_TRUE(first_reader.prepare().ok());
    TabletReader last_reader(tablet, Version(0, 0), schema, std::vector<RowsetSharedPtr>{rowset});
    ASSERT_TRUE(last_reader.prepare().ok());
    ASSERT_EQ(2, rowset->refs_by_reader());

    std::vector<std::string> short_keys;
    ASSERT_TRUE(rowset->get_segment_sk_index(&short_keys).ok());
    const size_t loaded_rowset_size = rowset->segment_memory_usage();
    ASSERT_GT(loaded_rowset_size, initial_rowset_size);

    first_reader.close();
    ASSERT_EQ(1, rowset->refs_by_reader());
    ASSERT_EQ(initial_cache_usage, _metadata_cache->get_memory_usage());
    last_reader.close();
    ASSERT_EQ(0, rowset->refs_by_reader());
    ASSERT_EQ(initial_cache_usage + loaded_rowset_size - initial_rowset_size, _metadata_cache->get_memory_usage());
    ASSERT_EQ(loaded_rowset_size, rowset->segment_memory_usage());
    ASSERT_FALSE(rowset->segments().front()->consume_lazy_mem_update());

    const size_t updated_cache_usage = _metadata_cache->get_memory_usage();
    TabletReader clean_reader(tablet, Version(0, 0), schema, std::vector<RowsetSharedPtr>{rowset});
    ASSERT_TRUE(clean_reader.prepare().ok());
    clean_reader.close();
    ASSERT_EQ(0, rowset->refs_by_reader());
    ASSERT_EQ(updated_cache_usage, _metadata_cache->get_memory_usage());
    ASSERT_EQ(0, _metadata_cache->_cache->get_lookup_count());
}

TEST_F(MetadataCacheReaderTest, disable_cache_before_charge_update) {
    const std::vector<int64_t> keys{1, 2, 3};
    auto tablet = create_tablet(1011, 10014);
    auto rowset = create_rowset(tablet, keys);
    ASSERT_TRUE(rowset->load().ok());
    _metadata_cache->cache_rowset(rowset.get());
    const size_t initial_cache_usage = _metadata_cache->get_memory_usage();

    auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
    TabletReader reader(tablet, Version(0, 0), schema, std::vector<RowsetSharedPtr>{rowset});
    ASSERT_TRUE(reader.prepare().ok());
    std::vector<std::string> short_keys;
    ASSERT_TRUE(rowset->get_segment_sk_index(&short_keys).ok());

    int snapshots = 0;
    int charge_updates = 0;
    SyncPoint::GetInstance()->SetCallBack("Rowset::segment_memory_usage", [&](void*) {
        ++snapshots;
        // The runtime setting can change after release() passes its first
        // config check but before it publishes the charge snapshot.
        config::metadata_cache_memory_limit_percent = 0;
    });
    SyncPoint::GetInstance()->SetCallBack("Rowset::_update_metadata_cache_charge", [&](void*) { ++charge_updates; });
    DeferOp clear_callbacks([] {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearCallBack("Rowset::segment_memory_usage");
        SyncPoint::GetInstance()->ClearCallBack("Rowset::_update_metadata_cache_charge");
    });
    SyncPoint::GetInstance()->EnableProcessing();

    reader.close();
    ASSERT_EQ(1, snapshots);
    ASSERT_EQ(0, charge_updates);
    ASSERT_EQ(0, rowset->refs_by_reader());
    ASSERT_EQ(initial_cache_usage, _metadata_cache->get_memory_usage());
    ASSERT_FALSE(rowset->segments().empty());
}

TEST_F(MetadataCacheReaderTest, close_finishes_deferred_rowset_unload) {
    const std::vector<int64_t> keys{1, 2, 3};
    auto tablet = create_tablet(1009, 10012);
    auto rowset = create_rowset(tablet, keys);
    ASSERT_TRUE(rowset->load().ok());
    _metadata_cache->cache_rowset(rowset.get());
    const size_t loaded_rowset_size = rowset->segment_memory_usage();
    ASSERT_GT(loaded_rowset_size, 0);
    auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());

    TabletReader first_reader(tablet, Version(0, 0), schema, std::vector<RowsetSharedPtr>{rowset});
    ASSERT_TRUE(first_reader.prepare().ok());
    TabletReader last_reader(tablet, Version(0, 0), schema, std::vector<RowsetSharedPtr>{rowset});
    ASSERT_TRUE(last_reader.prepare().ok());

    // Eviction requests close(), but both readers still need the segments.
    _metadata_cache->evict_rowset(rowset.get());
    ASSERT_EQ(0, _metadata_cache->get_memory_usage());
    ASSERT_EQ(loaded_rowset_size, rowset->segment_memory_usage());
    first_reader.close();
    ASSERT_EQ(1, rowset->refs_by_reader());
    ASSERT_EQ(loaded_rowset_size, rowset->segment_memory_usage());
    last_reader.close();
    ASSERT_EQ(0, rowset->refs_by_reader());
    ASSERT_EQ(0, rowset->segment_memory_usage());
    ASSERT_EQ(0, _metadata_cache->get_memory_usage());

    // The last close must finish the transition to UNLOADED, allowing a reload.
    ASSERT_TRUE(rowset->load().ok());
    ASSERT_GT(rowset->segment_memory_usage(), 0);
}

TEST_F(MetadataCacheReaderTest, charge_growth_can_evict_rowset_on_close) {
    const std::vector<int64_t> keys{1, 2, 3};
    auto tablet = create_tablet(1010, 10013);
    auto rowset = create_rowset(tablet, keys);
    ASSERT_TRUE(rowset->load().ok());
    const size_t initial_rowset_size = rowset->segment_memory_usage();
    const size_t initial_charge = initial_rowset_size + LRUCache::key_handle_size(CacheKey(rowset->rowset_id_str()));
    _metadata_cache->set_capacity(initial_charge * kNumShards);
    _metadata_cache->cache_rowset(rowset.get());
    ASSERT_EQ(initial_charge, _metadata_cache->get_memory_usage());

    auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
    TabletReader reader(tablet, Version(0, 0), schema, std::vector<RowsetSharedPtr>{rowset});
    ASSERT_TRUE(reader.prepare().ok());
    std::vector<std::string> short_keys;
    ASSERT_TRUE(rowset->get_segment_sk_index(&short_keys).ok());
    ASSERT_GT(rowset->segment_memory_usage(), initial_rowset_size);
    ASSERT_EQ(initial_charge, _metadata_cache->get_memory_usage());

    // Refreshing the charge now exceeds the shard capacity. Its deleter calls
    // Rowset::close(), which must run after release() drops the rowset lock.
    reader.close();
    ASSERT_EQ(0, rowset->refs_by_reader());
    ASSERT_EQ(0, _metadata_cache->get_memory_usage());
    ASSERT_EQ(0, rowset->segment_memory_usage());
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
