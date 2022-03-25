// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/update_manager.h"

#include <gtest/gtest.h>

#include "runtime/mem_tracker.h"
#include "storage/del_vector.h"
#include "storage/kv_store.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/vectorized/rowset_options.h"
#include "storage/storage_engine.h"
#include "storage/vectorized/chunk_helper.h"
#include "testutil/assert.h"
#include "util/file_utils.h"

using namespace std;

namespace starrocks {

class UpdateManagerTest : public testing::Test {
public:
    void SetUp() override {
        _root_path = "./ut_dir/olap_update_manager_test";
        FileUtils::remove_all(_root_path);
        FileUtils::create_dir(_root_path);
        _meta.reset(new KVStore(_root_path));
        ASSERT_TRUE(_meta->init().ok());
        ASSERT_TRUE(FileUtils::is_dir(_root_path + "/meta"));
        _root_mem_tracker.reset(new MemTracker(-1, "update"));
        _update_manager.reset(new UpdateManager(_root_mem_tracker.get()));
    }

    RowsetSharedPtr create_rowset(const vector<int64_t>& keys, vectorized::Column* one_delete = nullptr) {
        RowsetWriterContext writer_context(kDataFormatV2, config::storage_format_version);
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = _tablet->tablet_id();
        writer_context.tablet_schema_hash = _tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_type = BETA_ROWSET;
        writer_context.rowset_path_prefix = _tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &_tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema());
        auto chunk = vectorized::ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (size_t i = 0; i < keys.size(); i++) {
            cols[0]->append_datum(vectorized::Datum(keys[i]));
            cols[1]->append_datum(vectorized::Datum((int16_t)(keys[i] % 100 + 1)));
            cols[2]->append_datum(vectorized::Datum((int32_t)(keys[i] % 1000 + 2)));
        }
        if (one_delete == nullptr) {
            CHECK_OK(writer->flush_chunk(*chunk));
        } else {
            CHECK_OK(writer->flush_chunk_with_deletes(*chunk, *one_delete));
        }
        return *writer->build();
    }

    void create_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 6;
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
        ASSERT_TRUE(st.ok()) << st.to_string();
        _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        ASSERT_TRUE(_tablet);
    }

    void TearDown() override {
        _update_manager.reset();
        _root_mem_tracker.reset();
        _meta.reset();
        FileUtils::remove_all(_root_path);
        if (_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
    }

private:
    std::string _root_path;
    unique_ptr<MemTracker> _root_mem_tracker = nullptr;
    unique_ptr<KVStore> _meta;
    unique_ptr<UpdateManager> _update_manager;
    TabletSharedPtr _tablet;
};

TEST_F(UpdateManagerTest, testDelVec) {
    TabletSegmentId rssid;
    rssid.tablet_id = 0;
    rssid.segment_id = 0;
    DelVector empty;
    DelVectorPtr delvec;
    vector<uint32_t> dels3 = {1, 3, 5, 70, 9000};
    empty.add_dels_as_new_version(dels3, 3, &delvec);
    _update_manager->set_del_vec_in_meta(_meta.get(), rssid, *delvec);
    _update_manager->set_cached_del_vec(rssid, delvec);
    ASSERT_EQ(delvec->memory_usage(), _root_mem_tracker->consumption());
    vector<uint32_t> dels5 = {2, 4, 6, 80, 9000};
    DelVectorPtr delvec5;
    delvec->add_dels_as_new_version(dels5, 5, &delvec5);
    _update_manager->set_del_vec_in_meta(_meta.get(), rssid, *delvec5);
    _update_manager->set_cached_del_vec(rssid, delvec5);
    ASSERT_EQ(delvec5->memory_usage(), _root_mem_tracker->consumption());
    DelVectorPtr tmp;
    _update_manager->get_latest_del_vec(_meta.get(), rssid, &tmp);
    ASSERT_EQ(5, tmp->version());
    _update_manager->get_del_vec(_meta.get(), rssid, 2, &tmp);
    ASSERT_TRUE(tmp->empty());
    _update_manager->get_del_vec(_meta.get(), rssid, 4, &tmp);
    ASSERT_EQ(3, tmp->version());
    _update_manager->get_latest_del_vec(_meta.get(), rssid, &tmp);
    ASSERT_EQ(5, tmp->version());
}

TEST_F(UpdateManagerTest, testExpireEntry) {
    srand(time(NULL));
    create_tablet(rand(), rand());
    // write
    const int N = 8000;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    // set cache a small expire_ms
    _update_manager->set_cache_expire_ms(1000);

    // this entry will be expired
    auto rs0 = create_rowset(keys);
    _update_manager->on_rowset_finished(_tablet.get(), rs0.get());
    ASSERT_GT(_update_manager->update_state_cache().size(), 0);
    const auto expiring_size = _update_manager->update_state_cache().size();

    // dynamic cache reach to the peak size
    auto rs1 = create_rowset(keys);
    _update_manager->on_rowset_finished(_tablet.get(), rs1.get());
    ASSERT_GT(_update_manager->update_state_cache().size(), expiring_size);
    const auto peak_size = _update_manager->update_state_cache().size();

    // sleep for a while, waiting the earlier entry to be expired.
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    _update_manager->expire_cache();

    // call on_rowset_finished again, the earilier entry expired, the size shrinked,
    // but the later entry is still existing.
    _update_manager->on_rowset_finished(_tablet.get(), rs1.get());
    ASSERT_GT(_update_manager->update_state_cache().size(), 0);
    const auto remaining_size = _update_manager->update_state_cache().size();
    ASSERT_EQ(peak_size - expiring_size, remaining_size);
}

} // namespace starrocks
