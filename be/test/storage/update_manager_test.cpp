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

#include "storage/update_manager.h"

#include <gtest/gtest.h>

#include <memory>

#include "fs/fs_util.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/kv_store.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "testutil/assert.h"

using namespace std;

namespace starrocks {

class UpdateManagerTest : public testing::Test {
public:
    void SetUp() override {
        _root_path = "./olap_update_manager_test";
        fs::remove_all(_root_path);
        fs::create_directories(_root_path);
        _meta = std::make_unique<KVStore>(_root_path);
        ASSERT_TRUE(_meta->init().ok());
        ASSERT_TRUE(fs::is_directory(_root_path + "/meta").value());
        _root_mem_tracker = std::make_unique<MemTracker>(-1, "update");
        _update_manager = std::make_unique<UpdateManager>(_root_mem_tracker.get());
    }

    RowsetSharedPtr create_rowset(const vector<int64_t>& keys, Column* one_delete = nullptr) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = _tablet->tablet_id();
        writer_context.tablet_schema_hash = _tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = _tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = _tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (long key : keys) {
            cols[0]->append_datum(Datum(key));
            cols[1]->append_datum(Datum((int16_t)(key % 100 + 1)));
            cols[2]->append_datum(Datum((int32_t)(key % 1000 + 2)));
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
        ASSERT_TRUE(st.ok()) << st.to_string();
        _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        ASSERT_TRUE(_tablet);
    }

    void TearDown() override {
        _update_manager.reset();
        _root_mem_tracker.reset();
        _meta.reset();
        fs::remove_all(_root_path);
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
    srand(time(nullptr));
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

TEST_F(UpdateManagerTest, testSetEmptyCachedDeltaColumnGroup) {
    srand(time(nullptr));
    create_tablet(rand(), rand());
    TabletSegmentId tsid;
    tsid.tablet_id = _tablet->tablet_id();
    tsid.segment_id = 1;
    _update_manager->set_cached_empty_delta_column_group(_tablet->data_dir()->get_meta(), tsid);
    // search this empty dcg
    DeltaColumnGroupList dcgs;
    // search in cache
    ASSERT_TRUE(_update_manager->get_cached_delta_column_group(tsid, 1, &dcgs));
    ASSERT_TRUE(dcgs.empty());
    _update_manager->get_delta_column_group(_tablet->data_dir()->get_meta(), tsid, 1, &dcgs);
    ASSERT_TRUE(dcgs.empty());
}

} // namespace starrocks
