// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/persistent_index.h"

#include <gtest/gtest.h>

#include "gutil/strings/substitute.h"
#include "runtime/mem_tracker.h"
#include "storage/fs/file_block_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "testutil/parallel_test.h"
#include "util/file_utils.h"

namespace starrocks {

PARALLEL_TEST(PersistentIndexTest, test_mutable_index) {
    using Key = uint64_t;
    vector<Key> keys;
    vector<IndexValue> values;
    int N = 1000;
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        values.emplace_back(i * 2);
    }
    auto rs = MutableIndex::create(sizeof(Key));
    ASSERT_TRUE(rs.ok());
    std::unique_ptr<MutableIndex> idx = std::move(rs).value();

    // test insert
    ASSERT_TRUE(idx->insert(keys.size(), keys.data(), values.data()).ok());
    // insert duplicate should return error
    ASSERT_FALSE(idx->insert(keys.size(), keys.data(), values.data()).ok());

    // test get
    vector<IndexValue> get_values(keys.size());
    KeysInfo get_not_found;
    size_t get_num_found = 0;
    ASSERT_TRUE(idx->get(keys.size(), keys.data(), get_values.data(), &get_not_found, &get_num_found).ok());
    ASSERT_EQ(keys.size(), get_num_found);
    ASSERT_EQ(get_not_found.key_idxes.size(), 0);
    for (int i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }
    vector<Key> get2_keys;
    for (int i = 0; i < N; i++) {
        get2_keys.emplace_back(i * 2);
    }
    vector<IndexValue> get2_values(get2_keys.size());
    KeysInfo get2_not_found;
    size_t get2_num_found = 0;
    // should only find 0,2,..N-2, not found: N,N+2, .. N*2-2
    ASSERT_TRUE(
            idx->get(get2_keys.size(), get2_keys.data(), get2_values.data(), &get2_not_found, &get2_num_found).ok());
    ASSERT_EQ(N / 2, get2_num_found);

    // test erase
    vector<Key> erase_keys;
    for (int i = 0; i < N + 3; i += 3) {
        erase_keys.emplace_back(i);
    }
    vector<IndexValue> erase_old_values(erase_keys.size());
    KeysInfo erase_not_found;
    size_t erase_num_found = 0;
    ASSERT_TRUE(idx->erase(erase_keys.size(), erase_keys.data(), erase_old_values.data(), &erase_not_found,
                           &erase_num_found)
                        .ok());
    ASSERT_EQ(erase_num_found, (N + 2) / 3);
    // N+2 not found
    ASSERT_EQ(erase_not_found.key_idxes.size(), 1);

    // test upsert
    vector<Key> upsert_keys(N, 0);
    vector<IndexValue> upsert_values(upsert_keys.size());
    size_t expect_exists = 0;
    size_t expect_not_found = 0;
    for (int i = 0; i < N; i++) {
        upsert_keys[i] = i * 2;
        if (i % 3 != 0 && i * 2 < N) {
            expect_exists++;
        }
        if (i * 2 >= N && i * 2 != N + 2) {
            expect_not_found++;
        }
        upsert_values[i] = i * 3;
    }
    vector<IndexValue> upsert_old_values(upsert_keys.size());
    KeysInfo upsert_not_found;
    size_t upsert_num_found = 0;
    ASSERT_TRUE(idx->upsert(upsert_keys.size(), upsert_keys.data(), upsert_values.data(), upsert_old_values.data(),
                            &upsert_not_found, &upsert_num_found)
                        .ok());
    ASSERT_EQ(upsert_num_found, expect_exists);
    ASSERT_EQ(upsert_not_found.key_idxes.size(), expect_not_found);
}

TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
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
    CHECK(st.ok()) << st.to_string();
    return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
}

PARALLEL_TEST(PersistentIndexTest, test_mutable_index_wal) {
    Env* env = Env::Default();
    const std::string kPersistentIndexDir = "./ut_dir/persistent_index_test";
    const std::string kIndexFile = "./ut_dir/persistent_index_test/index_file";
    ASSERT_TRUE(env->create_dir(kPersistentIndexDir).ok());

    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions wblock_opts({kIndexFile});
    ASSERT_TRUE((block_mgr->create_block(wblock_opts, &wblock)).ok());

    using Key = uint64_t;
    EditVersion version(0, 0);
    PersistentIndex index(kIndexFile);
    ASSERT_TRUE(index.create(sizeof(Key), version).ok());
    TabletSharedPtr tablet = create_tablet(rand(), rand());

    // insert
    vector<Key> keys;
    vector<IndexValue> values;
    int N = 1000;
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        values.emplace_back(i * 2);
    }
    ASSERT_TRUE(index.insert(keys.size(), keys.data(), values.data(), false).ok());
    ASSERT_TRUE(index.commit().ok());
    ASSERT_TRUE(TabletMetaManager::write_persistent_index_meta(tablet->data_dir(), tablet->tablet_id(),
                                                               *(index.index_meta()))
                        .ok());
    // erase
    vector<Key> erase_keys;
    for (int i = 0; i < 100; i++) {
        erase_keys.emplace_back(i);
    }
    vector<IndexValue> erase_old_values(erase_keys.size());
    ASSERT_TRUE(index.erase(erase_keys.size(), erase_keys.data(), erase_old_values.data()).ok());
    // update PersistentMetaPB in memory
    ASSERT_TRUE(index.commit().ok());
    ASSERT_TRUE(TabletMetaManager::write_persistent_index_meta(tablet->data_dir(), tablet->tablet_id(),
                                                               *(index.index_meta()))
                        .ok());

    // generate persistent_index from index_meta
    PersistentIndex new_index(kIndexFile);
    ASSERT_TRUE(new_index.create(sizeof(Key), version).ok());
    ASSERT_TRUE(new_index.load(tablet.get()).ok());
    std::vector<IndexValue> get_values(keys.size());

    ASSERT_TRUE(new_index.get(keys.size(), keys.data(), get_values.data()).ok());
    ASSERT_EQ(keys.size(), get_values.size());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(NullIndexValue, get_values[i]);
    }
    for (int i = 100; i < values.size(); i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }
    ASSERT_TRUE(FileUtils::remove_all(kPersistentIndexDir).ok());
    wblock->close();
}

} // namespace starrocks
