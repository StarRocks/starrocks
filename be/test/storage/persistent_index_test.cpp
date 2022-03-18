// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/persistent_index.h"

#include <gtest/gtest.h>

#include "env/env_memory.h"
#include "storage/fs/file_block_manager.h"
#include "storage/fs/fs_util.h"
#include "storage/storage_engine.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/coding.h"
#include "util/faststring.h"
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

PARALLEL_TEST(PersistentIndexTest, test_mutable_index_wal) {
    Env* env = Env::Default();
    const std::string kPersistentIndexDir = "./ut_dir/persistent_index_test";
    const std::string kIndexFile = "./ut_dir/persistent_index_test/index.l0.0.0";
    bool created;
    ASSERT_OK(env->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = uint64_t;
    PersistentIndexMetaPB index_meta;
    // insert
    vector<Key> keys;
    vector<IndexValue> values;
    int N = 1000000;
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        values.emplace_back(i * 2);
    }
    // erase
    vector<Key> erase_keys;
    for (int i = 0; i < N / 2; i++) {
        erase_keys.emplace_back(i);
    }
    // append invalid wal
    std::vector<Key> invalid_keys;
    std::vector<IndexValue> invalid_values;
    for (int i = 0; i < N / 2; i++) {
        invalid_keys.emplace_back(i);
        invalid_values.emplace_back(i * 2);
    }

    {
        ASSIGN_OR_ABORT(auto block_mgr, fs::fs_util::block_manager("posix://"));
        std::unique_ptr<fs::WritableBlock> wblock;
        fs::CreateBlockOptions wblock_opts({kIndexFile});
        ASSERT_TRUE((block_mgr->create_block(wblock_opts, &wblock)).ok());
        wblock->close();
    }

    {
        EditVersion version(0, 0);
        index_meta.set_key_size(sizeof(Key));
        index_meta.set_size(0);
        version.to_pb(index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());

        PersistentIndex index(kPersistentIndexDir);
        ASSERT_TRUE(index.create(sizeof(Key), version).ok());

        ASSERT_TRUE(index.load(index_meta).ok());
        ASSERT_TRUE(index.prepare(EditVersion(1, 0)).ok());
        ASSERT_TRUE(index.insert(N, keys.data(), values.data(), false).ok());
        ASSERT_TRUE(index.commit(&index_meta).ok());
        ASSERT_TRUE(index.on_commited().ok());

        std::vector<IndexValue> old_values(keys.size());
        ASSERT_TRUE(index.prepare(EditVersion(2, 0)).ok());
        ASSERT_TRUE(index.upsert(keys.size(), keys.data(), values.data(), old_values.data()).ok());
        ASSERT_TRUE(index.commit(&index_meta).ok());
        ASSERT_TRUE(index.on_commited().ok());

        vector<IndexValue> erase_old_values(erase_keys.size());
        ASSERT_TRUE(index.prepare(EditVersion(3, 0)).ok());
        ASSERT_TRUE(index.erase(erase_keys.size(), erase_keys.data(), erase_old_values.data()).ok());
        // update PersistentMetaPB in memory
        ASSERT_TRUE(index.commit(&index_meta).ok());
        ASSERT_TRUE(index.on_commited().ok());

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(index.get(keys.size(), keys.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < N / 2; i++) {
            ASSERT_EQ(NullIndexValue, get_values[i]);
        }
        for (int i = N / 2; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }

    {
        // rebuild mutableindex according PersistentIndexMetaPB
        PersistentIndex new_index(kPersistentIndexDir);
        ASSERT_TRUE(new_index.create(sizeof(Key), EditVersion(3, 0)).ok());
        ASSERT_TRUE(new_index.load(index_meta).ok());
        std::vector<IndexValue> get_values(keys.size());

        ASSERT_TRUE(new_index.get(keys.size(), keys.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < N / 2; i++) {
            ASSERT_EQ(NullIndexValue, get_values[i]);
        }
        for (int i = N / 2; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }

        // upsert key/value to new_index
        vector<IndexValue> old_values(invalid_keys.size());
        ASSERT_TRUE(new_index.prepare(EditVersion(4, 0)).ok());
        ASSERT_TRUE(new_index.upsert(invalid_keys.size(), invalid_keys.data(), invalid_values.data(), old_values.data())
                            .ok());
        ASSERT_TRUE(new_index.commit(&index_meta).ok());
        ASSERT_TRUE(new_index.on_commited().ok());
    }
    // rebuild mutableindex according to PersistentIndexMetaPB
    {
        PersistentIndex index(kPersistentIndexDir);
        ASSERT_TRUE(index.create(sizeof(Key), EditVersion(4, 0)).ok());
        ASSERT_TRUE(index.load(index_meta).ok());
        std::vector<IndexValue> get_values(keys.size());

        ASSERT_TRUE(index.get(keys.size(), keys.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }
    ASSERT_TRUE(FileUtils::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_mutable_flush_to_immutable) {
    using Key = uint64_t;
    int N = 200000;
    vector<Key> keys(N);
    vector<IndexValue> values(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
        values[i] = i * 2;
    }
    auto rs = MutableIndex::create(sizeof(Key));
    ASSERT_TRUE(rs.ok());
    std::unique_ptr<MutableIndex> idx = std::move(rs).value();

    // test insert
    ASSERT_TRUE(idx->insert(keys.size(), keys.data(), values.data()).ok());

    ASSERT_TRUE(idx->flush_to_immutable_index(".", EditVersion(1, 1)).ok());

    std::unique_ptr<fs::ReadableBlock> rb;
    ASSIGN_OR_ABORT(auto block_mgr, fs::fs_util::block_manager("posix://"));
    ASSERT_TRUE(block_mgr->open_block("./index.l1.1.1", &rb).ok());
    auto st_load = ImmutableIndex::load(std::move(rb));
    if (!st_load.ok()) {
        LOG(WARNING) << st_load.status();
    }
    ASSERT_TRUE(st_load.ok());
    auto& idx_loaded = st_load.value();
    KeysInfo keys_info;
    for (size_t i = 0; i < N; i++) {
        keys_info.key_idxes.emplace_back(i);
        uint64_t h = key_index_hash(&keys[i], sizeof(Key));
        keys_info.hashes.emplace_back(h);
    }
    vector<IndexValue> get_values(N);
    size_t num_found = 0;
    auto st_get = idx_loaded->get(N, keys.data(), keys_info, get_values.data(), &num_found);
    if (!st_get.ok()) {
        LOG(WARNING) << st_get;
    }
    ASSERT_TRUE(st_get.ok());
    ASSERT_EQ(N, num_found);
    for (size_t i = 0; i < N; i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }
    ASSERT_TRUE(idx_loaded->check_not_exist(N, keys.data()).is_already_exist());

    vector<Key> check_not_exist_keys(10);
    for (int i = 0; i < 10; i++) {
        check_not_exist_keys[i] = N + i;
    }
    ASSERT_TRUE(idx_loaded->check_not_exist(10, check_not_exist_keys.data()).ok());
}

} // namespace starrocks
