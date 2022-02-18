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

PARALLEL_TEST(PersistentIndexTest, test_mutable_index_wal) {
    Env* env = Env::Default();
    const std::string kPersistentIndexDir = "./ut_dir/persistent_index_test";
    const std::string kIndexFile = "./ut_dir/persistent_index_test/index_file";
    const std::string kIndexMetaFile = "./ut_dir/persistent_index_test/index_meta";
    ASSERT_TRUE(env->create_dir(kPersistentIndexDir).ok());

    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions wblock_opts({kIndexFile});
    ASSERT_TRUE((block_mgr->create_block(wblock_opts, &wblock)).ok());

    using Key = uint64_t;
    EditVersion version(0, 0);
    PersistentIndex index(kIndexFile);
    ASSERT_TRUE(index.create(sizeof(Key), version).ok());

    // insert
    vector<Key> keys;
    vector<IndexValue> values;
    int N = 10000;
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        values.emplace_back(i * 2);
    }
    ASSERT_TRUE(index.insert(keys.size(), keys.data(), values.data(), false).ok());
    ASSERT_TRUE(index.commit().ok());

    // erase
    vector<Key> erase_keys;
    for (int i = 0; i < N / 2; i++) {
        erase_keys.emplace_back(i);
    }
    vector<IndexValue> erase_old_values(erase_keys.size());
    ASSERT_TRUE(index.erase(erase_keys.size(), erase_keys.data(), erase_old_values.data()).ok());
    // update PersistentMetaPB in memory
    ASSERT_TRUE(index.commit().ok());

    // write persistent index meta to meta file
    std::unique_ptr<fs::WritableBlock> meta_block;
    fs::CreateBlockOptions meta_block_opts({kIndexMetaFile});
    ASSERT_TRUE((block_mgr->create_block(meta_block_opts, &meta_block)).ok());
    PersistentIndexMetaPB* meta = index.index_meta();
    auto value = meta->SerializeAsString();
    ASSERT_TRUE(meta_block->append(value).ok());

    // generate persistent_index from index_meta
    PersistentIndex new_index(kIndexFile);
    ASSERT_TRUE(new_index.create(sizeof(Key), version).ok());
    std::unique_ptr<fs::ReadableBlock> rblock;
    ASSERT_TRUE(block_mgr->open_block(kIndexMetaFile, &rblock).ok());
    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, value.length());
    PersistentIndexMetaPB new_index_meta;
    ASSERT_TRUE(rblock->read(0, buff).ok());
    new_index_meta.ParseFromString(buff);

    ASSERT_TRUE(new_index.load(new_index_meta).ok());
    std::vector<IndexValue> get_values(keys.size());

    ASSERT_TRUE(new_index.get(keys.size(), keys.data(), get_values.data()).ok());
    ASSERT_EQ(keys.size(), get_values.size());
    for (int i = 0; i < N / 2; i++) {
        ASSERT_EQ(NullIndexValue, get_values[i]);
    }
    for (int i = N / 2; i < values.size(); i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }
    rblock->close();
    meta_block->close();
    wblock->close();
    FileUtils::remove_all(kPersistentIndexDir);
}

} // namespace starrocks
