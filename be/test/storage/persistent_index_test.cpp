// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/persistent_index.h"

#include <gtest/gtest.h>

#include <cstdlib>

#include "fs/fs_memory.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset_update_state.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/coding.h"
#include "util/faststring.h"

namespace starrocks {
PARALLEL_TEST(PersistentIndexTest, test_fixlen_mutable_index) {
    using Key = uint64_t;
    const int N = 1000;
    vector<Key> keys;
    vector<Slice> key_slices;
    vector<IndexValue> values;
    vector<size_t> idxes;
    keys.reserve(N);
    key_slices.reserve(N);
    idxes.reserve(N);
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        values.emplace_back(i * 2);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
        idxes.emplace_back(i);
    }
    ASSIGN_OR_ABORT(auto idx, MutableIndex::create(sizeof(Key)));
    ASSERT_OK(idx->insert(key_slices.data(), values.data(), idxes));
    // insert duplicate should return error
    ASSERT_FALSE(idx->insert(key_slices.data(), values.data(), idxes).ok());

    // test get
    vector<IndexValue> get_values(keys.size());
    KeysInfo get_not_found;
    size_t get_num_found = 0;
    ASSERT_TRUE(idx->get(key_slices.data(), get_values.data(), &get_not_found, &get_num_found, idxes).ok());
    ASSERT_EQ(keys.size(), get_num_found);
    ASSERT_EQ(get_not_found.size(), 0);
    for (int i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }
    vector<Key> get2_keys;
    vector<Slice> get2_key_slices;
    get2_keys.reserve(N);
    get2_key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        get2_keys.emplace_back(i * 2);
        get2_key_slices.emplace_back((uint8_t*)(&get2_keys[i]), sizeof(Key));
    }
    vector<IndexValue> get2_values(get2_keys.size());
    KeysInfo get2_not_found;
    size_t get2_num_found = 0;
    // should only find 0,2,..N-2, not found: N,N+2, .. N*2-2
    ASSERT_TRUE(idx->get(get2_key_slices.data(), get2_values.data(), &get2_not_found, &get2_num_found, idxes).ok());
    ASSERT_EQ(N / 2, get2_num_found);

    // test erase
    vector<Key> erase_keys;
    vector<Slice> erase_key_slices;
    erase_keys.reserve(N);
    erase_key_slices.reserve(N);
    idxes.clear();
    size_t num = 0;
    for (int i = 0; i < N + 3; i += 3) {
        erase_keys.emplace_back(i);
        erase_key_slices.emplace_back((uint8_t*)(&erase_keys[num]), sizeof(Key));
        idxes.emplace_back(num);
        num++;
    }
    vector<IndexValue> erase_old_values(erase_keys.size());
    KeysInfo erase_not_found;
    size_t erase_num_found = 0;
    ASSERT_TRUE(idx->erase(erase_key_slices.data(), erase_old_values.data(), &erase_not_found, &erase_num_found, idxes)
                        .ok());
    ASSERT_EQ(erase_num_found, (N + 2) / 3);
    // N+2 not found
    ASSERT_EQ(erase_not_found.size(), 1);

    // test upsert
    vector<Key> upsert_keys(N, 0);
    vector<Slice> upsert_key_slices;
    vector<IndexValue> upsert_values(upsert_keys.size());
    upsert_key_slices.reserve(N);
    size_t expect_exists = 0;
    size_t expect_not_found = 0;
    idxes.clear();
    for (int i = 0; i < N; i++) {
        upsert_keys[i] = i * 2;
        if (i % 3 != 0 && i * 2 < N) {
            expect_exists++;
        }
        upsert_key_slices.emplace_back((uint8_t*)(&upsert_keys[i]), sizeof(Key));
        if (i * 2 >= N && i * 2 != N + 2) {
            expect_not_found++;
        }
        upsert_values[i] = i * 3;
        idxes.emplace_back(i);
    }
    vector<IndexValue> upsert_old_values(upsert_keys.size());
    KeysInfo upsert_not_found;
    size_t upsert_num_found = 0;
    ASSERT_TRUE(idx->upsert(upsert_key_slices.data(), upsert_values.data(), upsert_old_values.data(), &upsert_not_found,
                            &upsert_num_found, idxes)
                        .ok());
    ASSERT_EQ(upsert_num_found, expect_exists);
    ASSERT_EQ(upsert_not_found.size(), expect_not_found);
}

PARALLEL_TEST(PersistentIndexTest, test_small_varlen_mutable_index) {
    using Key = std::string;
    const int N = 1000;
    vector<Key> keys(N);
    vector<Slice> key_slices;
    vector<IndexValue> values;
    vector<size_t> idxes;
    key_slices.reserve(N);
    idxes.reserve(N);
    for (int i = 0; i < N; i++) {
        keys[i] = "test_varlen_" + std::to_string(i);
        values.emplace_back(i * 2);
        key_slices.emplace_back(keys[i]);
        idxes.push_back(i);
    }
    ASSIGN_OR_ABORT(auto idx, MutableIndex::create(0));
    ASSERT_OK(idx->insert(key_slices.data(), values.data(), idxes));
    // insert duplicate should return error
    ASSERT_FALSE(idx->insert(key_slices.data(), values.data(), idxes).ok());

    vector<IndexValue> get_values(keys.size());
    KeysInfo get_not_found;
    size_t get_num_found = 0;
    ASSERT_TRUE(idx->get(key_slices.data(), get_values.data(), &get_not_found, &get_num_found, idxes).ok());
    ASSERT_EQ(keys.size(), get_num_found);
    ASSERT_EQ(get_not_found.size(), 0);
    for (int i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }
    vector<Key> get2_keys(N);
    vector<Slice> get2_key_slices;
    get2_key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        get2_keys[i] = "test_varlen_" + std::to_string(i * 2);
        get2_key_slices.emplace_back(get2_keys[i]);
    }
    vector<IndexValue> get2_values(get2_keys.size());
    KeysInfo get2_not_found;
    size_t get2_num_found = 0;
    // should only find 0,2,..N-2, not found: N,N+2, .. N*2-2
    ASSERT_TRUE(idx->get(get2_key_slices.data(), get2_values.data(), &get2_not_found, &get2_num_found, idxes).ok());
    ASSERT_EQ(N / 2, get2_num_found);

    // test erase
    vector<Key> erase_keys;
    vector<Slice> erase_key_slices;
    erase_keys.reserve(N);
    erase_key_slices.reserve(N);
    idxes.clear();
    size_t num = 0;
    for (int i = 0; i < N + 3; i += 3) {
        erase_keys.emplace_back("test_varlen_" + std::to_string(i));
        erase_key_slices.emplace_back(erase_keys.back());
        idxes.emplace_back(num);
        num++;
    }
    vector<IndexValue> erase_old_values(erase_keys.size());
    KeysInfo erase_not_found;
    size_t erase_num_found = 0;
    ASSERT_TRUE(idx->erase(erase_key_slices.data(), erase_old_values.data(), &erase_not_found, &erase_num_found, idxes)
                        .ok());
    ASSERT_EQ(erase_num_found, (N + 2) / 3);
    // N+2 not found
    ASSERT_EQ(erase_not_found.size(), 1);

    // test upsert
    vector<Key> upsert_keys(N);
    vector<Slice> upsert_key_slices;
    vector<IndexValue> upsert_values(upsert_keys.size());
    upsert_key_slices.reserve(N);
    size_t expect_exists = 0;
    size_t expect_not_found = 0;
    idxes.clear();
    for (int i = 0; i < N; i++) {
        upsert_keys[i] = "test_varlen_" + std::to_string(i * 2);
        if (i % 3 != 0 && i * 2 < N) {
            expect_exists++;
        }
        upsert_key_slices.emplace_back(upsert_keys[i]);
        if (i * 2 >= N && i * 2 != N + 2) {
            expect_not_found++;
        }
        upsert_values[i] = i * 3;
        idxes.emplace_back(i);
    }
    vector<IndexValue> upsert_old_values(upsert_keys.size());
    KeysInfo upsert_not_found;
    size_t upsert_num_found = 0;
    ASSERT_TRUE(idx->upsert(upsert_key_slices.data(), upsert_values.data(), upsert_old_values.data(), &upsert_not_found,
                            &upsert_num_found, idxes)
                        .ok());
    ASSERT_EQ(upsert_num_found, expect_exists);
    ASSERT_EQ(upsert_not_found.size(), expect_not_found);
}

static std::string gen_random_string_of_random_length(size_t floor, size_t ceil) {
    auto randchars = []() -> char {
        const char charset[] =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";
        const size_t kMaxIndex = (sizeof(charset) - 1);
        return charset[rand() % kMaxIndex];
    };
    int length = rand() % (ceil - floor + 1) + floor;
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchars);
    return str;
}

PARALLEL_TEST(PersistentIndexTest, test_large_varlen_mutable_index) {
    using Key = std::string;
    const int N = 1000;
    vector<Key> keys(N);
    vector<Slice> key_slices;
    vector<IndexValue> values;
    vector<size_t> idxes;
    key_slices.reserve(N);
    idxes.reserve(N);
    for (int i = 0; i < N; i++) {
        keys[i] = gen_random_string_of_random_length(42, 128);
        values.emplace_back(i * 2);
        key_slices.emplace_back(keys[i]);
        idxes.push_back(i);
    }
    ASSIGN_OR_ABORT(auto idx, MutableIndex::create(0));
    ASSERT_OK(idx->insert(key_slices.data(), values.data(), idxes));
    // insert duplicate should return error
    ASSERT_FALSE(idx->insert(key_slices.data(), values.data(), idxes).ok());

    vector<IndexValue> get_values(keys.size());
    KeysInfo get_not_found;
    size_t get_num_found = 0;
    ASSERT_TRUE(idx->get(key_slices.data(), get_values.data(), &get_not_found, &get_num_found, idxes).ok());
    ASSERT_EQ(keys.size(), get_num_found);
    ASSERT_EQ(get_not_found.size(), 0);
    for (int i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }
    vector<Key> get2_keys(N);
    vector<Slice> get2_key_slices;
    get2_keys.reserve(N);
    get2_key_slices.reserve(N);
    for (int i = 0; i < N / 2; i++) {
        get2_keys[i] = keys[i];
        get2_key_slices.emplace_back(get2_keys[i]);
    }
    for (int i = N / 2; i < N; i++) {
        get2_keys[i] = gen_random_string_of_random_length(24, 41);
        get2_key_slices.emplace_back(get2_keys[i]);
    }
    vector<IndexValue> get2_values(get2_keys.size());
    KeysInfo get2_not_found;
    size_t get2_num_found = 0;
    // should only find 0,2,..N-2, not found: N,N+2, .. N*2-2
    ASSERT_TRUE(idx->get(get2_key_slices.data(), get2_values.data(), &get2_not_found, &get2_num_found, idxes).ok());
    ASSERT_EQ(N / 2, get2_num_found);

    // test erase
    vector<Key> erase_keys;
    vector<Slice> erase_key_slices;
    erase_keys.reserve(N);
    erase_key_slices.reserve(N);
    idxes.clear();
    size_t num = 0;
    for (int i = 0; i < N / 2; i++) {
        erase_keys.emplace_back(keys[i]);
        erase_key_slices.emplace_back(erase_keys[i]);
        idxes.emplace_back(num++);
    }
    erase_keys.emplace_back(gen_random_string_of_random_length(24, 41));
    erase_key_slices.emplace_back(erase_keys.back());
    idxes.emplace_back(num++);
    vector<IndexValue> erase_old_values(erase_keys.size());
    KeysInfo erase_not_found;
    size_t erase_num_found = 0;
    ASSERT_TRUE(idx->erase(erase_key_slices.data(), erase_old_values.data(), &erase_not_found, &erase_num_found, idxes)
                        .ok());
    ASSERT_EQ(erase_num_found, N / 2);
    // N+2 not found
    ASSERT_EQ(erase_not_found.size(), 1);

    // test upsert
    vector<Key> upsert_keys(N);
    vector<Slice> upsert_key_slices;
    vector<IndexValue> upsert_values(upsert_keys.size());
    upsert_key_slices.reserve(N);
    size_t expect_exists = 0;
    size_t expect_not_found = 0;
    idxes.clear();
    for (int i = 0; i < N / 2; i++) {
        upsert_keys[i] = keys[i];
        upsert_key_slices.emplace_back(upsert_keys[i]);
        upsert_values[i] = i * 3;
        idxes.emplace_back(i);
    }
    for (int i = N / 2; i < N; i++) {
        if (i % 2) {
            upsert_keys[i] = keys[i];
            expect_exists++;
        } else {
            upsert_keys[i] = gen_random_string_of_random_length(24, 41);
            expect_not_found++;
        }
        upsert_key_slices.emplace_back(upsert_keys[i]);
        upsert_values[i] = i * 3;
        idxes.emplace_back(i);
    }
    vector<IndexValue> upsert_old_values(upsert_keys.size());
    KeysInfo upsert_not_found;
    size_t upsert_num_found = 0;
    ASSERT_TRUE(idx->upsert(upsert_key_slices.data(), upsert_values.data(), upsert_old_values.data(), &upsert_not_found,
                            &upsert_num_found, idxes)
                        .ok());
    ASSERT_EQ(upsert_num_found, expect_exists);
    ASSERT_EQ(upsert_not_found.size(), expect_not_found);
}

PARALLEL_TEST(PersistentIndexTest, test_fixlen_mutable_index_wal) {
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_fixlen_mutable_index_wal";
    const std::string kIndexFile = "./PersistentIndexTest_test_fixlen_mutable_index_wal/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = uint64_t;
    PersistentIndexMetaPB index_meta;
    const int N = 1000000;
    // insert
    vector<Key> keys;
    vector<Slice> key_slices;
    vector<IndexValue> values;
    keys.reserve(N);
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        values.emplace_back(i * 2);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
    }

    const int second_n = 50000;
    vector<Key> second_keys;
    vector<Slice> second_key_slices;
    vector<IndexValue> second_values;
    second_keys.reserve(second_n);
    second_key_slices.reserve(second_n);
    for (int i = 0; i < second_n; i++) {
        second_keys.emplace_back(i);
        second_values.emplace_back(i * 3);
        second_key_slices.emplace_back((uint8_t*)(&second_keys[i]), sizeof(Key));
    }

    // erase
    vector<Key> erase_keys;
    vector<Slice> erase_key_slices;
    erase_keys.reserve(second_n);
    erase_key_slices.reserve(second_n);
    for (int i = 0; i < second_n; i++) {
        erase_keys.emplace_back(i);
        erase_key_slices.emplace_back((uint8_t*)(&erase_keys[i]), sizeof(Key));
    }

    // append invalid wal
    std::vector<Key> invalid_keys;
    std::vector<Slice> invalid_key_slices;
    std::vector<IndexValue> invalid_values;
    invalid_keys.reserve(second_n);
    invalid_key_slices.reserve(second_n);
    for (int i = 0; i < second_n; i++) {
        invalid_keys.emplace_back(i);
        invalid_values.emplace_back(i * 2);
        invalid_key_slices.emplace_back((uint8_t*)(&invalid_keys[i]), sizeof(Key));
    }

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    {
        EditVersion version(0, 0);
        index_meta.set_key_size(sizeof(Key));
        index_meta.set_size(0);
        version.to_pb(index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());

        std::vector<IndexValue> old_values(N);
        PersistentIndex index(kPersistentIndexDir);
        // flush l0 first
        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(1, 0), N));
        ASSERT_OK(index.upsert(N, key_slices.data(), values.data(), old_values.data()));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());

        //std::vector<IndexValue> old_values(second_keys.size());
        ASSERT_OK(index.prepare(EditVersion(2, 0), N));
        ASSERT_OK(index.upsert(second_n, second_key_slices.data(), second_values.data(), old_values.data()));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < second_n; i++) {
            ASSERT_EQ(second_values[i], get_values[i]);
        }
        for (int i = second_n; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }

        vector<IndexValue> erase_old_values(erase_keys.size());
        ASSERT_TRUE(index.prepare(EditVersion(3, 0), erase_keys.size()).ok());
        ASSERT_TRUE(index.erase(erase_keys.size(), erase_key_slices.data(), erase_old_values.data()).ok());
        // update PersistentMetaPB in memory
        ASSERT_TRUE(index.commit(&index_meta).ok());
        ASSERT_TRUE(index.on_commited().ok());

        std::vector<IndexValue> new_get_values(keys.size());
        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), new_get_values.data()).ok());
        ASSERT_EQ(keys.size(), new_get_values.size());
        for (int i = 0; i < second_n; i++) {
            ASSERT_EQ(NullIndexValue, new_get_values[i].get_value());
        }
        for (int i = second_n; i < values.size(); i++) {
            ASSERT_EQ(values[i], new_get_values[i]);
        }
    }

    {
        // rebuild mutableindex according PersistentIndexMetaPB
        PersistentIndex new_index(kPersistentIndexDir);
        //ASSERT_TRUE(new_index.create(sizeof(Key), EditVersion(3, 0)).ok());
        ASSERT_TRUE(new_index.load(index_meta).ok());
        std::vector<IndexValue> get_values(keys.size());

        ASSERT_TRUE(new_index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < second_n; i++) {
            ASSERT_EQ(NullIndexValue, get_values[i].get_value());
        }
        for (int i = second_n; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }

        // upsert key/value to new_index
        vector<IndexValue> old_values(invalid_keys.size());
        ASSERT_TRUE(new_index.prepare(EditVersion(4, 0), invalid_keys.size()).ok());
        ASSERT_TRUE(new_index
                            .upsert(invalid_keys.size(), invalid_key_slices.data(), invalid_values.data(),
                                    old_values.data())
                            .ok());
        ASSERT_TRUE(new_index.commit(&index_meta).ok());
        ASSERT_TRUE(new_index.on_commited().ok());
    }
    // rebuild mutableindex according to PersistentIndexMetaPB
    {
        PersistentIndex index(kPersistentIndexDir);
        //ASSERT_TRUE(index.create(sizeof(Key), EditVersion(4, 0)).ok());
        ASSERT_TRUE(index.load(index_meta).ok());
        std::vector<IndexValue> get_values(keys.size());

        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }

    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_l0_max_file_size) {
    int64_t l0_max_file_size = config::l0_max_file_size;
    config::l0_max_file_size = 200000;
    config::l0_max_mem_usage = 10240;
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_flush_l0_max_file_size";
    const std::string kIndexFile = "./PersistentIndexTest_test_flush_l0_max_file_size/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = uint64_t;
    PersistentIndexMetaPB index_meta;
    const int N = 40000;
    vector<Key> keys;
    vector<Slice> key_slices;
    vector<IndexValue> values;
    keys.reserve(N);
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        values.emplace_back(i * 2);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
    }

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    EditVersion version(0, 0);
    index_meta.set_key_size(sizeof(Key));
    index_meta.set_size(0);
    version.to_pb(index_meta.mutable_version());
    MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
    IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
    version.to_pb(snapshot_meta->mutable_version());

    std::vector<IndexValue> old_values(N);
    PersistentIndex index(kPersistentIndexDir);
    auto one_time_num = N / 4;
    // do snapshot twice, when cannot do flush_l0, which mean index_file checker works
    for (auto i = 0; i < 2; ++i) {
        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(i + 1, 0), one_time_num));
        ASSERT_OK(index.upsert(one_time_num, key_slices.data() + one_time_num * i, values.data() + one_time_num * i,
                               old_values.data() + one_time_num * i));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());
        ASSERT_TRUE(index_meta.l0_meta().wals().empty());
    }

    // do flush_l0,
    for (auto i = 2; i < 3; ++i) {
        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(i + 1, 0), one_time_num));
        ASSERT_OK(index.upsert(one_time_num, key_slices.data() + one_time_num * i, values.data() + one_time_num * i,
                               old_values.data() + one_time_num * i));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());
        ASSERT_TRUE(index_meta.l0_meta().snapshot().dumped_shard_idxes().empty());
    }

    // do snapshot, when cannot do merge_compaction, which mean index_file checker works
    auto loaded_num = one_time_num * 3;
    one_time_num /= 10;
    for (auto i = 3; i < 4; ++i) {
        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(i + 1, 0), one_time_num));
        ASSERT_OK(index.upsert(one_time_num, key_slices.data() + loaded_num, values.data() + loaded_num,
                               old_values.data() + loaded_num));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());
        ASSERT_TRUE(index_meta.l0_meta().wals().empty());
    }

    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());

    config::l0_max_file_size = l0_max_file_size;
}

PARALLEL_TEST(PersistentIndexTest, test_l0_max_memory_usage) {
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_l0_max_memory_usage";
    const std::string kIndexFile = "./PersistentIndexTest_test_l0_max_memory_usage/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = std::string;
    PersistentIndexMetaPB index_meta;
    const int N = 1000;
    int64_t total_size = 0;
    vector<Key> keys(N);
    vector<Slice> key_slices(N);
    vector<IndexValue> values(N);
    for (int i = 0; i < N; i++) {
        keys[i] = "test_varlen_" + std::to_string(i);
        values[i] = i;
        key_slices[i] = Slice(keys[i].data(), keys[i].size());
        total_size += keys[i].size() + 8;
    }

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    EditVersion version(0, 0);
    index_meta.set_key_size(0);
    index_meta.set_size(0);
    version.to_pb(index_meta.mutable_version());
    MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
    IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
    version.to_pb(snapshot_meta->mutable_version());

    const int64_t old_l0_max_mem_usage = config::l0_max_mem_usage;
    std::vector<IndexValue> old_values(N);
    PersistentIndex index(kPersistentIndexDir);
    config::l0_max_mem_usage = 100;
    for (auto t = 0; t < 100; ++t) {
        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(t + 1, 0), N));
        ASSERT_OK(index.upsert(N, key_slices.data(), values.data(), old_values.data()));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());
        ASSERT_TRUE(index.memory_usage() <= config::l0_max_mem_usage);
        for (int i = 0; i < N; i++) {
            keys[i] = "test_varlen_" + std::to_string(i + (t + 1) * N);
            total_size += keys[i].size() + 8;
        }
        if (total_size > 3 * config::l0_max_mem_usage) {
            // increase l0 limit
            config::l0_max_mem_usage *= 10;
        }
    }
    config::l0_max_mem_usage = old_l0_max_mem_usage;

    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_l0_min_memory_usage) {
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_l0_min_memory_usage";
    const std::string kIndexFile = "./PersistentIndexTest_test_l0_min_memory_usage/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = std::string;
    PersistentIndexMetaPB index_meta;
    const int N = 1000;
    int64_t total_size = 0;
    vector<Key> keys(N);
    vector<Slice> key_slices(N);
    vector<IndexValue> values(N);
    for (int i = 0; i < N; i++) {
        keys[i] = "test_varlen_" + std::to_string(i);
        values[i] = i;
        key_slices[i] = Slice(keys[i].data(), keys[i].size());
        total_size += keys[i].size() + 8;
    }

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    auto manager = StorageEngine::instance()->update_manager();
    // set memory tracker limit
    manager->mem_tracker()->set_limit(1);
    manager->mem_tracker()->consume(2);

    EditVersion version(0, 0);
    index_meta.set_key_size(0);
    index_meta.set_size(0);
    version.to_pb(index_meta.mutable_version());
    MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
    IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
    version.to_pb(snapshot_meta->mutable_version());

    const int64_t old_l0_min_mem_usage = config::l0_min_mem_usage;
    const int64_t old_l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 1000000000000;
    std::vector<IndexValue> old_values(N);
    PersistentIndex index(kPersistentIndexDir);
    config::l0_min_mem_usage = 100;
    for (auto t = 0; t < 100; ++t) {
        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(t + 1, 0), N));
        ASSERT_OK(index.upsert(N, key_slices.data(), values.data(), old_values.data()));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());
        ASSERT_TRUE(index.memory_usage() <= config::l0_min_mem_usage);
        for (int i = 0; i < N; i++) {
            keys[i] = "test_varlen_" + std::to_string(i + (t + 1) * N);
            total_size += keys[i].size() + 8;
        }
        if (total_size > 3 * config::l0_min_mem_usage) {
            // increase l0 limit
            config::l0_min_mem_usage *= 10;
        }
    }
    config::l0_min_mem_usage = old_l0_min_mem_usage;
    config::l0_max_mem_usage = old_l0_max_mem_usage;
    manager->mem_tracker()->set_limit(-1);

    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_small_varlen_mutable_index_snapshot) {
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_small_varlen_mutable_index_snapshot";
    const std::string kIndexFile = "./PersistentIndexTest_test_small_varlen_mutable_index_snapshot/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = std::string;
    PersistentIndexMetaPB index_meta;
    const int N = 10;

    // insert
    vector<Key> keys(N);
    vector<Slice> key_slices;
    vector<IndexValue> values;
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys[i] = "test_varlen_" + std::to_string(i);
        values.emplace_back(i);
        key_slices.emplace_back(keys[i]);
    }

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    {
        EditVersion version(0, 0);
        index_meta.set_key_size(0);
        index_meta.set_size(0);
        version.to_pb(index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());

        PersistentIndex index(kPersistentIndexDir);

        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(1, 0), N));
        ASSERT_OK(index.insert(N, key_slices.data(), values.data(), false));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }

    // rebuild mutableindex according to PersistentIndexMetaPB
    {
        PersistentIndex index(kPersistentIndexDir);
        ASSERT_TRUE(index.load(index_meta).ok());
        std::vector<IndexValue> get_values(keys.size());

        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < get_values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }
    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_small_varlen_mutable_index_snapshot_wal) {
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_small_varlen_mutable_index_snapshot_wal";
    const std::string kIndexFile = "./PersistentIndexTest_test_small_varlen_mutable_index_snapshot_wal/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = std::string;
    PersistentIndexMetaPB index_meta;
    const int N = 100000;

    // insert
    vector<Key> keys(N);
    vector<Slice> key_slices;
    vector<IndexValue> values;
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys[i] = "test_varlen_" + std::to_string(i);
        values.emplace_back(i);
        key_slices.emplace_back(keys[i]);
    }

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    {
        EditVersion version(0, 0);
        index_meta.set_key_size(0);
        index_meta.set_size(0);
        version.to_pb(index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());

        PersistentIndex index(kPersistentIndexDir);

        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(1, 0), N));
        ASSERT_OK(index.insert(N, key_slices.data(), values.data(), false));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());

        const int NUM_SNAPSHOT = 10;
        vector<Key> snapshot_keys(NUM_SNAPSHOT);
        vector<Slice> snapshot_key_slices;
        vector<IndexValue> snapshot_values;
        snapshot_key_slices.reserve(NUM_SNAPSHOT);
        for (int i = 0; i < NUM_SNAPSHOT; i++) {
            snapshot_keys[i] = "test_varlen_" + std::to_string(i);
            snapshot_values.emplace_back(i * 2);
            snapshot_key_slices.emplace_back(snapshot_keys[i]);
        }

        std::vector<IndexValue> old_values(NUM_SNAPSHOT);
        ASSERT_OK(index.prepare(EditVersion(2, 0), NUM_SNAPSHOT));
        ASSERT_OK(index.upsert(NUM_SNAPSHOT, snapshot_key_slices.data(), snapshot_values.data(), old_values.data()));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());

        const int NUM_WAL = 20000;
        vector<Key> wal_keys(NUM_WAL);
        vector<Slice> wal_key_slices;
        vector<IndexValue> wal_values;
        wal_key_slices.reserve(NUM_WAL);
        for (int i = NUM_SNAPSHOT; i < NUM_WAL + NUM_SNAPSHOT; i++) {
            wal_keys[i - NUM_SNAPSHOT] = "test_varlen_" + std::to_string(i);
            wal_values.emplace_back(i * 3);
            wal_key_slices.emplace_back(wal_keys[i - NUM_SNAPSHOT]);
        }

        config::l0_l1_merge_ratio = 1;
        std::vector<IndexValue> wal_old_values(NUM_WAL);
        ASSERT_OK(index.prepare(EditVersion(3, 0), NUM_WAL));
        ASSERT_OK(index.upsert(NUM_WAL, wal_key_slices.data(), wal_values.data(), wal_old_values.data()));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());

        for (int i = 0; i < NUM_SNAPSHOT; i++) {
            ASSERT_EQ(snapshot_values[i], get_values[i]);
        }
        for (int i = NUM_SNAPSHOT; i < NUM_WAL + NUM_SNAPSHOT; i++) {
            ASSERT_EQ(wal_values[i - NUM_SNAPSHOT], get_values[i]);
        }
        for (int i = NUM_WAL + NUM_SNAPSHOT; i < N; i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }
    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_small_varlen_mutable_index_wal) {
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_small_varlen_mutable_index_wal";
    const std::string kIndexFile = "./PersistentIndexTest_test_small_varlen_mutable_index_wal/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = std::string;
    PersistentIndexMetaPB index_meta;
    const int N = 1000000;
    const int wal_n = 50000;
    // insert
    vector<Key> keys(N);
    vector<Slice> key_slices;
    vector<IndexValue> values;
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys[i] = "test_varlen_" + std::to_string(i);
        values.emplace_back(i);
        key_slices.emplace_back(keys[i]);
    }
    // erase
    vector<Key> erase_keys(wal_n);
    vector<Slice> erase_key_slices;
    erase_key_slices.reserve(wal_n);
    for (int i = 0; i < wal_n; i++) {
        erase_keys[i] = "test_varlen_" + std::to_string(i);
        erase_key_slices.emplace_back(erase_keys[i]);
    }
    // append invalid wal
    std::vector<Key> invalid_keys(wal_n);
    std::vector<Slice> invalid_key_slices;
    std::vector<IndexValue> invalid_values;
    invalid_key_slices.reserve(wal_n);
    for (int i = 0; i < wal_n; i++) {
        invalid_keys[i] = "test_varlen_" + std::to_string(i);
        invalid_values.emplace_back(i);
        invalid_key_slices.emplace_back(invalid_keys[i]);
    }

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    {
        EditVersion version(0, 0);
        index_meta.set_key_size(0);
        index_meta.set_size(0);
        version.to_pb(index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());

        PersistentIndex index(kPersistentIndexDir);

        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(1, 0), N));
        ASSERT_OK(index.insert(N, key_slices.data(), values.data(), false));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());

        std::vector<IndexValue> old_values(keys.size());
        ASSERT_TRUE(index.prepare(EditVersion(2, 0), keys.size()).ok());
        ASSERT_TRUE(index.upsert(keys.size(), key_slices.data(), values.data(), old_values.data()).ok());
        ASSERT_TRUE(index.commit(&index_meta).ok());
        ASSERT_TRUE(index.on_commited().ok());

        vector<IndexValue> erase_old_values(erase_keys.size());
        ASSERT_TRUE(index.prepare(EditVersion(3, 0), erase_keys.size()).ok());
        ASSERT_TRUE(index.erase(erase_keys.size(), erase_key_slices.data(), erase_old_values.data()).ok());
        // update PersistentMetaPB in memory
        ASSERT_TRUE(index.commit(&index_meta).ok());
        ASSERT_TRUE(index.on_commited().ok());

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < wal_n; i++) {
            ASSERT_EQ(NullIndexValue, get_values[i].get_value());
        }
        for (int i = wal_n; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }

    {
        // rebuild mutableindex according PersistentIndexMetaPB
        PersistentIndex new_index(kPersistentIndexDir);
        ASSERT_TRUE(new_index.load(index_meta).ok());

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(new_index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < wal_n; i++) {
            ASSERT_EQ(NullIndexValue, get_values[i].get_value());
        }
        for (int i = wal_n; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }

        // upsert key/value to new_index
        vector<IndexValue> old_values(invalid_keys.size());
        ASSERT_TRUE(new_index.prepare(EditVersion(4, 0), invalid_keys.size()).ok());
        ASSERT_TRUE(new_index
                            .upsert(invalid_keys.size(), invalid_key_slices.data(), invalid_values.data(),
                                    old_values.data())
                            .ok());
        ASSERT_TRUE(new_index.commit(&index_meta).ok());
        ASSERT_TRUE(new_index.on_commited().ok());
    }
    // rebuild mutableindex according to PersistentIndexMetaPB
    {
        PersistentIndex index(kPersistentIndexDir);
        ASSERT_TRUE(index.load(index_meta).ok());
        std::vector<IndexValue> get_values(keys.size());

        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }
    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_large_varlen_mutable_index_wal) {
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_large_varlen_mutable_index_wal";
    const std::string kIndexFile = "./PersistentIndexTest_test_large_varlen_mutable_index_wal/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));
    using Key = std::string;
    PersistentIndexMetaPB index_meta;
    const int N = 300000;
    const int wal_n = 15000;
    // insert
    vector<Key> keys(N);
    vector<Slice> key_slices;
    vector<IndexValue> values;
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys[i] = gen_random_string_of_random_length(42, 128);
        values.emplace_back(i);
        key_slices.emplace_back(keys[i]);
    }
    // erase
    vector<Key> erase_keys(wal_n);
    vector<Slice> erase_key_slices;
    erase_key_slices.reserve(wal_n);
    for (int i = 0; i < wal_n; i++) {
        erase_keys[i] = keys[i];
        erase_key_slices.emplace_back(erase_keys[i]);
    }
    // append invalid wal
    std::vector<Key> invalid_keys(wal_n);
    std::vector<Slice> invalid_key_slices;
    std::vector<IndexValue> invalid_values;
    invalid_key_slices.reserve(wal_n);
    for (int i = 0; i < wal_n; i++) {
        invalid_keys[i] = keys[i];
        invalid_values.emplace_back(i);
        invalid_key_slices.emplace_back(invalid_keys[i]);
    }

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    {
        EditVersion version(0, 0);
        index_meta.set_key_size(0);
        index_meta.set_size(0);
        version.to_pb(index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());

        PersistentIndex index(kPersistentIndexDir);

        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(1, 0), N));
        ASSERT_OK(index.insert(N, key_slices.data(), values.data(), false));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());

        std::vector<IndexValue> old_values(keys.size());
        ASSERT_TRUE(index.prepare(EditVersion(2, 0), keys.size()).ok());
        ASSERT_TRUE(index.upsert(keys.size(), key_slices.data(), values.data(), old_values.data()).ok());
        ASSERT_TRUE(index.commit(&index_meta).ok());
        ASSERT_TRUE(index.on_commited().ok());

        vector<IndexValue> erase_old_values(erase_keys.size());
        ASSERT_TRUE(index.prepare(EditVersion(3, 0), erase_keys.size()).ok());
        ASSERT_TRUE(index.erase(erase_keys.size(), erase_key_slices.data(), erase_old_values.data()).ok());
        // update PersistentMetaPB in memory
        ASSERT_TRUE(index.commit(&index_meta).ok());
        ASSERT_TRUE(index.on_commited().ok());

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < wal_n; i++) {
            ASSERT_EQ(NullIndexValue, get_values[i].get_value());
        }
        for (int i = wal_n; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }

    {
        // rebuild mutableindex according PersistentIndexMetaPB
        PersistentIndex new_index(kPersistentIndexDir);
        ASSERT_TRUE(new_index.load(index_meta).ok());

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(new_index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < wal_n; i++) {
            ASSERT_EQ(NullIndexValue, get_values[i].get_value());
        }
        for (int i = wal_n; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }

        // upsert key/value to new_index
        vector<IndexValue> old_values(invalid_keys.size());
        ASSERT_TRUE(new_index.prepare(EditVersion(4, 0), invalid_keys.size()).ok());
        ASSERT_TRUE(new_index
                            .upsert(invalid_keys.size(), invalid_key_slices.data(), invalid_values.data(),
                                    old_values.data())
                            .ok());
        ASSERT_TRUE(new_index.commit(&index_meta).ok());
        ASSERT_TRUE(new_index.on_commited().ok());
    }
    // rebuild mutableindex according to PersistentIndexMetaPB
    {
        PersistentIndex index(kPersistentIndexDir);
        ASSERT_TRUE(index.load(index_meta).ok());
        std::vector<IndexValue> get_values(keys.size());

        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }
    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_flush_fixlen_to_immutable) {
    using Key = uint64_t;
    const int N = 200000;
    vector<Key> keys(N);
    vector<IndexValue> values(N);
    vector<Slice> key_slices;
    vector<size_t> idxes;
    key_slices.reserve(N);
    idxes.reserve(N);
    for (int i = 0; i < N; i++) {
        keys[i] = i;
        values[i] = i * 2;
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
        idxes.push_back(i);
    }
    auto rs = MutableIndex::create(sizeof(Key));
    ASSERT_TRUE(rs.ok());
    std::unique_ptr<MutableIndex> idx = std::move(rs).value();

    ASSERT_TRUE(idx->insert(key_slices.data(), values.data(), idxes).ok());

    auto writer = std::make_unique<ImmutableIndexWriter>();
    ASSERT_TRUE(writer->init("./index.l1.1.1", EditVersion(1, 1), false).ok());

    auto [nshard, npage_hint] = MutableIndex::estimate_nshard_and_npage((sizeof(Key) + 8) * N);
    auto nbucket = MutableIndex::estimate_nbucket(sizeof(Key), N, nshard, npage_hint);

    ASSERT_TRUE(idx->flush_to_immutable_index(writer, nshard, npage_hint, nbucket, true).ok());
    writer->finish();

    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString("posix://"));
    ASSIGN_OR_ABORT(auto rf, fs->new_random_access_file("./index.l1.1.1"));
    auto st_load = ImmutableIndex::load(std::move(rf));
    if (!st_load.ok()) {
        LOG(WARNING) << st_load.status();
    }
    ASSERT_TRUE(st_load.ok());
    auto& idx_loaded = st_load.value();
    KeysInfo keys_info;
    for (size_t i = 0; i < N; i++) {
        uint64_t h = key_index_hash(&keys[i], sizeof(Key));
        keys_info.key_infos.emplace_back(i, h);
    }
    vector<IndexValue> get_values(N);
    //size_t num_found = 0;
    KeysInfo found_keys_info;
    auto st_get = idx_loaded->get(N, key_slices.data(), keys_info, get_values.data(), &found_keys_info, sizeof(Key));
    if (!st_get.ok()) {
        LOG(WARNING) << st_get;
    }
    ASSERT_TRUE(st_get.ok());
    ASSERT_EQ(N, found_keys_info.size());
    for (size_t i = 0; i < N; i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }
    ASSERT_TRUE(idx_loaded->check_not_exist(N, key_slices.data(), sizeof(Key)).is_already_exist());

    vector<Key> check_not_exist_keys(10);
    vector<Slice> check_not_exist_key_slices(10);
    for (int i = 0; i < 10; i++) {
        check_not_exist_keys[i] = N + i;
        check_not_exist_key_slices[i] = Slice((uint8_t*)(&check_not_exist_keys[i]), sizeof(Key));
    }
    ASSERT_TRUE(idx_loaded->check_not_exist(10, check_not_exist_key_slices.data(), sizeof(Key)).ok());
    ASSERT_TRUE(fs::remove_all("./index.l1.1.1").ok());
}

PARALLEL_TEST(PersistentIndexTest, test_flush_varlen_to_immutable) {
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_flush_varlen_to_immutable";
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString("posix://"));
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));
    PersistentIndex index(kPersistentIndexDir);
    using Key = std::string;
    const int N = 200000;
    EditVersion version(1, 0);
    vector<Key> keys(N);
    vector<Slice> keys_slice(N);
    vector<IndexValue> values(N);
    for (int i = 0; i < N; i++) {
        keys[i] = "test_varlen_" + std::to_string(i);
        values[i] = i;
        keys_slice[i] = Slice(keys[i].data(), keys[i].size());
    }
    std::string l1_file_path = kPersistentIndexDir + "/index.l1.1.0";
    auto flush_st =
            index.test_flush_varlen_to_immutable_index(l1_file_path, version, N, keys_slice.data(), values.data());
    if (!flush_st.ok()) {
        LOG(WARNING) << flush_st;
    }
    ASSERT_TRUE(flush_st.ok());

    ASSIGN_OR_ABORT(auto rf, fs->new_random_access_file(l1_file_path));
    auto st_load = ImmutableIndex::load(std::move(rf));
    if (!st_load.ok()) {
        LOG(WARNING) << st_load.status();
    }
    ASSERT_TRUE(st_load.ok());
    auto& idx_loaded = st_load.value();
    KeysInfo keys_info;
    for (size_t i = 0; i < N; i++) {
        uint64_t h = key_index_hash(keys[i].data(), keys[i].size());
        keys_info.key_infos.emplace_back(i, h);
    }
    vector<IndexValue> get_values(N);
    KeysInfo found_keys_info;
    auto st_get = idx_loaded->get(N, keys_slice.data(), keys_info, get_values.data(), &found_keys_info, 0);
    if (!st_get.ok()) {
        LOG(WARNING) << st_get;
    }
    ASSERT_TRUE(st_get.ok());
    ASSERT_EQ(N, found_keys_info.size());
    for (size_t i = 0; i < N; i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }

    auto st_check = idx_loaded->check_not_exist(N, keys_slice.data(), 0);
    LOG(WARNING) << "check status is " << st_check;
    ASSERT_TRUE(idx_loaded->check_not_exist(N, keys_slice.data(), 0).is_already_exist());

    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
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

RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                              vectorized::Column* one_delete = nullptr) {
    RowsetWriterContext writer_context;
    RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
    writer_context.rowset_id = rowset_id;
    writer_context.tablet_id = tablet->tablet_id();
    writer_context.tablet_schema_hash = tablet->schema_hash();
    writer_context.partition_id = 0;
    writer_context.rowset_path_prefix = tablet->schema_hash_path();
    writer_context.rowset_state = COMMITTED;
    writer_context.tablet_schema = &tablet->tablet_schema();
    writer_context.version.first = 0;
    writer_context.version.second = 0;
    writer_context.segments_overlap = NONOVERLAPPING;
    std::unique_ptr<RowsetWriter> writer;
    EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
    auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
    auto chunk = ChunkHelper::new_chunk(schema, keys.size());
    auto& cols = chunk->columns();
    size_t size = keys.size();
    for (size_t i = 0; i < size; i++) {
        cols[0]->append_datum(vectorized::Datum(keys[i]));
        cols[1]->append_datum(vectorized::Datum((int16_t)(keys[i] % size + 1)));
        cols[2]->append_datum(vectorized::Datum((int32_t)(keys[i] % size + 2)));
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

void build_persistent_index_from_tablet(size_t N) {
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./persistent_index_test_build_from_tablet";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    TabletSharedPtr tablet = create_tablet(rand(), rand());
    ASSERT_EQ(1, tablet->updates()->version_history_count());
    std::vector<int64_t> keys(N);
    std::vector<Slice> key_slices;
    key_slices.reserve(N);
    for (int64_t i = 0; i < N; ++i) {
        keys[i] = i;
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(uint64_t));
    }

    RowsetSharedPtr rowset = create_rowset(tablet, keys);
    auto pool = StorageEngine::instance()->update_manager()->apply_thread_pool();
    auto version = 2;
    auto st = tablet->rowset_commit(version, rowset, 0);
    ASSERT_TRUE(st.ok()) << st.to_string();
    // Ensure that there is at most one thread doing the version apply job.
    ASSERT_LE(pool->num_threads(), 1);
    ASSERT_EQ(version, tablet->updates()->max_version());
    ASSERT_EQ(version, tablet->updates()->version_history_count());
    // call `get_applied_rowsets` to wait rowset apply finish
    std::vector<RowsetSharedPtr> rowsets;
    EditVersion full_edit_version;
    ASSERT_TRUE(tablet->updates()->get_applied_rowsets(version, &rowsets, &full_edit_version).ok());

    auto manager = StorageEngine::instance()->update_manager();
    auto index_entry = manager->index_cache().get_or_create(tablet->tablet_id());
    index_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
    auto& primary_index = index_entry->value();
    st = primary_index.load(tablet.get());
    if (!st.ok()) {
        LOG(WARNING) << "load primary index from tablet failed";
        ASSERT_TRUE(false);
    }

    RowsetUpdateState state;
    st = state.load(tablet.get(), rowset.get());
    if (!st.ok()) {
        LOG(WARNING) << "failed to load rowset update state: " << st.to_string();
        ASSERT_TRUE(false);
    }
    using ColumnUniquePtr = std::unique_ptr<vectorized::Column>;
    const std::vector<ColumnUniquePtr>& upserts = state.upserts();

    PersistentIndex persistent_index(kPersistentIndexDir);
    ASSERT_TRUE(persistent_index.load_from_tablet(tablet.get()).ok());

    // check data in persistent index
    for (size_t i = 0; i < upserts.size(); ++i) {
        auto& pks = *upserts[i];

        std::vector<uint64_t> primary_results;
        std::vector<uint64_t> persistent_results;
        primary_results.resize(pks.size());
        persistent_results.resize(pks.size());
        primary_index.get(pks, &primary_results);
        if (pks.is_binary()) {
            persistent_index.get(pks.size(), reinterpret_cast<const Slice*>(pks.raw_data()),
                                 reinterpret_cast<IndexValue*>(persistent_results.data()));
        } else {
            size_t key_size = primary_index.key_size();
            ASSERT_TRUE(key_size == sizeof(uint64_t));
            std::vector<Slice> col_key_slices;
            for (size_t i = 0; i < pks.size(); ++i) {
                col_key_slices.emplace_back(pks.raw_data() + i * key_size, key_size);
            }
            persistent_index.get(pks.size(), col_key_slices.data(),
                                 reinterpret_cast<IndexValue*>(persistent_results.data()));
        }

        ASSERT_EQ(primary_results.size(), persistent_results.size());
        for (size_t j = 0; j < primary_results.size(); ++j) {
            ASSERT_EQ(primary_results[i], persistent_results[i]);
        }
        primary_results.clear();
        persistent_results.clear();
    }

    {
        // load data from index file
        PersistentIndex persistent_index(kPersistentIndexDir);
        Status st = persistent_index.load_from_tablet(tablet.get());
        if (!st.ok()) {
            LOG(WARNING) << "build persistent index failed: " << st.to_string();
            ASSERT_TRUE(false);
        }
        for (size_t i = 0; i < upserts.size(); ++i) {
            auto& pks = *upserts[i];
            std::vector<uint64_t> primary_results;
            std::vector<uint64_t> persistent_results;
            primary_results.resize(pks.size());
            persistent_results.resize(pks.size());
            primary_index.get(pks, &primary_results);
            if (pks.is_binary()) {
                persistent_index.get(pks.size(), reinterpret_cast<const Slice*>(pks.raw_data()),
                                     reinterpret_cast<IndexValue*>(persistent_results.data()));
            } else {
                size_t key_size = primary_index.key_size();
                std::vector<Slice> col_key_slices;
                for (size_t i = 0; i < pks.size(); ++i) {
                    col_key_slices.emplace_back(pks.raw_data() + i * key_size, key_size);
                }
                persistent_index.get(pks.size(), col_key_slices.data(),
                                     reinterpret_cast<IndexValue*>(persistent_results.data()));
            }
            ASSERT_EQ(primary_results.size(), persistent_results.size());
            for (size_t j = 0; j < primary_results.size(); ++j) {
                ASSERT_EQ(primary_results[i], persistent_results[i]);
            }
            primary_results.clear();
            persistent_results.clear();
        }
    }

    manager->index_cache().release(index_entry);
    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_build_from_tablet) {
    auto manager = StorageEngine::instance()->update_manager();
    config::l0_max_mem_usage = 104857600;
    manager->mem_tracker()->set_limit(-1);
    // dump snapshot
    build_persistent_index_from_tablet(100000);
    // write wal
    build_persistent_index_from_tablet(250000);
    // flush l1
    config::l0_max_mem_usage = 1000000;
    build_persistent_index_from_tablet(1000000);
    // flush one tmp l1
    config::l0_max_mem_usage = 18874368;
    build_persistent_index_from_tablet(1000000);
    config::l0_max_mem_usage = 104857600;
}

PARALLEL_TEST(PersistentIndexTest, test_fixlen_replace) {
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_fixlen_replace";
    const std::string kIndexFile = "./PersistentIndexTest_test_fixlen_replace/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = uint64_t;
    PersistentIndexMetaPB index_meta;
    // insert
    vector<Key> keys;
    vector<Slice> key_slices;
    vector<IndexValue> values;
    vector<uint32_t> src_rssid;
    vector<IndexValue> replace_values;
    const int N = 1000000;
    keys.reserve(N);
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
        values.emplace_back(i * 2);
        replace_values.emplace_back(i * 3);
    }

    for (int i = 0; i < N / 2; i++) {
        src_rssid.emplace_back(0);
    }
    for (int i = N / 2; i < N; i++) {
        src_rssid.emplace_back(1);
    }

    ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));

    EditVersion version(0, 0);
    index_meta.set_key_size(sizeof(Key));
    index_meta.set_size(0);
    version.to_pb(index_meta.mutable_version());
    MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
    IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
    version.to_pb(snapshot_meta->mutable_version());

    PersistentIndex index(kPersistentIndexDir);

    ASSERT_TRUE(index.load(index_meta).ok());
    ASSERT_TRUE(index.prepare(EditVersion(1, 0), N).ok());
    ASSERT_TRUE(index.insert(N, key_slices.data(), values.data(), false).ok());
    ASSERT_TRUE(index.commit(&index_meta).ok());
    ASSERT_TRUE(index.on_commited().ok());

    std::vector<IndexValue> get_values(keys.size());
    ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
    ASSERT_EQ(keys.size(), get_values.size());
    for (int i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }

    //replace
    std::vector<uint32_t> failed(keys.size());
    Status st = index.try_replace(N, key_slices.data(), replace_values.data(), src_rssid, &failed);
    ASSERT_TRUE(st.ok());
    std::vector<IndexValue> new_get_values(keys.size());
    ASSERT_TRUE(index.get(keys.size(), key_slices.data(), new_get_values.data()).ok());
    ASSERT_EQ(keys.size(), new_get_values.size());
    for (int i = 0; i < N / 2; i++) {
        ASSERT_EQ(replace_values[i], new_get_values[i]);
    }
    for (int i = N / 2; i < N; i++) {
        ASSERT_EQ(values[i], new_get_values[i]);
    }
    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_varlen_replace) {
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_varlen_replace";
    const std::string kIndexFile = "./PersistentIndexTest_test_varlen_replace/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = std::string;
    PersistentIndexMetaPB index_meta;
    const int N = 10;
    // insert
    vector<Key> keys(N);
    vector<Slice> key_slices;
    vector<IndexValue> values;
    vector<uint32_t> src_rssid;
    vector<IndexValue> replace_values;

    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys[i] = gen_random_string_of_random_length(42, 128);
        key_slices.emplace_back(keys[i]);
        values.emplace_back(i * 2);
        replace_values.emplace_back(i * 3);
    }

    for (int i = 0; i < N / 2; i++) {
        src_rssid.emplace_back(0);
    }
    for (int i = N / 2; i < N; i++) {
        src_rssid.emplace_back(1);
    }

    ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));

    EditVersion version(0, 0);
    index_meta.set_key_size(0);
    index_meta.set_size(0);
    version.to_pb(index_meta.mutable_version());
    MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
    IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
    version.to_pb(snapshot_meta->mutable_version());

    PersistentIndex index(kPersistentIndexDir);

    ASSERT_TRUE(index.load(index_meta).ok());
    ASSERT_TRUE(index.prepare(EditVersion(1, 0), N).ok());
    ASSERT_TRUE(index.insert(N, key_slices.data(), values.data(), false).ok());
    ASSERT_TRUE(index.commit(&index_meta).ok());
    ASSERT_TRUE(index.on_commited().ok());

    std::vector<IndexValue> get_values(keys.size());
    ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
    ASSERT_EQ(keys.size(), get_values.size());
    for (int i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }

    //replace
    std::vector<uint32_t> failed(keys.size());
    Status st = index.try_replace(N, key_slices.data(), replace_values.data(), src_rssid, &failed);
    ASSERT_TRUE(st.ok());
    std::vector<IndexValue> new_get_values(keys.size());
    ASSERT_TRUE(index.get(keys.size(), key_slices.data(), new_get_values.data()).ok());
    ASSERT_EQ(keys.size(), new_get_values.size());
    for (int i = 0; i < N / 2; i++) {
        ASSERT_EQ(replace_values[i], new_get_values[i]);
    }
    for (int i = N / 2; i < N; i++) {
        ASSERT_EQ(values[i], new_get_values[i]);
    }
    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_get_move_buckets) {
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_get_move_buckets";
    PersistentIndex index(kPersistentIndexDir);
    std::vector<uint8_t> bucket_packs_in_page;
    bucket_packs_in_page.reserve(16);
    srand((int)time(nullptr));
    for (int32_t i = 0; i < 16; ++i) {
        bucket_packs_in_page.emplace_back(rand() % 32);
    }
    int32_t sum = 0;
    for (int32_t i = 0; i < 16; ++i) {
        sum += bucket_packs_in_page[i];
    }

    for (int32_t i = 0; i < 100; ++i) {
        int32_t target = rand() % sum;
        auto ret = index.test_get_move_buckets(target, bucket_packs_in_page.data());
        int32_t find_target = 0;
        for (signed char i : ret) {
            find_target += bucket_packs_in_page[i];
        }
        ASSERT_TRUE(find_target >= target);
    }
    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_flush_l1_advance) {
    config::l0_max_mem_usage = 1048576;
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_flush_l1_advance";
    const std::string kIndexFile = "./PersistentIndexTest_test_flush_l1_advance/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = std::string;
    PersistentIndexMetaPB index_meta;
    const int N = 500000;
    vector<Key> keys(N);
    vector<Slice> key_slices;
    vector<IndexValue> values;
    key_slices.reserve(N);

    for (int i = 0; i < N; i++) {
        keys[i] = "test_varlen_" + std::to_string(i);
        values.emplace_back(i);
        key_slices.emplace_back(keys[i]);
    }

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    {
        EditVersion version(0, 0);
        index_meta.set_key_size(0);
        index_meta.set_size(0);
        version.to_pb(index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());

        PersistentIndex index(kPersistentIndexDir);
        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(1, 0), N));
        const int N = 100000;
        for (int i = 0; i < 5; i++) {
            vector<Key> keys(N);
            vector<Slice> key_slices;
            vector<IndexValue> values;
            key_slices.reserve(N);
            for (int j = 0; j < N; j++) {
                keys[j] = "test_varlen_" + std::to_string(i * N + j);
                values.emplace_back(i * N + j);
                key_slices.emplace_back(keys[j]);
            }
            ASSERT_OK(index.insert(N, key_slices.data(), values.data(), false));
            std::vector<IndexValue> get_values(N);
            ASSERT_OK(index.get(N, key_slices.data(), get_values.data()));
            for (int j = 0; j < N; j++) {
                ASSERT_EQ(values[j], get_values[j]);
            }
        }
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());
    }

    {
        // reload persistent index
        PersistentIndex index(kPersistentIndexDir);
        ASSERT_OK(index.load(index_meta));
        std::vector<IndexValue> get_values(keys.size());
        ASSERT_OK(index.get(N, key_slices.data(), get_values.data()));
        for (int i = 0; i < N; i++) {
            if (values[i].get_value() != get_values[i].get_value()) {
                LOG(INFO) << "values[" << i << "] is " << values[i].get_value() << ", get_values[" << i << "] is "
                          << get_values[i].get_value();
            }
            ASSERT_EQ(values[i], get_values[i]);
        }
    }

    {
        PersistentIndex index(kPersistentIndexDir);
        ASSERT_OK(index.load(index_meta));
        const int N = 100000;
        ASSERT_OK(index.prepare(EditVersion(2, 0), N));
        for (int i = 0; i < 5; i++) {
            vector<IndexValue> values;
            key_slices.reserve(N);
            for (int j = 0; j < N; j++) {
                values.emplace_back(i * j);
            }
            std::vector<IndexValue> old_values(N);
            ASSERT_OK(index.upsert(N, key_slices.data(), values.data(), old_values.data()));
            std::vector<IndexValue> get_values(N);
            ASSERT_OK(index.get(N, key_slices.data(), get_values.data()));
            for (int j = 0; j < N; j++) {
                ASSERT_EQ(values[j], get_values[j]);
            }
        }
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());
    }

    {
        // reload persistent index
        const int N = 100000;
        PersistentIndex index(kPersistentIndexDir);
        ASSERT_OK(index.load(index_meta));
        std::vector<IndexValue> get_values(N);
        ASSERT_OK(index.get(N, key_slices.data(), get_values.data()));
        for (int i = 0; i < N; i++) {
            ASSERT_EQ(values[i].get_value() * 4, get_values[i].get_value());
        }
    }

    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_multi_l2_tmp_l1) {
    config::l0_max_mem_usage = 1024;
    config::max_tmp_l1_num = 10;
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_multi_l2_tmp_l1";
    const std::string kIndexFile = "./PersistentIndexTest_test_multi_l2_tmp_l1/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = std::string;
    PersistentIndexMetaPB index_meta;
    const int N = 1000000;
    const int wal_n = 200000;
    int64_t cur_version = 0;
    // insert
    vector<Key> keys(N);
    vector<Slice> key_slices;
    vector<IndexValue> values;
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys[i] = "test_varlen_" + std::to_string(i);
        values.emplace_back(i);
        key_slices.emplace_back(keys[i]);
    }
    // erase
    vector<Key> erase_keys(wal_n);
    vector<Slice> erase_key_slices;
    erase_key_slices.reserve(wal_n);
    for (int i = 0; i < wal_n; i++) {
        erase_keys[i] = "test_varlen_" + std::to_string(i);
        erase_key_slices.emplace_back(erase_keys[i]);
    }
    // append invalid wal
    std::vector<Key> invalid_keys(wal_n);
    std::vector<Slice> invalid_key_slices;
    std::vector<IndexValue> invalid_values;
    invalid_key_slices.reserve(wal_n);
    for (int i = 0; i < wal_n; i++) {
        invalid_keys[i] = "test_varlen_" + std::to_string(i);
        invalid_values.emplace_back(i);
        invalid_key_slices.emplace_back(invalid_keys[i]);
    }

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    {
        EditVersion version(cur_version++, 0);
        index_meta.set_key_size(0);
        index_meta.set_size(0);
        version.to_pb(index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());

        PersistentIndex index(kPersistentIndexDir);

        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(cur_version++, 0), N));
        ASSERT_OK(index.insert(N, key_slices.data(), values.data(), false));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());

        // generate 3 versions
        for (int i = 0; i < 3; i++) {
            std::vector<IndexValue> old_values(keys.size());
            ASSERT_TRUE(index.prepare(EditVersion(cur_version++, 0), keys.size()).ok());
            ASSERT_TRUE(index.upsert(keys.size(), key_slices.data(), values.data(), old_values.data()).ok());
            ASSERT_TRUE(index.commit(&index_meta).ok());
            ASSERT_TRUE(index.on_commited().ok());
        }

        vector<IndexValue> erase_old_values(erase_keys.size());
        ASSERT_TRUE(index.prepare(EditVersion(cur_version++, 0), erase_keys.size()).ok());
        ASSERT_TRUE(index.erase(erase_keys.size(), erase_key_slices.data(), erase_old_values.data()).ok());
        // update PersistentMetaPB in memory
        ASSERT_TRUE(index.commit(&index_meta).ok());
        ASSERT_TRUE(index.on_commited().ok());

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < wal_n; i++) {
            ASSERT_EQ(NullIndexValue, get_values[i].get_value());
        }
        for (int i = wal_n; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }

    {
        // rebuild mutableindex according PersistentIndexMetaPB
        PersistentIndex new_index(kPersistentIndexDir);
        ASSERT_TRUE(new_index.load(index_meta).ok());

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(new_index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < wal_n; i++) {
            ASSERT_EQ(NullIndexValue, get_values[i].get_value());
        }
        for (int i = wal_n; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }

        // upsert key/value to new_index
        vector<IndexValue> old_values(invalid_keys.size());
        ASSERT_TRUE(new_index.prepare(EditVersion(cur_version++, 0), invalid_keys.size()).ok());
        ASSERT_TRUE(new_index
                            .upsert(invalid_keys.size(), invalid_key_slices.data(), invalid_values.data(),
                                    old_values.data())
                            .ok());
        ASSERT_TRUE(new_index.commit(&index_meta).ok());
        ASSERT_TRUE(new_index.on_commited().ok());
    }
    // rebuild mutableindex according to PersistentIndexMetaPB
    {
        PersistentIndex index(kPersistentIndexDir);
        ASSERT_TRUE(index.load(index_meta).ok());
        std::vector<IndexValue> get_values(keys.size());

        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }
    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_multi_l2_not_tmp_l1) {
    config::l0_max_mem_usage = 1 * 1024 * 1024; // 1MB
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_multi_l2_not_tmp_l1";
    const std::string kIndexFile = "./PersistentIndexTest_test_multi_l2_not_tmp_l1/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = std::string;
    PersistentIndexMetaPB index_meta;
    // total size
    const int N = 100000;
    // upsert size
    const int M = 1000;
    // K means each step size
    const int K = N / M;
    int64_t cur_version = 0;

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    // build index
    EditVersion version(cur_version++, 0);
    index_meta.set_key_size(0);
    index_meta.set_size(0);
    version.to_pb(index_meta.mutable_version());
    MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
    IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
    version.to_pb(snapshot_meta->mutable_version());

    PersistentIndex index(kPersistentIndexDir);

    {
        // continue upsert key from 0 to N
        vector<Key> keys(M);
        vector<Slice> key_slices(M);
        vector<IndexValue> values(M);

        auto incre_key = [&](int step) {
            for (int i = 0; i < M; i++) {
                keys[i] = "test_varlen_" + std::to_string(i + step * M);
                values[i] = i + step * M;
                key_slices[i] = keys[i];
            }
        };

        // 1. upsert
        for (int i = 0; i < K; i++) {
            incre_key(i);
            std::vector<IndexValue> old_values(M);
            ASSERT_OK(index.load(index_meta));
            ASSERT_OK(index.prepare(EditVersion(cur_version++, 0), M));
            ASSERT_OK(index.upsert(M, key_slices.data(), values.data(), old_values.data()));
            ASSERT_OK(index.commit(&index_meta));
            ASSERT_OK(index.on_commited());
        }
    }

    auto verify_fn = [&](PersistentIndex& cur_index) {
        vector<Key> keys(N);
        vector<Slice> key_slices;
        vector<IndexValue> values;
        key_slices.reserve(N);
        for (int i = 0; i < N; i++) {
            keys[i] = "test_varlen_" + std::to_string(i);
            values.emplace_back(i);
            key_slices.emplace_back(keys[i]);
        }

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(cur_index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    };

    {
        // 2. verify
        verify_fn(index);
    }

    {
        // 3. verify after l2 compaction
        ASSERT_OK(index.TEST_major_compaction(index_meta));
        verify_fn(index);
    }

    {
        // rebuild mutableindex according to PersistentIndexMetaPB
        PersistentIndex index2(kPersistentIndexDir);
        ASSERT_TRUE(index2.load(index_meta).ok());
        verify_fn(index2);
    }

    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_multi_l2_not_tmp_l1_fixlen) {
    config::l0_max_mem_usage = 1 * 1024 * 1024; // 1MB
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_multi_l2_not_tmp_l1_fixlen";
    const std::string kIndexFile = "./PersistentIndexTest_test_multi_l2_not_tmp_l1_fixlen/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = uint64_t;
    PersistentIndexMetaPB index_meta;
    // total size
    const int N = 100000;
    // upsert size
    const int M = 1000;
    // K means each step size
    const int K = N / M;
    int64_t cur_version = 0;

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    // build index
    EditVersion version(cur_version++, 0);
    index_meta.set_key_size(sizeof(Key));
    index_meta.set_size(0);
    version.to_pb(index_meta.mutable_version());
    MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
    IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
    version.to_pb(snapshot_meta->mutable_version());

    auto verify_fn = [&](PersistentIndex& cur_index) {
        vector<Key> keys(N);
        vector<Slice> key_slices;
        vector<IndexValue> values;
        key_slices.reserve(N);
        for (int i = 0; i < N; i++) {
            keys[i] = i;
            values.emplace_back(i);
            key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
        }

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(cur_index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    };

    {
        PersistentIndex index(kPersistentIndexDir);
        // continue upsert key from 0 to N
        vector<Key> keys(M);
        vector<Slice> key_slices(M);
        vector<IndexValue> values(M);

        auto incre_key = [&](int step) {
            for (int i = 0; i < M; i++) {
                keys[i] = i + step * M;
                values[i] = i + step * M;
                key_slices[i] = Slice((uint8_t*)(&keys[i]), sizeof(Key));
            }
        };

        // 1. upsert
        for (int i = 0; i < K; i++) {
            incre_key(i);
            std::vector<IndexValue> old_values(M);
            ASSERT_OK(index.load(index_meta));
            ASSERT_OK(index.prepare(EditVersion(cur_version++, 0), M));
            ASSERT_OK(index.upsert(M, key_slices.data(), values.data(), old_values.data()));
            ASSERT_OK(index.commit(&index_meta));
            ASSERT_OK(index.on_commited());
        }

        // 2. verify
        verify_fn(index);

        // 3. verify after l2 compaction
        ASSERT_OK(index.TEST_major_compaction(index_meta));
        verify_fn(index);
    }

    {
        // rebuild mutableindex according to PersistentIndexMetaPB
        PersistentIndex index(kPersistentIndexDir);
        ASSERT_TRUE(index.load(index_meta).ok());
        verify_fn(index);
    }

    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_multi_l2_delete) {
    config::l0_max_mem_usage = 1024;
    config::max_tmp_l1_num = 10;
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_multi_l2_delete";
    const std::string kIndexFile = "./PersistentIndexTest_test_multi_l2_delete/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = std::string;
    PersistentIndexMetaPB index_meta;
    const int N = 1000000;
    const int DEL_N = 900000;
    int64_t cur_version = 0;
    // insert
    vector<Key> keys(N);
    vector<Slice> key_slices;
    vector<IndexValue> values;
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys[i] = "test_varlen_" + std::to_string(i);
        values.emplace_back(i);
        key_slices.emplace_back(keys[i]);
    }
    // erase
    vector<Key> erase_keys(DEL_N);
    vector<Slice> erase_key_slices;
    erase_key_slices.reserve(DEL_N);
    for (int i = 0; i < DEL_N; i++) {
        erase_keys[i] = "test_varlen_" + std::to_string(i);
        erase_key_slices.emplace_back(erase_keys[i]);
    }

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    {
        EditVersion version(cur_version++, 0);
        index_meta.set_key_size(0);
        index_meta.set_size(0);
        version.to_pb(index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());

        PersistentIndex index(kPersistentIndexDir);

        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(cur_version++, 0), N));
        ASSERT_OK(index.insert(N, key_slices.data(), values.data(), false));
        ASSERT_OK(index.commit(&index_meta));
        ASSERT_OK(index.on_commited());

        // generate 3 versions
        for (int i = 0; i < 3; i++) {
            std::vector<IndexValue> old_values(keys.size());
            ASSERT_TRUE(index.prepare(EditVersion(cur_version++, 0), keys.size()).ok());
            ASSERT_TRUE(index.upsert(keys.size(), key_slices.data(), values.data(), old_values.data()).ok());
            ASSERT_TRUE(index.commit(&index_meta).ok());
            ASSERT_TRUE(index.on_commited().ok());
        }

        vector<IndexValue> erase_old_values(erase_keys.size());
        ASSERT_TRUE(index.prepare(EditVersion(cur_version++, 0), erase_keys.size()).ok());
        // not trigger l0 advance flush
        config::l0_max_mem_usage = 100 * 1024 * 1024; // 100MB
        ASSERT_TRUE(index.erase(erase_keys.size(), erase_key_slices.data(), erase_old_values.data()).ok());
        // update PersistentMetaPB in memory
        // trigger l0 flush
        config::l0_max_mem_usage = 1024;
        ASSERT_TRUE(index.commit(&index_meta).ok());
        ASSERT_TRUE(index.on_commited().ok());

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < DEL_N; i++) {
            ASSERT_EQ(NullIndexValue, get_values[i].get_value());
        }
        for (int i = DEL_N; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }

    {
        // rebuild mutableindex according PersistentIndexMetaPB
        PersistentIndex new_index(kPersistentIndexDir);
        ASSERT_TRUE(new_index.load(index_meta).ok());

        std::vector<IndexValue> get_values(keys.size());
        ASSERT_TRUE(new_index.get(keys.size(), key_slices.data(), get_values.data()).ok());
        ASSERT_EQ(keys.size(), get_values.size());
        for (int i = 0; i < DEL_N; i++) {
            ASSERT_EQ(NullIndexValue, get_values[i].get_value());
        }
        for (int i = DEL_N; i < values.size(); i++) {
            ASSERT_EQ(values[i], get_values[i]);
        }
    }
    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

PARALLEL_TEST(PersistentIndexTest, test_l2_versions) {
    EditVersionWithMerge m1(INT64_MAX, INT64_MAX, true);
    EditVersionWithMerge m2(INT64_MAX, INT64_MAX, false);
    EditVersionWithMerge m3(10, 0, true);
    EditVersionWithMerge m4(10, 0, false);
    EditVersionWithMerge m5(11, 0, true);
    EditVersionWithMerge m6(11, 0, false);
    EditVersionWithMerge m7(11, 1, true);
    EditVersionWithMerge m8(11, 1, false);
    EditVersionWithMerge m9(11, 2, true);
    EditVersionWithMerge m10(11, 2, false);
    ASSERT_TRUE(m2 < m1);
    ASSERT_FALSE(m1 < m2);
    ASSERT_TRUE(m3 < m2);
    ASSERT_FALSE(m2 < m3);
    ASSERT_TRUE(m4 < m3);
    ASSERT_FALSE(m3 < m4);
    ASSERT_TRUE(m3 < m6);
    ASSERT_FALSE(m6 < m3);
    ASSERT_TRUE(m6 < m5);
    ASSERT_FALSE(m5 < m6);
    ASSERT_TRUE(m5 < m7);
    ASSERT_TRUE(m8 < m9);
    ASSERT_TRUE(m10 < m9);
}

PARALLEL_TEST(PersistentIndexTest, test_index_keep_delete) {
    config::l0_max_mem_usage = 1024;
    config::enable_pindex_minor_compaction = false;
    FileSystem* fs = FileSystem::Default();
    const std::string kPersistentIndexDir = "./PersistentIndexTest_test_index_keep_delete";
    const std::string kIndexFile = "./PersistentIndexTest_test_index_keep_delete/index.l0.0.0";
    bool created;
    ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

    using Key = std::string;
    PersistentIndexMetaPB index_meta;
    const int N = 10000;
    const int DEL_N = 90000;
    int64_t cur_version = 0;

    // insert
    vector<Key> keys(N);
    vector<Slice> key_slices;
    vector<IndexValue> values;
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys[i] = "test_varlen_" + std::to_string(i);
        values.emplace_back(i);
        key_slices.emplace_back(keys[i]);
    }
    // erase
    vector<Key> erase_keys(DEL_N);
    vector<Slice> erase_key_slices;
    erase_key_slices.reserve(DEL_N);
    for (int i = 0; i < DEL_N; i++) {
        erase_keys[i] = "test_varlen_" + std::to_string(i);
        erase_key_slices.emplace_back(erase_keys[i]);
    }

    {
        ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
        ASSERT_OK(wfile->close());
    }

    {
        EditVersion version(cur_version++, 0);
        index_meta.set_key_size(0);
        index_meta.set_size(0);
        version.to_pb(index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());

        PersistentIndex index(kPersistentIndexDir);

        ASSERT_OK(index.load(index_meta));
        ASSERT_OK(index.prepare(EditVersion(cur_version++, 0), N));
        // erase non-exist keys
        // flush advance
        vector<IndexValue> erase_old_values(erase_keys.size());
        ASSERT_TRUE(index.erase(erase_keys.size(), erase_key_slices.data(), erase_old_values.data()).ok());
        ASSERT_TRUE(index.commit(&index_meta).ok());
        ASSERT_TRUE(index.on_commited().ok());
        ASSERT_EQ(0, index.kv_num_in_immutable_index());

        ASSERT_OK(index.prepare(EditVersion(cur_version++, 0), N));
        // erase non-exist keys
        // flush advance
        ASSERT_TRUE(index.erase(erase_keys.size(), erase_key_slices.data(), erase_old_values.data()).ok());
        // not trigger flush advance
        config::l0_max_mem_usage = 100 * 1024 * 1024; // 100MB
        std::vector<IndexValue> old_values(keys.size());
        ASSERT_TRUE(index.upsert(keys.size(), key_slices.data(), values.data(), old_values.data()).ok());
        ASSERT_TRUE(index.commit(&index_meta).ok());
        ASSERT_TRUE(index.on_commited().ok());
        ASSERT_EQ(N, index.kv_num_in_immutable_index());
    }
    ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
}

} // namespace starrocks
