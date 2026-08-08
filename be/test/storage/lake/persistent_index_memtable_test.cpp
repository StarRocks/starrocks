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

#include "storage/lake/persistent_index_memtable.h"

#include <gtest/gtest.h>

#include "base/string/string_util.h"
#include "base/testutil/assert.h"
#include "storage/lake/persistent_index_sstable.h"

namespace starrocks::lake {

TEST(PersistentIndexMemtableTest, test_basic_api) {
    using Key = uint64_t;
    const int N = 1000;
    vector<Key> keys;
    vector<Slice> key_slices;
    vector<IndexValue> values;
    vector<size_t> idxes;
    keys.reserve(N);
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        values.emplace_back(i * 2);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
    }
    auto memtable = std::make_unique<PersistentIndexMemtable>();
    ASSERT_OK(memtable->insert(N, key_slices.data(), values.data(), -1));
    // insert duplicate should return error
    ASSERT_FALSE(memtable->insert(N, key_slices.data(), values.data(), -1).ok());

    // test get
    vector<IndexValue> get_values(keys.size());
    KeyIndexSet get_not_found;
    ASSERT_TRUE(memtable->get(N, key_slices.data(), get_values.data(), &get_not_found, -1).ok());
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
    KeyIndexSet get2_not_found;
    // should only find 0,2,..N-2, not found: N,N+2, .. N*2-2
    ASSERT_TRUE(memtable->get(N, get2_key_slices.data(), get2_values.data(), &get2_not_found, -1).ok());
    ASSERT_EQ(N / 2, get2_not_found.size());

    // test erase
    vector<Key> erase_keys;
    vector<Slice> erase_key_slices;
    erase_keys.reserve(N);
    erase_key_slices.reserve(N);
    size_t num = 0;
    for (int i = 0; i < N + 3; i += 3) {
        erase_keys.emplace_back(i);
        erase_key_slices.emplace_back((uint8_t*)(&erase_keys[num]), sizeof(Key));
        num++;
    }
    vector<IndexValue> erase_old_values(erase_keys.size());
    KeyIndexSet erase_not_found;
    size_t erase_num_found = 0;
    ASSERT_TRUE(memtable->erase(num, erase_key_slices.data(), erase_old_values.data(), &erase_not_found,
                                &erase_num_found, -1, 1)
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
    KeyIndexSet upsert_not_found;
    size_t upsert_num_found = 0;
    ASSERT_TRUE(memtable->upsert(N, upsert_key_slices.data(), upsert_values.data(), upsert_old_values.data(),
                                 &upsert_not_found, &upsert_num_found, -1)
                        .ok());
    ASSERT_EQ(upsert_num_found, expect_exists);
    ASSERT_EQ(upsert_not_found.size(), expect_not_found);
}

TEST(PersistentIndexMemtableTest, test_replace) {
    using Key = uint64_t;
    vector<Key> keys;
    vector<Slice> key_slices;
    vector<IndexValue> values;
    vector<IndexValue> replace_values;
    const int N = 10000;
    keys.reserve(N);
    key_slices.reserve(N);
    vector<size_t> replace_idxes;
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
        values.emplace_back(i * 2);
        replace_values.emplace_back(i * 3);
        replace_idxes.emplace_back(i);
    }

    auto memtable = std::make_unique<PersistentIndexMemtable>();
    ASSERT_OK(memtable->insert(N, key_slices.data(), values.data(), -1));

    //replace
    Status st = memtable->replace(key_slices.data(), replace_values.data(), replace_idxes, -1);
    ASSERT_TRUE(st.ok());
    std::vector<IndexValue> new_get_values(keys.size());
    KeyIndexSet get_not_found;
    ASSERT_TRUE(memtable->get(keys.size(), key_slices.data(), new_get_values.data(), &get_not_found, -1).ok());
    ASSERT_EQ(keys.size(), new_get_values.size());
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(replace_values[i], new_get_values[i]);
    }
}

TEST(PersistentIndexMemtableTest, test_memory_usage) {
    using ExpectedMap = phmap::btree_map<std::string, IndexValueWithVer, std::less<>>;

    const int N = 512;
    vector<std::string> keys;
    vector<Slice> key_slices;
    vector<IndexValue> values;
    keys.reserve(N);
    key_slices.reserve(N);
    values.reserve(N);
    for (int i = 0; i < N; i++) {
        if (i % 2 == 0) {
            keys.emplace_back("k" + std::to_string(i));
        } else {
            keys.emplace_back("persistent-index-key-" + std::to_string(i) + std::string(32, 'x'));
        }
        values.emplace_back(i * 2);
    }
    for (const auto& key : keys) {
        key_slices.emplace_back(key);
    }

    auto memtable = std::make_unique<PersistentIndexMemtable>();
    const size_t empty_memory_usage = memtable->memory_usage();
    ExpectedMap expected_map;
    size_t expected_keys_heap_size = 0;
    auto expected_memory_usage = [&]() {
        return empty_memory_usage + expected_map.bytes_used() - sizeof(expected_map) + expected_keys_heap_size;
    };

    for (int i = 0; i < N; i++) {
        ASSERT_OK(memtable->insert(1, &key_slices[i], &values[i], -1));
        auto [it, inserted] = expected_map.emplace(std::string_view(keys[i]), std::make_pair(-1, values[i]));
        ASSERT_TRUE(inserted);
        expected_keys_heap_size += is_string_heap_allocated(it->first) ? it->first.capacity() : 0;
        ASSERT_EQ(expected_memory_usage(), memtable->memory_usage()) << "insert index: " << i;
    }

    const auto memory_usage_before_update = memtable->memory_usage();
    ASSERT_FALSE(memtable->insert(1, key_slices.data(), values.data(), -1).ok());
    ASSERT_EQ(memory_usage_before_update, memtable->memory_usage());

    std::vector<size_t> replace_idxes{0};
    IndexValue replacement(9999);
    ASSERT_OK(memtable->replace(key_slices.data(), &replacement, replace_idxes, 1));
    ASSERT_EQ(memory_usage_before_update, memtable->memory_usage());

    IndexValue old_value;
    KeyIndexSet not_founds;
    size_t num_found = 0;
    ASSERT_OK(memtable->upsert(1, key_slices.data(), &replacement, &old_value, &not_founds, &num_found, 2));
    ASSERT_EQ(1, num_found);
    ASSERT_TRUE(not_founds.empty());
    ASSERT_EQ(memory_usage_before_update, memtable->memory_usage());

    size_t num_erased = 0;
    ASSERT_OK(memtable->erase(1, key_slices.data(), &old_value, &not_founds, &num_erased, 3, 1));
    ASSERT_EQ(1, num_erased);
    ASSERT_EQ(memory_usage_before_update, memtable->memory_usage());

    expected_map.clear();
    memtable->clear();
    expected_keys_heap_size = 0;
    ASSERT_EQ(empty_memory_usage, memtable->memory_usage());
    ASSERT_EQ(expected_memory_usage(), memtable->memory_usage());

    ASSERT_OK(memtable->insert(1, key_slices.data(), values.data(), -1));
    auto [it, inserted] = expected_map.emplace(std::string_view(keys[0]), std::make_pair(-1, values[0]));
    ASSERT_TRUE(inserted);
    expected_keys_heap_size += is_string_heap_allocated(it->first) ? it->first.capacity() : 0;
    ASSERT_EQ(expected_memory_usage(), memtable->memory_usage());
}

} // namespace starrocks::lake
