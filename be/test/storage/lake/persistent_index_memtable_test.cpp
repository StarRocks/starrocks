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

#include "testutil/assert.h"

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
    ASSERT_OK(memtable->insert(N, key_slices.data(), values.data()));
    // insert duplicate should return error
    ASSERT_FALSE(memtable->insert(N, key_slices.data(), values.data()).ok());

    // test get
    vector<IndexValue> get_values(keys.size());
    KeyIndexesInfo get_not_found;
    size_t get_num_found = 0;
    ASSERT_TRUE(memtable->get(N, key_slices.data(), get_values.data(), &get_not_found, &get_num_found).ok());
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
    KeyIndexesInfo get2_not_found;
    size_t get2_num_found = 0;
    // should only find 0,2,..N-2, not found: N,N+2, .. N*2-2
    ASSERT_TRUE(memtable->get(N, get2_key_slices.data(), get2_values.data(), &get2_not_found, &get2_num_found).ok());
    ASSERT_EQ(N / 2, get2_num_found);

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
    KeyIndexesInfo erase_not_found;
    size_t erase_num_found = 0;
    ASSERT_TRUE(
            memtable->erase(num, erase_key_slices.data(), erase_old_values.data(), &erase_not_found, &erase_num_found)
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
    KeyIndexesInfo upsert_not_found;
    size_t upsert_num_found = 0;
    ASSERT_TRUE(memtable->upsert(N, upsert_key_slices.data(), upsert_values.data(), upsert_old_values.data(),
                                 &upsert_not_found, &upsert_num_found)
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
    const int N = 1000000;
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
    ASSERT_OK(memtable->insert(N, key_slices.data(), values.data()));

    //replace
    Status st = memtable->replace(key_slices.data(), replace_values.data(), replace_idxes);
    ASSERT_TRUE(st.ok());
    std::vector<IndexValue> new_get_values(keys.size());
    KeyIndexesInfo get_not_found;
    size_t get_num_found = 0;
    ASSERT_TRUE(
            memtable->get(keys.size(), key_slices.data(), new_get_values.data(), &get_not_found, &get_num_found).ok());
    ASSERT_EQ(keys.size(), new_get_values.size());
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(replace_values[i], new_get_values[i]);
    }
}

} // namespace starrocks::lake
