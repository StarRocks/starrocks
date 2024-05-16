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

#include "storage/lake/lake_persistent_index.h"

#include <gtest/gtest.h>

#include "test_util.h"
#include "testutil/assert.h"

namespace starrocks::lake {

class LakePersistentIndexTest : public TestBase {
public:
    LakePersistentIndexTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_unique<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }
    }

protected:
    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    constexpr static const char* const kTestDirectory = "test_lake_persistent_index";

    std::unique_ptr<TabletMetadata> _tablet_metadata;
};

TEST_F(LakePersistentIndexTest, test_basic_api) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
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
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->insert(N, key_slices.data(), values.data(), 0));
    // insert duplicate should return error
    // ASSERT_FALSE(index->insert(N, key_slices.data(), values.data(), 0).ok());

    // test get
    vector<IndexValue> get_values(keys.size());
    ASSERT_TRUE(index->get(N, key_slices.data(), get_values.data()).ok());
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
    vector<IndexValue> get2_values(keys.size());
    // should only find 0,2,..N-2, not found: N,N+2, .. N*2-2
    ASSERT_TRUE(index->get(N, get2_key_slices.data(), get2_values.data()).ok());
    for (int i = 0; i < N / 2; ++i) {
        ASSERT_EQ(values[i * 2], get2_values[i]);
    }
    for (int i = N / 2; i < N; ++i) {
        ASSERT_EQ(NullIndexValue, get2_values[i].get_value());
    }

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
    ASSERT_TRUE(index->erase(num, erase_key_slices.data(), erase_old_values.data()).ok());

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
    ASSERT_TRUE(index->upsert(N, upsert_key_slices.data(), upsert_values.data(), upsert_old_values.data()).ok());
    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_F(LakePersistentIndexTest, test_replace) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
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

    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->insert(N, key_slices.data(), values.data(), false));

    //replace
    std::vector<uint32_t> failed(keys.size());
    Status st = index->try_replace(N, key_slices.data(), replace_values.data(), N, &failed);
    ASSERT_TRUE(st.ok());
    std::vector<IndexValue> new_get_values(keys.size());
    ASSERT_TRUE(index->get(keys.size(), key_slices.data(), new_get_values.data()).ok());
    ASSERT_EQ(keys.size(), new_get_values.size());
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(replace_values[i], new_get_values[i]);
    }
    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_F(LakePersistentIndexTest, test_major_compaction) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    using Key = uint64_t;
    const int M = 5;
    const int N = 100;
    vector<Key> total_keys;
    vector<Slice> total_key_slices;
    vector<IndexValue> total_values;
    vector<size_t> idxes;
    total_key_slices.reserve(M * N);
    total_keys.reserve(M * N);
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    int k = 0;
    for (int i = 0; i < M; ++i) {
        vector<Key> keys;
        keys.reserve(N);
        vector<Slice> key_slices;
        key_slices.reserve(N);
        vector<IndexValue> values;
        values.reserve(N);
        for (int j = 0; j < N; j++) {
            keys.emplace_back(j);
            total_keys.emplace_back(j);
            key_slices.emplace_back((uint8_t*)(&keys[j]), sizeof(Key));
            total_key_slices.emplace_back((uint8_t*)(&total_keys[k]), sizeof(Key));
            values.emplace_back(j * 2);
            total_values.emplace_back(j * 2);
            ++k;
        }
        index->prepare(EditVersion(i, 0), 0);
        vector<IndexValue> upsert_old_values(keys.size());
        ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), upsert_old_values.data()));
    }

    index->minor_compact();
    vector<IndexValue> get_values(M * N);
    ASSERT_OK(index->get(M * N, total_key_slices.data(), get_values.data()));

    get_values.clear();
    get_values.reserve(M * N);
    auto txn_log = std::make_shared<TxnLogPB>();
    ASSERT_OK(index->major_compact(*_tablet_metadata, 0, txn_log.get()));
    ASSERT_OK(index->apply_opcompaction(txn_log->op_compaction()));
    ASSERT_OK(index->get(M * N, total_key_slices.data(), get_values.data()));
    for (int i = 0; i < M * N; i++) {
        ASSERT_EQ(total_values[i], get_values[i]);
    }
    config::l0_max_mem_usage = l0_max_mem_usage;
}

} // namespace starrocks::lake
