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

#include <gtest/gtest.h>

#include <cstdlib>

#include "fs/fs_memory.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/persistent_index.h"
#include "storage/persistent_index_compaction_manager.h"
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
#include "util/failpoint/fail_point.h"
#include "util/faststring.h"

namespace starrocks {

struct PersistentIndexSnapshotLoadTestParam {
    bool fixed_key_size = false; // if true, use fixed key size, otherwise use variable key size
};

class PersistentIndexSnapshotLoadTest : public testing::TestWithParam<PersistentIndexSnapshotLoadTestParam> {
public:
    virtual ~PersistentIndexSnapshotLoadTest() {}
    void SetUp() override {}

    template <typename KeyType>
    void run_test_logic(bool load_only) {
        using Key = KeyType;
        const int N = 1000;
        vector<Key> keys(N);
        vector<Slice> key_slices;
        vector<IndexValue> values;
        vector<size_t> idxes;
        key_slices.reserve(N);
        idxes.reserve(N);
        for (int i = 0; i < N; i++) {
            if constexpr (std::is_same_v<Key, uint32_t>) {
                ASSERT_TRUE(GetParam().fixed_key_size)
                        << "KeyType is uint32_t, but GetParam().fixed_key_size is false.";
                keys[i] = i;
                values.emplace_back(i * 2); // 假设 IndexValue 可以这样构造
                key_slices.emplace_back(reinterpret_cast<const uint8_t*>(&keys[i]), sizeof(Key));
                idxes.push_back(i);
            } else {
                ASSERT_FALSE(GetParam().fixed_key_size)
                        << "KeyType is std::string, but GetParam().fixed_key_size is true.";
                keys[i] = "test_varlen_" + std::to_string(i);
                values.emplace_back(i * 2);
                key_slices.emplace_back(keys[i]); // Slice可以直接从std::string构造
                idxes.push_back(i);
            }
        }
        ASSIGN_OR_ABORT(auto idx, MutableIndex::create(GetParam().fixed_key_size ? sizeof(Key) : 0));
        if (!load_only) {
            ASSERT_OK(idx->insert(key_slices.data(), values.data(), idxes));
        }

        auto check_fn = [&](MutableIndex* idx) {
            vector<IndexValue> get_values(keys.size());
            KeysInfo get_not_found;
            size_t get_num_found = 0;
            ASSERT_TRUE(idx->get(key_slices.data(), get_values.data(), &get_not_found, &get_num_found, idxes).ok());
            ASSERT_EQ(keys.size(), get_num_found);
            ASSERT_EQ(get_not_found.size(), 0);
            for (int i = 0; i < values.size(); i++) {
                ASSERT_EQ(values[i], get_values[i]);
            }
        };
        // check the index after insert
        check_fn(idx.get());

        // dump to file
        std::string file_name = "";
        if (GetParam().fixed_key_size) {
            file_name = "./test_fixed_index_snapshot_load";
        } else {
            file_name = "./test_var_index_snapshot_load";
        }

        if (!load_only) {
            (void)FileSystem::Default()->delete_file(file_name);
            phmap::BinaryOutputArchive ar(file_name.data());
            ASSERT_TRUE(idx->dump(ar));
            ASSERT_TRUE(ar.close());
        }

        ASSIGN_OR_ABORT(auto new_idx, MutableIndex::create(GetParam().fixed_key_size ? sizeof(Key) : 0));
        phmap::BinaryInputArchive ar_in(file_name.data());
        ASSERT_TRUE(new_idx->load_snapshot(ar_in).ok());
        // check the index after load
        check_fn(new_idx.get());
    }
};

TEST_P(PersistentIndexSnapshotLoadTest, test_index_snapshot_load) {
    if (GetParam().fixed_key_size) {
        using Key = uint32_t; // fixed key size
        run_test_logic<Key>(false);
    } else {
        using Key = std::string; // variable key size
        run_test_logic<Key>(false);
    }
}

INSTANTIATE_TEST_SUITE_P(PersistentIndexSnapshotLoadTest, PersistentIndexSnapshotLoadTest,
                         ::testing::Values(PersistentIndexSnapshotLoadTestParam{true}, // fixed key size
                                           PersistentIndexSnapshotLoadTestParam{false} // variable key size
                                           ));

} // namespace starrocks