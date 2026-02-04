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

#include "base/string/faststring.h"
#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
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
#include "util/coding.h"
#include "util/failpoint/fail_point.h"

namespace starrocks {

struct PersistentIndexSnapshotLoadTestParam {
    bool fixed_key_size = false; // if true, use fixed key size, otherwise use variable key size
};

class PersistentIndexSnapshotLoadTest : public testing::TestWithParam<PersistentIndexSnapshotLoadTestParam> {
public:
    virtual ~PersistentIndexSnapshotLoadTest() {}
    void SetUp() override {}

    template <typename KeyType>
    void test_index_snapshot_load_impl(bool load_only) {
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
                values.emplace_back(i * 2);
                key_slices.emplace_back(reinterpret_cast<const uint8_t*>(&keys[i]), sizeof(Key));
                idxes.push_back(i);
            } else {
                ASSERT_FALSE(GetParam().fixed_key_size)
                        << "KeyType is std::string, but GetParam().fixed_key_size is true.";
                keys[i] = "test_varlen_" + std::to_string(i);
                values.emplace_back(i * 2);
                key_slices.emplace_back(keys[i]);
                idxes.push_back(i);
            }
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

        ASSIGN_OR_ABORT(auto idx, MutableIndex::create(GetParam().fixed_key_size ? sizeof(Key) : 0));
        if (!load_only) {
            ASSERT_OK(idx->insert(key_slices.data(), values.data(), idxes));
            // check the index after insert
            check_fn(idx.get());
        }

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
            ASSERT_OK(idx->dump(ar));
            ASSERT_TRUE(ar.close());
        }

        ASSIGN_OR_ABORT(auto new_idx, MutableIndex::create(GetParam().fixed_key_size ? sizeof(Key) : 0));
        phmap::BinaryInputArchive ar_in(file_name.data());
        ASSERT_TRUE(new_idx->load_snapshot(ar_in).ok());
        // check the index after load
        check_fn(new_idx.get());
        // clean files
        (void)FileSystem::Default()->delete_file(file_name);
    }

    template <typename KeyType>
    void test_dump_load_snapshot_impl() {
        // skip if not fixed key size
        FileSystem* fs = FileSystem::Default();
        const std::string kPersistentIndexDir = "./PersistentIndexSnapshotLoadTest_test_dump_load_snapshot";
        const std::string kIndexFile = "./PersistentIndexSnapshotLoadTest_test_dump_load_snapshot/index.l0.0.0";
        bool created;
        ASSERT_OK(fs->create_dir_if_missing(kPersistentIndexDir, &created));

        using Key = KeyType;
        PersistentIndexMetaPB index_meta;
        const int N = 1000;
        vector<Key> keys;
        vector<Slice> key_slices;
        vector<IndexValue> values;
        keys.resize(N);
        for (int i = 0; i < N; i++) {
            if constexpr (std::is_same_v<Key, uint32_t>) {
                ASSERT_TRUE(GetParam().fixed_key_size)
                        << "KeyType is uint32_t, but GetParam().fixed_key_size is false.";
                keys[i] = i;
                values.emplace_back(i * 2);
                key_slices.emplace_back(reinterpret_cast<const uint8_t*>(&keys[i]), sizeof(Key));

            } else {
                ASSERT_FALSE(GetParam().fixed_key_size)
                        << "KeyType is std::string, but GetParam().fixed_key_size is true.";
                std::string prefix(64, 'A');
                keys[i] = prefix + "test_varlen_" + std::to_string(i);
                values.emplace_back(i * 2);
                key_slices.emplace_back(keys[i]);
            }
        }

        {
            ASSIGN_OR_ABORT(auto wfile, FileSystem::Default()->new_writable_file(kIndexFile));
            ASSERT_OK(wfile->close());
        }

        EditVersion version(0, 0);
        index_meta.set_key_size(GetParam().fixed_key_size ? sizeof(Key) : 0);
        index_meta.set_size(0);
        version.to_pb(index_meta.mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
        l0_meta->set_format_version(PERSISTENT_INDEX_VERSION_5);
        IndexSnapshotMetaPB* snapshot_meta = l0_meta->mutable_snapshot();
        version.to_pb(snapshot_meta->mutable_version());

        {
            std::vector<IndexValue> old_values(N, IndexValue(NullIndexValue));
            PersistentIndex index(kPersistentIndexDir);
            ASSERT_OK(index.load(index_meta));
            // 1. dump snapshot using version 1
            SyncPoint::GetInstance()->SetCallBack("FixedMutableIndex::dump::1", [](void* arg) { *(bool*)arg = true; });
            SyncPoint::GetInstance()->SetCallBack("ShardByLengthMutableIndex::dump::1",
                                                  [](void* arg) { *(bool*)arg = true; });
            SyncPoint::GetInstance()->EnableProcessing();
            DeferOp defer([]() {
                SyncPoint::GetInstance()->ClearCallBack("FixedMutableIndex::dump::1");
                SyncPoint::GetInstance()->ClearCallBack("ShardByLengthMutableIndex::dump::1");
                SyncPoint::GetInstance()->DisableProcessing();
            });
            index.test_force_dump();
            ASSERT_OK(index.prepare(EditVersion(1, 0), N));
            ASSERT_OK(index.upsert(N, key_slices.data(), values.data(), old_values.data()));
            ASSERT_OK(index.commit(&index_meta));
            ASSERT_OK(index.on_commited());
        }
        {
            // 2. load snapshot using version 2
            PersistentIndex index2(kPersistentIndexDir);
            ASSERT_OK(index2.load(index_meta));
            // check the index after load
            std::vector<IndexValue> get_values(keys.size());
            ASSERT_TRUE(index2.get(keys.size(), key_slices.data(), get_values.data()).ok());
            ASSERT_EQ(keys.size(), get_values.size());
            ASSERT_EQ(values.size(), get_values.size());
            for (int i = 0; i < values.size(); i++) {
                ASSERT_EQ(values[i], get_values[i]);
            }
        }

        ASSERT_TRUE(fs::remove_all(kPersistentIndexDir).ok());
    }
};

TEST_P(PersistentIndexSnapshotLoadTest, test_index_snapshot_load) {
    if (GetParam().fixed_key_size) {
        using Key = uint32_t; // fixed key size
        test_index_snapshot_load_impl<Key>(false);
    } else {
        using Key = std::string; // variable key size
        test_index_snapshot_load_impl<Key>(false);
    }
}

TEST_P(PersistentIndexSnapshotLoadTest, test_dump_load_snapshot) {
    if (GetParam().fixed_key_size) {
        using Key = uint32_t; // fixed key size
        test_dump_load_snapshot_impl<Key>();
    } else {
        using Key = std::string; // variable key size
        test_dump_load_snapshot_impl<Key>();
    }
}

INSTANTIATE_TEST_SUITE_P(PersistentIndexSnapshotLoadTest, PersistentIndexSnapshotLoadTest,
                         ::testing::Values(PersistentIndexSnapshotLoadTestParam{true}, // fixed key size
                                           PersistentIndexSnapshotLoadTestParam{false} // variable key size
                                           ));

} // namespace starrocks