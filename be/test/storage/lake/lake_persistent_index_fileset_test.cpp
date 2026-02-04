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

#include "base/testutil/assert.h"
#include "fs/fs_util.h"
#include "storage/lake/join_path.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/lake_persistent_index_parallel_compact_mgr.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/persistent_index_sstable_fileset.h"
#include "storage/persistent_index.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table_builder.h"
#include "test_util.h"

namespace starrocks::lake {

class LakePersistentIndexFilesetTest : public TestBase {
public:
    LakePersistentIndexFilesetTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_enable_persistent_index(true);
        _tablet_metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(PRIMARY_KEYS);
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

    // Helper function to create a test sstable file
    Status create_test_sstable(const std::string& filename, int start_key, int count, PersistentIndexSstablePB* sst_pb,
                               const UniqueId* fileset_id = nullptr) {
        sstable::Options options;
        std::string filepath = _tablet_mgr->sst_location(_tablet_metadata->id(), filename);
        ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(filepath));
        sstable::TableBuilder builder(options, wf.get());

        std::string first_key;
        std::string last_key;
        for (int i = 0; i < count; i++) {
            std::string key = fmt::format("key_{:016X}", start_key + i);
            if (i == 0) first_key = key;
            if (i == count - 1) last_key = key;
            IndexValue val(start_key + i);
            // Serialize as IndexValuesWithVerPB protobuf (required format)
            IndexValuesWithVerPB index_value_pb;
            auto* value = index_value_pb.add_values();
            value->set_version(1);
            value->set_rssid(val.get_rssid());
            value->set_rowid(val.get_rowid());
            builder.Add(Slice(key), Slice(index_value_pb.SerializeAsString()));
        }
        RETURN_IF_ERROR(builder.Finish());
        uint64_t filesize = builder.FileSize();
        RETURN_IF_ERROR(wf->sync()); // Ensure data is flushed
        RETURN_IF_ERROR(wf->close());

        // Fill the sstable pb
        sst_pb->set_filename(filename);
        sst_pb->set_filesize(filesize);
        if (count > 0) {
            sst_pb->mutable_range()->set_start_key(first_key);
            sst_pb->mutable_range()->set_end_key(last_key);
        }
        if (fileset_id != nullptr) {
            sst_pb->mutable_fileset_id()->CopyFrom(fileset_id->to_proto());
        }

        return Status::OK();
    }

    // Helper to create PersistentIndexSstable object
    StatusOr<std::unique_ptr<PersistentIndexSstable>> open_sstable(const PersistentIndexSstablePB& sst_pb) {
        RandomAccessFileOptions opts;
        std::string filepath = _tablet_mgr->sst_location(_tablet_metadata->id(), sst_pb.filename());
        LOG(INFO) << "Opening sstable file: " << filepath << ", size: " << sst_pb.filesize();

        ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(opts, filepath));
        auto sst = std::make_unique<PersistentIndexSstable>();

        auto st = sst->init(std::move(rf), sst_pb, nullptr, false);
        if (!st.ok()) {
            LOG(ERROR) << "Failed to init sstable: " << st.message();
            return st;
        }

        LOG(INFO) << "Sstable opened successfully, has_range: " << sst->sstable_pb().has_range();
        return sst;
    }

    constexpr static const char* const kTestDirectory = "test_lake_persistent_index_fileset";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
};

// ============================================================================
// Tests for PersistentIndexSstableFileset basic functionality
// ============================================================================

TEST_F(LakePersistentIndexFilesetTest, test_fileset_init_single_sstable) {
    auto fileset_id = UniqueId::gen_uid();
    PersistentIndexSstablePB sst_pb;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst_pb, &fileset_id));

    // Verify sst_pb has required fields
    ASSERT_TRUE(sst_pb.has_fileset_id());
    ASSERT_TRUE(sst_pb.has_range());
    ASSERT_GT(sst_pb.filesize(), 0);

    ASSIGN_OR_ABORT(auto sst, open_sstable(sst_pb));
    ASSERT_TRUE(sst != nullptr);

    // Verify the sstable was properly initialized
    ASSERT_TRUE(sst->sstable_pb().has_range());
    ASSERT_TRUE(sst->sstable_pb().has_fileset_id());

    PersistentIndexSstableFileset fileset;
    ASSERT_OK(fileset.init(sst));

    ASSERT_EQ(fileset_id.to_string(), fileset.fileset_id().to_string());
    ASSERT_FALSE(fileset.is_standalone_sstable());
}

TEST_F(LakePersistentIndexFilesetTest, test_fileset_init_multiple_sstables_in_order) {
    auto fileset_id = UniqueId::gen_uid();
    std::vector<PersistentIndexSstablePB> sst_pbs;
    std::vector<std::unique_ptr<PersistentIndexSstable>> sstables;

    // Create 3 non-overlapping sstables
    for (int i = 0; i < 3; i++) {
        PersistentIndexSstablePB sst_pb;
        ASSERT_OK(create_test_sstable(fmt::format("test_sst_{}.sst", i), i * 1000, 100, &sst_pb, &fileset_id));
        sst_pbs.push_back(sst_pb);
        ASSIGN_OR_ABORT(auto sst, open_sstable(sst_pb));
        sstables.push_back(std::move(sst));
    }

    PersistentIndexSstableFileset fileset;
    ASSERT_OK(fileset.init(sstables));

    ASSERT_EQ(fileset_id.to_string(), fileset.fileset_id().to_string());
    ASSERT_FALSE(fileset.is_standalone_sstable());

    // Verify all sstables are stored
    PersistentIndexSstableMetaPB retrieved_pbs;
    fileset.get_all_sstable_pbs(&retrieved_pbs);
    ASSERT_EQ(3, retrieved_pbs.sstables_size());
}

TEST_F(LakePersistentIndexFilesetTest, test_fileset_init_overlapping_sstables_error) {
    auto fileset_id = UniqueId::gen_uid();
    std::vector<std::unique_ptr<PersistentIndexSstable>> sstables;

    // Create 2 overlapping sstables (0-99 and 50-149)
    PersistentIndexSstablePB sst_pb1, sst_pb2;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst_pb1, &fileset_id));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 50, 100, &sst_pb2, &fileset_id));

    ASSIGN_OR_ABORT(auto sst1, open_sstable(sst_pb1));
    ASSIGN_OR_ABORT(auto sst2, open_sstable(sst_pb2));
    sstables.push_back(std::move(sst1));
    sstables.push_back(std::move(sst2));

    PersistentIndexSstableFileset fileset;
    auto st = fileset.init(sstables);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
}

TEST_F(LakePersistentIndexFilesetTest, test_fileset_init_out_of_order_error) {
    auto fileset_id = UniqueId::gen_uid();
    std::vector<std::unique_ptr<PersistentIndexSstable>> sstables;

    // Create sstables out of order (1000-1099, 0-99)
    PersistentIndexSstablePB sst_pb1, sst_pb2;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 1000, 100, &sst_pb1, &fileset_id));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 0, 100, &sst_pb2, &fileset_id));

    ASSIGN_OR_ABORT(auto sst1, open_sstable(sst_pb1));
    ASSIGN_OR_ABORT(auto sst2, open_sstable(sst_pb2));
    sstables.push_back(std::move(sst1));
    sstables.push_back(std::move(sst2));

    PersistentIndexSstableFileset fileset;
    auto st = fileset.init(sstables);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
}

TEST_F(LakePersistentIndexFilesetTest, test_fileset_init_standalone_sstable) {
    PersistentIndexSstablePB sst_pb;
    // Create sstable without fileset_id and range (standalone)
    ASSERT_OK(create_test_sstable("standalone.sst", 0, 100, &sst_pb, nullptr));
    sst_pb.clear_range();

    ASSIGN_OR_ABORT(auto sst, open_sstable(sst_pb));

    PersistentIndexSstableFileset fileset;
    ASSERT_OK(fileset.init(sst));

    ASSERT_TRUE(fileset.is_standalone_sstable());
    ASSERT_EQ("standalone.sst", fileset.standalone_sstable_filename());
}

TEST_F(LakePersistentIndexFilesetTest, test_fileset_multiple_standalone_error) {
    std::vector<std::unique_ptr<PersistentIndexSstable>> sstables;

    // Create 2 standalone sstables
    for (int i = 0; i < 2; i++) {
        PersistentIndexSstablePB sst_pb;
        ASSERT_OK(create_test_sstable(fmt::format("standalone_{}.sst", i), i * 100, 100, &sst_pb, nullptr));
        sst_pb.clear_range();
        ASSIGN_OR_ABORT(auto sst, open_sstable(sst_pb));
        sstables.push_back(std::move(sst));
    }

    PersistentIndexSstableFileset fileset;
    auto st = fileset.init(sstables);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
}

TEST_F(LakePersistentIndexFilesetTest, test_fileset_inconsistent_fileset_id_error) {
    auto fileset_id1 = UniqueId::gen_uid();
    auto fileset_id2 = UniqueId::gen_uid();
    std::vector<std::unique_ptr<PersistentIndexSstable>> sstables;

    // Create 2 sstables with different fileset_ids
    PersistentIndexSstablePB sst_pb1, sst_pb2;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst_pb1, &fileset_id1));
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 1000, 100, &sst_pb2, &fileset_id2));

    ASSIGN_OR_ABORT(auto sst1, open_sstable(sst_pb1));
    ASSIGN_OR_ABORT(auto sst2, open_sstable(sst_pb2));
    sstables.push_back(std::move(sst1));
    sstables.push_back(std::move(sst2));

    PersistentIndexSstableFileset fileset;
    auto st = fileset.init(sstables);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
}

TEST_F(LakePersistentIndexFilesetTest, test_fileset_merge_from) {
    auto fileset_id = UniqueId::gen_uid();

    // Initialize fileset with first sstable
    PersistentIndexSstablePB sst_pb1;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst_pb1, &fileset_id));
    ASSIGN_OR_ABORT(auto sst1, open_sstable(sst_pb1));

    PersistentIndexSstableFileset fileset;
    ASSERT_OK(fileset.init(sst1));

    // Merge second sstable
    PersistentIndexSstablePB sst_pb2;
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 1000, 100, &sst_pb2, &fileset_id));
    ASSIGN_OR_ABORT(auto sst2, open_sstable(sst_pb2));

    ASSERT_TRUE(fileset.append(sst2));

    // Verify both sstables are stored
    PersistentIndexSstableMetaPB retrieved_pbs;
    fileset.get_all_sstable_pbs(&retrieved_pbs);
    ASSERT_EQ(2, retrieved_pbs.sstables_size());
}

TEST_F(LakePersistentIndexFilesetTest, test_fileset_merge_from_overlapping_error) {
    auto fileset_id = UniqueId::gen_uid();

    // Initialize fileset with first sstable (0-99)
    PersistentIndexSstablePB sst_pb1;
    ASSERT_OK(create_test_sstable("test_sst_1.sst", 0, 100, &sst_pb1, &fileset_id));
    ASSIGN_OR_ABORT(auto sst1, open_sstable(sst_pb1));

    PersistentIndexSstableFileset fileset;
    ASSERT_OK(fileset.init(sst1));

    // Try to merge overlapping sstable (50-149)
    PersistentIndexSstablePB sst_pb2;
    ASSERT_OK(create_test_sstable("test_sst_2.sst", 50, 100, &sst_pb2, &fileset_id));
    ASSIGN_OR_ABORT(auto sst2, open_sstable(sst_pb2));

    ASSERT_FALSE(fileset.append(sst2));
}

// Note: can_append() method has been removed from the API
// Validation is now done in append() which returns error on overlap

TEST_F(LakePersistentIndexFilesetTest, test_fileset_multi_get_single_sstable) {
    auto fileset_id = UniqueId::gen_uid();

    // Create sstable with keys 0-99
    PersistentIndexSstablePB sst_pb;
    ASSERT_OK(create_test_sstable("test_sst.sst", 0, 100, &sst_pb, &fileset_id));
    ASSIGN_OR_ABORT(auto sst, open_sstable(sst_pb));

    PersistentIndexSstableFileset fileset;
    ASSERT_OK(fileset.init(sst));

    // Prepare keys to lookup
    const int N = 10;
    std::vector<std::string> key_strings(N);
    std::vector<Slice> keys(N);
    KeyIndexSet key_indexes;
    for (int i = 0; i < N; i++) {
        key_strings[i] = fmt::format("key_{:016X}", i * 10); // 0, 10, 20, ..., 90
        keys[i] = Slice(key_strings[i]);
        key_indexes.insert(i);
    }

    std::vector<IndexValue> values(N);
    KeyIndexSet found_key_indexes;

    ASSERT_OK(fileset.multi_get(keys.data(), key_indexes, -1, values.data(), &found_key_indexes));

    // Verify all keys were found
    ASSERT_EQ(N, found_key_indexes.size());
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(i * 10, values[i].get_value());
    }
}

TEST_F(LakePersistentIndexFilesetTest, test_fileset_multi_get_multiple_sstables) {
    auto fileset_id = UniqueId::gen_uid();
    std::vector<std::unique_ptr<PersistentIndexSstable>> sstables;

    // Create 3 sstables with keys 0-99, 1000-1099, 2000-2099
    for (int i = 0; i < 3; i++) {
        PersistentIndexSstablePB sst_pb;
        ASSERT_OK(create_test_sstable(fmt::format("test_sst_{}.sst", i), i * 1000, 100, &sst_pb, &fileset_id));
        ASSIGN_OR_ABORT(auto sst, open_sstable(sst_pb));
        sstables.push_back(std::move(sst));
    }

    PersistentIndexSstableFileset fileset;
    ASSERT_OK(fileset.init(sstables));

    // Prepare keys from different sstables
    const int N = 9; // 3 keys from each sstable
    std::vector<std::string> key_strings;
    std::vector<Slice> keys;
    KeyIndexSet key_indexes;
    for (int sst_idx = 0; sst_idx < 3; sst_idx++) {
        for (int i = 0; i < 3; i++) {
            int key_val = sst_idx * 1000 + i * 10;
            key_strings.push_back(fmt::format("key_{:016X}", key_val));
            keys.emplace_back(key_strings.back());
            key_indexes.insert(sst_idx * 3 + i);
        }
    }

    std::vector<IndexValue> values(N);
    KeyIndexSet found_key_indexes;

    ASSERT_OK(fileset.multi_get(keys.data(), key_indexes, -1, values.data(), &found_key_indexes));

    // Verify all keys were found
    ASSERT_EQ(N, found_key_indexes.size());
    for (int sst_idx = 0; sst_idx < 3; sst_idx++) {
        for (int i = 0; i < 3; i++) {
            int idx = sst_idx * 3 + i;
            int expected_val = sst_idx * 1000 + i * 10;
            ASSERT_EQ(expected_val, values[idx].get_value());
        }
    }
}

TEST_F(LakePersistentIndexFilesetTest, test_fileset_multi_get_partial_match) {
    auto fileset_id = UniqueId::gen_uid();

    // Create sstable with keys 0-99
    PersistentIndexSstablePB sst_pb;
    ASSERT_OK(create_test_sstable("test_sst.sst", 0, 100, &sst_pb, &fileset_id));
    ASSIGN_OR_ABORT(auto sst, open_sstable(sst_pb));

    PersistentIndexSstableFileset fileset;
    ASSERT_OK(fileset.init(sst));

    // Prepare mixed keys: some exist (0, 10, 20), some don't (1000, 2000, 3000)
    const int N = 6;
    std::vector<std::string> key_strings;
    std::vector<Slice> keys;
    KeyIndexSet key_indexes;

    for (int i = 0; i < 3; i++) {
        key_strings.push_back(fmt::format("key_{:016X}", i * 10)); // Existing keys
        keys.emplace_back(key_strings.back());
        key_indexes.insert(i);
    }
    for (int i = 0; i < 3; i++) {
        key_strings.push_back(fmt::format("key_{:016X}", 1000 + i * 1000)); // Non-existing keys
        keys.emplace_back(key_strings.back());
        key_indexes.insert(3 + i);
    }

    std::vector<IndexValue> values(N);
    KeyIndexSet found_key_indexes;

    ASSERT_OK(fileset.multi_get(keys.data(), key_indexes, -1, values.data(), &found_key_indexes));

    // Verify only existing keys were found
    ASSERT_EQ(3, found_key_indexes.size());
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(found_key_indexes.count(i) > 0);
        ASSERT_EQ(i * 10, values[i].get_value());
    }
}

TEST_F(LakePersistentIndexFilesetTest, test_fileset_memory_usage) {
    auto fileset_id = UniqueId::gen_uid();
    std::vector<std::unique_ptr<PersistentIndexSstable>> sstables;

    // Create multiple sstables
    for (int i = 0; i < 3; i++) {
        PersistentIndexSstablePB sst_pb;
        ASSERT_OK(create_test_sstable(fmt::format("test_sst_{}.sst", i), i * 1000, 100, &sst_pb, &fileset_id));
        ASSIGN_OR_ABORT(auto sst, open_sstable(sst_pb));
        sstables.push_back(std::move(sst));
    }

    PersistentIndexSstableFileset fileset;
    ASSERT_OK(fileset.init(sstables));

    // Memory usage should be > 0
    ASSERT_GT(fileset.memory_usage(), 0);
}

// ============================================================================
// Tests for LakePersistentIndex with fileset integration
// ============================================================================

TEST_F(LakePersistentIndexFilesetTest, test_index_basic_read_write) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10; // Force frequent flushes

    using Key = uint64_t;
    const int N = 1000;
    std::vector<Key> keys;
    std::vector<Slice> key_slices;
    std::vector<IndexValue> values;

    keys.reserve(N);
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        values.emplace_back(i * 2);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
    }

    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));

    // Insert data
    ASSERT_OK(index->insert(N, key_slices.data(), values.data(), 0));

    // Read back and verify
    std::vector<IndexValue> get_values(N);
    ASSERT_OK(index->get(N, key_slices.data(), get_values.data()));
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }

    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_F(LakePersistentIndexFilesetTest, test_index_reload_after_minor_compaction) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;

    using Key = uint64_t;
    const int N = 500;
    std::vector<Key> keys;
    std::vector<Slice> key_slices;
    std::vector<IndexValue> values;

    keys.reserve(N);
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        values.emplace_back(i * 2);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
    }

    auto tablet_id = _tablet_metadata->id();

    // Phase 1: Insert and minor compact
    {
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
        ASSERT_OK(index->init(_tablet_metadata));
        index->prepare(EditVersion(1, 0), 0);
        ASSERT_OK(index->insert(N, key_slices.data(), values.data(), 0));
        ASSERT_OK(index->flush_memtable(true));
        ASSERT_OK(index->sync_flush_all_memtables(60 * 1000 * 1000)); // Wait up to 60s

        // Commit to metadata
        Tablet tablet(_tablet_mgr.get(), tablet_id);
        auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
        tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
        MetaFileBuilder builder(tablet, tablet_metadata_ptr);
        ASSERT_OK(index->commit(&builder));
        ASSERT_OK(builder.finalize(1));

        // Update our metadata
        _tablet_metadata->CopyFrom(*tablet_metadata_ptr);
    }

    // Phase 2: Reload index and verify data
    {
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
        ASSERT_OK(index->init(_tablet_metadata));

        // Read back and verify all data
        std::vector<IndexValue> get_values(N);
        ASSERT_OK(index->get(N, key_slices.data(), get_values.data()));
        for (int i = 0; i < N; i++) {
            ASSERT_EQ(values[i], get_values[i]) << "Mismatch at index " << i;
        }
    }

    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_F(LakePersistentIndexFilesetTest, test_index_reload_after_major_compaction) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;

    using Key = uint64_t;
    const int M = 3;   // Number of batches
    const int N = 200; // Keys per batch
    std::vector<std::vector<Key>> all_keys(M);
    std::vector<std::vector<Slice>> all_key_slices(M);
    std::vector<std::vector<IndexValue>> all_values(M);

    auto tablet_id = _tablet_metadata->id();

    // Phase 1: Insert multiple batches and create multiple sst files
    {
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
        ASSERT_OK(index->init(_tablet_metadata));

        for (int m = 0; m < M; m++) {
            all_keys[m].reserve(N);
            all_key_slices[m].reserve(N);
            all_values[m].reserve(N);

            for (int i = 0; i < N; i++) {
                all_keys[m].emplace_back(i);
                all_values[m].emplace_back(i * 2 + m); // Different value for each batch
                all_key_slices[m].emplace_back((uint8_t*)(&all_keys[m][i]), sizeof(Key));
            }

            index->prepare(EditVersion(m, 0), 0);
            std::vector<IndexValue> old_values(N);
            ASSERT_OK(index->upsert(N, all_key_slices[m].data(), all_values[m].data(), old_values.data()));
            ASSERT_OK(index->flush_memtable(true));
            ASSERT_OK(index->sync_flush_all_memtables(60 * 1000 * 1000)); // Wait up to 60s
        }

        // Commit to metadata
        Tablet tablet(_tablet_mgr.get(), tablet_id);
        auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
        tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
        MetaFileBuilder builder(tablet, tablet_metadata_ptr);
        ASSERT_OK(index->commit(&builder));
        ASSERT_OK(builder.finalize(1));
        _tablet_metadata->CopyFrom(*tablet_metadata_ptr);
    }

    // Phase 2: Major compaction
    {
        auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
        tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
        auto txn_log = std::make_shared<TxnLogPB>();
        auto parallel_compact_mgr = std::make_unique<LakePersistentIndexParallelCompactMgr>(_tablet_mgr.get());
        ASSERT_OK(parallel_compact_mgr->init());

        ASSERT_OK(LakePersistentIndex::parallel_major_compact(parallel_compact_mgr.get(), _tablet_mgr.get(),
                                                              tablet_metadata_ptr, txn_log.get()));
        ASSERT_TRUE(txn_log->op_compaction().input_sstables_size() > 0);
        ASSERT_TRUE(txn_log->op_compaction().output_sstables_size() > 0);

        // Apply compaction result
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
        ASSERT_OK(index->init(_tablet_metadata));
        ASSERT_OK(index->apply_opcompaction(txn_log->op_compaction()));

        // Update metadata
        Tablet tablet(_tablet_mgr.get(), tablet_id);
        MetaFileBuilder builder(tablet, tablet_metadata_ptr);
        ASSERT_OK(index->commit(&builder));
        ASSERT_OK(builder.finalize(1));
        _tablet_metadata->CopyFrom(*tablet_metadata_ptr);
    }

    // Phase 3: Reload and verify
    {
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
        ASSERT_OK(index->init(_tablet_metadata));

        // Verify with latest values (from last batch M-1)
        std::vector<IndexValue> get_values(N);
        ASSERT_OK(index->get(N, all_key_slices[M - 1].data(), get_values.data()));
        for (int i = 0; i < N; i++) {
            ASSERT_EQ(all_values[M - 1][i], get_values[i]) << "Mismatch at index " << i;
        }
    }

    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_F(LakePersistentIndexFilesetTest, test_index_multiple_reload_cycles) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;

    using Key = uint64_t;
    const int CYCLES = 3;
    const int N = 300;

    auto tablet_id = _tablet_metadata->id();

    for (int cycle = 0; cycle < CYCLES; cycle++) {
        std::vector<Key> keys;
        std::vector<Slice> key_slices;
        std::vector<IndexValue> values;

        keys.reserve(N);
        key_slices.reserve(N);
        for (int i = 0; i < N; i++) {
            keys.emplace_back(i);
            values.emplace_back(i * 2 + cycle * 1000); // Different value for each cycle
            key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
        }

        // Write data
        {
            auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
            ASSERT_OK(index->init(_tablet_metadata));
            index->prepare(EditVersion(cycle, 0), 0);

            std::vector<IndexValue> old_values(N);
            ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), old_values.data()));
            ASSERT_OK(index->flush_memtable(true));
            ASSERT_OK(index->sync_flush_all_memtables(60 * 1000 * 1000)); // Wait up to 60s

            Tablet tablet(_tablet_mgr.get(), tablet_id);
            auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
            tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
            MetaFileBuilder builder(tablet, tablet_metadata_ptr);
            ASSERT_OK(index->commit(&builder));
            ASSERT_OK(builder.finalize(1));
            _tablet_metadata->CopyFrom(*tablet_metadata_ptr);
        }

        // Reload and verify
        {
            auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
            ASSERT_OK(index->init(_tablet_metadata));

            std::vector<IndexValue> get_values(N);
            ASSERT_OK(index->get(N, key_slices.data(), get_values.data()));
            for (int i = 0; i < N; i++) {
                ASSERT_EQ(values[i], get_values[i]) << "Cycle " << cycle << ", index " << i;
            }
        }
    }

    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_F(LakePersistentIndexFilesetTest, test_index_upsert_and_reload) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;

    using Key = uint64_t;
    const int N = 500;
    std::vector<Key> keys;
    std::vector<Slice> key_slices;

    keys.reserve(N);
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
    }

    auto tablet_id = _tablet_metadata->id();

    // Phase 1: Insert initial values
    {
        std::vector<IndexValue> values;
        values.reserve(N);
        for (int i = 0; i < N; i++) {
            values.emplace_back(i * 2);
        }

        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
        ASSERT_OK(index->init(_tablet_metadata));
        index->prepare(EditVersion(1, 0), 0);
        ASSERT_OK(index->insert(N, key_slices.data(), values.data(), 0));
        ASSERT_OK(index->flush_memtable(true));
        ASSERT_OK(index->sync_flush_all_memtables(60 * 1000 * 1000)); // Wait up to 60s

        Tablet tablet(_tablet_mgr.get(), tablet_id);
        auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
        tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
        MetaFileBuilder builder(tablet, tablet_metadata_ptr);
        ASSERT_OK(index->commit(&builder));
        ASSERT_OK(builder.finalize(1));
        _tablet_metadata->CopyFrom(*tablet_metadata_ptr);
    }

    // Phase 2: Upsert new values
    {
        std::vector<IndexValue> new_values;
        new_values.reserve(N);
        for (int i = 0; i < N; i++) {
            new_values.emplace_back(i * 3); // New value
        }

        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
        ASSERT_OK(index->init(_tablet_metadata));
        index->prepare(EditVersion(2, 0), 0);

        std::vector<IndexValue> old_values(N);
        ASSERT_OK(index->upsert(N, key_slices.data(), new_values.data(), old_values.data()));
        ASSERT_OK(index->flush_memtable(true));
        ASSERT_OK(index->sync_flush_all_memtables(60 * 1000 * 1000)); // Wait up to 60s

        Tablet tablet(_tablet_mgr.get(), tablet_id);
        auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
        tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
        MetaFileBuilder builder(tablet, tablet_metadata_ptr);
        ASSERT_OK(index->commit(&builder));
        ASSERT_OK(builder.finalize(1));
        _tablet_metadata->CopyFrom(*tablet_metadata_ptr);
    }

    // Phase 3: Reload and verify new values
    {
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
        ASSERT_OK(index->init(_tablet_metadata));

        std::vector<IndexValue> get_values(N);
        ASSERT_OK(index->get(N, key_slices.data(), get_values.data()));
        for (int i = 0; i < N; i++) {
            ASSERT_EQ(i * 3, get_values[i].get_value()) << "Mismatch at index " << i;
        }
    }

    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_F(LakePersistentIndexFilesetTest, test_index_concurrent_read_after_reload) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;

    using Key = uint64_t;
    const int N = 1000;
    std::vector<Key> keys;
    std::vector<Slice> key_slices;
    std::vector<IndexValue> values;

    keys.reserve(N);
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        values.emplace_back(i * 2);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
    }

    auto tablet_id = _tablet_metadata->id();

    // Write and compact
    {
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
        ASSERT_OK(index->init(_tablet_metadata));
        index->prepare(EditVersion(1, 0), 0);
        ASSERT_OK(index->insert(N, key_slices.data(), values.data(), 0));
        ASSERT_OK(index->flush_memtable(true));
        ASSERT_OK(index->sync_flush_all_memtables(60 * 1000 * 1000)); // Wait up to 60s

        Tablet tablet(_tablet_mgr.get(), tablet_id);
        auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
        tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
        MetaFileBuilder builder(tablet, tablet_metadata_ptr);
        ASSERT_OK(index->commit(&builder));
        ASSERT_OK(builder.finalize(1));
        _tablet_metadata->CopyFrom(*tablet_metadata_ptr);
    }

    // Reload and read multiple times (simulating concurrent access pattern)
    {
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
        ASSERT_OK(index->init(_tablet_metadata));

        for (int round = 0; round < 5; round++) {
            std::vector<IndexValue> get_values(N);
            ASSERT_OK(index->get(N, key_slices.data(), get_values.data()));
            for (int i = 0; i < N; i++) {
                ASSERT_EQ(values[i], get_values[i]) << "Round " << round << ", index " << i;
            }
        }
    }

    config::l0_max_mem_usage = l0_max_mem_usage;
}

} // namespace starrocks::lake
