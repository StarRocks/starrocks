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

#include <random>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "storage/chunk_helper.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/join_path.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/test_util.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

class LakePrimaryKeyPublishTest : public TestBase, testing::WithParamInterface<PrimaryKeyParam> {
public:
    LakePrimaryKeyPublishTest() : TestBase(kTestGroupPath) {
        _tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
        _tablet_metadata->set_enable_persistent_index(GetParam().enable_persistent_index);
        _tablet_metadata->set_persistent_index_type(GetParam().persistent_index_type);

        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(2, "__op", TypeDescriptor{LogicalType::TYPE_INT});
        _slot_pointers.emplace_back(&_slots[0]);
        _slot_pointers.emplace_back(&_slots[1]);
        _slot_pointers.emplace_back(&_slots[2]);

        _slot_cid_map.emplace(0, 0);
        _slot_cid_map.emplace(1, 1);
        _slot_cid_map.emplace(2, 2);

        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));

        // add encryption keys
        EncryptionKeyPB pb;
        pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
        pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
        pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
        pb.set_plain_key("0000000000000000");
        std::unique_ptr<EncryptionKey> root_encryption_key = EncryptionKey::create_from_pb(pb).value();
        auto val_st = root_encryption_key->generate_key();
        EXPECT_TRUE(val_st.ok());
        std::unique_ptr<EncryptionKey> encryption_key = std::move(val_st.value());
        encryption_key->set_id(2);
        KeyCache::instance().add_key(root_encryption_key);
        KeyCache::instance().add_key(encryption_key);
    }

    void SetUp() override {
        config::enable_transparent_data_encryption = GetParam().enable_transparent_data_encryption;
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override {
        // check primary index cache's ref
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(_tablet_metadata->id(), 1));
        (void)fs::remove_all(kTestGroupPath);
        config::enable_transparent_data_encryption = false;
    }

    ChunkPtr gen_data(int64_t chunk_size, int shift, bool random_shuffle, bool upsert) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        std::vector<uint8_t> v2(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i + shift * chunk_size;
        }
        if (random_shuffle) {
            auto rng = std::default_random_engine{};
            std::shuffle(v0.begin(), v0.end(), rng);
        }
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * 3;
        }
        for (int i = 0; i < chunk_size; i++) {
            v2[i] = upsert ? TOpType::UPSERT : TOpType::DELETE;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int8Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        c2->append_numbers(v2.data(), v2.size() * sizeof(uint8_t));
        return std::make_shared<Chunk>(Columns{c0, c1, c2}, _slot_cid_map);
    }

    std::pair<ChunkPtr, std::vector<uint32_t>> gen_data_and_index(int64_t chunk_size, int shift, bool random_shuffle,
                                                                  bool upsert) {
        auto chunk = gen_data(chunk_size, shift, random_shuffle, upsert);
        auto indexes = std::vector<uint32_t>(chunk->num_rows());
        for (uint32_t i = 0, n = chunk->num_rows(); i < n; i++) {
            indexes[i] = i;
        }
        return {std::move(chunk), std::move(indexes)};
    }

    ChunkPtr read(int64_t tablet_id, int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto ret = ChunkHelper::new_chunk(*_schema, 128);
        while (true) {
            auto tmp = ChunkHelper::new_chunk(*_schema, 128);
            auto st = reader->get_next(tmp.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            ret->append(*tmp);
        }
        return ret;
    }

    int64_t read_rows(int64_t tablet_id, int64_t version) {
        auto chunk = read(tablet_id, version);
        return chunk->num_rows();
    }

protected:
    constexpr static const char* const kTestGroupPath = "test_lake_primary_key";
    constexpr static const int kChunkSize = 12;

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;
    RuntimeProfile _dummy_runtime_profile{"dummy"};
};

TEST_P(LakePrimaryKeyPublishTest, test_write_read_success) {
    if (GetParam().enable_transparent_data_encryption) {
        return;
    }
    std::vector<int> k0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
    std::vector<int> v0{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 41, 44};

    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append_numbers(k0.data(), k0.size() * sizeof(int));
    c1->append_numbers(v0.data(), v0.size() * sizeof(int));

    Chunk chunk0({c0, c1}, _schema);
    auto rowset_txn_meta = std::make_unique<RowsetTxnMetaPB>();

    int64_t txn_id = next_id();
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
    std::shared_ptr<const TabletSchema> const_schema = _tablet_schema;
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
    ASSERT_OK(writer->open());

    // write segment #1
    ASSERT_OK(writer->write(chunk0));
    ASSERT_OK(writer->finish());

    // write txnlog
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(_tablet_metadata->id());
    txn_log->set_txn_id(txn_id);
    auto op_write = txn_log->mutable_op_write();
    for (auto& f : writer->files()) {
        op_write->mutable_rowset()->add_segments(std::move(f.path));
    }
    op_write->mutable_rowset()->set_num_rows(writer->num_rows());
    op_write->mutable_rowset()->set_data_size(writer->data_size());
    op_write->mutable_rowset()->set_overlapped(false);

    ASSERT_OK(_tablet_mgr->put_txn_log(txn_log));

    writer->close();

    ASSERT_OK(publish_single_version(_tablet_metadata->id(), 2, txn_id).status());
    // update memory usage, should large than zero
    EXPECT_TRUE(_update_mgr->mem_tracker()->consumption() > 0);
    EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(_tablet_metadata->id(), txn_id));

    // read at version 2
    ASSIGN_OR_ABORT(auto metadata_v2, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), 2));
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata_v2, *_schema);
    ASSERT_OK(reader->prepare());
    TabletReaderParams params;
    ASSERT_OK(reader->open(params));

    auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
    ASSERT_OK(reader->get_next(read_chunk_ptr.get()));
    ASSERT_EQ(k0.size(), read_chunk_ptr->num_rows());

    for (int i = 0, sz = k0.size(); i < sz; i++) {
        EXPECT_EQ(k0[i], read_chunk_ptr->get(i)[0].get_int32());
        EXPECT_EQ(v0[i], read_chunk_ptr->get(i)[1].get_int32());
    }
    EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() == 0);
}

TEST_P(LakePrimaryKeyPublishTest, test_write_multitime_check_result) {
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() > 0);
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
    }
    // update memory usage, should large than zero
    EXPECT_TRUE(_update_mgr->mem_tracker()->consumption() > 0);
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    _tablet_mgr->prune_metacache();
    // fill delvec cache again
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    EXPECT_TRUE(_tablet_mgr->metacache()->memory_usage() > 0);
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(2).num_dels(), 0);
    EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() == 0);
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePrimaryKeyPublishTest, test_write_fail_retry) {
    std::vector<ChunkPtr> chunks;
    for (int i = 0; i < 5; i++) {
        chunks.emplace_back(gen_data(kChunkSize, i, true, true));
    }
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // write success
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
    }
    // write failed
    for (int i = 3; i < 5; i++) {
        TEST_ENABLE_ERROR_POINT("TabletManager::put_tablet_metadata",
                                Status::IOError("injected put tablet metadata error"));

        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::put_tablet_metadata");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_ERROR(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
    }
    // write success
    for (int i = 3; i < 5; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
    }
    ASSERT_EQ(kChunkSize * 5, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 5);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(2).num_dels(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(3).num_dels(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(4).num_dels(), 0);
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePrimaryKeyPublishTest, test_publish_multi_segments) {
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto [chunk1, indexes1] = gen_data_and_index(kChunkSize, 5, true, true);
    auto [chunk2, indexes2] = gen_data_and_index(kChunkSize, 7, true, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    const int64_t old_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(*chunk1, indexes1.data(), indexes1.size()));
        ASSERT_OK(delta_writer->write(*chunk2, indexes2.data(), indexes2.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() > 0);
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
    }
    // update memory usage, should large than zero
    EXPECT_TRUE(_update_mgr->mem_tracker()->consumption() > 0);
    config::write_buffer_size = old_size;
    ASSERT_EQ(kChunkSize * 3, read_rows(tablet_id, version));
    EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() == 0);
}

TEST_P(LakePrimaryKeyPublishTest, test_publish_multi_times) {
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto txns = std::vector<int64_t>();
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() > 0);
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
        txns.push_back(txn_id);
    }
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    // duplicate publish
    ASSERT_OK(publish_single_version(tablet_id, version, txns.back()).status());
    // publish using old version
    ASSERT_OK(publish_single_version(tablet_id, version - 1, txns.back()).status());
    // advince publish should fail, because version + 1 don't exist
    ASSERT_ERROR(publish_single_version(tablet_id, version + 2, txns.back()).status());
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(2).num_dels(), 0);
    EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() == 0);
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePrimaryKeyPublishTest, test_publish_with_oom) {
    config::skip_pk_preload = true;
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto txns = std::vector<int64_t>();
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    const int64_t old_limit = _update_mgr->mem_tracker()->limit();
    _update_mgr->mem_tracker()->set_limit(1);
    ASSERT_TRUE(_update_mgr->mem_tracker()->any_limit_exceeded_precheck(2));
    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version fail because of oom
        ASSERT_ERROR(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
    }
    _update_mgr->mem_tracker()->set_limit(old_limit);
    config::skip_pk_preload = false;
}

TEST_P(LakePrimaryKeyPublishTest, test_publish_concurrent) {
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // start to publish using multi thread
        std::vector<std::thread> workers;
        for (int j = 0; j < 5; j++) {
            workers.emplace_back([&]() { (void)publish_single_version(tablet_id, version + 1, txn_id); });
        }
        for (auto& t : workers) {
            t.join();
        }
        version++;
    }
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(2).num_dels(), 0);
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePrimaryKeyPublishTest, test_resolve_conflict) {
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    std::vector<int64_t> txn_ids;
    // concurrent write
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        // will preload update state here.
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        txn_ids.push_back(txn_id);
    }
    EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() > 0);
    // publish in order
    for (int64_t txn_id : txn_ids) {
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
    }
    // check result
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(2).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(3).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(4).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(5).num_dels(), 0);
    EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() == 0);
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePrimaryKeyPublishTest, test_write_read_success_multiple_tablet) {
    auto [chunk0, indexes_1] = gen_data_and_index(kChunkSize * 1, 0, false, true);
    auto [chunk1, indexes_2] = gen_data_and_index(kChunkSize * 2, 1, false, true);

    auto tablet_id_1 = _tablet_metadata->id();
    auto tablet_id_2 = next_id();
    auto tablet_metadata_2 = std::make_unique<TabletMetadata>(*_tablet_metadata);
    tablet_metadata_2->set_id(tablet_id_2);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*tablet_metadata_2));

    auto version = 1;
    for (int i = 0; i < 2; i++) {
        int64_t txn_id = next_id();
        for (int j = 0; j < 2; j++) {
            auto tablet_id = (j == 0) ? tablet_id_1 : tablet_id_2;
            auto chunk_ptr = (j == 0) ? chunk0 : chunk1;
            auto indexes = (j == 0) ? indexes_1.data() : indexes_2.data();
            auto indexes_size = (j == 0) ? indexes_1.size() : indexes_2.size();
            ASSIGN_OR_ABORT(auto w, DeltaWriterBuilder()
                                            .set_tablet_manager(_tablet_mgr.get())
                                            .set_tablet_id(tablet_id)
                                            .set_txn_id(txn_id)
                                            .set_partition_id(_partition_id)
                                            .set_mem_tracker(_mem_tracker.get())
                                            .set_schema_id(_tablet_schema->id())
                                            .set_profile(&_dummy_runtime_profile)
                                            .build());
            ASSERT_OK(w->open());
            ASSERT_OK(w->write(*chunk_ptr, indexes, indexes_size));
            ASSERT_OK(w->finish_with_txnlog());
            w->close();
            // Publish version
            ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
            EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        }
        version++;
    }
    auto chunk2 = read(tablet_id_1, version);
    auto chunk3 = read(tablet_id_2, version);
    chunk0->remove_column_by_index(2);
    chunk1->remove_column_by_index(2);
    assert_chunk_equals(*chunk0, *chunk2);
    assert_chunk_equals(*chunk1, *chunk3);
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id_1, version);
        check_local_persistent_index_meta(tablet_id_2, version);
    }
}

TEST_P(LakePrimaryKeyPublishTest, test_write_largedata) {
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    const int N = 100;
    int64_t old_config = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 1024;
    for (int i = 0; i < N; i++) {
        auto [chunk0, indexes] = gen_data_and_index(kChunkSize, i, true, true);
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
    }
    ASSERT_EQ(kChunkSize * N, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(new_tablet_metadata->rowsets(i).num_dels(), 0);
    }
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
    config::l0_max_mem_usage = old_config;
}

TEST_P(LakePrimaryKeyPublishTest, test_recover) {
    config::enable_primary_key_recover = true;
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    bool ingest_failure = true;
    std::string sync_point = "lake_index_load.1";
    SyncPoint::GetInstance()->SetCallBack(sync_point, [&](void* arg) {
        if (ingest_failure) {
            *(Status*)arg = Status::AlreadyExist("ut_test");
            ingest_failure = false;
        } else {
            ingest_failure = true;
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(tablet_id, 1));
        EXPECT_TRUE(_update_mgr->try_remove_primary_index_cache(tablet_id));
        version++;
    }
    SyncPoint::GetInstance()->ClearCallBack(sync_point);
    SyncPoint::GetInstance()->DisableProcessing();
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
    config::enable_primary_key_recover = false;
}

TEST_P(LakePrimaryKeyPublishTest, test_recover_with_multi_reason) {
    config::enable_primary_key_recover = true;
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    auto old_l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    std::vector<std::string> sync_points = {"lake_index_load.1", "update_num_del_stat", "delvec_inconsistent"};
    for (int i = 0; i < 30; i++) {
        std::string sync_point = sync_points[i % sync_points.size()];
        bool ingest_failure = true;
        SyncPoint::GetInstance()->SetCallBack(sync_point, [&](void* arg) {
            if (ingest_failure) {
                *(Status*)arg = Status::AlreadyExist("ut_test");
                ingest_failure = false;
            } else {
                ingest_failure = true;
            }
        });
        SyncPoint::GetInstance()->EnableProcessing();
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(tablet_id, 1));
        EXPECT_TRUE(_update_mgr->try_remove_primary_index_cache(tablet_id));
        version++;
        SyncPoint::GetInstance()->ClearCallBack(sync_point);
        SyncPoint::GetInstance()->DisableProcessing();
    }
    config::l0_max_mem_usage = old_l0_max_mem_usage;
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 30);
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
    config::enable_primary_key_recover = false;
}

TEST_P(LakePrimaryKeyPublishTest, test_recover_with_dels) {
    config::enable_primary_key_recover = true;
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto [chunk1, indexes1] = gen_data_and_index(kChunkSize, 0, true, false);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    std::string sync_point = "lake_index_load.1";
    bool ingest_failure = true;
    SyncPoint::GetInstance()->SetCallBack(sync_point, [&](void* arg) {
        if (ingest_failure) {
            *(Status*)arg = Status::AlreadyExist("ut_test");
            ingest_failure = false;
        } else {
            ingest_failure = true;
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    for (int i = 0; i < 6; i++) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        if (i % 2 == 0) {
            // upsert
            ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        } else {
            // delete
            ASSERT_OK(delta_writer->write(*chunk1, indexes1.data(), indexes1.size()));
        }
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(tablet_id, 1));
        EXPECT_TRUE(_update_mgr->try_remove_primary_index_cache(tablet_id));
        version++;
    }
    SyncPoint::GetInstance()->ClearCallBack(sync_point);
    SyncPoint::GetInstance()->DisableProcessing();
    ASSERT_EQ(0, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
    for (int i = 0; i < 6; i++) {
        EXPECT_EQ(new_tablet_metadata->rowsets(i).del_files_size(), i % 2);
    }
    ASSERT_EQ(0, read_rows(tablet_id, version));
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
    config::enable_primary_key_recover = false;
}

TEST_P(LakePrimaryKeyPublishTest, test_recover_with_dels2) {
    config::enable_primary_key_recover = true;
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto [chunk1, indexes1] = gen_data_and_index(kChunkSize, 0, true, false);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    bool ingest_failure = true;
    std::string sync_point = "lake_index_load.1";
    SyncPoint::GetInstance()->SetCallBack(sync_point, [&](void* arg) {
        if (ingest_failure) {
            *(Status*)arg = Status::AlreadyExist("ut_test");
            ingest_failure = false;
        } else {
            ingest_failure = true;
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    for (int i = 0; i < 6; i++) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        // upsert
        const int64_t old_size = config::write_buffer_size;
        config::write_buffer_size = 1;
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        // delete
        for (int j = 0; j < i; j++) {
            ASSERT_OK(delta_writer->write(*chunk1, indexes1.data(), indexes1.size()));
        }
        if (i == 5) {
            ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        }
        ASSERT_OK(delta_writer->finish_with_txnlog());
        config::write_buffer_size = old_size;
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(tablet_id, 1));
        EXPECT_TRUE(_update_mgr->try_remove_primary_index_cache(tablet_id));
        version++;
    }
    SyncPoint::GetInstance()->ClearCallBack(sync_point);
    SyncPoint::GetInstance()->DisableProcessing();
    ASSERT_EQ(0, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
    for (int i = 0; i < 6; i++) {
        EXPECT_EQ(new_tablet_metadata->rowsets(i).del_files_size(), i);
    }
    ASSERT_EQ(0, read_rows(tablet_id, version));
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
    config::enable_primary_key_recover = false;
}

TEST_P(LakePrimaryKeyPublishTest, test_index_load_failure) {
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    bool ingest_failure = true;
    std::string sync_point = "lake_index_load.1";
    SyncPoint::GetInstance()->SetCallBack(sync_point, [&](void* arg) {
        if (ingest_failure) {
            *(Status*)arg = Status::AlreadyExist("ut_test");
            ingest_failure = false;
        } else {
            ingest_failure = true;
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    for (int i = 0; i < 6; i++) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        // upsert
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_FALSE(publish_single_version(tablet_id, version + 1, txn_id).ok());
        ASSERT_TRUE(publish_single_version(tablet_id, version + 1, txn_id).ok());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(tablet_id, 1));
        EXPECT_TRUE(_update_mgr->try_remove_primary_index_cache(tablet_id));
        version++;
    }
    SyncPoint::GetInstance()->ClearCallBack(sync_point);
    SyncPoint::GetInstance()->DisableProcessing();
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePrimaryKeyPublishTest, test_write_rebuild_persistent_index) {
    if (!GetParam().enable_persistent_index) {
        // only test persistent index
        return;
    }
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    if (GetParam().persistent_index_type == PersistentIndexTypePB::CLOUD_NATIVE) {
        config::l0_max_mem_usage = 10;
    }
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
        // clear persistent index path
        EXPECT_TRUE(_update_mgr->try_remove_primary_index_cache(tablet_id));
        std::string path = strings::Substitute(
                "$0/$1/", StorageEngine::instance()->get_persistent_index_store(tablet_id)->get_persistent_index_path(),
                tablet_id);
        ASSERT_OK(fs::remove_all(path));
    }
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(2).num_dels(), 0);
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_P(LakePrimaryKeyPublishTest, test_abort_txn) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->LoadDependency(
            {{"UpdateManager::preload_update_state:return", "transactions::abort_txn:enter"}});

    auto tablet_id = _tablet_metadata->id();
    auto txn_id = next_id();
    std::thread t1([&]() {
        auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
        auto tablet_id = _tablet_metadata->id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    });

    std::thread t2([&]() {
        AbortTxnRequest request;
        request.add_tablet_ids(tablet_id);
        request.add_txn_ids(txn_id);
        request.set_skip_cleanup(false);
        AbortTxnResponse response;
        auto lake_service = LakeServiceImpl(ExecEnv::GetInstance(), _tablet_mgr.get());
        lake_service.abort_txn(nullptr, &request, &response, nullptr);
    });

    t1.join();
    t2.join();
    ASSERT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
    SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(LakePrimaryKeyPublishTest, test_batch_publish) {
    auto [chunk0, indexes0] = gen_data_and_index(kChunkSize, 0, true, true);
    auto [chunk1, indexes1] = gen_data_and_index(kChunkSize, 0, false, false);
    auto base_version = 1;
    auto tablet_id = _tablet_metadata->id();
    std::vector<int64_t> txn_ids;
    auto txn_id = next_id();
    txn_ids.emplace_back(txn_id);
    ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .set_slot_descriptors(&_slot_pointers)
                                               .set_profile(&_dummy_runtime_profile)
                                               .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(*chunk0, indexes0.data(), indexes0.size()));
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();

    txn_id = next_id();
    txn_ids.emplace_back(txn_id);
    ASSIGN_OR_ABORT(delta_writer, DeltaWriterBuilder()
                                          .set_tablet_manager(_tablet_mgr.get())
                                          .set_tablet_id(tablet_id)
                                          .set_txn_id(txn_id)
                                          .set_partition_id(_partition_id)
                                          .set_mem_tracker(_mem_tracker.get())
                                          .set_schema_id(_tablet_schema->id())
                                          .set_slot_descriptors(&_slot_pointers)
                                          .set_profile(&_dummy_runtime_profile)
                                          .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(*chunk1, indexes1.data(), indexes1.size()));
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();

    auto new_version = base_version + 2;
    ASSERT_OK(batch_publish(tablet_id, base_version, new_version, txn_ids).status());

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 2);
    EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 12);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), 0);
    EXPECT_EQ(0, read_rows(tablet_id, new_version));
    _tablet_mgr->prune_metacache();
    _update_mgr->try_remove_primary_index_cache(tablet_id);

    // publish again
    ASSERT_OK(batch_publish(tablet_id, base_version, new_version, txn_ids).status());
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 2);
    EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 12);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), 0);
    EXPECT_EQ(0, read_rows(tablet_id, new_version));
}

TEST_P(LakePrimaryKeyPublishTest, test_batch_publish_1) {
    auto [chunk0, indexes0] = gen_data_and_index(kChunkSize, 0, false, true);
    auto [chunk1, indexes1] = gen_data_and_index(kChunkSize, 0, false, true);
    auto [chunk2, indexes2] = gen_data_and_index(kChunkSize, 0, false, true);
    auto base_version = 1;
    auto tablet_id = _tablet_metadata->id();
    std::vector<int64_t> txn_ids;
    auto txn_id = next_id();
    txn_ids.emplace_back(txn_id);
    ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .set_slot_descriptors(&_slot_pointers)
                                               .set_profile(&_dummy_runtime_profile)
                                               .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(*chunk0, indexes0.data(), indexes0.size()));
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();

    txn_id = next_id();
    txn_ids.emplace_back(txn_id);
    ASSIGN_OR_ABORT(delta_writer, DeltaWriterBuilder()
                                          .set_tablet_manager(_tablet_mgr.get())
                                          .set_tablet_id(tablet_id)
                                          .set_txn_id(txn_id)
                                          .set_partition_id(_partition_id)
                                          .set_mem_tracker(_mem_tracker.get())
                                          .set_schema_id(_tablet_schema->id())
                                          .set_slot_descriptors(&_slot_pointers)
                                          .set_profile(&_dummy_runtime_profile)
                                          .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(*chunk1, indexes1.data(), indexes1.size()));
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();

    auto new_version = base_version + 2;
    ASSERT_OK(batch_publish(tablet_id, base_version, new_version, txn_ids).status());
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 2);
    EXPECT_EQ(kChunkSize, read_rows(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), 0);

    txn_id = next_id();
    txn_ids.emplace_back(txn_id);
    ASSIGN_OR_ABORT(delta_writer, DeltaWriterBuilder()
                                          .set_tablet_manager(_tablet_mgr.get())
                                          .set_tablet_id(tablet_id)
                                          .set_txn_id(txn_id)
                                          .set_partition_id(_partition_id)
                                          .set_mem_tracker(_mem_tracker.get())
                                          .set_schema_id(_tablet_schema->id())
                                          .set_slot_descriptors(&_slot_pointers)
                                          .set_profile(&_dummy_runtime_profile)
                                          .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(*chunk2, indexes2.data(), indexes2.size()));
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();

    new_version = base_version + 3;
    ASSERT_OK(batch_publish(tablet_id, base_version, new_version, txn_ids).status());
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    EXPECT_EQ(kChunkSize, read_rows(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(2).num_dels(), 0);
}

TEST_P(LakePrimaryKeyPublishTest, test_transform_batch_to_single) {
    auto [chunk0, indexes0] = gen_data_and_index(kChunkSize, 0, false, true);
    auto [chunk1, indexes1] = gen_data_and_index(kChunkSize, 0, false, true);
    auto base_version = 1;
    auto tablet_id = _tablet_metadata->id();
    std::vector<int64_t> txn_ids;
    auto txn_id1 = next_id();
    txn_ids.emplace_back(txn_id1);
    ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id1)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .set_slot_descriptors(&_slot_pointers)
                                               .set_profile(&_dummy_runtime_profile)
                                               .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(*chunk0, indexes0.data(), indexes0.size()));
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();

    auto txn_id2 = next_id();
    txn_ids.emplace_back(txn_id2);
    ASSIGN_OR_ABORT(delta_writer, DeltaWriterBuilder()
                                          .set_tablet_manager(_tablet_mgr.get())
                                          .set_tablet_id(tablet_id)
                                          .set_txn_id(txn_id2)
                                          .set_partition_id(_partition_id)
                                          .set_mem_tracker(_mem_tracker.get())
                                          .set_schema_id(_tablet_schema->id())
                                          .set_slot_descriptors(&_slot_pointers)
                                          .set_profile(&_dummy_runtime_profile)
                                          .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(*chunk1, indexes1.data(), indexes1.size()));
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();

    auto new_version = base_version + 2;
    ASSERT_OK(batch_publish(tablet_id, base_version, new_version, txn_ids).status());
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 2);
    EXPECT_EQ(kChunkSize, read_rows(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), 0);

    // transform to single publish
    new_version = base_version + 1;
    ASSERT_OK(publish_single_version(tablet_id, new_version, txn_id1).status());
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(kChunkSize, read_rows(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 0);

    new_version = base_version + 2;
    ASSERT_OK(publish_single_version(tablet_id, new_version, txn_id2).status());
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 2);
    EXPECT_EQ(kChunkSize, read_rows(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 12);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), 0);
}

TEST_P(LakePrimaryKeyPublishTest, test_mem_tracker) {
    EXPECT_EQ(10 * 1024 * 1024, _mem_tracker->limit());
    EXPECT_EQ(10 * 1024 * 1024 * config::lake_pk_preload_memory_limit_percent / 100,
              _update_mgr->compaction_state_mem_tracker()->limit());
    EXPECT_EQ(10 * 1024 * 1024 * config::lake_pk_preload_memory_limit_percent / 100,
              _update_mgr->update_state_mem_tracker()->limit());
}

TEST_P(LakePrimaryKeyPublishTest, test_write_with_clear_txnlog) {
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        auto txn_log_st = delta_writer->finish_with_txnlog();
        ASSERT_OK(txn_log_st);
        _tablet_mgr->prune_metacache();
        std::const_pointer_cast<TxnLogPB>(txn_log_st.value())->Clear();
        delta_writer->close();
        EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() > 0);
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
    }
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
}

TEST_P(LakePrimaryKeyPublishTest, test_write_with_cloud_native_index_rebuild) {
    if (!GetParam().enable_persistent_index ||
        GetParam().persistent_index_type != PersistentIndexTypePB::CLOUD_NATIVE) {
        return;
    }
    std::vector<ChunkPtr> chunk_vec;
    std::vector<std::vector<uint32_t>> indexes_vec;
    auto [chunk0, indexes0] = gen_data_and_index(kChunkSize * 3, 0, true, true);
    chunk_vec.push_back(chunk0);
    indexes_vec.push_back(indexes0);
    auto [chunk1, indexes1] = gen_data_and_index(kChunkSize * 3, 1, true, true);
    chunk_vec.push_back(chunk1);
    indexes_vec.push_back(indexes1);
    auto [chunk2, indexes2] = gen_data_and_index(kChunkSize * 3, 2, true, true);
    chunk_vec.push_back(chunk2);
    indexes_vec.push_back(indexes2);
    auto [chunk3, indexes3] = gen_data_and_index(kChunkSize * 3, 3, true, true);
    chunk_vec.push_back(chunk3);
    indexes_vec.push_back(indexes3);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    auto do_load_func = [&]() {
        for (int i = 0; i < 4; i++) {
            int64_t txn_id = next_id();
            ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                       .set_tablet_manager(_tablet_mgr.get())
                                                       .set_tablet_id(tablet_id)
                                                       .set_txn_id(txn_id)
                                                       .set_partition_id(_partition_id)
                                                       .set_mem_tracker(_mem_tracker.get())
                                                       .set_schema_id(_tablet_schema->id())
                                                       .set_profile(&_dummy_runtime_profile)
                                                       .build());
            ASSERT_OK(delta_writer->open());
            ASSERT_OK(delta_writer->write(*chunk_vec[i], indexes_vec[i].data(), indexes_vec[i].size()));
            auto txn_log_st = delta_writer->finish_with_txnlog();
            ASSERT_OK(txn_log_st);
            delta_writer->close();
            // Publish version
            ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
            version++;
        }
    };
    // 1. first time
    const auto old_val = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    do_load_func();
    config::l0_max_mem_usage = old_val;
    ASSERT_EQ(kChunkSize * 3 * 4, read_rows(tablet_id, version));
    // 2. release persistent index and rebuild index
    _update_mgr->unload_and_remove_primary_index(tablet_id);
    // 3. second time
    do_load_func();
    ASSERT_EQ(kChunkSize * 3 * 4, read_rows(tablet_id, version));
}

TEST_P(LakePrimaryKeyPublishTest, test_index_rebuild_with_dels) {
    std::vector<std::pair<ChunkPtr, std::vector<uint32_t>>> chunks;
    // upsert + delete
    chunks.push_back(gen_data_and_index(kChunkSize, 0, true, true));
    chunks.push_back(gen_data_and_index(kChunkSize, 0, false, false));
    chunks.push_back(gen_data_and_index(kChunkSize, 1, true, true));
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    auto old_val = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 2;
    // publish upsert and delete on different txn
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*(chunks[0].first), chunks[0].second.data(), chunks[0].second.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*(chunks[2].first), chunks[2].second.data(), chunks[2].second.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    config::l0_max_mem_usage = old_val;
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*(chunks[1].first), chunks[1].second.data(), chunks[1].second.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).segments_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_rows(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).del_files_size(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).segments_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_rows(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).del_files_size(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(2).segments_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(2).num_dels(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(2).num_rows(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(2).del_files_size(), 1);
    EXPECT_EQ(kChunkSize, read_rows(tablet_id, version));
    // clear index, and then rebuild
    EXPECT_TRUE(_update_mgr->try_remove_primary_index_cache(tablet_id));
    {
        // re write chunk0
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*(chunks[0].first), chunks[0].second.data(), chunks[0].second.size() / 2));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 4);
    EXPECT_EQ(new_tablet_metadata->rowsets(3).segments_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(3).num_dels(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(3).num_rows(), kChunkSize / 2);
    EXPECT_EQ(new_tablet_metadata->rowsets(3).del_files_size(), 0);
    EXPECT_EQ(kChunkSize + kChunkSize / 2, read_rows(tablet_id, version));
    // Compaction
    {
        auto old_val = config::lake_pk_compaction_min_input_segments;
        config::lake_pk_compaction_min_input_segments = 1;
        int64_t txn_id = next_id();
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
        EXPECT_EQ(100, task_context->progress.value());
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        config::lake_pk_compaction_min_input_segments = old_val;
    }
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(kChunkSize + kChunkSize / 2, read_rows(tablet_id, version));
    // clear index, and then rebuild
    EXPECT_TRUE(_update_mgr->try_remove_primary_index_cache(tablet_id));
    {
        // re write chunk0
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*(chunks[0].first), chunks[0].second.data(), chunks[0].second.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 2);
    EXPECT_EQ(kChunkSize * 2, read_rows(tablet_id, version));
}

TEST_P(LakePrimaryKeyPublishTest, test_index_rebuild_with_dels2) {
    std::vector<std::pair<ChunkPtr, std::vector<uint32_t>>> chunks;
    // upsert + delete
    chunks.push_back(gen_data_and_index(kChunkSize, 0, true, true));
    chunks.push_back(gen_data_and_index(kChunkSize, 0, false, false));
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    const int64_t old_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    auto old_val = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 1;
    // publish upsert and delete on one txn
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*(chunks[0].first), chunks[0].second.data(), chunks[0].second.size()));
        ASSERT_OK(delta_writer->write(*(chunks[1].first), chunks[1].second.data(), chunks[1].second.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    config::write_buffer_size = old_size;
    config::l0_max_mem_usage = old_val;

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).segments_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_rows(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).del_files_size(), 1);
    EXPECT_EQ(0, read_rows(tablet_id, version));
    // clear index, and then rebuild
    EXPECT_TRUE(_update_mgr->try_remove_primary_index_cache(tablet_id));
    {
        // re write chunk0
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*(chunks[0].first), chunks[0].second.data(), chunks[0].second.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 2);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).segments_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_rows(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).del_files_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).segments_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), 0);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_rows(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).del_files_size(), 0);
    EXPECT_EQ(kChunkSize, read_rows(tablet_id, version));
}

TEST_P(LakePrimaryKeyPublishTest, test_index_rebuild_with_dels3) {
    if (!GetParam().enable_persistent_index ||
        GetParam().persistent_index_type != PersistentIndexTypePB::CLOUD_NATIVE) {
        return;
    }
    std::vector<std::pair<ChunkPtr, std::vector<uint32_t>>> chunks;
    //delete * 2
    chunks.push_back(gen_data_and_index(kChunkSize, 0, false, false));
    chunks.push_back(gen_data_and_index(kChunkSize, 1, true, false));
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // publish two delete on different txn
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*(chunks[0].first), chunks[0].second.data(), chunks[0].second.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*(chunks[1].first), chunks[1].second.data(), chunks[1].second.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // compact
    {
        auto old_val = config::lake_pk_compaction_min_input_segments;
        config::lake_pk_compaction_min_input_segments = 1;
        int64_t txn_id = next_id();
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
        EXPECT_EQ(100, task_context->progress.value());
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        config::lake_pk_compaction_min_input_segments = old_val;
    }

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).del_files_size(), 2);
}

TEST_P(LakePrimaryKeyPublishTest, test_index_rebuild_with_dels4) {
    std::vector<std::pair<ChunkPtr, std::vector<uint32_t>>> chunks;
    // upsert + delete
    chunks.push_back(gen_data_and_index(kChunkSize, 0, true, true));
    chunks.push_back(gen_data_and_index(kChunkSize, 0, false, false));
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    const int64_t old_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    auto old_val = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 1;
    // publish upsert and delete on one txn
    {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*(chunks[0].first), chunks[0].second.data(), chunks[0].second.size()));
        ASSERT_OK(delta_writer->write(*(chunks[0].first), chunks[0].second.data(), chunks[0].second.size()));
        ASSERT_OK(delta_writer->write(*(chunks[1].first), chunks[1].second.data(), chunks[1].second.size()));
        ASSERT_OK(delta_writer->write(*(chunks[0].first), chunks[0].second.data(), chunks[0].second.size()));
        ASSERT_OK(delta_writer->write(*(chunks[0].first), chunks[0].second.data(), chunks[0].second.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    config::write_buffer_size = old_size;
    config::l0_max_mem_usage = old_val;

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).segments_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).del_files_size(), 1);
    EXPECT_EQ(0, read_rows(tablet_id, version));

    // compact
    {
        auto old_val = config::lake_pk_compaction_min_input_segments;
        config::lake_pk_compaction_min_input_segments = 1;
        int64_t txn_id = next_id();
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
        EXPECT_EQ(100, task_context->progress.value());
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        config::lake_pk_compaction_min_input_segments = old_val;
    }
    // clear index, and then rebuild
    EXPECT_TRUE(_update_mgr->try_remove_primary_index_cache(tablet_id));
    {
        // re write chunk0
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*(chunks[0].first), chunks[0].second.data(), chunks[0].second.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    EXPECT_EQ(kChunkSize, read_rows(tablet_id, version));
}

TEST_P(LakePrimaryKeyPublishTest, test_cloud_native_index_minor_compact_because_files) {
    if (!GetParam().enable_persistent_index ||
        GetParam().persistent_index_type != PersistentIndexTypePB::CLOUD_NATIVE) {
        GTEST_SKIP() << "this case only for cloud native index";
        return;
    }
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i <= config::cloud_native_pk_index_rebuild_files_threshold; i++) {
        auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        if (i == config::cloud_native_pk_index_rebuild_files_threshold - 2) {
            // last one, check need rebuilt file cnt
            ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
            EXPECT_EQ(new_tablet_metadata->sstable_meta().sstables_size(), 0);
            EXPECT_EQ(LakePersistentIndex::need_rebuild_file_cnt(*new_tablet_metadata,
                                                                 new_tablet_metadata->sstable_meta()),
                      config::cloud_native_pk_index_rebuild_files_threshold - 1);
        }
    }
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    // make sure generate sst files
    EXPECT_TRUE(new_tablet_metadata->sstable_meta().sstables_size() > 0);
}

TEST_P(LakePrimaryKeyPublishTest, test_cloud_native_index_minor_compact_because_del_files) {
    if (!GetParam().enable_persistent_index ||
        GetParam().persistent_index_type != PersistentIndexTypePB::CLOUD_NATIVE) {
        GTEST_SKIP() << "this case only for cloud native index";
        return;
    }
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < config::cloud_native_pk_index_rebuild_files_threshold; i++) {
        auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, false);
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        if (i == config::cloud_native_pk_index_rebuild_files_threshold / 2 - 1) {
            // last one, check need rebuilt file cnt
            ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
            EXPECT_EQ(new_tablet_metadata->sstable_meta().sstables_size(), 0);
            EXPECT_EQ(LakePersistentIndex::need_rebuild_file_cnt(*new_tablet_metadata,
                                                                 new_tablet_metadata->sstable_meta()),
                      config::cloud_native_pk_index_rebuild_files_threshold);
        }
    }
    ASSERT_EQ(0, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    // make sure generate sst files
    EXPECT_TRUE(new_tablet_metadata->sstable_meta().sstables_size() > 0);
}

TEST_P(LakePrimaryKeyPublishTest, test_individual_index_compaction) {
    if (!GetParam().enable_persistent_index ||
        GetParam().persistent_index_type != PersistentIndexTypePB::CLOUD_NATIVE) {
        GTEST_SKIP() << "this case only for cloud native index";
        return;
    }
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    {
        // 1. upsert
        auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true, true);
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 0);
    // 2. multiple time delete, try to generate lots of small sst files
    int64_t old_config = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 1;
    for (int i = 0; i < 50; i++) {
        auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 1, true, false);
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    config::l0_max_mem_usage = old_config;
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 51);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 0);
    EXPECT_TRUE(new_tablet_metadata->sstable_meta().sstables_size() > 10);
    EXPECT_TRUE(compaction_score(_tablet_mgr.get(), new_tablet_metadata) > 10);
    // 3. compaction without sst
    {
        auto old_val = config::lake_pk_compaction_min_input_segments;
        auto old_val2 = config::lake_pk_index_sst_max_compaction_versions;
        config::lake_pk_compaction_min_input_segments = 1;
        config::lake_pk_index_sst_max_compaction_versions = 1;
        int64_t txn_id = next_id();
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
        EXPECT_EQ(100, task_context->progress.value());
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        config::lake_pk_compaction_min_input_segments = old_val;
        config::lake_pk_index_sst_max_compaction_versions = old_val2;
    }
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 0);
    size_t sst_cnt = new_tablet_metadata->sstable_meta().sstables_size();
    EXPECT_TRUE(sst_cnt > 10);
    EXPECT_TRUE(compaction_score(_tablet_mgr.get(), new_tablet_metadata) > 10);
    // 4. compaction with sst
    {
        int64_t txn_id = next_id();
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
        EXPECT_EQ(100, task_context->progress.value());
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_TRUE(compaction_score(_tablet_mgr.get(), new_tablet_metadata) == 1);
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 0);
    EXPECT_TRUE(new_tablet_metadata->sstable_meta().sstables_size() == 1);
    EXPECT_TRUE(new_tablet_metadata->orphan_files_size() >= (sst_cnt - 1));
}

TEST_P(LakePrimaryKeyPublishTest, test_publish_with_lazy_load) {
    const size_t N = 40000;
    auto [chunk0, indexes] = gen_data_and_index(N, 0, true, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    int64_t old_val = config::pk_column_lazy_load_threshold_bytes;
    config::pk_column_lazy_load_threshold_bytes = 1;
    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    config::pk_column_lazy_load_threshold_bytes = old_val;
    ASSERT_EQ(N, read_rows(tablet_id, version));
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

INSTANTIATE_TEST_SUITE_P(LakePrimaryKeyPublishTest, LakePrimaryKeyPublishTest,
                         ::testing::Values(PrimaryKeyParam{true}, PrimaryKeyParam{false},
                                           PrimaryKeyParam{true, PersistentIndexTypePB::CLOUD_NATIVE},
                                           PrimaryKeyParam{true, PersistentIndexTypePB::LOCAL,
                                                           PartialUpdateMode::ROW_MODE, true},
                                           PrimaryKeyParam{true, PersistentIndexTypePB::CLOUD_NATIVE,
                                                           PartialUpdateMode::ROW_MODE, true}));

} // namespace starrocks::lake
