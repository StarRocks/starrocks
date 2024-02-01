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
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/test_util.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

class LakePrimaryKeyPublishTest : public TestBase, testing::WithParamInterface<PrimaryKeyParam> {
public:
    LakePrimaryKeyPublishTest() : TestBase(kTestGroupPath) {
        _tablet_metadata = std::make_unique<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);
        _tablet_metadata->set_enable_persistent_index(GetParam().enable_persistent_index);

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
            c1->set_aggregation("REPLACE");
        }

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(*_tablet_schema));
    }

    void SetUp() override {
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
    }

    ChunkPtr gen_data(int64_t chunk_size, int shift, bool random_shuffle) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
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

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return std::make_shared<Chunk>(Columns{c0, c1}, _schema);
    }

    std::pair<ChunkPtr, std::vector<uint32_t>> gen_data_and_index(int64_t chunk_size, int shift, bool random_shuffle) {
        auto chunk = gen_data(chunk_size, shift, random_shuffle);
        auto indexes = std::vector<uint32_t>(chunk->num_rows());
        for (uint32_t i = 0, n = chunk->num_rows(); i < n; i++) {
            indexes[i] = i;
        }
        return {std::move(chunk), std::move(indexes)};
    }

    ChunkPtr read(int64_t tablet_id, int64_t version) {
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));
        ASSIGN_OR_ABORT(auto reader, tablet.new_reader(version, *_schema));
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

    std::unique_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
};

TEST_P(LakePrimaryKeyPublishTest, test_write_read_success) {
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
    EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(_tablet_metadata->id(), txn_id));

    // read at version 2
    ASSIGN_OR_ABORT(auto reader, tablet.new_reader(2, *_schema));
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
}

TEST_P(LakePrimaryKeyPublishTest, test_write_multitime_check_result) {
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
    }
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), kChunkSize);
    EXPECT_EQ(new_tablet_metadata->rowsets(2).num_dels(), 0);
    if (GetParam().enable_persistent_index) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePrimaryKeyPublishTest, test_write_fail_retry) {
    std::vector<ChunkPtr> chunks;
    for (int i = 0; i < 5; i++) {
        chunks.emplace_back(gen_data(kChunkSize, i, true));
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
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
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
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
<<<<<<< HEAD
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));
        auto txn_log_st = tablet.get_txn_log(txn_id);
        EXPECT_TRUE(txn_log_st.ok());
        auto& txn_log = txn_log_st.value();
        ASSIGN_OR_ABORT(auto base_metadata, tablet.get_metadata(version));
        auto new_metadata = std::make_shared<TabletMetadata>(*base_metadata);
        new_metadata->set_version(version + 1);
        std::unique_ptr<MetaFileBuilder> builder = std::make_unique<MetaFileBuilder>(tablet, new_metadata);
        // update primary table state, such as primary index
        std::unique_ptr<std::lock_guard<std::mutex>> lock = nullptr;
        ASSIGN_OR_ABORT(auto index_entry, tablet.update_mgr()->prepare_primary_index(
                                                  *new_metadata, &tablet, builder.get(), version, version + 1, lock));
        ASSERT_OK(tablet.update_mgr()->publish_primary_key_tablet(txn_log->op_write(), txn_log->txn_id(), *new_metadata,
                                                                  &tablet, index_entry, builder.get(), version));
        // if builder.finalize fail, remove primary index cache and retry
        tablet.update_mgr()->release_primary_index_cache(index_entry);
        builder->handle_failure();
=======
        // Publish version
        ASSERT_ERROR(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
>>>>>>> 3bf4bda790 ([Enhancement] Call unload before remove primary index to make sure clean the error pk index before retry (#40479))
    }
    // write success
    for (int i = 3; i < 5; i++) {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
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
    if (GetParam().enable_persistent_index) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePrimaryKeyPublishTest, test_publish_multi_times) {
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true);
    auto txns = std::vector<int64_t>();
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
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
    if (GetParam().enable_persistent_index) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePrimaryKeyPublishTest, test_publish_concurrent) {
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
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
    if (GetParam().enable_persistent_index) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePrimaryKeyPublishTest, test_resolve_conflict) {
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
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
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        // will preload update state here.
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        txn_ids.push_back(txn_id);
    }
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
    if (GetParam().enable_persistent_index) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePrimaryKeyPublishTest, test_write_read_success_multiple_tablet) {
    auto [chunk0, indexes_1] = gen_data_and_index(kChunkSize * 1, 0, false);
    auto [chunk1, indexes_2] = gen_data_and_index(kChunkSize * 2, 1, false);

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
            auto w = DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr,
                                         _mem_tracker.get());
            ASSERT_OK(w->open());
            ASSERT_OK(w->write(*chunk_ptr, indexes, indexes_size));
            ASSERT_OK(w->finish());
            w->close();
            // Publish version
            ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
            EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        }
        version++;
    }
    auto chunk2 = read(tablet_id_1, version);
    auto chunk3 = read(tablet_id_2, version);
    assert_chunk_equals(*chunk0, *chunk2);
    assert_chunk_equals(*chunk1, *chunk3);
    if (GetParam().enable_persistent_index) {
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
        auto [chunk0, indexes] = gen_data_and_index(kChunkSize, i, true);
        int64_t txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
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
    if (GetParam().enable_persistent_index) {
        check_local_persistent_index_meta(tablet_id, version);
    }
    config::l0_max_mem_usage = old_config;
}

TEST_P(LakePrimaryKeyPublishTest, test_recover) {
    config::enable_primary_key_recover = true;
    auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true);
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        std::string sync_point = "lake_index_load.1";
        // Publish version
        int retry_time = 0;
        SyncPoint::GetInstance()->SetCallBack(sync_point, [&](void* arg) {
            if (retry_time < 1) {
                *(Status*)arg = Status::AlreadyExist("ut_test");
                retry_time++;
            }
        });
        SyncPoint::GetInstance()->EnableProcessing();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        SyncPoint::GetInstance()->ClearCallBack(sync_point);
        SyncPoint::GetInstance()->DisableProcessing();
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(tablet_id, 1));
        version++;
    }
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));
    if (GetParam().enable_persistent_index) {
        check_local_persistent_index_meta(tablet_id, version);
    }
    config::enable_primary_key_recover = false;
}

TEST_P(LakePrimaryKeyPublishTest, test_abort_txn) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->LoadDependency(
            {{"UpdateManager::preload_update_state:return", "transactions::abort_txn:enter"}});

    auto tablet_id = _tablet_metadata->id();
    auto txn_id = next_id();
    std::thread t1([&]() {
        auto [chunk0, indexes] = gen_data_and_index(kChunkSize, 0, true);
        auto tablet_id = _tablet_metadata->id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
    });

    std::thread t2([&]() {
        lake::AbortTxnRequest request;
        request.add_tablet_ids(tablet_id);
        request.add_txn_ids(txn_id);
        request.set_skip_cleanup(false);
        lake::AbortTxnResponse response;
        auto lake_service = LakeServiceImpl(ExecEnv::GetInstance(), _tablet_mgr.get());
        lake_service.abort_txn(nullptr, &request, &response, nullptr);
    });

    t1.join();
    t2.join();
    ASSERT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
    SyncPoint::GetInstance()->DisableProcessing();
}

<<<<<<< HEAD
=======
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
                                               .set_index_id(_tablet_schema->id())
                                               .set_slot_descriptors(&_slot_pointers)
                                               .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(*chunk0, indexes0.data(), indexes0.size()));
    ASSERT_OK(delta_writer->finish());
    delta_writer->close();

    txn_id = next_id();
    txn_ids.emplace_back(txn_id);
    ASSIGN_OR_ABORT(delta_writer, DeltaWriterBuilder()
                                          .set_tablet_manager(_tablet_mgr.get())
                                          .set_tablet_id(tablet_id)
                                          .set_txn_id(txn_id)
                                          .set_partition_id(_partition_id)
                                          .set_mem_tracker(_mem_tracker.get())
                                          .set_index_id(_tablet_schema->id())
                                          .set_slot_descriptors(&_slot_pointers)
                                          .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(*chunk1, indexes1.data(), indexes1.size()));
    ASSERT_OK(delta_writer->finish());
    delta_writer->close();

    auto new_version = base_version + 2;
    ASSERT_OK(batch_publish(tablet_id, base_version, new_version, txn_ids).status());

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 2);
    EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 12);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), 0);
    EXPECT_EQ(0, read_rows(tablet_id, new_version));
    _tablet_mgr->prune_metacache();
    _update_mgr->try_remove_primary_index_cache(tablet_id);

    // publish again
    ASSERT_OK(batch_publish(tablet_id, base_version, new_version, txn_ids).status());
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, new_version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 2);
    EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 1);
    EXPECT_EQ(new_tablet_metadata->rowsets(0).num_dels(), 12);
    EXPECT_EQ(new_tablet_metadata->rowsets(1).num_dels(), 0);
    EXPECT_EQ(0, read_rows(tablet_id, new_version));
}

>>>>>>> 3bf4bda790 ([Enhancement] Call unload before remove primary index to make sure clean the error pk index before retry (#40479))
TEST_P(LakePrimaryKeyPublishTest, test_mem_tracker) {
    EXPECT_EQ(1024 * 1024, _mem_tracker->limit());
    EXPECT_EQ(1024 * 1024 * config::lake_pk_preload_memory_limit_percent / 100,
              _update_mgr->compaction_state_mem_tracker()->limit());
    EXPECT_EQ(1024 * 1024 * config::lake_pk_preload_memory_limit_percent / 100,
              _update_mgr->update_state_mem_tracker()->limit());
}

INSTANTIATE_TEST_SUITE_P(LakePrimaryKeyPublishTest, LakePrimaryKeyPublishTest,
                         ::testing::Values(PrimaryKeyParam{true}, PrimaryKeyParam{false}));

} // namespace starrocks::lake
