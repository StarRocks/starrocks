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

// Regression test for #78224.
//
// A multi-statement transaction (SQL BEGIN ... COMMIT, i.e. a TxnInfoPB carrying more than one
// load_id) is published by folding every statement's op_write into ONE merged rowset. A del file
// carries `op_offset`: the index of the last segment of its rowset that the delete may erase. Apply
// (UpdateManager::publish_primary_key_tablet) computes it from the statement that produced the del;
// persist (MetaFileBuilder) used to leave an unrecorded offset to be resolved against the MERGED
// rowset, i.e. after every LATER statement's segments too. On rebuild the delete then erased keys a
// later statement had re-inserted, whose rows are live and not delete-vector-masked -- the next
// upsert of such a key finds nothing in the index, writes no delete-vector mark, and leaves two live
// rows for one key, which the following rebuild reports as "insert found duplicate key".
//
// Both tests drive the real publish path (DeltaWriter -> LakeServiceImpl::publish_version with two
// load_ids -> rebuild on publish); nothing about the failing state is hand-assembled.

#include <gtest/gtest.h>
#include <unistd.h>

#include <ctime>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config.h"
#include "gen_cpp/Types_types.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/lake/update_manager.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

class LakeMergedDelOpOffsetTest : public TestBase {
public:
    LakeMergedDelOpOffsetTest() : TestBase(kTestGroupPath) {
        _tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
        _tablet_metadata->set_enable_persistent_index(true);
        _tablet_metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);

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
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        ExecEnv::GetInstance()->parallel_compact_mgr()->TEST_set_tablet_mgr(_tablet_mgr.get());
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    // One chunk of `kNumKeys` rows, keys 0..kNumKeys-1, all upserts or all deletes.
    ChunkPtr gen_chunk(bool upsert) {
        std::vector<int> v0(kNumKeys);
        std::vector<int> v1(kNumKeys);
        std::vector<uint8_t> v2(kNumKeys);
        for (int i = 0; i < kNumKeys; i++) {
            v0[i] = i;
            v1[i] = i * 3;
            v2[i] = upsert ? TOpType::UPSERT : TOpType::DELETE;
        }
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int8Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        c2->append_numbers(v2.data(), v2.size() * sizeof(uint8_t));
        return std::make_shared<Chunk>(Columns{std::move(c0), std::move(c1), std::move(c2)}, _slot_cid_map);
    }

    // Write one statement of a transaction. `load_id == nullptr` means an ordinary single-statement
    // load; otherwise the writer emits a per-load_id txn log, which is what a multi-statement
    // transaction does.
    void write_statement(int64_t txn_id, const PUniqueId* load_id, bool upsert) {
        auto chunk = gen_chunk(upsert);
        std::vector<uint32_t> indexes(chunk->num_rows());
        for (uint32_t i = 0; i < chunk->num_rows(); i++) {
            indexes[i] = i;
        }
        DeltaWriterBuilder builder;
        builder.set_tablet_manager(_tablet_mgr.get())
                .set_tablet_id(_tablet_metadata->id())
                .set_txn_id(txn_id)
                .set_partition_id(_partition_id)
                .set_mem_tracker(_mem_tracker.get())
                .set_schema_id(_tablet_schema->id())
                .set_slot_descriptors(&_slot_pointers)
                .set_profile(&_dummy_runtime_profile);
        if (load_id != nullptr) {
            builder.set_load_id(*load_id).set_is_multi_statements_txn(true);
        }
        ASSIGN_OR_ABORT(auto delta_writer, builder.build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    // Publish one transaction whose statements were written under `load_ids` (the merged-rowset
    // path), optionally rebuilding the persistent index first (what a CN restart does).
    StatusOr<TabletMetadataPtr> publish_multi_statement(int64_t base_version, int64_t new_version, int64_t txn_id,
                                                        const std::vector<PUniqueId>& load_ids, bool rebuild_pindex) {
        PublishVersionRequest request;
        PublishVersionResponse response;
        request.add_tablet_ids(_tablet_metadata->id());
        request.set_base_version(base_version);
        request.set_new_version(new_version);
        request.set_commit_time(time(nullptr));
        if (rebuild_pindex) {
            request.add_rebuild_pindex_tablet_ids(_tablet_metadata->id());
        }
        auto* info = request.add_txn_infos();
        info->set_txn_id(txn_id);
        info->set_txn_type(TXN_NORMAL);
        info->set_combined_txn_log(false);
        info->set_commit_time(time(nullptr));
        info->set_force_publish(false);
        for (const auto& load_id : load_ids) {
            info->add_load_ids()->CopyFrom(load_id);
        }
        auto lake_service = LakeServiceImpl(ExecEnv::GetInstance(), _tablet_mgr.get());
        lake_service.publish_version(nullptr, &request, &response, nullptr);
        if (response.failed_tablets_size() > 0) {
            if (response.status().status_code() != 0) {
                return Status(response.status());
            }
            return Status::InternalError("failed to publish version");
        }
        return _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), new_version);
    }

    int64_t read_rows(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        int64_t rows = 0;
        while (true) {
            auto chunk = ChunkHelper::new_chunk(*_schema, 128);
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            rows += chunk->num_rows();
        }
        return rows;
    }

    static PUniqueId make_load_id(int64_t hi, int64_t lo) {
        PUniqueId id;
        id.set_hi(hi);
        id.set_lo(lo);
        return id;
    }

    static uint32_t max_segment_idx(const RowsetMetadataPB& rowset) {
        uint32_t max_idx = 0;
        for (int i = 0; i < rowset.segment_metas_size(); i++) {
            const auto& seg = rowset.segment_metas(i);
            max_idx = std::max(max_idx, seg.has_segment_idx() ? seg.segment_idx() : static_cast<uint32_t>(i));
        }
        return max_idx;
    }

    // The single rowset carrying a del file, i.e. the merged rowset of the multi-statement txn.
    static const RowsetMetadataPB* rowset_with_del(const TabletMetadataPB& metadata) {
        const RowsetMetadataPB* found = nullptr;
        for (const auto& rowset : metadata.rowsets()) {
            if (rowset.del_files_size() > 0) {
                EXPECT_EQ(nullptr, found) << "more than one rowset carries a del file";
                found = &rowset;
            }
        }
        return found;
    }

    // Seed the table at version 2, then run the multi-statement transaction at version 3:
    // statement 1 deletes every key, statement 2 re-upserts every key.
    void seed_and_run_multi_statement_txn() {
        // version 2: an ordinary load establishing the keys.
        auto seed_txn = next_id();
        write_statement(seed_txn, nullptr, /*upsert=*/true);
        ASSERT_OK(publish_single_version(_tablet_metadata->id(), 2, seed_txn).status());
        ASSERT_EQ(kNumKeys, read_rows(2));

        // version 3: BEGIN; DELETE every key; INSERT every key back; COMMIT;
        auto txn_id = next_id();
        auto del_load_id = make_load_id(1, 1);
        auto reinsert_load_id = make_load_id(1, 2);
        write_statement(txn_id, &del_load_id, /*upsert=*/false);
        write_statement(txn_id, &reinsert_load_id, /*upsert=*/true);
        ASSERT_OK(publish_multi_statement(2, 3, txn_id, {del_load_id, reinsert_load_id},
                                          /*rebuild_pindex=*/false)
                          .status());
        // The re-inserted rows are the live ones; the seeded rows are delete-vector-masked.
        ASSERT_EQ(kNumKeys, read_rows(3));
    }

    inline static const std::string kTestGroupPath = "test_lake_merged_del_op_offset_" + std::to_string(getpid());
    constexpr static int kNumKeys = 20;

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;
    RuntimeProfile _dummy_runtime_profile{"dummy"};
};

// A del file's persisted op_offset must name the last segment of the STATEMENT that produced it, not
// the last segment of the merged rowset: it is the only thing that tells the rebuild the delete came
// before the later statements' segments.
TEST_F(LakeMergedDelOpOffsetTest, del_op_offset_stays_within_its_own_statement) {
    // branch-4.1's product default: the writer records no per-del op_offset, so persist has to
    // derive it. Pinned, so the test exercises that path whatever the default is.
    ConfigResetGuard preserve_order_guard(&config::lake_enable_pk_preserve_txn_delete_order, false);
    seed_and_run_multi_statement_txn();

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), 3));
    const auto* merged = rowset_with_del(*metadata);
    ASSERT_NE(nullptr, merged);
    ASSERT_EQ(1, merged->del_files_size());
    // The re-upsert statement's segments were appended after the delete statement's reserved slot.
    ASSERT_GT(max_segment_idx(*merged), 0u);
    ASSERT_TRUE(merged->del_files(0).has_op_offset());
    EXPECT_LT(merged->del_files(0).op_offset(), max_segment_idx(*merged))
            << "op_offset " << merged->del_files(0).op_offset() << " reaches the merged rowset's last segment, so the "
            << "rebuild replays this delete over the re-inserted rows instead of before them";
}

// The end-to-end mechanism: rebuild the persistent index (what a CN restart or a tablet
// re-placement does) and then upsert the same keys again. If the rebuild dropped the re-inserted
// keys from the index, that upsert cannot mask their rows and the table ends up with two live rows
// per key.
TEST_F(LakeMergedDelOpOffsetTest, rebuild_after_delete_and_reinsert_keeps_one_row_per_key) {
    // branch-4.1's product default: the writer records no per-del op_offset, so persist has to
    // derive it. Pinned, so the test exercises that path whatever the default is.
    ConfigResetGuard preserve_order_guard(&config::lake_enable_pk_preserve_txn_delete_order, false);
    seed_and_run_multi_statement_txn();

    // version 4: rebuild the index from object storage, then upsert the same keys.
    auto txn_id = next_id();
    write_statement(txn_id, nullptr, /*upsert=*/true);
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), 4, txn_id, /*rebuild_pindex=*/true).status());
    EXPECT_EQ(kNumKeys, read_rows(4)) << "the rebuild lost the re-inserted keys from the index, so the upsert at "
                                      << "version 4 wrote no delete-vector mark and left two live rows per key";
}

} // namespace starrocks::lake
