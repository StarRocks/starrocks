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

// A multi-statement transaction (BEGIN; INSERT ...; INSERT ...; COMMIT) on a file_bundling table writes one
// txn log per statement, each through the statement's own bundle file. A writer bundles only the one segment
// it writes at end of stream, so a statement with a small share of a tablet leaves a bundled log while a
// statement that flushed mid-load leaves standalone segments. Publish folds the statements into one rowset;
// it used to refuse the mix (non primary key: "Inconsistent bundle_file_offsets across txn logs"; primary
// key: "lake rowset has a mix of bundled and standalone segments" on save), and since the logs never change,
// every retry failed and the transaction stayed COMMITTED.
//
// The tests drive the real write and publish path: DeltaWriter per statement -> lake::publish_version with
// the statements' load_ids -> read back.

#include <gtest/gtest.h>
#include <unistd.h>

#include <ctime>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config_ingest_fwd.h"
#include "fs/bundle_file.h"
#include "gen_cpp/Types_types.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/storage_env.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

class LakeMultiStatementBundleMixTest : public TestBase, public testing::WithParamInterface<KeysType> {
public:
    LakeMultiStatementBundleMixTest() : TestBase(kTestGroupPath) {
        _tablet_metadata = generate_simple_tablet_metadata(GetParam());
        if (GetParam() == PRIMARY_KEYS) {
            _tablet_metadata->set_enable_persistent_index(true);
            _tablet_metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        }
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    // Rows with keys [first_key, first_key + kChunkSize), c1 = 3 * c0.
    Chunk gen_chunk(int first_key) {
        std::vector<int> v0(kChunkSize);
        std::vector<int> v1(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) {
            v0[i] = first_key + i;
            v1[i] = 3 * (first_key + i);
        }
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return Chunk({std::move(c0), std::move(c1)}, _schema);
    }

    // Write one statement of the transaction through its own bundle file: `chunks` chunks of distinct keys
    // starting at `first_key`. max_buffer_size 0 keeps the default memtable size, so the statement stays in
    // memory and is bundled at end of stream; 1 byte fills the memtable on every write, so each chunk is
    // flushed mid-load into its own standalone segment.
    void write_statement(int64_t txn_id, const PUniqueId& load_id, int first_key, int chunks, int64_t max_buffer_size) {
        BundleWritableFileContext bundle_context;
        std::vector<uint32_t> indexes(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) {
            indexes[i] = i;
        }
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(_tablet_metadata->id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .set_bundle_writable_file_context(&bundle_context)
                                                   .set_load_id(load_id)
                                                   .set_is_multi_statements_txn(true)
                                                   .set_max_buffer_size(max_buffer_size)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        for (int i = 0; i < chunks; i++) {
            auto chunk = gen_chunk(first_key + i * kChunkSize);
            ASSERT_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        }
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    TxnLogPtr get_statement_log(int64_t txn_id, const PUniqueId& load_id) {
        auto log_path = _tablet_mgr->txn_log_location(_tablet_metadata->id(), txn_id, load_id);
        ASSIGN_OR_ABORT(auto log, _tablet_mgr->get_txn_log(log_path, false));
        return log;
    }

    StatusOr<TabletMetadataPtr> publish_multi_statement(int64_t new_version, int64_t txn_id,
                                                        const std::vector<PUniqueId>& load_ids) {
        TxnInfoPB info;
        info.set_txn_id(txn_id);
        info.set_txn_type(TXN_NORMAL);
        info.set_combined_txn_log(false);
        info.set_commit_time(time(nullptr));
        info.set_force_publish(false);
        for (const auto& load_id : load_ids) {
            info.add_load_ids()->CopyFrom(load_id);
        }
        std::vector<TxnInfoPB> txns{info};
        return publish_version(_tablet_mgr.get(), PublishTabletInfo(_tablet_metadata->id()), new_version - 1,
                               new_version, txns, false);
    }

    // Row count, sum(c0), sum(c1) at `version`.
    std::tuple<int64_t, int64_t, int64_t> read_back(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        int64_t rows = 0;
        int64_t sum_c0 = 0;
        int64_t sum_c1 = 0;
        while (true) {
            auto chunk = ChunkFactory::new_chunk(*_schema, 128);
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            for (size_t i = 0; i < chunk->num_rows(); i++) {
                sum_c0 += chunk->get(i)[0].get_int32();
                sum_c1 += chunk->get(i)[1].get_int32();
            }
            rows += chunk->num_rows();
        }
        return {rows, sum_c0, sum_c1};
    }

    static PUniqueId make_load_id(int64_t hi, int64_t lo) {
        PUniqueId id;
        id.set_hi(hi);
        id.set_lo(lo);
        return id;
    }

    // Publish BEGIN; INSERT <small>; INSERT <large>; COMMIT; (or the two statements the other way round)
    // and check every row reads back.
    void run_bundled_and_standalone_statements(bool bundled_first) {
        // Without load spill every full memtable is flushed straight into its own standalone segment.
        auto backup = config::enable_load_spill;
        config::enable_load_spill = false;
        DeferOp defer([&]() { config::enable_load_spill = backup; });

        const int64_t txn_id = next_id();
        const auto small_load_id = make_load_id(1, 1);
        const auto large_load_id = make_load_id(1, 2);
        constexpr int kLargeChunks = 3;
        if (bundled_first) {
            write_statement(txn_id, small_load_id, 0, 1, 0);
            write_statement(txn_id, large_load_id, kChunkSize, kLargeChunks, 1);
        } else {
            write_statement(txn_id, large_load_id, kChunkSize, kLargeChunks, 1);
            write_statement(txn_id, small_load_id, 0, 1, 0);
        }

        // The shape the two statements leave behind: one bundled slice, and standalone segments.
        auto small_log = get_statement_log(txn_id, small_load_id);
        ASSERT_EQ(1, small_log->op_write().rowset().segment_metas_size());
        ASSERT_TRUE(small_log->op_write().rowset().segment_metas(0).has_bundle_file_offset());
        auto large_log = get_statement_log(txn_id, large_load_id);
        ASSERT_EQ(kLargeChunks, large_log->op_write().rowset().segment_metas_size());
        for (const auto& segment_meta : large_log->op_write().rowset().segment_metas()) {
            ASSERT_FALSE(segment_meta.has_bundle_file_offset());
        }

        std::vector<PUniqueId> load_ids = bundled_first ? std::vector<PUniqueId>{small_load_id, large_load_id}
                                                        : std::vector<PUniqueId>{large_load_id, small_load_id};
        ASSIGN_OR_ABORT(auto metadata, publish_multi_statement(2, txn_id, load_ids));
        ASSERT_EQ(1, metadata->rowsets_size());
        const auto& rowset = metadata->rowsets(0);
        ASSERT_EQ(1 + kLargeChunks, rowset.segment_metas_size());
        const auto& bundled_segment = small_log->op_write().rowset().segment_metas(0);
        for (const auto& segment_meta : rowset.segment_metas()) {
            ASSERT_TRUE(segment_meta.has_bundle_file_offset());
            if (segment_meta.filename() == bundled_segment.filename()) {
                EXPECT_EQ(bundled_segment.bundle_file_offset(), segment_meta.bundle_file_offset());
                EXPECT_FALSE(segment_meta.synthetic_bundle_file_offset());
            } else {
                EXPECT_EQ(0, segment_meta.bundle_file_offset());
                EXPECT_TRUE(segment_meta.synthetic_bundle_file_offset());
            }
        }

        // Read back from the persisted metadata, not the cached copy publish returned.
        _tablet_mgr->metacache()->prune();
        const int64_t total_rows = (1 + kLargeChunks) * kChunkSize;
        const int64_t expected_sum_c0 = total_rows * (total_rows - 1) / 2;
        auto [rows, sum_c0, sum_c1] = read_back(2);
        EXPECT_EQ(total_rows, rows);
        EXPECT_EQ(expected_sum_c0, sum_c0);
        EXPECT_EQ(3 * expected_sum_c0, sum_c1);
    }

    inline static const std::string kTestGroupPath = "test_lake_multi_statement_bundle_mix_" + std::to_string(getpid());
    constexpr static int kChunkSize = 128;

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
    RuntimeProfile _dummy_runtime_profile{"dummy"};
};

TEST_P(LakeMultiStatementBundleMixTest, bundled_statement_then_standalone_statement) {
    run_bundled_and_standalone_statements(/*bundled_first=*/true);
}

TEST_P(LakeMultiStatementBundleMixTest, standalone_statement_then_bundled_statement) {
    run_bundled_and_standalone_statements(/*bundled_first=*/false);
}

INSTANTIATE_TEST_SUITE_P(LakeMultiStatementBundleMixTest, LakeMultiStatementBundleMixTest,
                         testing::Values(DUP_KEYS, PRIMARY_KEYS));

} // namespace starrocks::lake
