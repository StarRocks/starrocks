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

// A multi-statement transaction (BEGIN; INSERT ...; INSERT ...; COMMIT) writes one txn log per statement and
// tablet, named after the statement's load id, and publish reads them back through the load ids FE sends. A
// tablet of a published partition always has a log of some statement, so publish must not succeed when it
// finds none: FE may send load ids that name no log (an older FE journaled them as 0-0, so the FE that
// published after a restart or a leader change sent those), and a retry may arrive after the logs of the
// published version were deleted. Applying no statement in either case made the transaction visible without
// its data, or overwrote the published version with one that lacks it.

#include <gtest/gtest.h>
#include <unistd.h>

#include <ctime>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/lake/transactions.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

class LakeMultiStatementPublishTest : public TestBase, public testing::WithParamInterface<KeysType> {
public:
    LakeMultiStatementPublishTest() : TestBase(kTestGroupPath) {
        _tablet_metadata = generate_simple_tablet_metadata(GetParam());
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

    // One statement of the transaction: kChunkSize rows starting at `first_key`, logged under `load_id`.
    void write_statement(int64_t txn_id, const PUniqueId& load_id, int first_key) {
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
                                                   .set_load_id(load_id)
                                                   .set_is_multi_statements_txn(true)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        auto chunk = gen_chunk(first_key);
        ASSERT_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    static TxnInfoPB make_txn_info(int64_t txn_id, const std::vector<PUniqueId>& load_ids) {
        TxnInfoPB info;
        info.set_txn_id(txn_id);
        info.set_txn_type(TXN_NORMAL);
        info.set_combined_txn_log(false);
        info.set_commit_time(time(nullptr));
        info.set_force_publish(false);
        for (const auto& load_id : load_ids) {
            info.add_load_ids()->CopyFrom(load_id);
        }
        return info;
    }

    StatusOr<TabletMetadataPtr> publish_multi_statement(int64_t new_version, int64_t txn_id,
                                                        const std::vector<PUniqueId>& load_ids) {
        std::vector<TxnInfoPB> txns{make_txn_info(txn_id, load_ids)};
        return publish_version(_tablet_mgr.get(), PublishTabletInfo(_tablet_metadata->id()), new_version - 1,
                               new_version, txns, false);
    }

    Status publish_multi_statement_log_version(int64_t log_version, int64_t txn_id,
                                               const std::vector<PUniqueId>& load_ids) {
        auto info = make_txn_info(txn_id, load_ids);
        return publish_log_version(_tablet_mgr.get(), _tablet_metadata->id(), std::span<const TxnInfoPB>(&info, 1),
                                   &log_version);
    }

    void delete_statement_logs(int64_t txn_id, const std::vector<PUniqueId>& load_ids) {
        for (const auto& load_id : load_ids) {
            auto st = fs::delete_file(_tablet_mgr->txn_log_location(_tablet_metadata->id(), txn_id, load_id));
            ASSERT_TRUE(st.ok() || st.is_not_found()) << st;
        }
    }

    // Row count and sum(c0) at `version`, read from the persisted metadata.
    std::pair<int64_t, int64_t> read_back(int64_t version) {
        _tablet_mgr->metacache()->prune();
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        int64_t rows = 0;
        int64_t sum_c0 = 0;
        while (true) {
            auto chunk = ChunkFactory::new_chunk(*_schema, 128);
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            for (size_t i = 0; i < chunk->num_rows(); i++) {
                sum_c0 += chunk->get(i)[0].get_int32();
            }
            rows += chunk->num_rows();
        }
        return {rows, sum_c0};
    }

    static PUniqueId make_load_id(int64_t hi, int64_t lo) {
        PUniqueId id;
        id.set_hi(hi);
        id.set_lo(lo);
        return id;
    }

    inline static const std::string kTestGroupPath = "test_lake_multi_statement_publish_" + std::to_string(getpid());
    constexpr static int kChunkSize = 128;
    // Two statements of kChunkSize rows each: keys [0, 2 * kChunkSize).
    constexpr static int64_t kTotalRows = 2 * kChunkSize;
    constexpr static int64_t kSumC0 = kTotalRows * (kTotalRows - 1) / 2;

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
    RuntimeProfile _dummy_runtime_profile{"dummy"};
};

// The load ids an older FE journaled read back as 0-0, so the FE that published the transaction after a
// restart or a leader change named no log of either statement.
TEST_P(LakeMultiStatementPublishTest, publish_with_load_ids_naming_no_txn_log_fails) {
    const int64_t txn_id = next_id();
    const auto load_id_1 = make_load_id(11, 12);
    const auto load_id_2 = make_load_id(21, 22);
    write_statement(txn_id, load_id_1, 0);
    write_statement(txn_id, load_id_2, kChunkSize);

    const auto lost_load_id = make_load_id(0, 0);
    auto st = publish_multi_statement(2, txn_id, {lost_load_id, lost_load_id});
    ASSERT_TRUE(st.status().is_not_found()) << st.status();
    ASSERT_TRUE(_tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), 2).status().is_not_found());

    // Nothing was consumed: the publish succeeds once it names the statements.
    ASSERT_OK(publish_multi_statement(2, txn_id, {load_id_1, load_id_2}).status());
    auto [rows, sum_c0] = read_back(2);
    EXPECT_EQ(kTotalRows, rows);
    EXPECT_EQ(kSumC0, sum_c0);
}

// FE retries the publish of a whole partition when one of its tablets failed, so a tablet that already
// published gets the same request again, possibly after its txn logs were deleted and on a node that does
// not have the published metadata cached.
TEST_P(LakeMultiStatementPublishTest, retry_after_txn_logs_deleted_keeps_published_version) {
    const int64_t txn_id = next_id();
    const auto load_id_1 = make_load_id(31, 32);
    const auto load_id_2 = make_load_id(41, 42);
    write_statement(txn_id, load_id_1, 0);
    write_statement(txn_id, load_id_2, kChunkSize);

    ASSERT_OK(publish_multi_statement(2, txn_id, {load_id_1, load_id_2}).status());
    delete_statement_logs(txn_id, {load_id_1, load_id_2});
    _tablet_mgr->metacache()->prune();

    ASSIGN_OR_ABORT(auto metadata, publish_multi_statement(2, txn_id, {load_id_1, load_id_2}));
    EXPECT_EQ(2, metadata->version());
    auto [rows, sum_c0] = read_back(2);
    EXPECT_EQ(kTotalRows, rows);
    EXPECT_EQ(kSumC0, sum_c0);
}

// During a schema change publish_log_version folds the statements of a shadow tablet into one .vlog. A retry
// after the statements' logs were deleted finds the .vlog and succeeds; without either there is nothing to
// publish.
TEST_P(LakeMultiStatementPublishTest, publish_log_version_retry_after_txn_logs_deleted) {
    const int64_t txn_id = next_id();
    const auto load_id_1 = make_load_id(51, 52);
    const auto load_id_2 = make_load_id(61, 62);
    write_statement(txn_id, load_id_1, 0);
    write_statement(txn_id, load_id_2, kChunkSize);

    const int64_t log_version = 2;
    ASSERT_OK(publish_multi_statement_log_version(log_version, txn_id, {load_id_1, load_id_2}));
    ASSERT_TRUE(fs::path_exist(_tablet_mgr->txn_vlog_location(_tablet_metadata->id(), log_version)));
    delete_statement_logs(txn_id, {load_id_1, load_id_2});

    ASSERT_OK(publish_multi_statement_log_version(log_version, txn_id, {load_id_1, load_id_2}));
    auto st = publish_multi_statement_log_version(log_version + 1, next_id(), {load_id_1, load_id_2});
    ASSERT_TRUE(st.is_not_found()) << st;
}

INSTANTIATE_TEST_SUITE_P(LakeMultiStatementPublishTest, LakeMultiStatementPublishTest,
                         testing::Values(DUP_KEYS, PRIMARY_KEYS));

} // namespace starrocks::lake
