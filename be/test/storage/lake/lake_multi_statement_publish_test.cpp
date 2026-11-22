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
//
// FE also publishes consecutive transactions of a partition in one batch, which writes the metadata of the
// batch's last version only. A batch attempt that FE does not see succeed (another tablet of the partition
// failed, or the node was stopped before it replied) is retried with the batch FE builds then, often longer
// and possibly on another node, so the retry has to apply every transaction of the batch again.

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <ctime>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config_lake_fwd.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/filenames.h"
#include "storage/lake/metacache.h"
#include "storage/lake/options.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/lake/transactions.h"
#include "storage/lake/vacuum.h"
#include "storage/storage_engine.h"
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
        write_rows(txn_id, &load_id, first_key);
    }

    // An autocommit load: kChunkSize rows starting at `first_key`, logged without a load id.
    void write_autocommit_txn(int64_t txn_id, int first_key) { write_rows(txn_id, nullptr, first_key); }

    void write_rows(int64_t txn_id, const PUniqueId* load_id, int first_key) {
        std::vector<uint32_t> indexes(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) {
            indexes[i] = i;
        }
        DeltaWriterBuilder builder;
        builder.set_tablet_manager(_tablet_mgr.get())
                .set_tablet_id(_tablet_metadata->id())
                .set_txn_id(txn_id)
                .set_partition_id(_partition_id)
                .set_mem_tracker(_mem_tracker.get())
                .set_schema_id(_tablet_schema->id())
                .set_profile(&_dummy_runtime_profile);
        if (load_id != nullptr) {
            builder.set_load_id(*load_id).set_is_multi_statements_txn(true);
        }
        ASSIGN_OR_ABORT(auto delta_writer, builder.build());
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

    // Publish `txns` as one batch: versions base_version + 1 .. base_version + txns.size().
    StatusOr<TabletMetadataPtr> publish_batch(int64_t base_version, const std::vector<TxnInfoPB>& txns) {
        return publish_version(_tablet_mgr.get(), PublishTabletInfo(_tablet_metadata->id()), base_version,
                               base_version + static_cast<int64_t>(txns.size()), txns, false);
    }

    // What a publish finds on a node other than the one that ran the previous attempt: neither the metadata,
    // the txn logs nor the primary index of the tablet are cached there.
    void switch_node() {
        StorageEngine::instance()->wait_storage_cleanup_tasks();
        _tablet_mgr->metacache()->prune();
        _primary_key_mgr->unload_and_remove_primary_index(_tablet_metadata->id());
    }

    bool txn_log_exists(int64_t txn_id, const PUniqueId* load_id) {
        auto path = load_id == nullptr ? _tablet_mgr->txn_log_location(_tablet_metadata->id(), txn_id)
                                       : _tablet_mgr->txn_log_location(_tablet_metadata->id(), txn_id, *load_id);
        return fs::path_exist(path);
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

    // sum(c0) of the kChunkSize rows gen_chunk(first_key) produces.
    static constexpr int64_t chunk_sum_c0(int first_key) {
        return int64_t{kChunkSize} * first_key + int64_t{kChunkSize} * (kChunkSize - 1) / 2;
    }

    inline static const std::string kTestGroupPath = "test_lake_multi_statement_publish_" + std::to_string(getpid());
    constexpr static int kChunkSize = 128;
    // Two statements of kChunkSize rows each: keys [0, 2 * kChunkSize).
    constexpr static int64_t kTotalRows = 2 * kChunkSize;
    constexpr static int64_t kSumC0 = kTotalRows * (kTotalRows - 1) / 2;
    // Keys of the autocommit loads around the transaction, disjoint from the transaction's keys.
    constexpr static int kBeforeTxnFirstKey = 10000;
    constexpr static int kAfterTxnFirstKey = 20000;
    constexpr static int kLaterLoadFirstKey = 30000;

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

// The reader-first rollout must not change the default on-disk format. Older CNs must still be able
// to read a newly materialized multi-statement version log until group writing is explicitly enabled.
TEST_P(LakeMultiStatementPublishTest, publish_log_version_defaults_to_legacy_format) {
    ASSERT_FALSE(config::lake_enable_txn_vlog_group);
    const int64_t txn_id = next_id();
    const auto first_load = make_load_id(71, 72);
    const auto second_load = make_load_id(81, 82);
    write_statement(txn_id, first_load, 0);
    write_statement(txn_id, second_load, kChunkSize);

    ASSERT_OK(publish_multi_statement_log_version(2, txn_id, {first_load, second_load}));
    _tablet_mgr->metacache()->prune();
    EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_vlog_group_location(_tablet_metadata->id(), 2)));
    const auto path = _tablet_mgr->txn_vlog_location(_tablet_metadata->id(), 2);
    auto legacy_log = _tablet_mgr->get_txn_vlog(path, false);
    ASSERT_OK(legacy_log.status());
    EXPECT_EQ(kTotalRows, legacy_log.value()->op_write().rowset().num_rows());
    EXPECT_EQ(2, legacy_log.value()->op_write().rowset().segment_metas_size());

    ASSIGN_OR_ABORT(auto new_reader_logs, _tablet_mgr->get_txn_vlog_group(path, false));
    ASSERT_EQ(1, new_reader_logs.size());
    EXPECT_EQ(legacy_log.value()->SerializeAsString(), new_reader_logs.front()->SerializeAsString());
}

TEST_P(LakeMultiStatementPublishTest, publish_log_version_group_write_is_opt_in) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, true);
    const int64_t txn_id = next_id();
    const auto first_load = make_load_id(91, 92);
    const auto second_load = make_load_id(101, 102);
    write_statement(txn_id, first_load, 0);
    write_statement(txn_id, second_load, kChunkSize);

    ASSERT_OK(publish_multi_statement_log_version(2, txn_id, {first_load, second_load}));
    _tablet_mgr->metacache()->prune();
    // The group lives in a file of its own. A CN without the group reader finds no legacy version log
    // instead of misreading the group, and its legacy reader rejects the group file itself.
    const auto legacy_path = _tablet_mgr->txn_vlog_location(_tablet_metadata->id(), 2);
    EXPECT_TRUE(_tablet_mgr->get_txn_vlog(legacy_path, false).status().is_not_found());
    const auto path = _tablet_mgr->txn_vlog_group_location(_tablet_metadata->id(), 2);
    EXPECT_TRUE(_tablet_mgr->get_txn_vlog(path, false).status().is_corruption());

    // Turning off future group writes must not disable reading a group already persisted.
    config::lake_enable_txn_vlog_group = false;
    ASSIGN_OR_ABORT(auto logs, _tablet_mgr->get_txn_vlog_group(path, false));
    ASSERT_EQ(2, logs.size());
    EXPECT_EQ(first_load.SerializeAsString(), logs[0]->load_id().SerializeAsString());
    EXPECT_EQ(second_load.SerializeAsString(), logs[1]->load_id().SerializeAsString());
    EXPECT_EQ(kChunkSize, logs[0]->op_write().rowset().num_rows());
    EXPECT_EQ(kChunkSize, logs[1]->op_write().rowset().num_rows());
}

TEST_P(LakeMultiStatementPublishTest, publish_log_version_single_statement_keeps_legacy_format) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, true);
    const int64_t txn_id = next_id();
    const auto load_id = make_load_id(111, 112);
    write_statement(txn_id, load_id, 0);
    ASSERT_OK(publish_multi_statement_log_version(2, txn_id, {load_id}));
    _tablet_mgr->metacache()->prune();
    auto legacy = _tablet_mgr->get_txn_vlog(_tablet_mgr->txn_vlog_location(_tablet_metadata->id(), 2), false);
    ASSERT_OK(legacy.status());
    EXPECT_EQ(kChunkSize, legacy.value()->op_write().rowset().num_rows());
}

// A retry whose sources were partly or fully deleted reuses the group it finds, whatever the writer flag says now.
TEST_P(LakeMultiStatementPublishTest, publish_log_version_reuses_existing_group_with_writes_disabled) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, false);
    for (int missing_sources : {1, 2}) {
        const int64_t txn_id = next_id();
        const auto first_load = make_load_id(121, missing_sources * 2);
        const auto second_load = make_load_id(121, missing_sources * 2 + 1);
        write_statement(txn_id, first_load, 0);
        write_statement(txn_id, second_load, kChunkSize);
        ASSIGN_OR_ABORT(auto first, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, first_load));
        ASSIGN_OR_ABORT(auto second, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, second_load));
        const int64_t version = 2 + missing_sources;
        // Model a previous successful group save whose sources were then partly or fully swept.
        ASSERT_OK(_tablet_mgr->put_txn_vlog_group({first, second}, version));
        delete_statement_logs(txn_id, {first_load});
        if (missing_sources == 2) delete_statement_logs(txn_id, {second_load});
        _tablet_mgr->metacache()->prune();

        ASSERT_OK(publish_multi_statement_log_version(version, txn_id, {first_load, second_load}));
        _tablet_mgr->metacache()->prune();
        // No legacy file is added next to the group, which keeps its statements as written.
        EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_vlog_location(_tablet_metadata->id(), version)));
        const auto path = _tablet_mgr->txn_vlog_group_location(_tablet_metadata->id(), version);
        ASSIGN_OR_ABORT(auto logs, _tablet_mgr->get_txn_vlog_group(path, false));
        ASSERT_EQ(2, logs.size());
        EXPECT_EQ(first->SerializeAsString(), logs[0]->SerializeAsString());
        EXPECT_EQ(second->SerializeAsString(), logs[1]->SerializeAsString());
    }
}

TEST_P(LakeMultiStatementPublishTest, publish_log_version_rejects_inconsistent_existing_group) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, false);
    for (int invalid_case : {0, 1, 2}) {
        const int64_t txn_id = next_id();
        const auto first_load = make_load_id(131, invalid_case * 2);
        const auto second_load = make_load_id(131, invalid_case * 2 + 1);
        write_statement(txn_id, first_load, 0);
        write_statement(txn_id, second_load, kChunkSize);
        ASSIGN_OR_ABORT(auto first, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, first_load));
        ASSIGN_OR_ABORT(auto second, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, second_load));
        auto invalid_first = std::make_shared<TxnLog>(*first);
        auto invalid_second = std::make_shared<TxnLog>(*second);
        if (invalid_case == 0) {
            invalid_first->set_txn_id(txn_id + 1);
            invalid_second->set_txn_id(txn_id + 1);
        }
        TxnLogVector invalid{std::move(invalid_first), std::move(invalid_second)};
        if (invalid_case == 1) {
            std::swap(invalid[0], invalid[1]);
        } else if (invalid_case == 2) {
            invalid.push_back(std::make_shared<TxnLog>(*second));
        }
        const int64_t version = 2 + invalid_case;
        ASSERT_OK(_tablet_mgr->put_txn_vlog_group(invalid, version));
        // Only a retry that lacks a source looks for an existing target.
        delete_statement_logs(txn_id, {second_load});
        _tablet_mgr->metacache()->prune();

        EXPECT_TRUE(publish_multi_statement_log_version(version, txn_id, {first_load, second_load}).is_corruption());
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_metadata->id(), txn_id, first_load)));
        ASSIGN_OR_ABORT(auto retained, _tablet_mgr->get_txn_vlog_group(_tablet_metadata->id(), version));
        ASSERT_EQ(invalid.size(), retained.size());
        for (size_t i = 0; i < invalid.size(); ++i) {
            EXPECT_EQ(invalid[i]->SerializeAsString(), retained[i]->SerializeAsString());
        }
    }
}

TEST_P(LakeMultiStatementPublishTest, publish_log_version_rejects_corrupt_group_without_overwriting_it) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, false);
    ConfigResetGuard cache_recovery_guard(&config::lake_clear_corrupted_cache_meta, false);
    const int64_t txn_id = next_id();
    const auto first_load = make_load_id(141, 142);
    const auto second_load = make_load_id(151, 152);
    write_statement(txn_id, first_load, 0);
    write_statement(txn_id, second_load, kChunkSize);
    ASSIGN_OR_ABORT(auto first, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, first_load));
    ASSIGN_OR_ABORT(auto second, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, second_load));
    ASSERT_OK(_tablet_mgr->put_txn_vlog_group({first, second}, 2));
    const auto path = _tablet_mgr->txn_vlog_group_location(_tablet_metadata->id(), 2);
    auto fs = FileSystem::Default();
    ASSIGN_OR_ABORT(auto file, fs->new_random_access_file(path));
    ASSIGN_OR_ABORT(auto content, file->read_all());
    ASSERT_FALSE(content.empty());
    content.back() ^= 1;
    {
        WritableFileOptions opts{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_ABORT(auto writer, fs->new_writable_file(opts, path));
        ASSERT_OK(writer->append(content));
        ASSERT_OK(writer->close());
    }
    // A retry that lacks a source must find the group intact before it deletes the remaining source.
    delete_statement_logs(txn_id, {second_load});
    _tablet_mgr->metacache()->prune();

    EXPECT_TRUE(publish_multi_statement_log_version(2, txn_id, {first_load, second_load}).is_corruption());
    ASSIGN_OR_ABORT(auto retained_file, fs->new_random_access_file(path));
    ASSIGN_OR_ABORT(auto retained, retained_file->read_all());
    EXPECT_EQ(content, retained);
    EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_metadata->id(), txn_id, first_load)));
}

// A retry that still has every source writes the configured format without looking for an earlier target, even a
// damaged one: group writes rewrite the group, legacy writes add the legacy file that readers then take.
TEST_P(LakeMultiStatementPublishTest, publish_log_version_complete_sources_write_the_configured_format) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, false);
    ConfigResetGuard cache_recovery_guard(&config::lake_clear_corrupted_cache_meta, false);
    for (bool group_writes : {false, true}) {
        SCOPED_TRACE(fmt::format("group_writes={}", group_writes));
        const int64_t txn_id = next_id();
        const auto first_load = make_load_id(361, group_writes);
        const auto second_load = make_load_id(371, group_writes);
        write_statement(txn_id, first_load, 0);
        write_statement(txn_id, second_load, kChunkSize);
        ASSIGN_OR_ABORT(auto first, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, first_load));
        ASSIGN_OR_ABORT(auto second, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, second_load));
        const int64_t version = group_writes ? 3 : 2;
        ASSERT_OK(_tablet_mgr->put_txn_vlog_group({first, second}, version));
        const auto group_path = _tablet_mgr->txn_vlog_group_location(_tablet_metadata->id(), version);
        {
            WritableFileOptions opts{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
            ASSIGN_OR_ABORT(auto writer, FileSystem::Default()->new_writable_file(opts, group_path));
            ASSERT_OK(writer->append("damaged"));
            ASSERT_OK(writer->close());
        }

        config::lake_enable_txn_vlog_group = group_writes;
        ASSERT_OK(publish_multi_statement_log_version(version, txn_id, {first_load, second_load}));
        _tablet_mgr->metacache()->prune();
        std::string read_path;
        ASSIGN_OR_ABORT(auto logs, _tablet_mgr->get_txn_vlog_group(_tablet_metadata->id(), version, &read_path));
        if (group_writes) {
            EXPECT_EQ(group_path, read_path);
            ASSERT_EQ(2, logs.size());
            EXPECT_EQ(first->SerializeAsString(), logs[0]->SerializeAsString());
            EXPECT_EQ(second->SerializeAsString(), logs[1]->SerializeAsString());
        } else {
            EXPECT_EQ(_tablet_mgr->txn_vlog_location(_tablet_metadata->id(), version), read_path);
            ASSERT_EQ(1, logs.size());
            EXPECT_FALSE(logs[0]->has_load_id());
            EXPECT_EQ(kTotalRows, logs[0]->op_write().rowset().num_rows());
        }
    }
}

TEST_P(LakeMultiStatementPublishTest, publish_log_version_rejects_corrupt_prefix_with_missing_sources) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, false);
    ConfigResetGuard cache_recovery_guard(&config::lake_clear_corrupted_cache_meta, false);
    for (bool group_writes : {false, true}) {
        config::lake_enable_txn_vlog_group = group_writes;
        for (int missing_sources : {1, 2}) {
            // Corrupt each magic byte separately, plus a prefix truncated below the magic size.
            for (int corrupt_byte = 0; corrupt_byte <= 8; ++corrupt_byte) {
                SCOPED_TRACE(fmt::format("group_writes={} missing_sources={} corrupt_byte={}", group_writes,
                                         missing_sources, corrupt_byte));
                const int64_t txn_id = next_id();
                const auto first_load = make_load_id(201, 202);
                const auto second_load = make_load_id(211, 212);
                write_statement(txn_id, first_load, 0);
                write_statement(txn_id, second_load, kChunkSize);
                ASSIGN_OR_ABORT(auto first, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, first_load));
                ASSIGN_OR_ABORT(auto second, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, second_load));
                ASSERT_OK(_tablet_mgr->put_txn_vlog_group({first, second}, 2));
                const auto path = _tablet_mgr->txn_vlog_group_location(_tablet_metadata->id(), 2);
                auto fs = FileSystem::Default();
                ASSIGN_OR_ABORT(auto file, fs->new_random_access_file(path));
                ASSIGN_OR_ABORT(auto content, file->read_all());
                ASSERT_GE(content.size(), 8);
                if (corrupt_byte < 8) {
                    content[corrupt_byte] ^= 1;
                } else {
                    content.resize(3);
                }
                {
                    WritableFileOptions opts{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
                    ASSIGN_OR_ABORT(auto writer, fs->new_writable_file(opts, path));
                    ASSERT_OK(writer->append(content));
                    ASSERT_OK(writer->close());
                }
                delete_statement_logs(txn_id, {first_load});
                if (missing_sources == 2) delete_statement_logs(txn_id, {second_load});
                _tablet_mgr->metacache()->prune();

                auto st = publish_multi_statement_log_version(2, txn_id, {first_load, second_load});
                EXPECT_TRUE(st.is_corruption()) << st;
                ASSIGN_OR_ABORT(auto retained_file, fs->new_random_access_file(path));
                ASSIGN_OR_ABORT(auto retained, retained_file->read_all());
                EXPECT_EQ(content, retained);
                if (missing_sources == 1) {
                    EXPECT_TRUE(
                            fs::path_exist(_tablet_mgr->txn_log_location(_tablet_metadata->id(), txn_id, second_load)));
                }
            }
        }
    }
}

TEST_P(LakeMultiStatementPublishTest, publish_log_version_reuses_valid_legacy_with_missing_sources) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, false);
    ConfigResetGuard checksum_guard(&config::lake_enable_protobuf_file_checksum, false);
    for (bool checksum : {false, true}) {
        config::lake_enable_protobuf_file_checksum = checksum;
        for (bool group_writes : {false, true}) {
            const int64_t txn_id = next_id();
            const auto first_load = make_load_id(221, 222);
            const auto second_load = make_load_id(231, 232);
            write_statement(txn_id, first_load, 0);
            write_statement(txn_id, second_load, kChunkSize);
            config::lake_enable_txn_vlog_group = false;
            ASSERT_OK(publish_multi_statement_log_version(2, txn_id, {first_load, second_load}));
            delete_statement_logs(txn_id, {first_load, second_load});
            _tablet_mgr->metacache()->prune();

            config::lake_enable_txn_vlog_group = group_writes;
            ASSERT_OK(publish_multi_statement_log_version(2, txn_id, {first_load, second_load}));
            ASSIGN_OR_ABORT(auto logs, _tablet_mgr->get_txn_vlog_group(_tablet_metadata->id(), 2));
            ASSERT_EQ(1, logs.size());
            EXPECT_EQ(txn_id, logs.front()->txn_id());
            EXPECT_EQ(kTotalRows, logs.front()->op_write().rowset().num_rows());
        }
    }
}

TEST_P(LakeMultiStatementPublishTest, publish_log_version_rejects_wrong_legacy_identity_with_missing_sources) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, false);
    for (int invalid_case : {0, 1, 2}) {
        SCOPED_TRACE(fmt::format("invalid_case={}", invalid_case));
        const int64_t txn_id = next_id();
        const auto first_load = make_load_id(241, 242);
        const auto second_load = make_load_id(251, 252);
        write_statement(txn_id, first_load, 0);
        write_statement(txn_id, second_load, kChunkSize);
        ASSIGN_OR_ABORT(auto first, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, first_load));
        auto wrong = std::make_shared<TxnLog>(*first);
        // Cases 0 and 1 are merged legacy targets, which carry no load_id, of another tablet or txn. A statement
        // log keeps its load_id and would be rejected by the statement-group validation before the identity check.
        if (invalid_case == 0) {
            wrong->clear_load_id();
            wrong->set_tablet_id(_tablet_metadata->id() + 1);
        } else if (invalid_case == 1) {
            wrong->clear_load_id();
            wrong->set_txn_id(txn_id + 1);
        }
        // Case 2 has matching IDs but only one statement's load_id, not a merged target.
        const auto path = _tablet_mgr->txn_vlog_location(_tablet_metadata->id(), 2);
        ASSERT_OK(_tablet_mgr->put_txn_log(wrong, path));
        delete_statement_logs(txn_id, {first_load});
        _tablet_mgr->metacache()->prune();

        auto st = publish_multi_statement_log_version(2, txn_id, {first_load, second_load});
        EXPECT_TRUE(st.is_corruption()) << st;
        if (invalid_case != 2) {
            EXPECT_TRUE(st.message().find("txn vlog does not match publish request") != std::string::npos) << st;
        }
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_metadata->id(), txn_id, second_load)));
    }
}

TEST_P(LakeMultiStatementPublishTest, publish_log_version_repairs_truncated_legacy_target_from_sources) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, false);
    const int64_t txn_id = next_id();
    const auto first_load = make_load_id(161, 162);
    const auto second_load = make_load_id(171, 172);
    write_statement(txn_id, first_load, 0);
    write_statement(txn_id, second_load, kChunkSize);
    const auto path = _tablet_mgr->txn_vlog_location(_tablet_metadata->id(), 2);
    {
        ASSIGN_OR_ABORT(auto incomplete, FileSystem::Default()->new_writable_file(path));
        ASSERT_OK(incomplete->append("bad"));
        ASSERT_OK(incomplete->close());
    }
    ASSERT_OK(publish_multi_statement_log_version(2, txn_id, {first_load, second_load}));
    _tablet_mgr->metacache()->prune();
    auto legacy = _tablet_mgr->get_txn_vlog(path, false);
    ASSERT_OK(legacy.status());
    EXPECT_EQ(kTotalRows, legacy.value()->op_write().rowset().num_rows());
}

TEST_P(LakeMultiStatementPublishTest, publish_log_version_preserves_sources_on_target_probe_failure) {
    const int64_t txn_id = next_id();
    const auto first_load = make_load_id(181, 182);
    const auto second_load = make_load_id(191, 192);
    write_statement(txn_id, first_load, 0);
    write_statement(txn_id, second_load, kChunkSize);
    // Only a retry that lacks a source looks the target up.
    delete_statement_logs(txn_id, {second_load});
    _tablet_mgr->metacache()->prune();
    TEST_ENABLE_ERROR_POINT("TabletManager::get_txn_vlog", Status::IOError("target probe failed"));
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp clear_injection([] {
        SyncPoint::GetInstance()->DisableProcessing();
        TEST_DISABLE_ERROR_POINT("TabletManager::get_txn_vlog");
    });

    auto st = publish_multi_statement_log_version(2, txn_id, {first_load, second_load});
    EXPECT_TRUE(st.is_io_error()) << st;
    EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_metadata->id(), txn_id, first_load)));
    EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_vlog_location(_tablet_metadata->id(), 2)));
    EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_vlog_group_location(_tablet_metadata->id(), 2)));
}

// A retry that still has every source writes the configured format. When group writes were enabled after an
// earlier attempt wrote the legacy log, both files exist and readers keep taking the legacy one.
TEST_P(LakeMultiStatementPublishTest, publish_log_version_retry_across_enabling_group_writes_reads_the_legacy_log) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, true);
    const int64_t txn_id = next_id();
    const auto first_load = make_load_id(341, 342);
    const auto second_load = make_load_id(351, 352);
    write_statement(txn_id, first_load, 0);
    write_statement(txn_id, second_load, kChunkSize);
    // Model the legacy log of an earlier attempt made while group writes were off.
    ASSIGN_OR_ABORT(auto first, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, first_load));
    auto legacy = std::make_shared<TxnLog>(*first);
    legacy->clear_load_id();
    legacy->mutable_op_write()->mutable_rowset()->set_num_rows(kTotalRows);
    ASSERT_OK(_tablet_mgr->put_txn_vlog(legacy, 2));

    ASSERT_OK(publish_multi_statement_log_version(2, txn_id, {first_load, second_load}));
    EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_vlog_group_location(_tablet_metadata->id(), 2)));
    _tablet_mgr->metacache()->prune();
    std::string read_path;
    ASSIGN_OR_ABORT(auto logs, _tablet_mgr->get_txn_vlog_group(_tablet_metadata->id(), 2, &read_path));
    EXPECT_EQ(_tablet_mgr->txn_vlog_location(_tablet_metadata->id(), 2), read_path);
    ASSERT_EQ(1, logs.size());
    EXPECT_FALSE(logs[0]->has_load_id());
    EXPECT_EQ(kTotalRows, logs[0]->op_write().rowset().num_rows());
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

// The statements of a multi-statement transaction are applied together, and a statement that overwrites
// rows of old segments adds its deletes to each of those segments' delete vector at the publish's base
// version -- through the metadata the publish already read, not by reading that version back from storage
// once per segment (RB-792). With a metacache that keeps nothing, a transaction that deletes rows from many
// old segments reads its base version exactly as often as one that deletes from none.
TEST_P(LakeMultiStatementPublishTest, deletes_from_old_segments_reuse_base_metadata) {
    if (GetParam() != PRIMARY_KEYS) {
        GTEST_SKIP() << "only a primary key tablet has delete vectors";
    }
    const int64_t tablet_id = _tablet_metadata->id();
    int64_t version = 1;
    // Writes one statement of kChunkSize keys per element of |first_keys|, starting at that key, and
    // returns the transaction's id and the statements' load ids.
    auto write_txn = [&](const std::vector<int>& first_keys) {
        const int64_t txn_id = next_id();
        std::vector<PUniqueId> load_ids;
        for (size_t i = 0; i < first_keys.size(); i++) {
            load_ids.push_back(make_load_id(txn_id, static_cast<int64_t>(i) + 1));
            write_statement(txn_id, load_ids.back(), first_keys[i]);
        }
        return std::make_pair(txn_id, std::move(load_ids));
    };
    // kOldLoads transactions of one statement each: keys [0, kOldLoads * kChunkSize), one segment per load.
    constexpr int kOldLoads = 4;
    for (int i = 0; i < kOldLoads; i++) {
        auto [txn_id, load_ids] = write_txn({i * kChunkSize});
        ASSERT_OK(publish_multi_statement(version + 1, txn_id, load_ids).status());
        version++;
    }

    // A metacache that keeps nothing once it is released, like a shard the base metadata outgrew: every
    // read of the base version goes to storage.
    const size_t metacache_capacity = _tablet_mgr->metacache()->tablet_metadata_capacity();
    _tablet_mgr->metacache()->update_tablet_metadata_capacity(1);
    DeferOp restore_metacache([&]() { _tablet_mgr->metacache()->update_tablet_metadata_capacity(metacache_capacity); });
    ASSERT_EQ(1, _tablet_mgr->metacache()->tablet_metadata_capacity());
    _tablet_mgr->metacache()->prune();

    // While one publish runs, count its reads of the version it publishes on. The callback fires for the
    // whole process, so this relies on nothing else reading this tablet's metadata meanwhile, which holds
    // here: the test runs nothing in the background.
    std::atomic<const std::string*> counted_file = nullptr;
    std::atomic<int> base_metadata_reads = 0;
    SyncPoint::GetInstance()->SetCallBack("TabletManager::load_tablet_metadata:path", [&](void* arg) {
        const std::string* file = counted_file.load();
        if (file != nullptr && std::string_view(*static_cast<std::string*>(arg)).ends_with(*file)) {
            ++base_metadata_reads;
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp clear_callbacks([]() {
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::load_tablet_metadata:path");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    auto publish_counting = [&](const std::pair<int64_t, std::vector<PUniqueId>>& txn) {
        const std::string base_file = tablet_metadata_filename(tablet_id, version);
        base_metadata_reads = 0;
        counted_file = &base_file;
        auto metadata_or = publish_multi_statement(version + 1, txn.first, txn.second);
        counted_file = nullptr;
        version++;
        return metadata_or;
    };

    // New keys only: the publish deletes rows from no old segment and reads its base version for itself.
    ASSERT_OK(publish_counting(write_txn({kOldLoads * kChunkSize, (kOldLoads + 1) * kChunkSize})).status());
    const int own_reads = base_metadata_reads.load();
    ASSERT_GE(own_reads, 1) << "the publish should have read its base version from storage";

    // One statement per old load, each overwriting all of it: the publish deletes rows from every old segment.
    std::vector<int> first_keys;
    for (int i = 0; i < kOldLoads; i++) {
        first_keys.push_back(i * kChunkSize);
    }
    ASSERT_OK(publish_counting(write_txn(first_keys)).status());
    // Nothing beyond the publish's own reads. Before RB-792 it read the base version once more for each of
    // the old segments.
    EXPECT_EQ(own_reads, base_metadata_reads.load());

    // Keys [0, (kOldLoads + 2) * kChunkSize), each once.
    constexpr int64_t kRows = (kOldLoads + 2) * kChunkSize;
    auto [rows, sum_c0] = read_back(version);
    EXPECT_EQ(kRows, rows);
    EXPECT_EQ(kRows * (kRows - 1) / 2, sum_c0);
}

// FE sends the load ids of every statement of the transaction, but a statement that loaded another table or
// partition wrote no log for this tablet. publish_log_version materializes the statements that did, and a retry
// after their logs were deleted reuses the result instead of failing until the transaction and the alter wedge.
TEST_P(LakeMultiStatementPublishTest, publish_log_version_materializes_statements_that_logged_the_tablet) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, false);
    for (bool group_writes : {false, true}) {
        SCOPED_TRACE(fmt::format("group_writes={}", group_writes));
        config::lake_enable_txn_vlog_group = group_writes;
        const int64_t txn_id = next_id();
        const auto first_load = make_load_id(261, 262);
        const auto other_table_load = make_load_id(271, 272);
        const auto last_load = make_load_id(281, 282);
        write_statement(txn_id, first_load, 0);
        write_statement(txn_id, last_load, kChunkSize);
        const std::vector<PUniqueId> load_ids{first_load, other_table_load, last_load};
        const int64_t version = group_writes ? 3 : 2;

        ASSERT_OK(publish_multi_statement_log_version(version, txn_id, load_ids));
        delete_statement_logs(txn_id, load_ids);
        _tablet_mgr->metacache()->prune();
        ASSERT_OK(publish_multi_statement_log_version(version, txn_id, load_ids));

        ASSIGN_OR_ABORT(auto logs, _tablet_mgr->get_txn_vlog_group(_tablet_metadata->id(), version));
        if (group_writes) {
            ASSERT_EQ(2, logs.size());
            EXPECT_EQ(first_load.SerializeAsString(), logs[0]->load_id().SerializeAsString());
            EXPECT_EQ(last_load.SerializeAsString(), logs[1]->load_id().SerializeAsString());
        } else {
            ASSERT_EQ(1, logs.size());
            EXPECT_FALSE(logs[0]->has_load_id());
        }
        int64_t rows = 0;
        for (const auto& log : logs) {
            rows += log->op_write().rowset().num_rows();
        }
        EXPECT_EQ(kTotalRows, rows);
    }
}

// With a single statement that logged the tablet, a group write keeps that statement's load id in a legacy-format
// log. A retry after its source was deleted must still match it against the request's statements.
TEST_P(LakeMultiStatementPublishTest, publish_log_version_materializes_single_statement_that_logged_the_tablet) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, false);
    for (bool group_writes : {false, true}) {
        SCOPED_TRACE(fmt::format("group_writes={}", group_writes));
        config::lake_enable_txn_vlog_group = group_writes;
        const int64_t txn_id = next_id();
        const auto logged_load = make_load_id(291, 292);
        const auto other_table_load = make_load_id(301, 302);
        write_statement(txn_id, logged_load, 0);
        const std::vector<PUniqueId> load_ids{other_table_load, logged_load};
        const int64_t version = group_writes ? 3 : 2;

        ASSERT_OK(publish_multi_statement_log_version(version, txn_id, load_ids));
        delete_statement_logs(txn_id, load_ids);
        _tablet_mgr->metacache()->prune();
        ASSERT_OK(publish_multi_statement_log_version(version, txn_id, load_ids));

        ASSIGN_OR_ABORT(auto logs, _tablet_mgr->get_txn_vlog_group(_tablet_metadata->id(), version));
        ASSERT_EQ(1, logs.size());
        EXPECT_EQ(group_writes, logs[0]->has_load_id());
        EXPECT_EQ(kChunkSize, logs[0]->op_write().rowset().num_rows());
    }
}

// A materialized log may leave out only the statements that have no log for the tablet. Reusing one that leaves out
// a logged statement would delete that statement's log without publishing it.
TEST_P(LakeMultiStatementPublishTest, publish_log_version_rejects_group_omitting_a_logged_statement) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, false);
    const int64_t txn_id = next_id();
    const auto first_load = make_load_id(311, 312);
    const auto middle_load = make_load_id(321, 322);
    const auto last_load = make_load_id(331, 332);
    write_statement(txn_id, first_load, 0);
    write_statement(txn_id, middle_load, kChunkSize);
    write_statement(txn_id, last_load, 2 * kChunkSize);
    ASSIGN_OR_ABORT(auto first, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, first_load));
    ASSIGN_OR_ABORT(auto last, _tablet_mgr->get_txn_log(_tablet_metadata->id(), txn_id, last_load));
    ASSERT_OK(_tablet_mgr->put_txn_vlog_group({first, last}, 2));
    // A retry after the first statement's source was swept looks the target up.
    delete_statement_logs(txn_id, {first_load});
    _tablet_mgr->metacache()->prune();

    auto st = publish_multi_statement_log_version(2, txn_id, {first_load, middle_load, last_load});
    EXPECT_TRUE(st.is_corruption()) << st;
    for (const auto& load_id : {middle_load, last_load}) {
        EXPECT_TRUE(fs::path_exist(_tablet_mgr->txn_log_location(_tablet_metadata->id(), txn_id, load_id)));
    }
}

// The first attempt applied [load, txn] on this tablet. More loads committed before FE retried, so the retry
// carries [load, txn, load] and runs on another node, which has to read the transaction's logs again.
TEST_P(LakeMultiStatementPublishTest, batch_retry_with_more_txns_applies_txn_again) {
    const int64_t load_before = next_id();
    const int64_t txn_id = next_id();
    const int64_t load_after = next_id();
    const auto load_id_1 = make_load_id(71, 72);
    const auto load_id_2 = make_load_id(81, 82);
    write_autocommit_txn(load_before, kBeforeTxnFirstKey);
    write_statement(txn_id, load_id_1, 0);
    write_statement(txn_id, load_id_2, kChunkSize);
    write_autocommit_txn(load_after, kAfterTxnFirstKey);

    const auto info_before = make_txn_info(load_before, {});
    const auto info_txn = make_txn_info(txn_id, {load_id_1, load_id_2});
    const auto info_after = make_txn_info(load_after, {});
    ASSERT_OK(publish_batch(1, {info_before, info_txn}).status());
    switch_node();

    auto res = publish_batch(1, {info_before, info_txn, info_after});
    ASSERT_TRUE(res.ok()) << res.status();
    EXPECT_EQ(4, res.value()->version());
    auto [rows, sum_c0] = read_back(4);
    EXPECT_EQ(kTotalRows + 2 * kChunkSize, rows);
    EXPECT_EQ(kSumC0 + chunk_sum_c0(kBeforeTxnFirstKey) + chunk_sum_c0(kAfterTxnFirstKey), sum_c0);
}

// The transaction heads the batch. A batch writes the metadata of its last version only, so a retry on another
// node finds no version to skip the transaction with and has to read its logs again.
TEST_P(LakeMultiStatementPublishTest, batch_retry_with_txn_first_applies_txn_again) {
    const int64_t txn_id = next_id();
    const int64_t load_after = next_id();
    const auto load_id_1 = make_load_id(91, 92);
    const auto load_id_2 = make_load_id(101, 102);
    write_statement(txn_id, load_id_1, 0);
    write_statement(txn_id, load_id_2, kChunkSize);
    write_autocommit_txn(load_after, kAfterTxnFirstKey);

    const std::vector<TxnInfoPB> batch{make_txn_info(txn_id, {load_id_1, load_id_2}), make_txn_info(load_after, {})};
    ASSERT_OK(publish_batch(1, batch).status());
    switch_node();

    auto res = publish_batch(1, batch);
    ASSERT_TRUE(res.ok()) << res.status();
    EXPECT_EQ(3, res.value()->version());
    auto [rows, sum_c0] = read_back(3);
    EXPECT_EQ(kTotalRows + kChunkSize, rows);
    EXPECT_EQ(kSumC0 + chunk_sum_c0(kAfterTxnFirstKey), sum_c0);
}

// A batch publish leaves the logs of a transaction to vacuum, as it does those of an autocommit load; a
// publish of the transaction alone still deletes them.
TEST_P(LakeMultiStatementPublishTest, batch_publish_leaves_txn_logs_to_vacuum) {
    const int64_t load_before = next_id();
    const int64_t txn_id = next_id();
    const int64_t single_txn_id = next_id();
    const auto load_id_1 = make_load_id(111, 112);
    const auto load_id_2 = make_load_id(121, 122);
    const auto single_load_id = make_load_id(131, 132);
    write_autocommit_txn(load_before, kBeforeTxnFirstKey);
    write_statement(txn_id, load_id_1, 0);
    write_statement(txn_id, load_id_2, kChunkSize);
    write_statement(single_txn_id, single_load_id, kAfterTxnFirstKey);

    ASSERT_OK(
            publish_batch(1, {make_txn_info(load_before, {}), make_txn_info(txn_id, {load_id_1, load_id_2})}).status());
    ASSERT_OK(publish_multi_statement(4, single_txn_id, {single_load_id}).status());
    StorageEngine::instance()->wait_storage_cleanup_tasks();
    EXPECT_TRUE(txn_log_exists(load_before, nullptr));
    EXPECT_TRUE(txn_log_exists(txn_id, &load_id_1));
    EXPECT_TRUE(txn_log_exists(txn_id, &load_id_2));
    EXPECT_FALSE(txn_log_exists(single_txn_id, &single_load_id));

    // While the transaction is still active (not yet visible, so a retried batch may need its logs), vacuum
    // reclaims only the logs of older transactions.
    const auto root_location = _tablet_mgr->tablet_root_location(_tablet_metadata->id());
    int64_t vacuumed_files = 0;
    int64_t vacuumed_file_size = 0;
    ASSERT_OK(vacuum_txn_log(root_location, txn_id, &vacuumed_files, &vacuumed_file_size));
    EXPECT_EQ(1, vacuumed_files);
    EXPECT_FALSE(txn_log_exists(load_before, nullptr));
    EXPECT_TRUE(txn_log_exists(txn_id, &load_id_1));
    EXPECT_TRUE(txn_log_exists(txn_id, &load_id_2));

    // Once no older transaction is active, vacuum reclaims the transaction's logs too.
    vacuumed_files = 0;
    vacuumed_file_size = 0;
    ASSERT_OK(vacuum_txn_log(root_location, single_txn_id, &vacuumed_files, &vacuumed_file_size));
    EXPECT_EQ(2, vacuumed_files);
    EXPECT_FALSE(txn_log_exists(txn_id, &load_id_1));
    EXPECT_FALSE(txn_log_exists(txn_id, &load_id_2));

    auto [rows, sum_c0] = read_back(4);
    EXPECT_EQ(kTotalRows + 2 * kChunkSize, rows);
    EXPECT_EQ(kSumC0 + chunk_sum_c0(kBeforeTxnFirstKey) + chunk_sum_c0(kAfterTxnFirstKey), sum_c0);
}

// StarRocksTest#12646. A multi-table transaction published this partition on its own, but another of its
// partitions failed, so it stayed COMMITTED and the next batch put it together with the transaction after it.
// The batch publishes the partition again from the partition's visible version, and publishing the transaction
// alone had deleted its logs, so the batch has to start from the version that publish wrote. Applying no
// statement instead wrote the batch's version without the transaction's rows. The other partition keeps failing,
// so the batch is retried on another node with one more load, and it applies the second transaction again.
TEST_P(LakeMultiStatementPublishTest, batch_after_txn_published_alone_keeps_its_rows) {
    const int64_t txn_id = next_id();
    const int64_t next_txn_id = next_id();
    const int64_t load_after = next_id();
    const auto load_id_1 = make_load_id(141, 142);
    const auto load_id_2 = make_load_id(151, 152);
    const auto next_load_id = make_load_id(161, 162);
    write_statement(txn_id, load_id_1, 0);
    write_statement(txn_id, load_id_2, kChunkSize);
    write_statement(next_txn_id, next_load_id, kAfterTxnFirstKey);
    write_autocommit_txn(load_after, kLaterLoadFirstKey);
    const auto info_txn = make_txn_info(txn_id, {load_id_1, load_id_2});
    const auto info_next = make_txn_info(next_txn_id, {next_load_id});
    const auto info_after = make_txn_info(load_after, {});

    ASSERT_OK(publish_multi_statement(2, txn_id, {load_id_1, load_id_2}).status());
    switch_node();
    ASSERT_FALSE(txn_log_exists(txn_id, &load_id_1));
    ASSERT_FALSE(txn_log_exists(txn_id, &load_id_2));

    auto res = publish_batch(1, {info_txn, info_next});
    ASSERT_TRUE(res.ok()) << res.status();
    EXPECT_EQ(3, res.value()->version());
    auto [rows, sum_c0] = read_back(3);
    EXPECT_EQ(kTotalRows + kChunkSize, rows);
    EXPECT_EQ(kSumC0 + chunk_sum_c0(kAfterTxnFirstKey), sum_c0);

    switch_node();
    res = publish_batch(1, {info_txn, info_next, info_after});
    ASSERT_TRUE(res.ok()) << res.status();
    EXPECT_EQ(4, res.value()->version());
    auto [rows_after_retry, sum_c0_after_retry] = read_back(4);
    EXPECT_EQ(kTotalRows + 2 * kChunkSize, rows_after_retry);
    EXPECT_EQ(kSumC0 + chunk_sum_c0(kAfterTxnFirstKey) + chunk_sum_c0(kLaterLoadFirstKey), sum_c0_after_retry);
}

INSTANTIATE_TEST_SUITE_P(LakeMultiStatementPublishTest, LakeMultiStatementPublishTest,
                         testing::Values(DUP_KEYS, PRIMARY_KEYS));

} // namespace starrocks::lake
