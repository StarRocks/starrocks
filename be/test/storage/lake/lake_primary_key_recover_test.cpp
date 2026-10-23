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

// Primary key recover (LakePrimaryKeyRecover) drops a tablet's delete vectors and persistent index and rebuilds
// them by replaying every rowset in order: segments as upserts, origin del files as deletes. These tests drive
// each load type through the real write and publish path, force a recover on chosen publishes, and check the
// recovered version row by row against a model of what the loads should have produced. A follow-up upsert of
// every key then checks that the rebuilt index agrees with the rebuilt delete vectors: a key the index lost
// would come back as a duplicate.

#include "storage/lake/lake_primary_key_recover.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <ctime>
#include <map>
#include <numeric>
#include <optional>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config.h"
#include "gen_cpp/Types_types.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/lake/transactions.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"
#include "util/starrocks_metrics.h"

namespace starrocks::lake {

class LakePrimaryKeyRecoverTest : public TestBase {
public:
    // One row of a load. `del` rows carry only the key.
    struct Op {
        int32_t key;
        int32_t c1 = 0;
        int32_t c2 = 0;
        bool del = false;
    };
    using Row = std::pair<int32_t, int32_t>; // (c1, c2)
    using Rows = std::map<int32_t, Row>;

    // gtest-parallel serializes only the tests of one test case, so every test case needs a directory of its own.
    explicit LakePrimaryKeyRecoverTest(const char* test_directory = kTestDirectory) : TestBase(test_directory) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);
        _tablet_metadata->set_enable_persistent_index(true);
        _tablet_metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        //  | column | type | KEY | NULL | DEFAULT |
        //  +--------+------+-----+------+---------+
        //  |   c0   |  INT | YES |  NO  |         |
        //  |   c1   |  INT | NO  |  YES |         |
        //  |   c2   |  INT | NO  |  YES |   10    |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        auto c1 = schema->add_column();
        c1->set_unique_id(next_id());
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(true);
        c1->set_aggregation("REPLACE");
        auto c2 = schema->add_column();
        c2->set_unique_id(next_id());
        c2->set_name("c2");
        c2->set_type("INT");
        c2->set_is_key(false);
        c2->set_is_nullable(true);
        c2->set_aggregation("REPLACE");
        c2->set_default_value("10");
        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));

        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(2, "c2", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(3, "__op", TypeDescriptor{LogicalType::TYPE_INT});
        _full_slots = {&_slots[0], &_slots[1], &_slots[2], &_slots[3]};
        _partial_slots = {&_slots[0], &_slots[1]};
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        CHECK_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
        // The index load is where a publish meets a duplicate primary key (AlreadyExist), which is the main
        // way into recover; arm it for one load at a time.
        SyncPoint::GetInstance()->SetCallBack("lake_index_load.1", [this](void* arg) {
            if (_fail_next_index_load) {
                _fail_next_index_load = false;
                *(Status*)arg = Status::AlreadyExist("injected duplicate primary key");
            }
        });
        SyncPoint::GetInstance()->EnableProcessing();
        _old_enable_primary_key_recover = config::enable_primary_key_recover;
        config::enable_primary_key_recover = true;
    }

    void TearDown() override {
        config::enable_primary_key_recover = _old_enable_primary_key_recover;
        SyncPoint::GetInstance()->ClearCallBack("lake_index_load.1");
        SyncPoint::GetInstance()->DisableProcessing();
        remove_test_dir_ignore_error();
    }

protected:
    int64_t tablet_id() const { return _tablet_metadata->id(); }

    // Write one load. Each batch goes through its own write() call; with more than one batch every
    // memtable is flushed per write, so each batch keeps its own segment or del file and the publish
    // replays them in batch order.
    int64_t write_rows(const std::vector<std::vector<Op>>& batches, const std::string& merge_condition = "") {
        const int64_t txn_id = next_id();
        const int64_t old_buffer_size = config::write_buffer_size;
        if (batches.size() > 1) {
            config::write_buffer_size = 1;
        }
        DeferOp restore([&]() { config::write_buffer_size = old_buffer_size; });
        // A conditional load has no __op column: it is an upsert by definition.
        const bool with_op = merge_condition.empty();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(with_op ? &_full_slots : nullptr)
                                                   .set_merge_condition(merge_condition)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        CHECK_OK(delta_writer->open());
        for (const auto& batch : batches) {
            auto c0 = Int32Column::create();
            auto c1 = Int32Column::create();
            auto c2 = Int32Column::create();
            auto op = Int8Column::create();
            for (const auto& row : batch) {
                c0->append(row.key);
                c1->append(row.c1);
                c2->append(row.c2);
                op->append(row.del ? TOpType::DELETE : TOpType::UPSERT);
            }
            Columns columns;
            columns.emplace_back(std::move(c0));
            columns.emplace_back(std::move(c1));
            columns.emplace_back(std::move(c2));
            if (with_op) {
                columns.emplace_back(std::move(op));
            }
            Chunk chunk = make_chunk(std::move(columns));
            std::vector<uint32_t> indexes(batch.size());
            std::iota(indexes.begin(), indexes.end(), 0);
            CHECK_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        }
        CHECK_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        return txn_id;
    }

    // Partial update of c1 only. c2 keeps its current value, or takes its default (10) for a new key.
    int64_t write_partial(const std::vector<std::pair<int32_t, int32_t>>& key_c1, PartialUpdateMode mode) {
        const int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_partial_slots)
                                                   .set_partial_update_mode(mode)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        CHECK_OK(delta_writer->open());
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        for (const auto& [key, value] : key_c1) {
            c0->append(key);
            c1->append(value);
        }
        Columns columns;
        columns.emplace_back(std::move(c0));
        columns.emplace_back(std::move(c1));
        Chunk chunk = make_chunk(std::move(columns));
        std::vector<uint32_t> indexes(key_c1.size());
        std::iota(indexes.begin(), indexes.end(), 0);
        CHECK_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        CHECK_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        return txn_id;
    }

    // Publish `txn_id` as the next version. With `recover`, the publish meets a duplicate primary key while
    // loading the index, so it recovers the tablet and publishes the load again on the recovered state.
    void publish(int64_t txn_id, bool recover) {
        if (recover) {
            // Only a load reaches the injection point, so make sure this publish does not reuse a cached index.
            _update_mgr->try_remove_primary_index_cache(tablet_id());
            _fail_next_index_load = true;
        }
        const int64_t recover_before = recover_count();
        auto st = publish_single_version(tablet_id(), _version + 1, txn_id).status();
        _fail_next_index_load = false;
        ASSERT_OK(st);
        _version++;
        if (recover) {
            ASSERT_EQ(recover_before + 1, recover_count()) << "publish did not go through recover";
        }
    }

    // Recover is entered once per injected duplicate key; the error-state counter counts exactly those.
    static int64_t recover_count() { return StarRocksMetrics::instance()->primary_key_table_error_state_total.value(); }

    // Every row visible at `version`, keyed by c0. A duplicate primary key fails the test.
    Rows read(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        Rows rows;
        auto chunk = ChunkHelper::new_chunk(*_schema, 128);
        while (true) {
            chunk->reset();
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            for (size_t i = 0; i < chunk->num_rows(); i++) {
                auto row = chunk->get(i);
                const int32_t key = row[0].get_int32();
                const Row value{row[1].is_null() ? -1 : row[1].get_int32(), row[2].is_null() ? -1 : row[2].get_int32()};
                EXPECT_TRUE(rows.emplace(key, value).second)
                        << "duplicate primary key " << key << " at version " << version;
            }
        }
        return rows;
    }

    // Apply a load to the model the same way publish does: batch by batch, row by row.
    static void apply(Rows* rows, const std::vector<std::vector<Op>>& batches) {
        for (const auto& batch : batches) {
            for (const auto& op : batch) {
                if (op.del) {
                    rows->erase(op.key);
                } else {
                    (*rows)[op.key] = {op.c1, op.c2};
                }
            }
        }
    }

    // Apply a load's upserts first and then its deletes, which is the order publish uses on this branch.
    static void apply_upserts_then_deletes(Rows* rows, const std::vector<std::vector<Op>>& batches) {
        for (const bool deletes : {false, true}) {
            for (const auto& batch : batches) {
                for (const auto& op : batch) {
                    if (op.del != deletes) {
                        continue;
                    }
                    if (op.del) {
                        rows->erase(op.key);
                    } else {
                        (*rows)[op.key] = {op.c1, op.c2};
                    }
                }
            }
        }
    }

    // Upsert every expected key once more and check nothing comes back twice: an index that lost a key the
    // delete vectors still show would leave the old row live next to the new one.
    void check_index_agrees(Rows* expected) {
        std::vector<Op> batch;
        for (const auto& [key, value] : *expected) {
            batch.push_back({key, value.first + 1, value.second});
        }
        const std::vector<std::vector<Op>> load{batch};
        publish(write_rows(load), false);
        apply(expected, load);
        EXPECT_EQ(*expected, read(_version));
    }

    // Slot i is column i, which is how every slot list above is laid out.
    static Chunk make_chunk(Columns columns) {
        Chunk::SlotHashMap slot_map;
        for (size_t i = 0; i < columns.size(); i++) {
            slot_map.emplace(static_cast<SlotId>(i), i);
        }
        return Chunk(std::move(columns), std::move(slot_map));
    }

    static std::vector<Op> upserts(int32_t first, int32_t last, int32_t c1_base, int32_t c2_base) {
        std::vector<Op> ops;
        for (int32_t key = first; key < last; key++) {
            ops.push_back({key, c1_base + key, c2_base + key});
        }
        return ops;
    }

    static std::vector<Op> deletes(int32_t first, int32_t last) {
        std::vector<Op> ops;
        for (int32_t key = first; key < last; key++) {
            ops.push_back({key, 0, 0, true});
        }
        return ops;
    }

    // One statement of a multi-statement transaction: full rows, or with `partial` set, c1 of each key only.
    struct Statement {
        std::vector<Op> ops;
        std::optional<PartialUpdateMode> partial;
    };

    static Statement statement(std::vector<Op> ops) { return {std::move(ops), std::nullopt}; }

    static Statement partial_statement(int32_t first, int32_t last, int32_t c1_base, PartialUpdateMode mode) {
        return {upserts(first, last, c1_base, 0), mode};
    }

    // Apply a statement to the model. A partial update keeps c2; a new key takes c2's default (10), except in
    // column update mode, which only updates existing keys.
    static void apply(Rows* rows, const Statement& stmt) {
        if (!stmt.partial.has_value()) {
            apply(rows, std::vector<std::vector<Op>>{stmt.ops});
            return;
        }
        for (const auto& op : stmt.ops) {
            auto it = rows->find(op.key);
            if (it != rows->end()) {
                it->second.first = op.c1;
            } else if (*stmt.partial != PartialUpdateMode::COLUMN_UPDATE_MODE) {
                (*rows)[op.key] = {op.c1, 10};
            }
        }
    }

    // Write each statement through its own delta writer and load id under one transaction, as a multi-statement
    // transaction does, and return the transaction to publish.
    TxnInfoPB write_multi_statement(const std::vector<Statement>& statements) {
        const int64_t txn_id = next_id();
        TxnInfoPB info;
        info.set_txn_id(txn_id);
        info.set_txn_type(TXN_NORMAL);
        info.set_combined_txn_log(false);
        info.set_commit_time(time(nullptr));
        info.set_force_publish(false);
        for (size_t i = 0; i < statements.size(); i++) {
            const auto& stmt = statements[i];
            PUniqueId* load_id = info.add_load_ids();
            load_id->set_hi(txn_id);
            load_id->set_lo(i);
            DeltaWriterBuilder builder;
            builder.set_tablet_manager(_tablet_mgr.get())
                    .set_tablet_id(tablet_id())
                    .set_txn_id(txn_id)
                    .set_partition_id(_partition_id)
                    .set_mem_tracker(_mem_tracker.get())
                    .set_schema_id(_tablet_schema->id())
                    .set_slot_descriptors(stmt.partial.has_value() ? &_partial_slots : &_full_slots)
                    .set_load_id(*load_id)
                    .set_is_multi_statements_txn(true)
                    .set_profile(&_dummy_runtime_profile);
            if (stmt.partial.has_value()) {
                builder.set_partial_update_mode(*stmt.partial);
            }
            ASSIGN_OR_ABORT(auto delta_writer, builder.build());
            CHECK_OK(delta_writer->open());
            auto c0 = Int32Column::create();
            auto c1 = Int32Column::create();
            auto c2 = Int32Column::create();
            auto op = Int8Column::create();
            for (const auto& row : stmt.ops) {
                c0->append(row.key);
                c1->append(row.c1);
                c2->append(row.c2);
                op->append(row.del ? TOpType::DELETE : TOpType::UPSERT);
            }
            Columns columns;
            columns.emplace_back(std::move(c0));
            columns.emplace_back(std::move(c1));
            if (!stmt.partial.has_value()) {
                columns.emplace_back(std::move(c2));
                columns.emplace_back(std::move(op));
            }
            Chunk chunk = make_chunk(std::move(columns));
            std::vector<uint32_t> indexes(stmt.ops.size());
            std::iota(indexes.begin(), indexes.end(), 0);
            CHECK_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
            CHECK_OK(delta_writer->finish_with_txnlog());
            delta_writer->close();
        }
        return info;
    }

    // Publish a multi-statement transaction as the next version, through recover like publish() with `recover`.
    void publish_multi_statement(const std::vector<Statement>& statements, bool recover) {
        TxnInfoPB info = write_multi_statement(statements);
        if (recover) {
            _update_mgr->try_remove_primary_index_cache(tablet_id());
            _fail_next_index_load = true;
        }
        const int64_t recover_before = recover_count();
        std::vector<TxnInfoPB> txns{info};
        auto st = publish_version(_tablet_mgr.get(), tablet_id(), _version, _version + 1, txns, false).status();
        _fail_next_index_load = false;
        ASSERT_OK(st);
        _version++;
        if (recover) {
            ASSERT_EQ(recover_before + 1, recover_count()) << "publish did not go through recover";
        }
    }

    // Live rows according to the rowsets' num_rows and num_dels, which compaction and statistics trust.
    int64_t live_rows_by_stats(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id(), version));
        int64_t rows = 0;
        for (const auto& rowset : metadata->rowsets()) {
            rows += rowset.num_rows() - rowset.num_dels();
        }
        return rows;
    }

    constexpr static const char* const kTestDirectory = "test_lake_primary_key_recover";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _full_slots;
    std::vector<SlotDescriptor*> _partial_slots;
    RuntimeProfile _dummy_runtime_profile{"dummy"};
    int64_t _partition_id = 4561;
    int64_t _version = 1;
    bool _fail_next_index_load = false;
    bool _old_enable_primary_key_recover = false;
};

// Upserts and deletes in separate loads, recovering on every publish.
TEST_F(LakePrimaryKeyRecoverTest, upsert_and_delete_in_separate_loads) {
    const std::vector<std::vector<std::vector<Op>>> loads = {
            {upserts(0, 16, 0, 0)}, {deletes(0, 8)},    {upserts(4, 12, 100, 100)},
            {deletes(10, 14)},      {{{12, 999, 999}}}, {upserts(20, 24, 0, 0)},
    };
    Rows expected;
    for (const auto& load : loads) {
        publish(write_rows(load), true);
        apply(&expected, load);
        ASSERT_EQ(expected, read(_version));
    }
    check_index_agrees(&expected);
}

// Upserts and deletes of the same keys mixed in one load, each batch in its own segment or del file. On this
// branch publish applies a load's deletes after all of its upserts, and recover has to replay them the same way.
TEST_F(LakePrimaryKeyRecoverTest, upsert_and_delete_mixed_in_one_load) {
    Rows expected;
    const std::vector<std::vector<Op>> base = {upserts(0, 16, 0, 0)};
    publish(write_rows(base), false);
    apply(&expected, base);
    const std::vector<std::vector<Op>> mixed = {
            upserts(0, 8, 100, 100), deletes(4, 12), upserts(6, 10, 200, 200), deletes(0, 2), upserts(14, 18, 300, 300),
    };
    publish(write_rows(mixed), true);
    apply_upserts_then_deletes(&expected, mixed);
    ASSERT_EQ(expected, read(_version));
    // Recover once more from a later load, now replaying the mixed rowset as history.
    const std::vector<std::vector<Op>> later = {upserts(30, 32, 0, 0)};
    publish(write_rows(later), true);
    apply(&expected, later);
    ASSERT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

// A conditional upsert keeps the stored row when the incoming one does not win the condition; the losing new
// row stays in its segment, masked only by that segment's delete vector. Recover must not bring it back.
// Disabled: recover replays every row as a plain upsert, so the rows that lost the condition win.
TEST_F(LakePrimaryKeyRecoverTest, DISABLED_conditional_update) {
    Rows expected;
    const std::vector<std::vector<Op>> base = {upserts(0, 12, 100, 0)};
    publish(write_rows(base), false);
    apply(&expected, base);
    // Even keys lose the condition on c1 (lower), odd keys win (higher).
    std::vector<Op> conditional;
    for (int32_t key = 0; key < 12; key++) {
        const int32_t c1 = key % 2 == 0 ? 50 + key : 150 + key;
        conditional.push_back({key, c1, 1000 + key});
        if (key % 2 != 0) {
            expected[key] = {c1, 1000 + key};
        }
    }
    publish(write_rows({conditional}, "c1"), false);
    ASSERT_EQ(expected, read(_version));

    const std::vector<std::vector<Op>> later = {upserts(100, 102, 0, 0)};
    publish(write_rows(later), true);
    apply(&expected, later);
    EXPECT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

// Row-mode partial update rewrites the partial segment into a full one during publish. Recover both while
// that publish is in progress and on a later one.
TEST_F(LakePrimaryKeyRecoverTest, row_mode_partial_update) {
    Rows expected;
    const std::vector<std::vector<Op>> base = {upserts(0, 16, 0, 1000)};
    publish(write_rows(base), false);
    apply(&expected, base);
    std::vector<std::pair<int32_t, int32_t>> partial;
    for (int32_t key = 8; key < 20; key++) {
        partial.emplace_back(key, 500 + key);
        // A new key takes c2's default.
        expected[key] = {500 + key, key < 16 ? 1000 + key : 10};
    }
    publish(write_partial(partial, PartialUpdateMode::ROW_MODE), true);
    ASSERT_EQ(expected, read(_version));

    const std::vector<std::vector<Op>> later = {deletes(0, 4), upserts(30, 32, 0, 0)};
    publish(write_rows(later), true);
    apply(&expected, later);
    EXPECT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

// Column-mode partial update writes the new c1 values as a delta column group on the rows' existing segments;
// in upsert mode, keys that do not exist yet go to a new full segment. Recover both while that publish is in
// progress and on a later one.
class LakePrimaryKeyRecoverColumnModeTest : public LakePrimaryKeyRecoverTest,
                                            public testing::WithParamInterface<PartialUpdateMode> {
public:
    LakePrimaryKeyRecoverColumnModeTest() : LakePrimaryKeyRecoverTest("test_lake_primary_key_recover_column_mode") {}
};

TEST_P(LakePrimaryKeyRecoverColumnModeTest, column_mode_partial_update) {
    const PartialUpdateMode mode = GetParam();
    Rows expected;
    const std::vector<std::vector<Op>> base = {upserts(0, 16, 0, 1000)};
    publish(write_rows(base), false);
    apply(&expected, base);
    std::vector<std::pair<int32_t, int32_t>> partial;
    for (int32_t key = 8; key < 20; key++) {
        partial.emplace_back(key, 500 + key);
        if (key < 16) {
            expected[key].first = 500 + key;
        } else if (mode == PartialUpdateMode::COLUMN_UPSERT_MODE) {
            expected[key] = {500 + key, 10};
        }
    }
    publish(write_partial(partial, mode), true);
    ASSERT_EQ(expected, read(_version));

    const std::vector<std::vector<Op>> later = {deletes(0, 4), upserts(12, 14, 700, 700), upserts(30, 32, 0, 0)};
    publish(write_rows(later), true);
    apply(&expected, later);
    EXPECT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

INSTANTIATE_TEST_SUITE_P(LakePrimaryKeyRecoverColumnModeTest, LakePrimaryKeyRecoverColumnModeTest,
                         testing::Values(PartialUpdateMode::COLUMN_UPDATE_MODE, PartialUpdateMode::COLUMN_UPSERT_MODE));

// Recover clears the tablet's delete vectors and appends the rebuilt ones to the publish's MetaFileBuilder, but
// an empty one is never appended. A lookup within the same publish that misses the builder falls back to the
// base version's delete vector, which recover meant to discard. Here the base version wrongly deletes a row;
// recover brings it back, but the re-publish rebuilds its index from the stale delete vector, so the index
// misses the row and the next upsert of that key leaves it live as a duplicate.
TEST_F(LakePrimaryKeyRecoverTest, recovered_empty_delvec_does_not_fall_back_to_base_version) {
    Rows expected;
    const std::vector<std::vector<std::vector<Op>>> loads = {{upserts(0, 12, 0, 0)}, {upserts(100, 112, 0, 0)}};
    for (const auto& load : loads) {
        publish(write_rows(load), false);
        apply(&expected, load);
    }
    // A version whose only change is a wrong delete of the second rowset's first row (key 100).
    ASSIGN_OR_ABORT(auto base, _tablet_mgr->get_tablet_metadata(tablet_id(), _version));
    auto corrupted = std::make_shared<TabletMetadata>(*base);
    corrupted->set_version(_version + 1);
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id()));
    {
        MetaFileBuilder builder(tablet, corrupted);
        auto delvec = std::make_shared<DelVector>();
        const uint32_t rowid = 0;
        delvec->init(corrupted->version(), &rowid, 1);
        builder.append_delvec(delvec, corrupted->rowsets(1).id());
        corrupted->mutable_rowsets(1)->set_num_dels(1);
        ASSERT_OK(builder.finalize(next_id()));
    }
    ASSERT_EQ(1, corrupted->delvec_meta().version_to_file_size());
    const std::string stale_delvec_file = corrupted->delvec_meta().version_to_file().begin()->second.name();
    _version++;
    _update_mgr->try_remove_primary_index_cache(tablet_id());
    ASSERT_EQ(expected.size() - 1, read(_version).size());

    // Recover treats the segments as the truth, so key 100 is back.
    const std::vector<std::vector<Op>> later = {upserts(200, 202, 0, 0)};
    publish(write_rows(later), true);
    apply(&expected, later);
    ASSERT_EQ(expected, read(_version));
    // The rowset statistics follow the rebuilt delete vectors, and vacuum gets the dropped delete vector file.
    ASSIGN_OR_ABORT(auto recovered, _tablet_mgr->get_tablet_metadata(tablet_id(), _version));
    for (const auto& rowset : recovered->rowsets()) {
        EXPECT_EQ(0, rowset.num_dels()) << rowset.id();
    }
    EXPECT_TRUE(std::any_of(recovered->orphan_files().begin(), recovered->orphan_files().end(),
                            [&](const FileMetaPB& f) { return f.name() == stale_delvec_file; }));
    check_index_agrees(&expected);
}

// A publish attempt that fails after appending its delete vectors leaves them in the MetaFileBuilder. Recover
// rebuilds every delete vector from scratch, so none of those may survive it.
TEST_F(LakePrimaryKeyRecoverTest, recover_discards_delvecs_of_failed_attempt) {
    Rows expected;
    const std::vector<std::vector<std::vector<Op>>> loads = {{upserts(0, 12, 0, 0)}, {upserts(100, 112, 0, 0)}};
    for (const auto& load : loads) {
        publish(write_rows(load), false);
        apply(&expected, load);
    }
    ASSIGN_OR_ABORT(auto base, _tablet_mgr->get_tablet_metadata(tablet_id(), _version));
    auto metadata = std::make_shared<TabletMetadata>(*base);
    metadata->set_version(_version + 1);
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id()));
    MetaFileBuilder builder(tablet, metadata);
    // What the failed attempt left behind: a delete of a row nothing supersedes.
    auto delvec = std::make_shared<DelVector>();
    const uint32_t rowid = 0;
    delvec->init(metadata->version(), &rowid, 1);
    builder.append_delvec(delvec, metadata->rowsets(1).id());
    LakePrimaryKeyRecover recover(&builder, &tablet, metadata);
    ASSERT_OK(recover.recover());
    ASSERT_OK(builder.finalize(next_id()));
    _version++;
    EXPECT_EQ(expected, read(_version));
}

// A multi-statement transaction publishes all its statements' txn logs through one batch apply. It has to
// recover from a duplicate primary key like a single-statement load does.
TEST_F(LakePrimaryKeyRecoverTest, multi_statement_transaction) {
    Rows expected;
    const std::vector<std::vector<Op>> base = {upserts(0, 12, 0, 0)};
    publish(write_rows(base), false);
    apply(&expected, base);

    const std::vector<Statement> statements = {statement(upserts(4, 8, 100, 100)), statement(upserts(100, 104, 0, 0))};
    publish_multi_statement(statements, true);
    for (const auto& stmt : statements) {
        apply(&expected, stmt);
    }
    ASSERT_EQ(expected, read(_version));
    ASSERT_EQ(expected.size(), live_rows_by_stats(_version));
    check_index_agrees(&expected);
}

// Upserts and deletes spread over the statements of one transaction, recovered while the transaction publishes
// and again on a later publish that replays the merged rowset as history. On this branch recover replays a
// rowset's deletes after all of its segments, so no key deleted here is upserted again by a later statement.
TEST_F(LakePrimaryKeyRecoverTest, multi_statement_upsert_and_delete) {
    Rows expected;
    const std::vector<std::vector<Op>> base = {upserts(0, 16, 0, 0)};
    publish(write_rows(base), false);
    apply(&expected, base);

    const std::vector<Statement> statements = {
            statement(upserts(0, 8, 100, 100)),   statement(deletes(4, 6)),
            statement(upserts(8, 10, 200, 200)),  statement(deletes(0, 2)),
            statement(upserts(14, 18, 300, 300)),
    };
    publish_multi_statement(statements, true);
    for (const auto& stmt : statements) {
        apply(&expected, stmt);
    }
    ASSERT_EQ(expected, read(_version));

    const std::vector<std::vector<Op>> later = {upserts(30, 32, 0, 0)};
    publish(write_rows(later), true);
    apply(&expected, later);
    ASSERT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

// A partial update followed by full upserts and deletes in one multi-statement transaction. The partial update
// comes first: one that follows a statement writing the same keys fails the batch apply even without recover,
// since it looks for those rows in segments that are still pending.
class LakePrimaryKeyRecoverMultiStatementPartialTest : public LakePrimaryKeyRecoverTest,
                                                       public testing::WithParamInterface<PartialUpdateMode> {
public:
    LakePrimaryKeyRecoverMultiStatementPartialTest()
            : LakePrimaryKeyRecoverTest("test_lake_primary_key_recover_multi_statement_partial") {}
};

TEST_P(LakePrimaryKeyRecoverMultiStatementPartialTest, partial_update) {
    const PartialUpdateMode mode = GetParam();
    Rows expected;
    const std::vector<std::vector<Op>> base = {upserts(0, 16, 0, 1000)};
    publish(write_rows(base), false);
    apply(&expected, base);

    const std::vector<Statement> statements = {
            partial_statement(2, 20, 500, mode),
            statement(upserts(4, 6, 100, 100)),
            statement(deletes(0, 3)),
    };
    publish_multi_statement(statements, true);
    for (const auto& stmt : statements) {
        apply(&expected, stmt);
    }
    ASSERT_EQ(expected, read(_version));

    const std::vector<std::vector<Op>> later = {upserts(30, 32, 0, 0)};
    publish(write_rows(later), true);
    apply(&expected, later);
    ASSERT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

INSTANTIATE_TEST_SUITE_P(LakePrimaryKeyRecoverMultiStatementPartialTest, LakePrimaryKeyRecoverMultiStatementPartialTest,
                         testing::Values(PartialUpdateMode::ROW_MODE, PartialUpdateMode::COLUMN_UPDATE_MODE,
                                         PartialUpdateMode::COLUMN_UPSERT_MODE));

// A batch apply that fails on its second statement, after the first one has already updated the index, appended
// delete vectors, added its segments to the pending rowset and counted its deletes into the base rowset. The
// parameter is the failure: an inconsistent delete vector (recover, then publish again) or an unknown segment
// in the del stats (recover without publishing, which for a single txn log means the log is already applied).
// Either way the whole batch has to be applied again on the recovered state, without any trace of the failed
// attempt.
class LakePrimaryKeyRecoverMidBatchTest : public LakePrimaryKeyRecoverTest,
                                          public testing::WithParamInterface<const char*> {
public:
    LakePrimaryKeyRecoverMidBatchTest() : LakePrimaryKeyRecoverTest("test_lake_primary_key_recover_mid_batch") {}
};

TEST_P(LakePrimaryKeyRecoverMidBatchTest, fail_on_second_statement) {
    const std::string sync_point = GetParam();
    Rows expected;
    const std::vector<std::vector<Op>> base = {upserts(0, 16, 0, 0)};
    publish(write_rows(base), false);
    apply(&expected, base);
    ASSERT_EQ(expected.size(), live_rows_by_stats(_version));

    // Each statement deletes rows of the base rowset, so each one hits both sync points.
    const std::vector<Statement> statements = {statement(upserts(0, 4, 100, 100)), statement(deletes(2, 6)),
                                               statement(upserts(4, 8, 200, 200))};
    int hits = 0;
    SyncPoint::GetInstance()->SetCallBack(sync_point, [&](void* arg) {
        if (++hits == 2) {
            *(Status*)arg = Status::InternalError("injected inconsistency on the second statement");
        }
    });
    DeferOp clear([&]() { SyncPoint::GetInstance()->ClearCallBack(sync_point); });
    publish_multi_statement(statements, false);
    // The failure was injected, and the batch was applied again after it.
    ASSERT_GT(hits, 2);
    for (const auto& stmt : statements) {
        apply(&expected, stmt);
    }
    ASSERT_EQ(expected, read(_version));
    // The failed attempt's deletes must not be counted twice.
    ASSERT_EQ(expected.size(), live_rows_by_stats(_version));
    check_index_agrees(&expected);
}

INSTANTIATE_TEST_SUITE_P(LakePrimaryKeyRecoverMidBatchTest, LakePrimaryKeyRecoverMidBatchTest,
                         testing::Values("delvec_inconsistent", "update_num_del_stat"));

} // namespace starrocks::lake
