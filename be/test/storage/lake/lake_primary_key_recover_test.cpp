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

// Primary key recover (LakePrimaryKeyRecover) keeps a tablet's delete vectors, deletes every live row that a newer
// live row with the same key supersedes, and discards the persistent index so that it is rebuilt. It runs on the
// retry of a publish that found the tablet inconsistent: that publish marks the tablet and fails. These tests drive
// each load type through the real write and publish path, make chosen publishes fail that way, and check the
// recovered version row by row against a model of what the loads should have produced. A follow-up upsert of
// every key then checks that the rebuilt index agrees with the delete vectors: a key the index lost would come
// back as a duplicate.

#include "storage/lake/lake_primary_key_recover.h"

#include <gtest/gtest.h>

#include <ctime>
#include <functional>
#include <map>
#include <numeric>
#include <optional>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config_ingest_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "gen_cpp/Types_types.h"
#include "storage/chunk_helper.h"
#include "storage/datum_variant.h"
#include "storage/del_vector.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/compaction_task_context.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/primary_key_manager.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/lake/transactions.h"
#include "storage/options.h"
#include "storage/storage_metrics.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"
#include "storage/variant_tuple.h"

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
    }

    void TearDown() override {
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

    // Publish `txn_id` as the next version. With `recover`, the publish goes through recover: see
    // publish_through_recover().
    void publish(int64_t txn_id, bool recover) {
        auto attempt = [&]() { return publish_single_version(tablet_id(), _version + 1, txn_id).status(); };
        if (recover) {
            publish_through_recover(attempt);
        } else {
            ASSERT_OK(attempt());
            _version++;
        }
    }

    // Publish the next version, meeting a duplicate primary key while loading the index on the first attempt.
    void publish_through_recover(const std::function<Status()>& attempt) {
        // Only a load reaches the injection point, so make sure this publish does not reuse a cached index.
        _primary_key_mgr->try_remove_primary_index_cache(tablet_id());
        _fail_next_index_load = true;
        const int64_t errors_before = error_count();
        publish_failing_once(attempt);
        EXPECT_FALSE(_fail_next_index_load) << "the publish did not load the index";
        _fail_next_index_load = false;
        EXPECT_EQ(errors_before + 1, error_count());
    }

    // Publish the next version through recover, the way FE drives it: the first attempt finds the tablet
    // inconsistent, marks it and fails; the retry recovers the tablet and succeeds. Only a retry that recovered
    // clears the mark.
    void publish_failing_once(const std::function<Status()>& attempt) {
        ASSERT_FALSE(_primary_key_mgr->need_recover(tablet_id()));
        auto st = attempt();
        ASSERT_FALSE(st.ok()) << "the publish did not fail";
        ASSERT_TRUE(_primary_key_mgr->need_recover(tablet_id()))
                << "the failed publish did not mark the tablet: " << st;
        ASSERT_OK(attempt());
        ASSERT_FALSE(_primary_key_mgr->need_recover(tablet_id())) << "the retry did not recover";
        _version++;
    }

    // Each inconsistency a publish finds in the tablet itself counts here once.
    static int64_t error_count() { return StorageMetrics::instance()->primary_key_table_error_state_total.value(); }

    // Persist the whole index into sstables in a version of its own, as a publish eventually does.
    void flush_index() {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id(), _version));
        ASSIGN_OR_ABORT(auto flushed, _primary_key_mgr->flush_pk_memtable(metadata, _version + 1));
        auto next = std::make_shared<TabletMetadata>(*flushed);
        next->set_version(_version + 1);
        ASSERT_GT(next->sstable_meta().sstables_size(), 0);
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(*next));
        _version++;
        _primary_key_mgr->try_remove_primary_index_cache(tablet_id());
    }

    // A copy of the current version to corrupt, and the call that makes it the next version.
    MutableTabletMetadataPtr copy_version() {
        ASSIGN_OR_ABORT(auto base, _tablet_mgr->get_tablet_metadata(tablet_id(), _version));
        auto copy = std::make_shared<TabletMetadata>(*base);
        copy->set_version(_version + 1);
        return copy;
    }
    void put_version(const MutableTabletMetadataPtr& metadata) {
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        _version++;
        _primary_key_mgr->try_remove_primary_index_cache(tablet_id());
    }

    // The row ids the delete vector of segment `rssid` deletes at `version`.
    std::vector<uint32_t> deleted_rows(int64_t version, uint32_t rssid) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id(), version));
        DelVector delvec;
        CHECK_OK(get_del_vec(_tablet_mgr.get(), *metadata, rssid, false, LakeIOOptions(), &delvec));
        std::vector<uint32_t> rows;
        if (!delvec.empty()) {
            for (uint32_t rowid : *delvec.roaring()) {
                rows.push_back(rowid);
            }
        }
        return rows;
    }

    // Every row visible at `version`, keyed by c0. A duplicate primary key fails the test.
    Rows read(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        Rows rows;
        auto chunk = ChunkFactory::new_chunk(*_schema, 128);
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
    ConfigResetGuard<bool> _recover_guard{&config::enable_primary_key_recover, true};
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

// Upserts and deletes of the same keys mixed in one load, each batch in its own segment or del file, so the
// recovered result depends on replaying deletes at their position between the segments.
TEST_F(LakePrimaryKeyRecoverTest, upsert_and_delete_mixed_in_one_load) {
    ConfigResetGuard<bool> order_guard(&config::lake_enable_pk_preserve_txn_delete_order, true);
    Rows expected;
    const std::vector<std::vector<Op>> base = {upserts(0, 16, 0, 0)};
    publish(write_rows(base), false);
    apply(&expected, base);
    const std::vector<std::vector<Op>> mixed = {
            upserts(0, 8, 100, 100), deletes(4, 12), upserts(6, 10, 200, 200), deletes(0, 2), upserts(14, 18, 300, 300),
    };
    publish(write_rows(mixed), true);
    apply(&expected, mixed);
    ASSERT_EQ(expected, read(_version));
    // Recover once more from a later load, now replaying the mixed rowset as history.
    const std::vector<std::vector<Op>> later = {upserts(30, 32, 0, 0)};
    publish(write_rows(later), true);
    apply(&expected, later);
    ASSERT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

// A delete load followed by a compaction that takes the delete rowset but not the rowset holding the deleted
// keys. The parameter decides whether the index has flushed those deletes into sstables before the compaction:
// if not, the compaction carries the del file over to its output rowset; if so, it drops it, since the
// sstables already have it. Either way the delete vector of the first rowset deletes those keys, and recover keeps
// it, so the deleted keys stay deleted.
class LakePrimaryKeyRecoverCompactionTest : public LakePrimaryKeyRecoverTest, public testing::WithParamInterface<bool> {
public:
    LakePrimaryKeyRecoverCompactionTest() : LakePrimaryKeyRecoverTest("test_lake_primary_key_recover_compaction") {}
};

TEST_P(LakePrimaryKeyRecoverCompactionTest, delete_then_compaction) {
    const bool with_sstables = GetParam();
    Rows expected;
    const std::vector<std::vector<std::vector<Op>>> loads = {
            {upserts(0, 16, 0, 0)}, {upserts(100, 104, 0, 0)}, {deletes(0, 8)}, {upserts(200, 204, 0, 0)}};
    for (const auto& load : loads) {
        publish(write_rows(load), false);
        apply(&expected, load);
    }
    ASSERT_EQ(expected, read(_version));
    if (with_sstables) {
        flush_index();
    }
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id(), _version));

    // Compact everything but the first rowset, which still holds the deleted keys.
    auto rowsets = Rowset::get_rowsets(_tablet_mgr.get(), metadata);
    ASSERT_EQ(4, rowsets.size());
    const uint32_t first_rowset_id = rowsets[0]->id();
    std::vector<RowsetPtr> inputs(rowsets.begin() + 1, rowsets.end());
    const int64_t compaction_txn_id = next_id();
    auto context =
            std::make_unique<CompactionTaskContext>(compaction_txn_id, tablet_id(), _version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(context.get(), inputs));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    publish(compaction_txn_id, false);
    ASSERT_EQ(expected, read(_version));
    ASSIGN_OR_ABORT(metadata, _tablet_mgr->get_tablet_metadata(tablet_id(), _version));
    ASSERT_EQ(2, metadata->rowsets_size());
    ASSERT_EQ(first_rowset_id, metadata->rowsets(0).id());
    // The del file goes with the compaction output only while the sstables do not cover it yet.
    ASSERT_EQ(with_sstables ? 0 : 1, metadata->rowsets(1).del_files_size());

    const std::vector<std::vector<Op>> later = {upserts(300, 302, 0, 0)};
    publish(write_rows(later), true);
    apply(&expected, later);
    EXPECT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

INSTANTIATE_TEST_SUITE_P(LakePrimaryKeyRecoverCompactionTest, LakePrimaryKeyRecoverCompactionTest,
                         testing::Values(false, true));

// A conditional upsert keeps the stored row when the incoming one does not win the condition; the losing new
// row stays in its segment, masked only by that segment's delete vector. Recover keeps that delete vector, so the
// losing rows stay deleted.
TEST_F(LakePrimaryKeyRecoverTest, conditional_update) {
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

// Recover takes the base version's delete vectors as the truth, even where one deletes a row no load deleted. Here
// a version wrongly deletes key 100: recover keeps it deleted, the rebuilt index agrees, and a later upsert of key
// 100 brings it back exactly once.
TEST_F(LakePrimaryKeyRecoverTest, recover_keeps_deletes_of_base_version) {
    Rows expected;
    const std::vector<std::vector<std::vector<Op>>> loads = {{upserts(0, 12, 0, 0)}, {upserts(100, 112, 0, 0)}};
    for (const auto& load : loads) {
        publish(write_rows(load), false);
        apply(&expected, load);
    }
    // A version whose only change is a wrong delete of the second rowset's first row (key 100).
    auto corrupted = copy_version();
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id()));
    {
        MetaFileBuilder builder(tablet, corrupted);
        auto delvec = std::make_shared<DelVector>();
        const uint32_t rowid = 0;
        delvec->init(corrupted->version(), &rowid, 1);
        builder.append_delvec(delvec, corrupted->rowsets(1).id());
        ASSERT_OK(builder.finalize(next_id()));
    }
    _version++;
    _primary_key_mgr->try_remove_primary_index_cache(tablet_id());
    expected.erase(100);
    ASSERT_EQ(expected, read(_version));

    const std::vector<std::vector<Op>> later = {upserts(200, 202, 0, 0)};
    publish(write_rows(later), true);
    apply(&expected, later);
    ASSERT_EQ(expected, read(_version));
    check_index_agrees(&expected);

    const std::vector<std::vector<Op>> again = {{{100, 7, 7}}};
    publish(write_rows(again), false);
    apply(&expected, again);
    EXPECT_EQ(expected, read(_version));
}

// A publish attempt can fail after it has updated the index and appended delete vectors to its MetaFileBuilder.
// None of that survives: the retry starts from the base version with a fresh builder and recovers the tablet. The
// parameter is the point where the attempt fails.
class LakePrimaryKeyRecoverFailedAttemptTest : public LakePrimaryKeyRecoverTest,
                                               public testing::WithParamInterface<const char*> {
public:
    LakePrimaryKeyRecoverFailedAttemptTest()
            : LakePrimaryKeyRecoverTest("test_lake_primary_key_recover_failed_attempt") {}
};

TEST_P(LakePrimaryKeyRecoverFailedAttemptTest, recover_discards_delvecs_of_failed_attempt) {
    Rows expected;
    const std::vector<std::vector<std::vector<Op>>> loads = {{upserts(0, 12, 0, 0)}, {upserts(100, 112, 0, 0)}};
    for (const auto& load : loads) {
        publish(write_rows(load), false);
        apply(&expected, load);
    }
    const std::string sync_point = GetParam();
    bool fail = true;
    SyncPoint::GetInstance()->SetCallBack(sync_point, [&](void* arg) {
        if (fail) {
            fail = false;
            *(Status*)arg = Status::InternalError("injected publish failure");
        }
    });
    DeferOp clear_callback([&]() { SyncPoint::GetInstance()->ClearCallBack(sync_point); });
    // Replaces keys of both earlier loads, so the attempt deletes rows of both rowsets before it fails.
    const std::vector<std::vector<Op>> load = {upserts(6, 106, 500, 500)};
    const int64_t txn_id = write_rows(load);
    publish_failing_once([&]() { return publish_single_version(tablet_id(), _version + 1, txn_id).status(); });
    EXPECT_FALSE(fail) << "the publish did not reach " << sync_point;
    apply(&expected, load);
    EXPECT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

INSTANTIATE_TEST_SUITE_P(LakePrimaryKeyRecoverFailedAttemptTest, LakePrimaryKeyRecoverFailedAttemptTest,
                         testing::Values("update_num_del_stat", "delvec_inconsistent"));

// The three ways a publish finds a tablet inconsistent, each with a real inconsistency rather than an injected
// failure: the index load meets a key live twice, the index points at a row a delete vector already deletes, and
// the index points at a segment the tablet does not have.

// A version whose delete vector misses the older copy of keys a later load replaced, so those keys are live twice
// and rebuilding the index meets them as duplicates. Recover deletes the older copies, keeps the newer ones, and
// leaves every other delete vector as it was.
TEST_F(LakePrimaryKeyRecoverTest, duplicate_keys_keep_the_newest_row) {
    Rows expected;
    const std::vector<std::vector<std::vector<Op>>> loads = {{upserts(0, 8, 0, 0)},
                                                             {upserts(4, 12, 100, 100)},
                                                             {upserts(20, 24, 0, 0)},
                                                             {deletes(20, 22)},
                                                             {upserts(10, 12, 200, 200)}};
    for (const auto& load : loads) {
        publish(write_rows(load), false);
        apply(&expected, load);
    }
    ASSERT_EQ(expected, read(_version));
    const int64_t good_version = _version;
    ASSIGN_OR_ABORT(auto good, _tablet_mgr->get_tablet_metadata(tablet_id(), good_version));
    const uint32_t first_rssid = good->rowsets(0).id();
    // The second load deleted keys 4..7 from the first rowset.
    ASSERT_EQ((std::vector<uint32_t>{4, 5, 6, 7}), deleted_rows(good_version, first_rssid));

    // Lose that delete vector, and the sstables that would let the index load skip the rebuild.
    auto corrupted = copy_version();
    corrupted->mutable_delvec_meta()->mutable_delvecs()->erase(first_rssid);
    corrupted->clear_sstable_meta();
    put_version(corrupted);

    const std::vector<std::vector<Op>> later = {upserts(300, 302, 0, 0)};
    const int64_t txn_id = write_rows(later);
    const int64_t errors_before = error_count();
    publish_failing_once([&]() { return publish_single_version(tablet_id(), _version + 1, txn_id).status(); });
    EXPECT_EQ(errors_before + 1, error_count());
    apply(&expected, later);
    EXPECT_EQ(expected, read(_version));
    EXPECT_EQ((std::vector<uint32_t>{4, 5, 6, 7}), deleted_rows(_version, first_rssid));
    // The lost delete vector left num_dels counting its 4 deletes; recover counts them again, not on top.
    ASSIGN_OR_ABORT(auto recovered, _tablet_mgr->get_tablet_metadata(tablet_id(), _version));
    EXPECT_EQ(4, recovered->rowsets(0).num_dels());
    for (int i = 1; i < good->rowsets_size(); i++) {
        const auto& rowset = good->rowsets(i);
        for (int j = 0; j < rowset.segment_metas_size(); j++) {
            const uint32_t rssid = rowset.id() + get_segment_idx(rowset, j);
            EXPECT_EQ(deleted_rows(good_version, rssid), deleted_rows(_version, rssid)) << "segment " << rssid;
        }
    }
    check_index_agrees(&expected);
}

// The index in the sstables still points at a row that a delete vector of a later version deletes, so the next
// upsert of that key finds a delete vector that already deletes the row it replaces.
TEST_F(LakePrimaryKeyRecoverTest, index_points_at_deleted_row) {
    Rows expected;
    const std::vector<std::vector<Op>> base = {upserts(0, 16, 0, 0)};
    publish(write_rows(base), false);
    apply(&expected, base);
    flush_index();
    // A version that deletes key 5 without the index knowing.
    auto corrupted = copy_version();
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id()));
    {
        MetaFileBuilder builder(tablet, corrupted);
        auto delvec = std::make_shared<DelVector>();
        const uint32_t rowid = 5;
        delvec->init(corrupted->version(), &rowid, 1);
        builder.append_delvec(delvec, corrupted->rowsets(0).id());
        ASSERT_OK(builder.finalize(next_id()));
    }
    _version++;
    _primary_key_mgr->try_remove_primary_index_cache(tablet_id());
    expected.erase(5);
    ASSERT_EQ(expected, read(_version));

    const std::vector<std::vector<Op>> later = {upserts(4, 8, 100, 100)};
    const int64_t txn_id = write_rows(later);
    const int64_t errors_before = error_count();
    publish_failing_once([&]() { return publish_single_version(tablet_id(), _version + 1, txn_id).status(); });
    EXPECT_EQ(errors_before + 1, error_count());
    apply(&expected, later);
    EXPECT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

// The index in the sstables still points at the rows of a rowset that a later version no longer has, so the next
// upsert of those keys deletes rows of a segment the tablet does not have.
TEST_F(LakePrimaryKeyRecoverTest, index_points_at_missing_segment) {
    Rows expected;
    const std::vector<std::vector<std::vector<Op>>> loads = {{upserts(0, 8, 0, 0)}, {upserts(100, 108, 0, 0)}};
    for (const auto& load : loads) {
        publish(write_rows(load), false);
        apply(&expected, load);
    }
    flush_index();
    // A version without the first rowset.
    auto corrupted = copy_version();
    corrupted->mutable_rowsets()->DeleteSubrange(0, 1);
    put_version(corrupted);
    std::erase_if(expected, [](const auto& entry) { return entry.first < 8; });
    ASSERT_EQ(expected, read(_version));

    const std::vector<std::vector<Op>> later = {upserts(0, 4, 50, 50)};
    const int64_t txn_id = write_rows(later);
    const int64_t errors_before = error_count();
    publish_failing_once([&]() { return publish_single_version(tablet_id(), _version + 1, txn_id).status(); });
    EXPECT_EQ(errors_before + 1, error_count());
    apply(&expected, later);
    EXPECT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

// A tablet split leaves each child reading the parent's shared segments through its own key range, so a segment
// iterator over such a segment starts at the first in-range row, not at row 0. Recover must address the rows it
// reads by their physical row ids, or the delete vectors it writes mark the wrong rows.
class LakePrimaryKeyRecoverSplitTest : public LakePrimaryKeyRecoverTest {
public:
    LakePrimaryKeyRecoverSplitTest() : LakePrimaryKeyRecoverTest("test_lake_primary_key_recover_split") {}

protected:
    // Two loads, {0..11} and then {6..9}, on a tablet that then becomes a split child owning [6, +inf) and
    // sharing both rowsets' segments with its sibling.
    void make_split_child(Rows* expected) {
        constexpr int32_t kLowerBound = 6;
        // Range distribution requires the order-preserving primary key encoding.
        _tablet_metadata->mutable_schema()->set_primary_key_encoding_type(
                PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        ASSERT_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));

        const std::vector<std::vector<std::vector<Op>>> loads = {{upserts(0, 12, 0, 0)}, {upserts(6, 10, 100, 100)}};
        for (const auto& load : loads) {
            publish(write_rows(load), false);
            apply(expected, load);
        }
        auto child = copy_version();
        {
            auto* range_pb = child->mutable_range();
            DatumVariant lower(get_type_info(LogicalType::TYPE_INT), Datum(kLowerBound));
            VariantTuple tuple;
            tuple.append(lower);
            tuple.to_proto(range_pb->mutable_lower_bound());
            range_pb->set_lower_bound_included(true);
        }
        for (auto& rowset : *child->mutable_rowsets()) {
            for (auto& segment : *rowset.mutable_segment_metas()) {
                segment.set_shared(true);
            }
        }
        put_version(child);
        std::erase_if(*expected, [](const auto& entry) { return entry.first < kLowerBound; });
    }
};

TEST_F(LakePrimaryKeyRecoverSplitTest, shared_segment_read_through_tablet_range) {
    Rows expected;
    make_split_child(&expected);
    ASSERT_EQ(expected, read(_version));

    const std::vector<std::vector<Op>> later = {upserts(100, 102, 0, 0)};
    publish(write_rows(later), true);
    apply(&expected, later);
    EXPECT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

// The split child loses the delete vector that deletes the first rowset's keys 6..9, at its physical rows 6..9, so
// the second load's copies of those keys are live twice. Recover has to delete rows 6..9, not the rows the range
// iterator happens to return first.
TEST_F(LakePrimaryKeyRecoverSplitTest, shared_segment_duplicate_keys) {
    Rows expected;
    make_split_child(&expected);
    ASSIGN_OR_ABORT(auto child, _tablet_mgr->get_tablet_metadata(tablet_id(), _version));
    const uint32_t first_rssid = child->rowsets(0).id();
    ASSERT_EQ((std::vector<uint32_t>{6, 7, 8, 9}), deleted_rows(_version, first_rssid));
    auto corrupted = copy_version();
    corrupted->mutable_delvec_meta()->mutable_delvecs()->erase(first_rssid);
    corrupted->clear_sstable_meta();
    put_version(corrupted);

    const std::vector<std::vector<Op>> later = {upserts(100, 102, 0, 0)};
    const int64_t txn_id = write_rows(later);
    const int64_t errors_before = error_count();
    publish_failing_once([&]() { return publish_single_version(tablet_id(), _version + 1, txn_id).status(); });
    EXPECT_EQ(errors_before + 1, error_count());
    apply(&expected, later);
    EXPECT_EQ(expected, read(_version));
    EXPECT_EQ((std::vector<uint32_t>{6, 7, 8, 9}), deleted_rows(_version, first_rssid));
    check_index_agrees(&expected);
}

// A multi-statement transaction publishes all its statements' txn logs through one batch apply. Its retry has to
// recover before that batch apply like a single-statement load does.
TEST_F(LakePrimaryKeyRecoverTest, multi_statement_transaction) {
    Rows expected;
    const std::vector<std::vector<Op>> base = {upserts(0, 12, 0, 0)};
    publish(write_rows(base), false);
    apply(&expected, base);

    const int64_t txn_id = next_id();
    std::vector<PUniqueId> load_ids;
    const std::vector<std::vector<Op>> statements = {upserts(4, 8, 100, 100), upserts(100, 104, 0, 0)};
    for (size_t i = 0; i < statements.size(); i++) {
        PUniqueId load_id;
        load_id.set_hi(txn_id);
        load_id.set_lo(i);
        load_ids.push_back(load_id);
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_full_slots)
                                                   .set_load_id(load_id)
                                                   .set_is_multi_statements_txn(true)
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        auto op = Int8Column::create();
        for (const auto& row : statements[i]) {
            c0->append(row.key);
            c1->append(row.c1);
            c2->append(row.c2);
            op->append(TOpType::UPSERT);
        }
        Columns columns;
        columns.emplace_back(std::move(c0));
        columns.emplace_back(std::move(c1));
        columns.emplace_back(std::move(c2));
        columns.emplace_back(std::move(op));
        Chunk chunk = make_chunk(std::move(columns));
        std::vector<uint32_t> indexes(statements[i].size());
        std::iota(indexes.begin(), indexes.end(), 0);
        ASSERT_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }
    apply(&expected, statements);

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
    publish_through_recover([&]() {
        return publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id()), _version, _version + 1, txns, false)
                .status();
    });
    EXPECT_EQ(expected, read(_version));
    check_index_agrees(&expected);
}

} // namespace starrocks::lake
