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

// Flexible partial update on a shared-data primary key table: every row of one load updates only the columns
// of its own column set, and keeps the current value of the others (a new key takes the defaults). The load
// carries each row's set-id in the hidden "__cset__" column and the dictionary of sets in the txn meta; the
// update file holds a placeholder in every cell a row did not declare, which must never reach the table.
//
// Both apply paths are covered with the same data: column mode (partial_update_mode=flexible), which writes
// ordinary row-complete `.cols` delta column groups, and row mode (flexible_row), which rewrites whole rows.

#include "common/flexible_partial_update.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <map>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "common/config_ingest_fwd.h"
#include "gen_cpp/Types_types.h"
#include "storage/chunk_helper.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/compaction_task_context.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

namespace {

constexpr int kNumValueColumns = 3;
// Default values of c1, c2, c3: what an omitted column of a NEW key must read as.
constexpr std::array<int, kNumValueColumns> kDefaults = {0, 0, 7};
// What a row carries in a column it does not declare. Must never be read back.
constexpr int kPlaceholder = -999999;

struct Row {
    int key;
    // The value columns this row declares, a subset of {"c1", "c2", "c3"}.
    std::vector<std::string> cols;
    // New values of c1..c3; only the declared ones count.
    std::array<int, kNumValueColumns> vals{};
    bool del = false;
};

using Table = std::map<int, std::array<int, kNumValueColumns>>;

bool declares(const Row& row, int value_column) {
    const std::string name = "c" + std::to_string(value_column + 1);
    return std::find(row.cols.begin(), row.cols.end(), name) != row.cols.end();
}

// Applies |rows| to |table| in load order: a declared column takes the row's value, an undeclared column
// keeps the current value, or the default for a key that does not exist yet.
void apply_to_model(const std::vector<Row>& rows, Table* table) {
    for (const auto& row : rows) {
        if (row.del) {
            table->erase(row.key);
            continue;
        }
        auto it = table->find(row.key);
        if (it == table->end()) {
            it = table->emplace(row.key, kDefaults).first;
        }
        for (int c = 0; c < kNumValueColumns; ++c) {
            if (declares(row, c)) {
                it->second[c] = row.vals[c];
            }
        }
    }
}

// The rows of one memtable that decide the result: a delete followed by more rows of its key is dropped
// together with the rows of the key before it, as the memtable of a plain load drops them.
std::vector<Row> rows_kept_by_memtable(const std::vector<Row>& rows) {
    std::map<int, size_t> last_delete;
    std::map<int, size_t> last_row;
    for (size_t i = 0; i < rows.size(); ++i) {
        if (rows[i].del) {
            last_delete[rows[i].key] = i;
        }
        last_row[rows[i].key] = i;
    }
    std::vector<Row> kept;
    for (size_t i = 0; i < rows.size(); ++i) {
        auto it = last_delete.find(rows[i].key);
        if (it != last_delete.end() && it->second < last_row[rows[i].key] && i <= it->second) {
            continue;
        }
        kept.push_back(rows[i]);
    }
    return kept;
}

// Rows that repeat keys of the base table [0, 100) and new keys, by occurrence: batch i holds the i-th row
// of every key that has one.
std::vector<std::vector<Row>> repeated_rows_by_occurrence() {
    return {
            {
                    {5, {"c1"}, {501, 0, 0}},
                    {6, {"c1", "c2"}, {601, 602, 0}},
                    {7, {"c1"}, {701, 0, 0}},
                    {8, {"c1", "c2", "c3"}, {801, 802, 803}},
                    {9, {"c1"}, {901, 0, 0}},
                    {10, {}, {0, 0, 0}},
                    {11, {"c3"}, {0, 0, 1103}},
                    {1000, {"c1"}, {1, 0, 0}},
                    {1001, {"c2"}, {0, 2, 0}},
                    {1002, {"c1", "c2"}, {1, 2, 0}},
            },
            {
                    {5, {"c2"}, {0, 502, 0}},                 // disjoint
                    {6, {"c2", "c3"}, {0, 612, 613}},         // overlapping
                    {7, {"c1", "c2", "c3"}, {711, 712, 713}}, // superset
                    {8, {"c2"}, {0, 822, 0}},                 // subset
                    {9, {"c2"}, {0, 902, 0}},                 // disjoint, then a third row
                    {10, {"c3"}, {0, 0, 1003}},               // after a row declaring only the key
                    {11, {}, {0, 0, 0}},                      // declaring only the key
                    {1000, {"c3"}, {0, 0, 3}},                // new key, disjoint
                    {1001, {"c2"}, {0, 22, 0}},               // new key, the same set
                    {1002, {"c2", "c3"}, {0, 12, 13}},        // new key, overlapping, then a third row
            },
            {
                    {9, {"c1"}, {911, 0, 0}},
                    {1002, {"c1"}, {21, 0, 0}},
            },
    };
}

} // namespace

// Table: c0 INT key; c1, c2 INT nullable default 0; c3 INT nullable default 7.
class LakeFlexiblePartialUpdateTest : public TestBase {
public:
    LakeFlexiblePartialUpdateTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);
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
        for (int c = 0; c < kNumValueColumns; ++c) {
            auto col = schema->add_column();
            col->set_unique_id(next_id());
            col->set_name("c" + std::to_string(c + 1));
            col->set_type("INT");
            col->set_is_key(false);
            col->set_is_nullable(true);
            col->set_aggregation("REPLACE");
            col->set_default_value(std::to_string(kDefaults[c]));
        }
        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));

        // The load's slots: the key, every value column any row declares, then the hidden per-row
        // column-set id right before "__op", as FE plans a flexible load.
        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        for (int c = 0; c < kNumValueColumns; ++c) {
            _slots.emplace_back(c + 1, "c" + std::to_string(c + 1), TypeDescriptor{LogicalType::TYPE_INT});
        }
        _slots.emplace_back(4, LOAD_CSET_COLUMN, TypeDescriptor{LogicalType::TYPE_SMALLINT});
        _slots.emplace_back(5, "__op", TypeDescriptor{LogicalType::TYPE_TINYINT});
        for (auto& slot : _slots) {
            _slot_pointers.emplace_back(&slot);
        }
        for (int i = 0; i < static_cast<int>(_slots.size()); ++i) {
            _slot_cid_map.emplace(i, i);
        }
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        CHECK_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
    }

    void TearDown() override { remove_test_dir_or_die(); }

protected:
    // Base rows for keys [0, n): c1 = k * 11, c2 = k * 12, c3 = k * 13.
    void write_base(int n, int64_t* version, Table* model) {
        auto c0 = Int32Column::create();
        std::array<MutableColumnPtr, kNumValueColumns> values;
        for (auto& v : values) {
            v = Int32Column::create();
        }
        for (int k = 0; k < n; ++k) {
            c0->append(k);
            for (int c = 0; c < kNumValueColumns; ++c) {
                down_cast<Int32Column*>(values[c].get())->append(k * (11 + c));
            }
            (*model)[k] = {k * 11, k * 12, k * 13};
        }
        Chunk::SlotHashMap slot_map;
        for (int i = 0; i <= kNumValueColumns; ++i) {
            slot_map.emplace(i, i);
        }
        Columns columns;
        columns.emplace_back(std::move(c0));
        for (auto& v : values) {
            columns.emplace_back(std::move(v));
        }
        Chunk chunk(std::move(columns), slot_map);
        std::vector<uint32_t> indexes(n);
        for (int i = 0; i < n; ++i) indexes[i] = i;
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(_tablet_metadata->id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        CHECK_OK(delta_writer->open());
        CHECK_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        CHECK_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        CHECK_OK(publish_single_version(_tablet_metadata->id(), *version + 1, txn_id).status());
        ++(*version);
    }

    // Builds the chunk of one batch: every row carries every value column (a placeholder where it declares
    // nothing), its set-id interned into |dict| (the json scanner's job), and its op.
    Chunk build_chunk(const std::vector<Row>& rows, ColumnSetDict* dict, bool upper_case_names) {
        auto c0 = Int32Column::create();
        std::array<MutableColumnPtr, kNumValueColumns> values;
        for (auto& v : values) {
            v = NullableColumn::create(Int32Column::create(), NullColumn::create());
        }
        auto cset = Int16Column::create();
        auto op = Int8Column::create();
        for (const auto& row : rows) {
            c0->append(row.key);
            for (int c = 0; c < kNumValueColumns; ++c) {
                values[c]->append_datum(Datum(declares(row, c) ? row.vals[c] : kPlaceholder));
            }
            std::vector<std::string> names = row.cols;
            names.emplace_back("c0");
            if (upper_case_names) {
                for (auto& name : names) {
                    std::transform(name.begin(), name.end(), name.begin(), ::toupper);
                }
            }
            const ColumnSetId set_id = dict->intern(names);
            CHECK_NE(kInvalidColumnSetId, set_id);
            cset->append(static_cast<int16_t>(set_id));
            op->append(row.del ? TOpType::DELETE : TOpType::UPSERT);
        }
        Columns columns;
        columns.emplace_back(std::move(c0));
        for (auto& v : values) {
            columns.emplace_back(std::move(v));
        }
        columns.emplace_back(std::move(cset));
        columns.emplace_back(std::move(op));
        return Chunk(std::move(columns), _slot_cid_map);
    }

    struct LoadOptions {
        // Put the load's dictionary in the registry, as the json scanner of the load does.
        bool register_dictionary = true;
        // Intern the column names in upper case, as a `columns` header written in another case would.
        bool upper_case_names = false;
        std::string merge_condition;
        // Flush after every batch, so that every batch becomes its own segment.
        bool flush_each_batch = true;
    };

    // Writes |batches| as ONE flexible load, by default flushing after each batch so that every batch
    // becomes its own segment, and publishes it.
    Status flexible_load(const std::vector<std::vector<Row>>& batches, PartialUpdateMode mode, int64_t* version,
                         const LoadOptions& options) {
        auto txn_id = next_id();
        ColumnSetDict unregistered;
        ColumnSetDictPtr dict;
        if (options.register_dictionary) {
            dict = FlexiblePartialUpdateRegistry::instance()->retain(txn_id);
        }
        DeferOp release([&]() {
            if (options.register_dictionary) {
                FlexiblePartialUpdateRegistry::instance()->release(txn_id);
            }
        });
        ASSIGN_OR_RETURN(auto delta_writer, DeltaWriterBuilder()
                                                    .set_tablet_manager(_tablet_mgr.get())
                                                    .set_tablet_id(_tablet_metadata->id())
                                                    .set_txn_id(txn_id)
                                                    .set_partition_id(_partition_id)
                                                    .set_mem_tracker(_mem_tracker.get())
                                                    .set_schema_id(_tablet_schema->id())
                                                    .set_slot_descriptors(&_slot_pointers)
                                                    .set_merge_condition(options.merge_condition)
                                                    .set_flexible_partial_update(true)
                                                    .set_partial_update_mode(mode)
                                                    .build());
        DeferOp close([&]() { delta_writer->close(); });
        RETURN_IF_ERROR(delta_writer->open());
        for (const auto& rows : batches) {
            auto chunk = build_chunk(rows, dict != nullptr ? dict.get() : &unregistered, options.upper_case_names);
            std::vector<uint32_t> indexes(rows.size());
            for (size_t i = 0; i < rows.size(); ++i) indexes[i] = static_cast<uint32_t>(i);
            RETURN_IF_ERROR(delta_writer->write(chunk, indexes.data(), indexes.size()));
            if (options.flush_each_batch) {
                RETURN_IF_ERROR(delta_writer->flush());
            }
        }
        RETURN_IF_ERROR(delta_writer->finish_with_txnlog().status());
        RETURN_IF_ERROR(publish_single_version(_tablet_metadata->id(), *version + 1, txn_id).status());
        ++(*version);
        return Status::OK();
    }

    Status flexible_load(const std::vector<std::vector<Row>>& batches, PartialUpdateMode mode, int64_t* version) {
        return flexible_load(batches, mode, version, LoadOptions{});
    }

    Table read_table(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*_schema, 256);
        Table table;
        while (true) {
            chunk->reset();
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            for (size_t i = 0; i < chunk->num_rows(); ++i) {
                const int key = chunk->get_column_by_index(0)->get(i).get_int32();
                CHECK(table.count(key) == 0) << "duplicate key " << key;
                auto& values = table[key];
                for (int c = 0; c < kNumValueColumns; ++c) {
                    const auto datum = chunk->get_column_by_index(c + 1)->get(i);
                    // NULL is never expected: read it as the placeholder so that a mismatch names it.
                    values[c] = datum.is_null() ? kPlaceholder : datum.get_int32();
                }
            }
        }
        return table;
    }

    static void expect_table_eq(const Table& expected, const Table& actual) {
        EXPECT_EQ(expected.size(), actual.size());
        for (const auto& [key, values] : expected) {
            auto it = actual.find(key);
            ASSERT_TRUE(it != actual.end()) << "missing key " << key;
            for (int c = 0; c < kNumValueColumns; ++c) {
                EXPECT_EQ(values[c], it->second[c]) << "key " << key << " column c" << (c + 1);
            }
        }
        for (const auto& [key, values] : actual) {
            EXPECT_TRUE(expected.count(key) > 0) << "unexpected key " << key;
        }
    }

    // The delta column group files of |version|: every one of them must be an ordinary `.cols` file.
    int count_cols_files(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        int files = 0;
        for (const auto& [rssid, dcg] : metadata->dcg_meta().dcgs()) {
            for (const auto& file : dcg.column_files()) {
                EXPECT_TRUE(file.size() > 5 && file.compare(file.size() - 5, 5, ".cols") == 0) << file;
                ++files;
            }
        }
        return files;
    }

    // Rows over the base table [0, 100): different column sets, new keys, and deletes.
    static std::vector<Row> mixed_rows() {
        return {
                {5, {"c1"}, {5001, 0, 0}},
                {9, {"c2", "c3"}, {0, 9002, 9003}},
                {20, {"c1", "c3"}, {20001, 0, 20003}},
                {21, {}, {0, 0, 0}}, // declares only the key: nothing changes
                {30, {"c1", "c2", "c3"}, {30001, 30002, 30003}},
                {40, {}, {0, 0, 0}, true},
                {1000, {"c2"}, {0, 1000002, 0}}, // new key: c1 and c3 take their defaults
                {1001, {"c1", "c3"}, {1001001, 0, 1001003}},
                {1002, {}, {0, 0, 0}}, // new key declaring nothing but the key: all defaults
        };
    }

    constexpr static const char* const kTestDirectory = "test_lake_flexible_partial_update";
    constexpr static int kBaseRows = 100;

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = 4461;
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;
};

// Column mode: each column is overlaid only at the rows that declare it, new keys get the defaults of what
// they omit, and the result is stored as ordinary `.cols` files.
TEST_F(LakeFlexiblePartialUpdateTest, column_mode_applies_each_row_to_its_own_columns) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);

    const auto rows = mixed_rows();
    ASSERT_OK(flexible_load({rows}, PartialUpdateMode::COLUMN_UPDATE_MODE, &version));
    apply_to_model(rows, &model);
    expect_table_eq(model, read_table(version));
    EXPECT_GE(count_cols_files(version), 1);
}

// Row mode: the same load, applied by rewriting the full rows.
TEST_F(LakeFlexiblePartialUpdateTest, row_mode_applies_each_row_to_its_own_columns) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);

    const auto rows = mixed_rows();
    ASSERT_OK(flexible_load({rows}, PartialUpdateMode::ROW_MODE, &version));
    apply_to_model(rows, &model);
    expect_table_eq(model, read_table(version));
    EXPECT_EQ(0, count_cols_files(version));
}

// The same cases in both apply modes.
class LakeFlexiblePartialUpdateModeTest : public LakeFlexiblePartialUpdateTest,
                                          public ::testing::WithParamInterface<PartialUpdateMode> {};

INSTANTIATE_TEST_SUITE_P(FlexibleModes, LakeFlexiblePartialUpdateModeTest,
                         ::testing::Values(PartialUpdateMode::COLUMN_UPDATE_MODE, PartialUpdateMode::ROW_MODE));

// Every row declaring the same single column behaves like a plain partial update of that column.
TEST_P(LakeFlexiblePartialUpdateModeTest, homogeneous_column_set) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);
    std::vector<Row> rows;
    for (int k : {3, 50, 99}) {
        rows.push_back({k, {"c2"}, {0, k * 1000, 0}});
    }
    ASSERT_OK(flexible_load({rows}, GetParam(), &version));
    apply_to_model(rows, &model);
    expect_table_eq(model, read_table(version));
}

// The same existing key updated by two segments of one load, each declaring other columns: the later
// segment wins only for the columns it declares.
TEST_P(LakeFlexiblePartialUpdateModeTest, existing_key_in_two_segments_of_one_load) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);
    const std::vector<std::vector<Row>> batches = {
            {{7, {"c1"}, {7001, 0, 0}}, {8, {"c1", "c2"}, {8001, 8002, 0}}},
            {{7, {"c2"}, {0, 7002, 0}}, {8, {"c3"}, {0, 0, 8003}}},
    };
    ASSERT_OK(flexible_load(batches, GetParam(), &version));
    apply_to_model(batches[0], &model);
    apply_to_model(batches[1], &model);
    expect_table_eq(model, read_table(version));
}

// A new key written by two segments of one load keeps what the first one declared and the second one did
// not.
TEST_P(LakeFlexiblePartialUpdateModeTest, new_key_in_two_segments_of_one_load) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);
    const std::vector<std::vector<Row>> batches = {
            {{2000, {"c1"}, {2000001, 0, 0}}},
            {{2000, {"c3"}, {0, 0, 2000003}}},
    };
    ASSERT_OK(flexible_load(batches, GetParam(), &version));
    apply_to_model(batches[0], &model);
    apply_to_model(batches[1], &model);
    expect_table_eq(model, read_table(version));
}

// Rows of one memtable that repeat a key are merged column by column: every column takes the value of the
// last row that declares it.
TEST_P(LakeFlexiblePartialUpdateModeTest, repeated_key_in_one_memtable) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);
    std::vector<Row> rows;
    for (const auto& batch : repeated_rows_by_occurrence()) {
        rows.insert(rows.end(), batch.begin(), batch.end());
    }
    ASSERT_OK(flexible_load({rows}, GetParam(), &version));
    apply_to_model(rows, &model);
    expect_table_eq(model, read_table(version));
}

// The same rows, every occurrence of a key in its own segment.
TEST_P(LakeFlexiblePartialUpdateModeTest, repeated_key_across_segments) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);
    const auto batches = repeated_rows_by_occurrence();
    ASSERT_OK(flexible_load(batches, GetParam(), &version));
    for (const auto& batch : batches) {
        apply_to_model(batch, &model);
    }
    expect_table_eq(model, read_table(version));
}

// A memtable that fills up is flushed without the early merge of a plain load, whose aggregation would let
// the last row of a key replace the rows before it.
TEST_P(LakeFlexiblePartialUpdateModeTest, repeated_key_when_memtable_fills_up) {
    const auto saved_write_buffer_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    DeferOp restore([&]() { config::write_buffer_size = saved_write_buffer_size; });
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);
    const auto batches = repeated_rows_by_occurrence();
    LoadOptions options;
    options.flush_each_batch = false;
    ASSERT_OK(flexible_load(batches, GetParam(), &version, options));
    for (const auto& batch : batches) {
        apply_to_model(batch, &model);
    }
    expect_table_eq(model, read_table(version));
}

// Deletes among the rows of a key keep the semantics of a plain load: a key whose last row is a delete is
// deleted, and a delete followed by more rows is dropped with the rows before it.
TEST_P(LakeFlexiblePartialUpdateModeTest, repeated_key_with_deletes_in_one_memtable) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);
    const std::vector<Row> rows = {
            {12, {"c1"}, {1201, 0, 0}},  {13, {"c1"}, {1301, 0, 0}}, {14, {}, {0, 0, 0}, true},
            {15, {"c1"}, {1501, 0, 0}},  {16, {"c1"}, {1601, 0, 0}}, {2000, {"c1"}, {1, 0, 0}},
            {2001, {"c1"}, {1, 0, 0}},   {12, {}, {0, 0, 0}, true}, // deleted
            {13, {}, {0, 0, 0}, true},                              // then updated again: c1 keeps the table's value
            {14, {"c3"}, {0, 0, 1403}},                             // updated after a delete
            {15, {"c2"}, {0, 1502, 0}},                             // deleted after two updates
            {16, {}, {0, 0, 0}, true},   // then updated twice: both updates count, c1 keeps the table's value
            {2000, {}, {0, 0, 0}, true}, // a new key deleted again
            {2001, {}, {0, 0, 0}, true}, // a new key deleted, then inserted again
            {13, {"c2"}, {0, 1302, 0}},  {15, {}, {0, 0, 0}, true},  {16, {"c2"}, {0, 1602, 0}},
            {2001, {"c2"}, {0, 2, 0}},   {16, {"c3"}, {0, 0, 1603}},
    };
    ASSERT_OK(flexible_load({rows}, GetParam(), &version));
    apply_to_model(rows_kept_by_memtable(rows), &model);
    EXPECT_EQ(0, model.count(12));
    EXPECT_EQ(13 * 11, model[13][0]);
    EXPECT_EQ((std::array<int, kNumValueColumns>{16 * 11, 1602, 1603}), model[16]);
    EXPECT_EQ(0, model.count(2000));
    EXPECT_EQ(kDefaults[0], model[2001][0]);
    expect_table_eq(model, read_table(version));
}

// A flush holding only deletes leaves a segment without rows; the publish must still succeed and apply
// the other segments.
TEST_P(LakeFlexiblePartialUpdateModeTest, delete_only_segment) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);
    const std::vector<std::vector<Row>> batches = {
            {{10, {}, {0, 0, 0}, true}, {11, {}, {0, 0, 0}, true}},
            {{12, {"c2"}, {0, 12002, 0}}, {3000, {"c1"}, {3000001, 0, 0}}},
    };
    ASSERT_OK(flexible_load(batches, GetParam(), &version));
    apply_to_model(batches[0], &model);
    apply_to_model(batches[1], &model);
    expect_table_eq(model, read_table(version));
}

// The dictionary names the load's source columns, which FE matched to the table's columns ignoring case.
TEST_P(LakeFlexiblePartialUpdateModeTest, column_set_names_match_ignoring_case) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);
    const auto rows = mixed_rows();
    LoadOptions options;
    options.upper_case_names = true;
    ASSERT_OK(flexible_load({rows}, GetParam(), &version, options));
    apply_to_model(rows, &model);
    expect_table_eq(model, read_table(version));
}

// Rows carrying set-ids without the dictionary to decode them cannot be applied: as a plain partial update
// they would overwrite every column a row did not declare. The load fails instead.
TEST_P(LakeFlexiblePartialUpdateModeTest, missing_dictionary_fails_the_load) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);
    LoadOptions options;
    options.register_dictionary = false;
    auto st = flexible_load({mixed_rows()}, GetParam(), &version, options);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.message().find("column-set dictionary") != std::string::npos) << st;
    EXPECT_EQ(2, version);
    expect_table_eq(model, read_table(version));
}

// There is no flexible-aware conditional apply.
TEST_F(LakeFlexiblePartialUpdateTest, merge_condition_is_rejected) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);
    LoadOptions options;
    options.merge_condition = "c1";
    auto st = flexible_load({mixed_rows()}, PartialUpdateMode::COLUMN_UPDATE_MODE, &version, options);
    ASSERT_TRUE(st.is_not_supported()) << st;
    expect_table_eq(model, read_table(version));
}

// Every flexible load writes a `.cols` file covering all the columns it loads, which supersedes the one
// of the previous load, so the cells a load does not declare must carry the values of the earlier loads.
// A compaction then folds the `.cols` file into the base without changing a value.
TEST_F(LakeFlexiblePartialUpdateTest, column_mode_layers_survive_compaction) {
    int64_t version = 1;
    Table model;
    write_base(kBaseRows, &version, &model);
    // Each load declares a single other column for the same keys.
    for (int round = 0; round < 3; ++round) {
        const std::string col = "c" + std::to_string(round + 1);
        std::array<int, kNumValueColumns> vals{};
        vals[round] = 15000 + round;
        std::vector<Row> rows = {{15, {col}, vals}, {16, {}, {}}, {4000 + round, {col}, vals}};
        vals[round] = 17000 + round;
        rows.push_back({17 + round, {col}, vals});
        ASSERT_OK(flexible_load({rows}, PartialUpdateMode::COLUMN_UPDATE_MODE, &version));
        apply_to_model(rows, &model);
        expect_table_eq(model, read_table(version));
    }
    EXPECT_GE(count_cols_files(version), 1);

    auto txn_id = next_id();
    auto task_context =
            std::make_unique<CompactionTaskContext>(txn_id, _tablet_metadata->id(), version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id(), version));
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get(), tablet.get_rowsets()));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    ++version;

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    EXPECT_EQ(1, metadata->rowsets_size());
    EXPECT_EQ(0, count_cols_files(version));
    expect_table_eq(model, read_table(version));
}

} // namespace starrocks::lake
