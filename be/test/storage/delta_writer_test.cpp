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

#include "storage/delta_writer.h"

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/txn_manager.h"

namespace starrocks {

TEST(DeltaWriterTest, test_partial_update_sort_key_conflict_check) {
    {
        // case-1. Row mode
        std::vector<int32_t> referenced_column_ids = {0, 1, 2, 3, 4};
        std::vector<ColumnId> sort_key_idxes = {0, 1, 2};
        size_t num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::ROW_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::ROW_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_TRUE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::ROW_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
    }
    {
        // case-2. Column update mode
        std::vector<int32_t> referenced_column_ids = {0, 1, 2, 3, 4};
        std::vector<ColumnId> sort_key_idxes = {0, 1, 2};
        size_t num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPDATE_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_TRUE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPDATE_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {1, 2};
        num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPDATE_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPDATE_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
    }

    {
        // case-3. Column update mode with upsert
        std::vector<int32_t> referenced_column_ids = {0, 1, 2, 3, 4};
        std::vector<ColumnId> sort_key_idxes = {0, 1, 2};
        size_t num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPSERT_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_TRUE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPSERT_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {1, 2};
        num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPSERT_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_TRUE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPSERT_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {0, 1};
        num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::COLUMN_UPSERT_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
    }

    {
        // case-4. auto mode & unknow mode
        std::vector<int32_t> referenced_column_ids = {0, 1, 2, 3, 4};
        std::vector<ColumnId> sort_key_idxes = {0, 1, 2};
        size_t num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::AUTO_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::UNKNOWN_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 3, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::AUTO_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
        ASSERT_FALSE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::UNKNOWN_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));

        referenced_column_ids = {0, 1, 2, 4};
        sort_key_idxes = {2, 3};
        num_key_columns = 3;
        ASSERT_TRUE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::AUTO_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
        ASSERT_TRUE(DeltaWriter::is_partial_update_with_sort_key_conflict(
                PartialUpdateMode::UNKNOWN_MODE, referenced_column_ids, sort_key_idxes, num_key_columns));
    }
}

class DeltaWriterMergeConditionTest : public testing::Test {
public:
    void SetUp() override {
        TCreateTabletReq request;
        request.tablet_id = kTabletId;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = kSchemaHash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;
        for (size_t i = 0; i < kColumnNames.size(); i++) {
            TColumn column;
            column.column_name = kColumnNames[i];
            column.__set_is_key(i == 0);
            column.column_type.type = TPrimitiveType::BIGINT;
            request.tablet_schema.columns.emplace_back(column);
        }
        auto st = StorageEngine::instance()->create_tablet(request);
        ASSERT_TRUE(st.ok()) << st;
        _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(kTabletId, false);
        ASSERT_TRUE(_tablet != nullptr);
    }

    void TearDown() override {
        if (_tablet != nullptr) {
            (void)StorageEngine::instance()->tablet_manager()->drop_tablet(kTabletId);
            _tablet.reset();
        }
    }

protected:
    // Slots for the first `num_columns` columns of the tablet, in schema order.
    const std::vector<SlotDescriptor*>& create_slots(size_t num_columns) {
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;
        for (size_t i = 0; i < num_columns; i++) {
            TSlotDescriptorBuilder slot_builder;
            tuple_builder.add_slot(slot_builder.type(LogicalType::TYPE_BIGINT)
                                           .column_name(kColumnNames[i])
                                           .column_pos(static_cast<int>(i))
                                           .nullable(false)
                                           .build());
        }
        tuple_builder.build(&table_builder);
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, /*chunk_size=*/4096).ok());
        return tbl->get_tuple_descriptor(0)->slots();
    }

    StatusOr<std::unique_ptr<DeltaWriter>> open_writer(int64_t txn_id, const std::vector<SlotDescriptor*>& slots,
                                                       PartialUpdateMode mode, const std::string& merge_condition) {
        DeltaWriterOptions options{};
        options.tablet_id = kTabletId;
        options.schema_hash = kSchemaHash;
        options.txn_id = txn_id;
        options.partition_id = kPartitionId;
        options.slots = &slots;
        options.replica_state = ReplicaState::Primary;
        options.partial_update_mode = mode;
        options.merge_condition = merge_condition;
        return DeltaWriter::open(options, &_mem_tracker);
    }

    bool has_txn(int64_t txn_id) {
        return StorageEngine::instance()->txn_manager()->has_txn(kPartitionId, txn_id, kTabletId,
                                                                 _tablet->schema_hash(), _tablet->tablet_uid());
    }

    static constexpr int64_t kTabletId = 20321;
    static constexpr int32_t kSchemaHash = 20321;
    static constexpr int64_t kPartitionId = 20321;
    inline static const std::vector<std::string> kColumnNames = {"k", "v1", "v2"};

    TabletSharedPtr _tablet;
    MemTracker _mem_tracker;
    RuntimeState _runtime_state;
    ObjectPool _pool;
};

TEST_F(DeltaWriterMergeConditionTest, test_column_mode_rejects_merge_condition) {
    const auto& partial_slots = create_slots(2);
    const auto& all_slots = create_slots(3);

    // Applying a column-mode partial update never evaluates merge_condition, so the writer must refuse the
    // combination when it opens, and roll back the transaction it has already prepared.
    int64_t txn_id = 1;
    for (auto mode : {PartialUpdateMode::COLUMN_UPSERT_MODE, PartialUpdateMode::COLUMN_UPDATE_MODE}) {
        auto writer = open_writer(txn_id, partial_slots, mode, "v1");
        ASSERT_TRUE(writer.status().is_not_supported()) << writer.status();
        ASSERT_FALSE(has_txn(txn_id));
        txn_id++;
    }

    // Row mode evaluates merge_condition when it applies the update.
    auto row_mode = open_writer(txn_id++, partial_slots, PartialUpdateMode::ROW_MODE, "v1");
    ASSERT_TRUE(row_mode.ok()) << row_mode.status();
    // Column mode without merge_condition has no condition to drop.
    auto no_condition = open_writer(txn_id++, partial_slots, PartialUpdateMode::COLUMN_UPSERT_MODE, "");
    ASSERT_TRUE(no_condition.ok()) << no_condition.status();
    // A load that writes every column is a full upsert, which evaluates merge_condition.
    auto all_columns = open_writer(txn_id++, all_slots, PartialUpdateMode::COLUMN_UPSERT_MODE, "v1");
    ASSERT_TRUE(all_columns.ok()) << all_columns.status();
}

} // namespace starrocks