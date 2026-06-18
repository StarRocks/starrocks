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

#include <limits>

#include "column/chunk.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/type_traits.h"
#include "exec/range_tablet_sink_sender.h"
#include "exec/tablet_info.h"
#include "gen_cpp/descriptors.pb.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "testutil/assert.h"

namespace starrocks {

// Sender-only invariant: per-index distributed_exprs routing must happen at the sink
// sender, never on a remote add-chunks BE. The proto POlapTableIndexSchema therefore must
// NOT carry a distributed_exprs field, so a remote delta-writer BE has no way to route by
// per-index keys. This is a compile-time guard against accidentally adding such a field.
template <typename T, typename = void>
struct HasDistributedExprsField : std::false_type {};
template <typename T>
struct HasDistributedExprsField<T, std::void_t<decltype(std::declval<T>().distributed_exprs_size())>> : std::true_type {
};
static_assert(!HasDistributedExprsField<POlapTableIndexSchema>::value,
              "proto POlapTableIndexSchema must not carry distributed_exprs: per-index range routing is sender-only");

class TestRangeTabletSinkSender : public RangeTabletSinkSender {
public:
    TestRangeTabletSinkSender(PUniqueId load_id, int64_t txn_id, IndexIdToTabletBEMap index_id_to_tablet_be_map,
                              OlapTablePartitionParam* partition_params, std::vector<IndexChannel*> channels,
                              std::unordered_map<int64_t, NodeChannel*> node_channels,
                              std::vector<ExprContext*> output_expr_ctxs, bool enable_replicated_storage,
                              TWriteQuorumType::type write_quorum_type, int num_repicas)
            : RangeTabletSinkSender(std::move(load_id), txn_id, index_id_to_tablet_be_map, partition_params,
                                    std::move(channels), std::move(node_channels), std::move(output_expr_ctxs),
                                    enable_replicated_storage, write_quorum_type, num_repicas),
              _index_id_to_tablet_be_map_for_test(index_id_to_tablet_be_map),
              _enable_replicated_storage_for_test(enable_replicated_storage) {}

    // Override to capture routing info instead of real RPC.
    Status _send_chunk_by_node(Chunk* /*chunk*/, IndexChannel* channel,
                               const std::vector<uint16_t>& selection_idx) override {
        SentBatch batch;
        batch.index_id = channel->index_id();
        batch.selection_idx = selection_idx;
        // Snapshot current tablet_ids for selected rows.
        for (auto idx : selection_idx) {
            batch.tablet_ids.push_back(_tablet_ids[idx]);
        }
        _sent_batches.push_back(std::move(batch));

        // Additionally simulate BE-level routing based on IndexIdToTabletBEMap.
        auto it = _index_id_to_tablet_be_map_for_test.find(batch.index_id);
        if (it != _index_id_to_tablet_be_map_for_test.end()) {
            const auto& tablet_to_bes = it->second;
            for (auto idx : selection_idx) {
                auto tablet_it = tablet_to_bes.find(_tablet_ids[idx]);
                if (tablet_it == tablet_to_bes.end()) {
                    continue;
                }
                const auto& be_ids = tablet_it->second;
                if (_enable_replicated_storage_for_test) {
                    if (!be_ids.empty()) {
                        int64_t primary_be = be_ids[0];
                        _be_routing[batch.index_id][primary_be].push_back(idx);
                    }
                } else {
                    for (int64_t be_id : be_ids) {
                        _be_routing[batch.index_id][be_id].push_back(idx);
                    }
                }
            }
        }
        return Status::OK();
    }

    struct SentBatch {
        int64_t index_id;
        std::vector<uint16_t> selection_idx;
        std::vector<int64_t> tablet_ids;
    };

    const std::vector<SentBatch>& sent_batches() const { return _sent_batches; }

    // index_id -> (be_id -> row indices)
    const std::unordered_map<int64_t, std::unordered_map<int64_t, std::vector<uint16_t>>>& be_routing() const {
        return _be_routing;
    }

private:
    std::vector<SentBatch> _sent_batches;
    IndexIdToTabletBEMap _index_id_to_tablet_be_map_for_test;
    bool _enable_replicated_storage_for_test;
    std::unordered_map<int64_t, std::unordered_map<int64_t, std::vector<uint16_t>>> _be_routing;
};

class TabletSinkSenderRangeTest : public testing::Test {
public:
    void SetUp() override {
        _object_pool = std::make_unique<ObjectPool>();
        _schema_param = _object_pool->add(new OlapTableSchemaParam());
        _index_id = 100;
        _table_id = 10;
        _db_id = 1;
    }

    void TearDown() override {}

protected:
    TTabletRange _make_range(int64_t lower, bool lower_included, int64_t upper, bool upper_included) {
        TTabletRange range;

        TTuple lower_tuple;
        TVariant lower_var;
        lower_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        lower_var.__set_value(std::to_string(lower));
        lower_tuple.__set_values({lower_var});
        range.__set_lower_bound(lower_tuple);
        range.__set_lower_bound_included(lower_included);

        TTuple upper_tuple;
        TVariant upper_var;
        upper_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        upper_var.__set_value(std::to_string(upper));
        upper_tuple.__set_values({upper_var});
        range.__set_upper_bound(upper_tuple);
        range.__set_upper_bound_included(upper_included);

        return range;
    }

    void _init_schema_param(int num_indexes = 1) {
        TOlapTableSchemaParam tschema;
        tschema.db_id = _db_id;
        tschema.table_id = _table_id;
        tschema.version = 0;

        // Tuple descriptor
        TTupleDescriptor tuple_desc;
        tuple_desc.id = 0;
        tuple_desc.byteSize = 16;
        tuple_desc.numNullBytes = 0;
        tuple_desc.tableId = _table_id;
        tschema.tuple_desc = tuple_desc;

        // Slot descriptors
        TSlotDescriptor slot1;
        slot1.id = 0;
        slot1.parent = 0;
        slot1.colName = "c1";
        slot1.slotType.types.emplace_back();
        slot1.slotType.types.back().__set_type(TTypeNodeType::SCALAR);
        slot1.slotType.types.back().__set_scalar_type(TScalarType());
        slot1.slotType.types.back().scalar_type.__set_type(TPrimitiveType::BIGINT);
        tschema.slot_descs.push_back(slot1);

        for (int i = 0; i < num_indexes; ++i) {
            // Index schema
            TOlapTableIndexSchema index_schema;
            index_schema.id = _index_id + i;
            index_schema.columns = {"c1"};
            index_schema.schema_hash = 0;

            TOlapTableColumnParam column_param;
            TColumn tcol;
            tcol.column_name = "c1";
            tcol.column_type.type = TPrimitiveType::BIGINT;
            tcol.is_key = true;
            tcol.is_allow_null = false;
            tcol.col_unique_id = 0;
            column_param.columns.push_back(tcol);
            column_param.sort_key_uid.push_back(0); // Set sort key

            // Use thrift setter to correctly populate __isset.column_param,
            // so BE side `OlapTableSchemaParam::init` will see column_param.
            index_schema.__set_column_param(column_param);
            tschema.indexes.push_back(index_schema);
        }

        ASSERT_OK(_schema_param->init(tschema));
    }

    void _init_schema_param_two_columns() {
        TOlapTableSchemaParam tschema;
        tschema.db_id = _db_id;
        tschema.table_id = _table_id;
        tschema.version = 0;

        // Tuple descriptor
        TTupleDescriptor tuple_desc;
        tuple_desc.id = 0;
        tuple_desc.byteSize = 32;
        tuple_desc.numNullBytes = 0;
        tuple_desc.tableId = _table_id;
        tschema.tuple_desc = tuple_desc;

        // Slot descriptor for c1 BIGINT
        TSlotDescriptor slot1;
        slot1.id = 0;
        slot1.parent = 0;
        slot1.colName = "c1";
        slot1.slotType.types.emplace_back();
        slot1.slotType.types.back().__set_type(TTypeNodeType::SCALAR);
        slot1.slotType.types.back().__set_scalar_type(TScalarType());
        slot1.slotType.types.back().scalar_type.__set_type(TPrimitiveType::BIGINT);
        tschema.slot_descs.push_back(slot1);

        // Slot descriptor for c2 BIGINT
        TSlotDescriptor slot2;
        slot2.id = 1;
        slot2.parent = 0;
        slot2.colName = "c2";
        slot2.slotType.types.emplace_back();
        slot2.slotType.types.back().__set_type(TTypeNodeType::SCALAR);
        slot2.slotType.types.back().__set_scalar_type(TScalarType());
        slot2.slotType.types.back().scalar_type.__set_type(TPrimitiveType::BIGINT);
        tschema.slot_descs.push_back(slot2);

        // Single index using (c1,c2)
        TOlapTableIndexSchema index_schema;
        index_schema.id = _index_id;
        index_schema.columns = {"c1", "c2"};
        index_schema.schema_hash = 0;

        TOlapTableColumnParam column_param;
        {
            TColumn tcol1;
            tcol1.column_name = "c1";
            tcol1.column_type.type = TPrimitiveType::BIGINT;
            tcol1.is_key = true;
            tcol1.is_allow_null = false;
            tcol1.col_unique_id = 0;
            column_param.columns.push_back(tcol1);
            column_param.sort_key_uid.push_back(0);
        }
        {
            TColumn tcol2;
            tcol2.column_name = "c2";
            tcol2.column_type.type = TPrimitiveType::BIGINT;
            tcol2.is_key = true;
            tcol2.is_allow_null = false;
            tcol2.col_unique_id = 1;
            column_param.columns.push_back(tcol2);
            column_param.sort_key_uid.push_back(1);
        }

        index_schema.__set_column_param(column_param);
        tschema.indexes.push_back(index_schema);

        ASSERT_OK(_schema_param->init(tschema));
    }

    OlapTablePartitionParam* _create_partition_param(const std::vector<std::vector<TTabletRange>>& index_ranges) {
        TOlapTablePartitionParam tpartition_param;
        tpartition_param.db_id = _db_id;
        tpartition_param.table_id = _table_id;
        tpartition_param.version = 0;
        tpartition_param.__set_distributed_columns({"c1"});
        tpartition_param.__set_distribution_type(TOlapTableDistributionType::RANGE);

        TOlapTablePartition tpartition;
        tpartition.id = 1000;

        for (size_t i = 0; i < index_ranges.size(); ++i) {
            TOlapTableIndexTablets index_tablets;
            index_tablets.index_id = _index_id + i;
            const auto& ranges = index_ranges[i];

            for (size_t j = 0; j < ranges.size(); ++j) {
                int64_t tablet_id = 10000 + i * 100 + j;
                index_tablets.tablet_ids.push_back(tablet_id);

                TOlapTableTablet tablet;
                tablet.id = tablet_id;
                // Use thrift setter to correctly populate __isset.range.
                tablet.__set_range(ranges[j]);
                index_tablets.tablets.push_back(tablet);
            }
            tpartition.indexes.push_back(index_tablets);
        }

        tpartition_param.partitions.push_back(tpartition);

        // Hack for ObjectPool lifecycle
        return _object_pool->add(new OlapTablePartitionParam(
                std::shared_ptr<OlapTableSchemaParam>(_schema_param, [](auto*) {}), tpartition_param));
    }

    ChunkPtr _create_chunk(const std::vector<int64_t>& values) {
        auto col = FixedLengthColumn<int64_t>::create();
        for (int64_t v : values) {
            col->append(v);
        }

        Columns cols;
        cols.emplace_back(col);

        // Mock Schema
        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, "c1", get_type_info(TYPE_BIGINT), false));
        auto schema = std::make_shared<Schema>(fields);

        auto chunk = std::make_shared<Chunk>(std::move(cols), std::move(schema));
        // Set slot_id to column index mapping
        chunk->set_slot_id_to_index(0, 0); // slot_id 0 -> column index 0
        return chunk;
    }

    ChunkPtr _create_two_column_chunk(const std::vector<int64_t>& c1_values, const std::vector<int64_t>& c2_values) {
        DCHECK_EQ(c1_values.size(), c2_values.size());
        auto c1 = FixedLengthColumn<int64_t>::create();
        auto c2 = FixedLengthColumn<int64_t>::create();
        for (size_t i = 0; i < c1_values.size(); ++i) {
            c1->append(c1_values[i]);
            c2->append(c2_values[i]);
        }

        Columns cols;
        cols.emplace_back(c1);
        cols.emplace_back(c2);

        Fields fields;
        fields.emplace_back(std::make_shared<Field>(0, "c1", get_type_info(TYPE_BIGINT), false));
        fields.emplace_back(std::make_shared<Field>(1, "c2", get_type_info(TYPE_BIGINT), false));
        auto schema = std::make_shared<Schema>(fields);

        auto chunk = std::make_shared<Chunk>(std::move(cols), std::move(schema));
        // Set slot_id to column index mapping
        chunk->set_slot_id_to_index(0, 0); // slot_id 0 -> column index 0
        chunk->set_slot_id_to_index(1, 1); // slot_id 1 -> column index 1
        return chunk;
    }

    // Build a BIGINT slot-ref TExpr referencing the given slot of tuple 0.
    static TExpr _make_slot_ref_texpr(int slot_id) {
        TExpr texpr;
        TExprNode node;
        node.node_type = TExprNodeType::SLOT_REF;
        node.type = gen_type_desc(TPrimitiveType::BIGINT);
        node.num_children = 0;
        TSlotRef t_slot_ref;
        t_slot_ref.slot_id = slot_id;
        t_slot_ref.tuple_id = 0;
        node.__set_slot_ref(t_slot_ref);
        node.is_nullable = false;
        texpr.nodes.emplace_back(node);
        return texpr;
    }

    RuntimeState* _make_runtime_state() {
        TUniqueId query_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        TUniqueId fragment_id;
        auto* exec_env = ExecEnv::GetInstance();
        auto* state =
                _object_pool->add(new RuntimeState(query_id, fragment_id, query_options, query_globals, exec_env));
        state->init_mem_trackers(query_id);
        return state;
    }

    std::unique_ptr<ObjectPool> _object_pool;
    OlapTableSchemaParam* _schema_param = nullptr;
    int64_t _index_id;
    int64_t _table_id;
    int64_t _db_id;
};

// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, NormalRouting) {
    _init_schema_param();

    // Use full-coverage ranges to satisfy RangeRouter invariants while keeping
    // the same routing semantics for tested values:
    //   R0: (-inf, 10)   -> 10000
    //   R1: [10, +inf)   -> 10001
    std::vector<TTabletRange> ranges;
    {
        // R0: (-inf, 10)
        TTabletRange r0;
        TTuple upper_tuple;
        TVariant upper_var;
        upper_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        upper_var.__set_value(std::to_string(10));
        upper_tuple.__set_values({upper_var});
        r0.__set_upper_bound(upper_tuple);
        r0.__set_upper_bound_included(false);
        ranges.push_back(r0);
    }
    {
        // R1: [10, +inf)
        TTabletRange r1;
        TTuple lower_tuple;
        TVariant lower_var;
        lower_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        lower_var.__set_value(std::to_string(10));
        lower_tuple.__set_values({lower_var});
        r1.__set_lower_bound(lower_tuple);
        r1.__set_lower_bound_included(true);
        ranges.push_back(r1);
    }

    auto* partition_param = _create_partition_param({ranges});
    ASSERT_OK(partition_param->init(nullptr));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    index_id_to_tablet_be_map[_index_id] = {};

    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    auto chunk = _create_chunk({5, 15, 0, 19});

    std::vector<OlapTablePartition*> partitions;
    auto* part = partition_param->get_partitions().begin()->second;
    partitions.resize(4, part);

    std::vector<uint16_t> validate_select_idx = {0, 1, 2, 3};
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_OK(st);

    const auto& batches = sender.sent_batches();
    ASSERT_EQ(1, batches.size());
    const auto& tablet_ids = batches[0].tablet_ids;

    ASSERT_EQ(4, tablet_ids.size());
    EXPECT_EQ(10000, tablet_ids[0]);
    EXPECT_EQ(10001, tablet_ids[1]);
    EXPECT_EQ(10000, tablet_ids[2]);
    EXPECT_EQ(10001, tablet_ids[3]);

    // index_id_partition_id should record the single partition for this index.
    ASSERT_EQ(1, index_id_partition_id[_index_id].size());
    EXPECT_EQ(1000, *index_id_partition_id[_index_id].begin());
}

// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, MultiIndexRouting) {
    _init_schema_param(2); // Two indexes

    // Index 1 (100):
    //   R0: (-inf, 10)   -> 10000
    //   R1: [10, +inf)   -> 10001
    // Index 2 (101):
    //   R0: (-inf, 5)    -> 10100
    //   R1: [5, +inf)    -> 10101
    std::vector<TTabletRange> ranges1;
    {
        // Index 1, R0: (-inf, 10)
        TTabletRange r0;
        TTuple upper_tuple;
        TVariant upper_var;
        upper_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        upper_var.__set_value(std::to_string(10));
        upper_tuple.__set_values({upper_var});
        r0.__set_upper_bound(upper_tuple);
        r0.__set_upper_bound_included(false);
        ranges1.push_back(r0);
    }
    {
        // Index 1, R1: [10, +inf)
        TTabletRange r1;
        TTuple lower_tuple;
        TVariant lower_var;
        lower_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        lower_var.__set_value(std::to_string(10));
        lower_tuple.__set_values({lower_var});
        r1.__set_lower_bound(lower_tuple);
        r1.__set_lower_bound_included(true);
        ranges1.push_back(r1);
    }

    std::vector<TTabletRange> ranges2;
    {
        // Index 2, R0: (-inf, 5)
        TTabletRange r0;
        TTuple upper_tuple;
        TVariant upper_var;
        upper_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        upper_var.__set_value(std::to_string(5));
        upper_tuple.__set_values({upper_var});
        r0.__set_upper_bound(upper_tuple);
        r0.__set_upper_bound_included(false);
        ranges2.push_back(r0);
    }
    {
        // Index 2, R1: [5, +inf)
        TTabletRange r1;
        TTuple lower_tuple;
        TVariant lower_var;
        lower_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        lower_var.__set_value(std::to_string(5));
        lower_tuple.__set_values({lower_var});
        r1.__set_lower_bound(lower_tuple);
        r1.__set_lower_bound_included(true);
        ranges2.push_back(r1);
    }

    auto* partition_param = _create_partition_param({ranges1, ranges2});
    ASSERT_OK(partition_param->init(nullptr));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    index_id_to_tablet_be_map[_index_id] = {};
    index_id_to_tablet_be_map[_index_id + 1] = {};

    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id + 1, nullptr)));

    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    // Data: 2, 7, 15
    auto chunk = _create_chunk({2, 7, 15});
    std::vector<OlapTablePartition*> partitions;
    auto* part = partition_param->get_partitions().begin()->second;
    partitions.resize(3, part);
    std::vector<uint16_t> validate_select_idx = {0, 1, 2};
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_OK(st);

    const auto& batches = sender.sent_batches();
    ASSERT_EQ(2, batches.size());

    // Check Index 1 (Batch 0)
    // 2 -> [0, 10) -> 10000
    // 7 -> [0, 10) -> 10000
    // 15 -> [10, 20) -> 10001
    EXPECT_EQ(_index_id, batches[0].index_id);
    EXPECT_EQ(10000, batches[0].tablet_ids[0]);
    EXPECT_EQ(10000, batches[0].tablet_ids[1]);
    EXPECT_EQ(10001, batches[0].tablet_ids[2]);

    // Check Index 2 (Batch 1)
    // 2 -> [0, 5) -> 10100
    // 7 -> [5, 20) -> 10101
    // 15 -> [5, 20) -> 10101
    EXPECT_EQ(_index_id + 1, batches[1].index_id);
    EXPECT_EQ(10100, batches[1].tablet_ids[0]);
    EXPECT_EQ(10101, batches[1].tablet_ids[1]);
    EXPECT_EQ(10101, batches[1].tablet_ids[2]);

    // Both indexes should record the same partition id.
    ASSERT_EQ(1, index_id_partition_id[_index_id].size());
    ASSERT_EQ(1, index_id_partition_id[_index_id + 1].size());
    EXPECT_EQ(1000, *index_id_partition_id[_index_id].begin());
    EXPECT_EQ(1000, *index_id_partition_id[_index_id + 1].begin());
}

// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, GapRangeWithPartialMatchAndError) {
    _init_schema_param();

    // This test verifies that misconfigured ranges (missing -inf/+inf coverage)
    // are rejected when initializing RangeRouter inside RangeTabletSinkSender.
    // Tablet 1: [0, 5)
    // Tablet 2: [10, 20)
    std::vector<TTabletRange> ranges;
    ranges.push_back(_make_range(0, true, 5, false));
    ranges.push_back(_make_range(10, true, 20, false));

    auto* partition_param = _create_partition_param({ranges});
    ASSERT_OK(partition_param->init(nullptr));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    index_id_to_tablet_be_map[_index_id] = {};
    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    // 2 and 12 have matching ranges; 7 falls into the gap [5,10) logically,
    // but with the new RangeRouter invariants, such a configuration is treated
    // as invalid at initialization time.
    auto chunk = _create_chunk({2, 7, 12});

    std::vector<OlapTablePartition*> partitions;
    auto* part = partition_param->get_partitions().begin()->second;
    partitions.resize(3, part);
    std::vector<uint16_t> validate_select_idx = {0, 1, 2};
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.message().find("lower_inf_count and upper_inf_count must be 1") != std::string::npos);
}

// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, MultiColumnRangeRouting) {
    // Two range columns (c1, c2), both BIGINT
    _init_schema_param_two_columns();

    // Partition 1000, index 100:
    // Use two ranges that fully cover the space lexicographically while
    // preserving the expected routing for tested points:
    //   R0: (-inf, -inf) .. (3, MAX)   -> 10000
    //   R1: (3, MAX) .. (+inf, +inf)   -> 10001
    TOlapTablePartitionParam tpartition_param;
    tpartition_param.db_id = _db_id;
    tpartition_param.table_id = _table_id;
    tpartition_param.version = 0;
    tpartition_param.__set_distributed_columns({"c1", "c2"});
    tpartition_param.__set_distribution_type(TOlapTableDistributionType::RANGE);

    TOlapTablePartition tpartition;
    tpartition.id = 1000;

    TOlapTableIndexTablets index_tablets;
    index_tablets.index_id = _index_id;

    // Helper to build a 2-column BIGINT tuple.
    auto make_two_bigint_tuple = [](int64_t v1, int64_t v2) {
        TVariant var1;
        var1.__set_type(TYPE_BIGINT_DESC.to_thrift());
        var1.__set_value(std::to_string(v1));
        TVariant var2;
        var2.__set_type(TYPE_BIGINT_DESC.to_thrift());
        var2.__set_value(std::to_string(v2));
        TTuple t;
        t.__set_values(std::vector<TVariant>{var1, var2});
        return t;
    };

    // Tablet 10000: (-inf, -inf) .. (3, MAX]
    {
        int64_t tablet_id = 10000;
        index_tablets.tablet_ids.push_back(tablet_id);

        TTabletRange range;
        // upper bound (3, MAX), no lower bound => (-inf, -inf)..(3, MAX)
        int64_t max_v = std::numeric_limits<int64_t>::max();
        range.__set_upper_bound(make_two_bigint_tuple(3, max_v));
        range.__set_upper_bound_included(false);

        TOlapTableTablet tablet;
        tablet.id = tablet_id;
        tablet.__set_range(range);
        index_tablets.tablets.push_back(tablet);
    }

    // Tablet 10001: [3, MAX] .. (+inf, +inf)
    {
        int64_t tablet_id = 10001;
        index_tablets.tablet_ids.push_back(tablet_id);

        TTabletRange range;
        int64_t max_v = std::numeric_limits<int64_t>::max();
        range.__set_lower_bound(make_two_bigint_tuple(3, max_v));
        range.__set_lower_bound_included(true);

        TOlapTableTablet tablet;
        tablet.id = tablet_id;
        tablet.__set_range(range);
        index_tablets.tablets.push_back(tablet);
    }

    tpartition.indexes.push_back(index_tablets);
    tpartition_param.partitions.push_back(tpartition);

    auto* partition_param = _object_pool->add(new OlapTablePartitionParam(
            std::shared_ptr<OlapTableSchemaParam>(_schema_param, [](auto*) {}), tpartition_param));
    ASSERT_OK(partition_param->init(nullptr));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    index_id_to_tablet_be_map[_index_id] = {};
    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    // (c1,c2) rows:
    // idx 0: (2,10)  -> tablet 10000
    // idx 1: (2,50)  -> tablet 10000
    // idx 2: (3,10)  -> tablet 10000
    // idx 3: (3,60)  -> tablet 10000
    // idx 4: (4,10)  -> tablet 10001
    // idx 5: (4,50)  -> tablet 10001
    auto chunk = _create_two_column_chunk({2, 2, 3, 3, 4, 4},        // c1
                                          {10, 50, 10, 60, 10, 50}); // c2

    std::vector<OlapTablePartition*> partitions;
    auto* part = partition_param->get_partitions().begin()->second;
    partitions.resize(chunk->num_rows(), part);

    std::vector<uint16_t> validate_select_idx = {0, 1, 2, 3, 4, 5};
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_OK(st);

    const auto& batches = sender.sent_batches();
    ASSERT_EQ(1, batches.size());
    const auto& tablet_ids = batches[0].tablet_ids;
    ASSERT_EQ(6, tablet_ids.size());

    // First four rows should go to tablet 10000, last two to 10001.
    EXPECT_EQ(10000, tablet_ids[0]);
    EXPECT_EQ(10000, tablet_ids[1]);
    EXPECT_EQ(10000, tablet_ids[2]);
    EXPECT_EQ(10000, tablet_ids[3]);
    EXPECT_EQ(10001, tablet_ids[4]);
    EXPECT_EQ(10001, tablet_ids[5]);

    ASSERT_EQ(1, index_id_partition_id[_index_id].size());
    EXPECT_EQ(1000, *index_id_partition_id[_index_id].begin());
}

// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, MultiPartitionRoutingSingleIndex) {
    _init_schema_param();

    // Build partition param with two partitions, each having a single
    // (-inf, +inf) range. Partition selection is done outside RangeRouter.
    TOlapTablePartitionParam tpartition_param;
    tpartition_param.db_id = _db_id;
    tpartition_param.table_id = _table_id;
    tpartition_param.version = 0;
    tpartition_param.__set_distributed_columns({"c1"});
    tpartition_param.__set_distribution_type(TOlapTableDistributionType::RANGE);

    // Partition 1000
    {
        TOlapTablePartition tpartition;
        tpartition.id = 1000;

        TOlapTableIndexTablets index_tablets;
        index_tablets.index_id = _index_id;

        // (-inf,+inf) -> tablet 10000
        int64_t tablet_id = 10000;
        index_tablets.tablet_ids.push_back(tablet_id);
        TOlapTableTablet tablet;
        tablet.id = tablet_id;
        // No lower/upper bound in the range => (-inf, +inf)
        {
            TTabletRange full_range;
            tablet.__set_range(full_range);
        }
        index_tablets.tablets.push_back(tablet);

        tpartition.indexes.push_back(index_tablets);
        tpartition_param.partitions.push_back(tpartition);
    }

    // Partition 1001
    {
        TOlapTablePartition tpartition;
        tpartition.id = 1001;

        TOlapTableIndexTablets index_tablets;
        index_tablets.index_id = _index_id;

        // (-inf,+inf) -> tablet 10001
        int64_t tablet_id = 10001;
        index_tablets.tablet_ids.push_back(tablet_id);
        TOlapTableTablet tablet;
        tablet.id = tablet_id;
        // No lower/upper bound in the range => (-inf, +inf)
        {
            TTabletRange full_range;
            tablet.__set_range(full_range);
        }
        index_tablets.tablets.push_back(tablet);

        tpartition.indexes.push_back(index_tablets);
        tpartition_param.partitions.push_back(tpartition);
    }

    auto* partition_param = _object_pool->add(new OlapTablePartitionParam(
            std::shared_ptr<OlapTableSchemaParam>(_schema_param, [](auto*) {}), tpartition_param));
    ASSERT_OK(partition_param->init(nullptr));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    index_id_to_tablet_be_map[_index_id] = {};
    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    // Data: 5 (partition 1000), 15 (partition 1001)
    auto chunk = _create_chunk({5, 15});

    // Partition array is per-row: row0 -> partition 1000, row1 -> partition 1001.
    std::vector<OlapTablePartition*> partitions;
    auto it = partition_param->get_partitions().begin();
    OlapTablePartition* p0 = it->second;
    ++it;
    OlapTablePartition* p1 = it->second;
    partitions.push_back(p0); // row 0
    partitions.push_back(p1); // row 1

    std::vector<uint16_t> validate_select_idx = {0, 1};
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_OK(st);

    const auto& batches = sender.sent_batches();
    ASSERT_EQ(1, batches.size());
    ASSERT_EQ(2, batches[0].tablet_ids.size());
    EXPECT_EQ(10000, batches[0].tablet_ids[0]);
    EXPECT_EQ(10001, batches[0].tablet_ids[1]);

    // index_id_partition_id should record two partition ids for this index.
    ASSERT_EQ(2, index_id_partition_id[_index_id].size());
    EXPECT_TRUE(index_id_partition_id[_index_id].find(1000) != index_id_partition_id[_index_id].end());
    EXPECT_TRUE(index_id_partition_id[_index_id].find(1001) != index_id_partition_id[_index_id].end());
}

// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, OverlappingRangesRoutingOrder) {
    _init_schema_param();

    // Realistic overlap-like scenario with a point range at boundary, but
    // expressed in a way that satisfies RangeRouter invariants:
    // Tablet 1: (-inf, 10)      -> 10000
    // Tablet 2: [10, 11)        -> 10001
    // Tablet 3: [11, +inf)      -> 10002
    std::vector<TTabletRange> ranges;
    {
        // Tablet 1: (-inf, 10)
        TTabletRange r0;
        TTuple upper_tuple;
        TVariant upper_var;
        upper_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        upper_var.__set_value(std::to_string(10));
        upper_tuple.__set_values({upper_var});
        r0.__set_upper_bound(upper_tuple);
        r0.__set_upper_bound_included(false);
        ranges.push_back(r0);
    }
    {
        // Tablet 2: [10, 11)
        ranges.push_back(_make_range(10, true, 11, false));
    }
    {
        // Tablet 3: [11, +inf)
        TTabletRange r2;
        TTuple lower_tuple;
        TVariant lower_var;
        lower_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        lower_var.__set_value(std::to_string(11));
        lower_tuple.__set_values({lower_var});
        r2.__set_lower_bound(lower_tuple);
        r2.__set_lower_bound_included(true);
        ranges.push_back(r2);
    }

    auto* partition_param = _create_partition_param({ranges});
    ASSERT_OK(partition_param->init(nullptr));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    index_id_to_tablet_be_map[_index_id] = {};
    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    // Test values: 5 (in range 1), 10 (in range 2), 15 (in range 3)
    auto chunk = _create_chunk({5, 10, 15});

    std::vector<OlapTablePartition*> partitions;
    auto* part = partition_param->get_partitions().begin()->second;
    partitions.resize(3, part);
    std::vector<uint16_t> validate_select_idx = {0, 1, 2};
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_OK(st);

    const auto& batches = sender.sent_batches();
    ASSERT_EQ(1, batches.size());
    ASSERT_EQ(3, batches[0].tablet_ids.size());
    EXPECT_EQ(10000, batches[0].tablet_ids[0]); // 5 -> (0, 10)
    EXPECT_EQ(10001, batches[0].tablet_ids[1]); // 10 -> [10, 10]
    EXPECT_EQ(10002, batches[0].tablet_ids[2]); // 15 -> (10, 20]
}

// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, MultiBENonReplicatedRouting) {
    _init_schema_param();

    // Tablet 1: (-inf, 10)  -> 10000, replicas on BE 101, 102
    // Tablet 2: [10, +inf)  -> 10001, replica on BE 102
    std::vector<TTabletRange> ranges;
    {
        TTabletRange r0;
        TTuple upper_tuple;
        TVariant upper_var;
        upper_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        upper_var.__set_value(std::to_string(10));
        upper_tuple.__set_values({upper_var});
        r0.__set_upper_bound(upper_tuple);
        r0.__set_upper_bound_included(false);
        ranges.push_back(r0);
    }
    {
        TTabletRange r1;
        TTuple lower_tuple;
        TVariant lower_var;
        lower_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        lower_var.__set_value(std::to_string(10));
        lower_tuple.__set_values({lower_var});
        r1.__set_lower_bound(lower_tuple);
        r1.__set_lower_bound_included(true);
        ranges.push_back(r1);
    }

    auto* partition_param = _create_partition_param({ranges});
    ASSERT_OK(partition_param->init(nullptr));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    // index 100: tablet -> [be_ids]
    index_id_to_tablet_be_map[_index_id][10000] = {101, 102};
    index_id_to_tablet_be_map[_index_id][10001] = {102};

    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    // enable_replicated_storage = false
    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    // Rows: 5  -> tablet 10000 -> BE 101,102
    //       15 -> tablet 10001 -> BE 102
    auto chunk = _create_chunk({5, 15});

    std::vector<OlapTablePartition*> partitions;
    auto* part = partition_param->get_partitions().begin()->second;
    partitions.resize(2, part);
    std::vector<uint16_t> validate_select_idx = {0, 1};
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_OK(st);

    const auto& batches = sender.sent_batches();
    ASSERT_EQ(1, batches.size());
    ASSERT_EQ(2, batches[0].tablet_ids.size());
    EXPECT_EQ(10000, batches[0].tablet_ids[0]);
    EXPECT_EQ(10001, batches[0].tablet_ids[1]);

    const auto& be_routing = sender.be_routing();
    auto it_index = be_routing.find(_index_id);
    ASSERT_NE(it_index, be_routing.end());

    const auto& be_map = it_index->second;
    auto it_be101 = be_map.find(101);
    ASSERT_NE(it_be101, be_map.end());
    std::vector<uint16_t> expected_101 = {0};
    EXPECT_EQ(expected_101, it_be101->second);

    auto it_be102 = be_map.find(102);
    ASSERT_NE(it_be102, be_map.end());
    std::vector<uint16_t> expected_102 = {0, 1};
    EXPECT_EQ(expected_102, it_be102->second);
}

// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, MultiBEReplicatedPrimaryOnlyRouting) {
    _init_schema_param();

    // Tablet 1: (-inf, 10)  -> 10000, replicas on BE 201(primary), 202
    // Tablet 2: [10, +inf)  -> 10001, replicas on BE 202(primary), 203
    std::vector<TTabletRange> ranges;
    {
        TTabletRange r0;
        TTuple upper_tuple;
        TVariant upper_var;
        upper_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        upper_var.__set_value(std::to_string(10));
        upper_tuple.__set_values({upper_var});
        r0.__set_upper_bound(upper_tuple);
        r0.__set_upper_bound_included(false);
        ranges.push_back(r0);
    }
    {
        TTabletRange r1;
        TTuple lower_tuple;
        TVariant lower_var;
        lower_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        lower_var.__set_value(std::to_string(10));
        lower_tuple.__set_values({lower_var});
        r1.__set_lower_bound(lower_tuple);
        r1.__set_lower_bound_included(true);
        ranges.push_back(r1);
    }

    auto* partition_param = _create_partition_param({ranges});
    ASSERT_OK(partition_param->init(nullptr));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    index_id_to_tablet_be_map[_index_id][10000] = {201, 202};
    index_id_to_tablet_be_map[_index_id][10001] = {202, 203};

    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    // enable_replicated_storage = true，只发 primary replica。
    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, true, TWriteQuorumType::MAJORITY, 2);

    // Rows: 5  -> tablet 10000 -> primary BE 201
    //       15 -> tablet 10001 -> primary BE 202
    auto chunk = _create_chunk({5, 15});

    std::vector<OlapTablePartition*> partitions;
    auto* part = partition_param->get_partitions().begin()->second;
    partitions.resize(2, part);
    std::vector<uint16_t> validate_select_idx = {0, 1};
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_OK(st);

    const auto& be_routing = sender.be_routing();
    auto it_index = be_routing.find(_index_id);
    ASSERT_NE(it_index, be_routing.end());

    const auto& be_map = it_index->second;

    auto it_be201 = be_map.find(201);
    ASSERT_NE(it_be201, be_map.end());
    std::vector<uint16_t> expected_201 = {0};
    EXPECT_EQ(expected_201, it_be201->second);

    auto it_be202 = be_map.find(202);
    ASSERT_NE(it_be202, be_map.end());
    std::vector<uint16_t> expected_202 = {1};
    EXPECT_EQ(expected_202, it_be202->second);
}

// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, NoMatchError) {
    _init_schema_param();
    std::vector<TTabletRange> ranges;
    // Single finite range [0,10) without -inf/+inf coverage should be rejected
    // when initializing RangeRouter inside RangeTabletSinkSender.
    ranges.push_back(_make_range(0, true, 10, false));
    auto* partition_param = _create_partition_param({ranges});
    ASSERT_OK(partition_param->init(nullptr));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    index_id_to_tablet_be_map[_index_id] = {};
    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    auto chunk = _create_chunk({15});
    std::vector<OlapTablePartition*> partitions;
    partitions.push_back(partition_param->get_partitions().begin()->second);
    std::vector<uint16_t> validate_select_idx = {0};
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.message().find("lower_inf_count and upper_inf_count must be 1") != std::string::npos);
}

// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, BoundaryRoutingAtEdges) {
    _init_schema_param();

    // Tablet 1: (-inf, 10) -> 10000
    // Tablet 2: [10, +inf) -> 10001
    std::vector<TTabletRange> ranges;
    {
        TTabletRange r0;
        TTuple upper_tuple;
        TVariant upper_var;
        upper_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        upper_var.__set_value(std::to_string(10));
        upper_tuple.__set_values({upper_var});
        r0.__set_upper_bound(upper_tuple);
        r0.__set_upper_bound_included(false);
        ranges.push_back(r0);
    }
    {
        TTabletRange r1;
        TTuple lower_tuple;
        TVariant lower_var;
        lower_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        lower_var.__set_value(std::to_string(10));
        lower_tuple.__set_values({lower_var});
        r1.__set_lower_bound(lower_tuple);
        r1.__set_lower_bound_included(true);
        ranges.push_back(r1);
    }

    auto* partition_param = _create_partition_param({ranges});
    ASSERT_OK(partition_param->init(nullptr));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    index_id_to_tablet_be_map[_index_id] = {};
    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    // Values exactly on the boundary: 9, 10, 19
    auto chunk = _create_chunk({9, 10, 19});

    std::vector<OlapTablePartition*> partitions;
    auto* part = partition_param->get_partitions().begin()->second;
    partitions.resize(3, part);
    std::vector<uint16_t> validate_select_idx = {0, 1, 2};
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_OK(st);

    const auto& batches = sender.sent_batches();
    ASSERT_EQ(1, batches.size());
    const auto& tablet_ids = batches[0].tablet_ids;
    ASSERT_EQ(3, tablet_ids.size());
    // 9  -> tablet 10000 ([0,10))
    // 10 -> tablet 10001 ([10,20))
    // 19 -> tablet 10001 ([10,20))
    EXPECT_EQ(10000, tablet_ids[0]);
    EXPECT_EQ(10001, tablet_ids[1]);
    EXPECT_EQ(10001, tablet_ids[2]);
}

// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, SparseDataRouting) {
    _init_schema_param();
    // Ranges:
    //   R0: (-inf, 101)   -> 10000
    //   R1: [101, +inf)   -> 10001
    std::vector<TTabletRange> ranges;
    {
        // R0: (-inf, 101)
        TTabletRange r0;
        TTuple upper_tuple;
        TVariant upper_var;
        upper_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        upper_var.__set_value(std::to_string(101));
        upper_tuple.__set_values({upper_var});
        r0.__set_upper_bound(upper_tuple);
        r0.__set_upper_bound_included(false);
        ranges.push_back(r0);
    }
    {
        // R1: [101, +inf)
        TTabletRange r1;
        TTuple lower_tuple;
        TVariant lower_var;
        lower_var.__set_type(TYPE_BIGINT_DESC.to_thrift());
        lower_var.__set_value(std::to_string(101));
        lower_tuple.__set_values({lower_var});
        r1.__set_lower_bound(lower_tuple);
        r1.__set_lower_bound_included(true);
        ranges.push_back(r1);
    }

    auto* partition_param = _create_partition_param({ranges});
    ASSERT_OK(partition_param->init(nullptr));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    index_id_to_tablet_be_map[_index_id] = {};
    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    // Construct a large chunk with sparse row indices to ensure routing works
    // correctly even when validate_select_idx contains large gaps.
    // Indices: 0, 100, 200.
    auto col = FixedLengthColumn<int64_t>::create();
    col->resize(201);
    col->get_data()[0] = 1;
    col->get_data()[100] = 2;
    col->get_data()[200] = 3;

    Columns cols;
    cols.emplace_back(col);
    Fields fields;
    fields.emplace_back(std::make_shared<Field>(0, "c1", get_type_info(TYPE_BIGINT), false));
    auto schema = std::make_shared<Schema>(fields);
    auto chunk = std::make_shared<Chunk>(std::move(cols), std::move(schema));
    // Set slot_id to column index mapping
    chunk->set_slot_id_to_index(0, 0); // slot_id 0 -> column index 0

    std::vector<OlapTablePartition*> partitions;
    auto* part = partition_param->get_partitions().begin()->second;
    partitions.resize(201, part);

    std::vector<uint16_t> validate_select_idx = {0, 100, 200}; // Sparse selection
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_OK(st);

    const auto& batches = sender.sent_batches();
    ASSERT_EQ(1, batches.size());
    ASSERT_EQ(3, batches[0].tablet_ids.size());
    EXPECT_EQ(10000, batches[0].tablet_ids[0]);
    EXPECT_EQ(10000, batches[0].tablet_ids[1]);
    EXPECT_EQ(10000, batches[0].tablet_ids[2]);
}

// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, EmptyInput) {
    _init_schema_param();
    std::vector<TTabletRange> ranges;
    ranges.push_back(_make_range(0, true, 10, false));
    auto* partition_param = _create_partition_param({ranges});

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    std::vector<IndexChannel*> channels;
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    std::vector<uint16_t> validate_select_idx; // Empty
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;
    std::vector<OlapTablePartition*> partitions;
    Chunk chunk;

    Status st = sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, &chunk);
    ASSERT_OK(st);
    ASSERT_TRUE(sender.sent_batches().empty());
}

// Two indexes route independently: index A carries per-index distributed_exprs over
// column c2 and routes by c2 boundaries, while index B (no exprs) routes by the shared
// partition distribution slot c1. With rows whose c1 and c2 orderings disagree, the two
// indexes must route the same row to different tablets.
// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, PerIndexDistributedExprsRouting) {
    // ---- Schema: two columns (c1, c2) BIGINT, two indexes ----
    TOlapTableSchemaParam tschema;
    tschema.db_id = _db_id;
    tschema.table_id = _table_id;
    tschema.version = 0;

    TTupleDescriptor tuple_desc;
    tuple_desc.id = 0;
    tuple_desc.byteSize = 32;
    tuple_desc.numNullBytes = 0;
    tuple_desc.tableId = _table_id;
    tschema.tuple_desc = tuple_desc;

    auto make_bigint_slot = [](int id, const std::string& name) {
        TSlotDescriptor slot;
        slot.id = id;
        slot.parent = 0;
        slot.colName = name;
        slot.slotType.types.emplace_back();
        slot.slotType.types.back().__set_type(TTypeNodeType::SCALAR);
        slot.slotType.types.back().__set_scalar_type(TScalarType());
        slot.slotType.types.back().scalar_type.__set_type(TPrimitiveType::BIGINT);
        return slot;
    };
    tschema.slot_descs.push_back(make_bigint_slot(0, "c1"));
    tschema.slot_descs.push_back(make_bigint_slot(1, "c2"));

    auto make_index = [](int64_t index_id, const std::vector<std::string>& cols, const std::vector<int>& uids) {
        TOlapTableIndexSchema index_schema;
        index_schema.id = index_id;
        index_schema.columns = cols;
        index_schema.schema_hash = 0;
        TOlapTableColumnParam column_param;
        for (size_t k = 0; k < cols.size(); ++k) {
            TColumn tcol;
            tcol.column_name = cols[k];
            tcol.column_type.type = TPrimitiveType::BIGINT;
            tcol.is_key = true;
            tcol.is_allow_null = false;
            tcol.col_unique_id = uids[k];
            column_param.columns.push_back(tcol);
            column_param.sort_key_uid.push_back(uids[k]);
        }
        index_schema.__set_column_param(column_param);
        return index_schema;
    };

    // Index A (100): carries distributed_exprs over c2 (slot 1).
    TOlapTableIndexSchema index_a = make_index(_index_id, {"c1", "c2"}, {0, 1});
    index_a.__set_distributed_exprs({_make_slot_ref_texpr(1)});
    tschema.indexes.push_back(index_a);
    // Index B (101): no distributed_exprs => falls back to partition slot c1.
    tschema.indexes.push_back(make_index(_index_id + 1, {"c1", "c2"}, {0, 1}));

    auto* state = _make_runtime_state();
    ASSERT_OK(_schema_param->init(tschema, state));

    // ---- Partition param: distribute by c1; per-index ranges below. ----
    auto make_single_bigint_range = [](std::optional<int64_t> lower, bool lower_incl, std::optional<int64_t> upper,
                                       bool upper_incl) {
        TTabletRange range;
        if (lower.has_value()) {
            TVariant v;
            v.__set_type(TYPE_BIGINT_DESC.to_thrift());
            v.__set_value(std::to_string(lower.value()));
            TTuple t;
            t.__set_values({v});
            range.__set_lower_bound(t);
            range.__set_lower_bound_included(lower_incl);
        }
        if (upper.has_value()) {
            TVariant v;
            v.__set_type(TYPE_BIGINT_DESC.to_thrift());
            v.__set_value(std::to_string(upper.value()));
            TTuple t;
            t.__set_values({v});
            range.__set_upper_bound(t);
            range.__set_upper_bound_included(upper_incl);
        }
        return range;
    };

    // Index A boundaries (interpreted over c2): (-inf,10)->10000, [10,+inf)->10001
    std::vector<TTabletRange> ranges_a;
    ranges_a.push_back(make_single_bigint_range(std::nullopt, false, 10, false));
    ranges_a.push_back(make_single_bigint_range(10, true, std::nullopt, false));
    // Index B boundaries (over c1): (-inf,5)->10100, [5,+inf)->10101
    std::vector<TTabletRange> ranges_b;
    ranges_b.push_back(make_single_bigint_range(std::nullopt, false, 5, false));
    ranges_b.push_back(make_single_bigint_range(5, true, std::nullopt, false));

    auto* partition_param = _create_partition_param({ranges_a, ranges_b});
    ASSERT_OK(partition_param->init(state));
    ASSERT_OK(partition_param->prepare(state));
    ASSERT_OK(partition_param->open(state));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    index_id_to_tablet_be_map[_index_id] = {};
    index_id_to_tablet_be_map[_index_id + 1] = {};

    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id + 1, nullptr)));
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    // Rows: (c1, c2)
    //   row 0: (2, 50)  -> A by c2=50 => 10001 ; B by c1=2 => 10100
    //   row 1: (7,  3)  -> A by c2=3  => 10000 ; B by c1=7 => 10101
    auto chunk = _create_two_column_chunk({2, 7}, {50, 3});

    std::vector<OlapTablePartition*> partitions;
    auto* part = partition_param->get_partitions().begin()->second;
    partitions.resize(2, part);
    std::vector<uint16_t> validate_select_idx = {0, 1};
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_OK(st);
    partition_param->close(state);

    const auto& batches = sender.sent_batches();
    ASSERT_EQ(2, batches.size());

    // Index A routes by c2.
    EXPECT_EQ(_index_id, batches[0].index_id);
    ASSERT_EQ(2, batches[0].tablet_ids.size());
    EXPECT_EQ(10001, batches[0].tablet_ids[0]); // c2=50 -> [10,+inf)
    EXPECT_EQ(10000, batches[0].tablet_ids[1]); // c2=3  -> (-inf,10)

    // Index B routes by c1 (shared partition slot).
    EXPECT_EQ(_index_id + 1, batches[1].index_id);
    ASSERT_EQ(2, batches[1].tablet_ids.size());
    EXPECT_EQ(10100, batches[1].tablet_ids[0]); // c1=2 -> (-inf,5)
    EXPECT_EQ(10101, batches[1].tablet_ids[1]); // c1=7 -> [5,+inf)
}

// K=1: an index whose distributed_exprs is set but empty has a single tablet per
// partition. The sender must fill that single candidate directly without initializing or
// invoking the RangeRouter (the router would error on empty boundaries).
// NOLINTNEXTLINE
TEST_F(TabletSinkSenderRangeTest, EmptyDistributedExprsSingleTablet) {
    TOlapTableSchemaParam tschema;
    tschema.db_id = _db_id;
    tschema.table_id = _table_id;
    tschema.version = 0;

    TTupleDescriptor tuple_desc;
    tuple_desc.id = 0;
    tuple_desc.byteSize = 16;
    tuple_desc.numNullBytes = 0;
    tuple_desc.tableId = _table_id;
    tschema.tuple_desc = tuple_desc;

    TSlotDescriptor slot1;
    slot1.id = 0;
    slot1.parent = 0;
    slot1.colName = "c1";
    slot1.slotType.types.emplace_back();
    slot1.slotType.types.back().__set_type(TTypeNodeType::SCALAR);
    slot1.slotType.types.back().__set_scalar_type(TScalarType());
    slot1.slotType.types.back().scalar_type.__set_type(TPrimitiveType::BIGINT);
    tschema.slot_descs.push_back(slot1);

    TOlapTableIndexSchema index_schema;
    index_schema.id = _index_id;
    index_schema.columns = {"c1"};
    index_schema.schema_hash = 0;
    TOlapTableColumnParam column_param;
    TColumn tcol;
    tcol.column_name = "c1";
    tcol.column_type.type = TPrimitiveType::BIGINT;
    tcol.is_key = true;
    tcol.is_allow_null = false;
    tcol.col_unique_id = 0;
    column_param.columns.push_back(tcol);
    column_param.sort_key_uid.push_back(0);
    index_schema.__set_column_param(column_param);
    // K=1: distributed_exprs set but empty (no per-index routing key).
    index_schema.__set_distributed_exprs(std::vector<TExpr>{});
    tschema.indexes.push_back(index_schema);

    // distributed_exprs is set but empty => no RuntimeState required for parsing.
    ASSERT_OK(_schema_param->init(tschema, nullptr));
    ASSERT_TRUE(_schema_param->indexes()[0]->has_distributed_exprs);
    ASSERT_TRUE(_schema_param->indexes()[0]->distributed_expr_ctxs.empty());

    // Single (-inf, +inf) tablet for the partition.
    std::vector<TTabletRange> ranges(1);
    auto* partition_param = _create_partition_param({ranges});
    ASSERT_OK(partition_param->init(nullptr));

    PUniqueId load_id;
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    index_id_to_tablet_be_map[_index_id] = {};
    std::vector<IndexChannel*> channels;
    channels.push_back(_object_pool->add(new IndexChannel(nullptr, _index_id, nullptr)));
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    std::vector<ExprContext*> output_expr_ctxs;

    TestRangeTabletSinkSender sender(load_id, 1, index_id_to_tablet_be_map, partition_param, channels, node_channels,
                                     output_expr_ctxs, false, TWriteQuorumType::MAJORITY, 1);

    auto chunk = _create_chunk({5, 15, 0, 19});
    std::vector<OlapTablePartition*> partitions;
    auto* part = partition_param->get_partitions().begin()->second;
    partitions.resize(4, part);
    std::vector<uint16_t> validate_select_idx = {0, 1, 2, 3};
    std::unordered_map<int64_t, std::set<int64_t>> index_id_partition_id;

    Status st =
            sender.send_chunk(_schema_param, partitions, {}, validate_select_idx, index_id_partition_id, chunk.get());
    ASSERT_OK(st);

    const auto& batches = sender.sent_batches();
    ASSERT_EQ(1, batches.size());
    const auto& tablet_ids = batches[0].tablet_ids;
    ASSERT_EQ(4, tablet_ids.size());
    // All rows go to the single tablet, regardless of value.
    for (int64_t id : tablet_ids) {
        EXPECT_EQ(10000, id);
    }
    ASSERT_EQ(1, index_id_partition_id[_index_id].size());
}

} // namespace starrocks
