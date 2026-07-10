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

#include "exec/pipeline/enforce_unique_row_locator_operator.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config_exec_fwd.h"
#include "exec/enforce_unique_row_locator_node.h"
#include "exec/pipeline/empty_set_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_builder.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// Helper that builds a two-column chunk:
//   col 0 — nullable BinaryColumn  (file_path)
//   col 1 — nullable FixedLengthColumn<int64_t> (row_position)
//
// Pass an empty string "" as file_path to represent a NULL value for that row.
// Pass INT64_MIN as row_position to represent a NULL value for that row.
static ChunkPtr build_chunk(const std::vector<std::string>& files, const std::vector<int64_t>& positions,
                            bool nullable = true) {
    DCHECK_EQ(files.size(), positions.size());

    auto chunk = std::make_shared<Chunk>();

    if (nullable) {
        auto file_data = BinaryColumn::create();
        auto file_null = NullColumn::create();
        for (const auto& f : files) {
            if (f.empty()) {
                file_data->append_default(); // placeholder value
                file_null->append(1);        // NULL
            } else {
                file_data->append(f);
                file_null->append(0);
            }
        }
        chunk->append_column(NullableColumn::create(std::move(file_data), std::move(file_null)), 0);

        auto pos_data = FixedLengthColumn<int64_t>::create();
        auto pos_null = NullColumn::create();
        for (auto p : positions) {
            if (p == INT64_MIN) {
                pos_data->append_default(); // placeholder
                pos_null->append(1);        // NULL
            } else {
                pos_data->append(p);
                pos_null->append(0);
            }
        }
        chunk->append_column(NullableColumn::create(std::move(pos_data), std::move(pos_null)), 1);
    } else {
        auto file_col = BinaryColumn::create();
        for (const auto& f : files) {
            file_col->append(f);
        }
        chunk->append_column(std::move(file_col), 0);

        auto pos_col = FixedLengthColumn<int64_t>::create();
        for (auto p : positions) {
            pos_col->append(p);
        }
        chunk->append_column(std::move(pos_col), 1);
    }

    return chunk;
}

class EnforceUniqueRowLocatorOperatorTest : public ::testing::Test {
protected:
    void SetUp() override {}

    EnforceUniqueRowLocatorOperatorFactory make_factory() {
        return EnforceUniqueRowLocatorOperatorFactory(/*id=*/0, /*plan_node_id=*/0, std::vector<int32_t>{0, 1});
    }

    RuntimeState _state;
};

// All rows have distinct (file_path, row_position) pairs — push must succeed.
TEST_F(EnforceUniqueRowLocatorOperatorTest, NoDuplicates) {
    auto factory = make_factory();
    auto op = factory.create(/*dop=*/1, /*driver_seq=*/0);

    auto chunk = build_chunk({"file1.parquet", "file1.parquet", "file2.parquet"}, {0, 1, 0});
    ASSERT_OK(op->push_chunk(&_state, chunk));

    ASSERT_TRUE(op->has_output());
    ASSIGN_OR_ABORT(auto pulled, op->pull_chunk(&_state));
    ASSERT_NE(pulled, nullptr);
    EXPECT_EQ(pulled->num_rows(), 3);
}

// Two rows share the same (file_path, row_position) — push must return an error.
TEST_F(EnforceUniqueRowLocatorOperatorTest, DuplicateDetected) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    auto chunk = build_chunk({"file1.parquet", "file1.parquet"}, {0, 0});
    auto st = op->push_chunk(&_state, chunk);
    EXPECT_FALSE(st.ok());
}

// Rows with NULL keys must be skipped — no duplicate check applied for them.
TEST_F(EnforceUniqueRowLocatorOperatorTest, NullKeysSkipped) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    // Row 0: (file1.parquet, 0)   — valid key, inserted
    // Row 1: (NULL, NULL)         — skipped
    // Row 2: (NULL, NULL)         — skipped (would be a "duplicate" but must be ignored)
    auto chunk = build_chunk({"file1.parquet", "", ""}, {0, INT64_MIN, INT64_MIN});
    ASSERT_OK(op->push_chunk(&_state, chunk));

    ASSERT_TRUE(op->has_output());
    ASSIGN_OR_ABORT(auto pulled, op->pull_chunk(&_state));
    ASSERT_NE(pulled, nullptr);
    EXPECT_EQ(pulled->num_rows(), 3);
}

// A duplicate that spans two separate chunks must still be detected.
TEST_F(EnforceUniqueRowLocatorOperatorTest, CrossChunkDuplicate) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    // First chunk — (file1.parquet, 0) is accepted.
    auto chunk1 = build_chunk({"file1.parquet"}, {0});
    ASSERT_OK(op->push_chunk(&_state, chunk1));

    // Pull so that the operator is ready for the next input.
    ASSERT_TRUE(op->has_output());
    ASSIGN_OR_ABORT(auto pulled1, op->pull_chunk(&_state));
    ASSERT_NE(pulled1, nullptr);

    // Second chunk — same (file1.parquet, 0) must now trigger a duplicate error.
    auto chunk2 = build_chunk({"file1.parquet"}, {0});
    auto st = op->push_chunk(&_state, chunk2);
    EXPECT_FALSE(st.ok());
}

// An empty chunk must be accepted without error.
TEST_F(EnforceUniqueRowLocatorOperatorTest, EmptyChunk) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    auto chunk = build_chunk({}, {});
    ASSERT_OK(op->push_chunk(&_state, chunk));

    // Empty chunk is not stored as output — need_input should still be true.
    EXPECT_FALSE(op->has_output());
    EXPECT_TRUE(op->need_input());
}

// Non-nullable columns must also work (operator handles both paths).
TEST_F(EnforceUniqueRowLocatorOperatorTest, NonNullableColumnsDuplicate) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    auto chunk = build_chunk({"file1.parquet", "file1.parquet"}, {5, 5}, /*nullable=*/false);
    auto st = op->push_chunk(&_state, chunk);
    EXPECT_FALSE(st.ok());
}

// Non-nullable columns with unique rows must pass.
TEST_F(EnforceUniqueRowLocatorOperatorTest, NonNullableColumnsNoDuplicates) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    auto chunk = build_chunk({"file1.parquet", "file2.parquet"}, {0, 0}, /*nullable=*/false);
    ASSERT_OK(op->push_chunk(&_state, chunk));

    ASSERT_TRUE(op->has_output());
    ASSIGN_OR_ABORT(auto pulled, op->pull_chunk(&_state));
    ASSERT_NE(pulled, nullptr);
    EXPECT_EQ(pulled->num_rows(), 2);
}

// set_finishing transitions the operator to the finished state.
TEST_F(EnforceUniqueRowLocatorOperatorTest, SetFinishing) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    EXPECT_FALSE(op->is_finished());
    ASSERT_OK(op->set_finishing(&_state));
    // No pending output, so operator should be finished.
    EXPECT_TRUE(op->is_finished());
}

// Keys are resolved by SLOT ID, not physical position: with a leading unrelated
// column the key columns sit at physical indices 1/2 but keep slot ids 7/9, and
// the duplicate must still be caught.
TEST_F(EnforceUniqueRowLocatorOperatorTest, KeysResolvedBySlotIdNotPosition) {
    EnforceUniqueRowLocatorOperatorFactory factory(/*id=*/0, /*plan_node_id=*/0, std::vector<int32_t>{7, 9});
    auto op = factory.create(1, 0);

    auto chunk = std::make_shared<Chunk>();
    auto data_col = FixedLengthColumn<int64_t>::create();
    data_col->append(100);
    data_col->append(200);
    chunk->append_column(std::move(data_col), /*slot_id=*/3);
    auto file_col = BinaryColumn::create();
    file_col->append("file1.parquet");
    file_col->append("file1.parquet");
    chunk->append_column(std::move(file_col), /*slot_id=*/7);
    auto pos_col = FixedLengthColumn<int64_t>::create();
    pos_col->append(0);
    pos_col->append(0);
    chunk->append_column(std::move(pos_col), /*slot_id=*/9);

    auto st = op->push_chunk(&_state, chunk);
    EXPECT_FALSE(st.ok());
}

// A key slot id that does not exist in the chunk must yield a clear error.
TEST_F(EnforceUniqueRowLocatorOperatorTest, MissingKeySlotRejected) {
    EnforceUniqueRowLocatorOperatorFactory factory(/*id=*/0, /*plan_node_id=*/0, std::vector<int32_t>{42, 1});
    auto op = factory.create(1, 0);

    auto chunk = build_chunk({"file1.parquet"}, {0});
    auto st = op->push_chunk(&_state, chunk);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.message().find("does not exist") != std::string::npos) << st.message();
}

// prepare() must register the seen-set profile counters and close() must report
// them and release the seen-set/path-id maps. Exercises the full operator
// lifecycle including _seen_memory_usage().
TEST_F(EnforceUniqueRowLocatorOperatorTest, PrepareThenCloseReleasesState) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);
    ASSERT_OK(op->prepare(&_state));

    auto chunk = build_chunk({"file1.parquet", "file2.parquet"}, {0, 1});
    ASSERT_OK(op->push_chunk(&_state, chunk));
    ASSIGN_OR_ABORT(auto pulled, op->pull_chunk(&_state));
    ASSERT_NE(pulled, nullptr);

    EXPECT_FALSE(op->get_name().empty());
    // close() reports the counters and force-deallocates the seen-set.
    op->close(&_state);
}

// The _file key column must be a binary column; a wrong type must be rejected
// with a diagnosable error rather than crashing on an unchecked down_cast.
TEST_F(EnforceUniqueRowLocatorOperatorTest, FilePathColumnTypeMismatchRejected) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    // slot 0 (_file) is wrongly an int64 column instead of binary.
    auto chunk = std::make_shared<Chunk>();
    auto wrong_file = FixedLengthColumn<int64_t>::create();
    wrong_file->append(1);
    chunk->append_column(std::move(wrong_file), /*slot_id=*/0);
    auto pos_col = FixedLengthColumn<int64_t>::create();
    pos_col->append(0);
    chunk->append_column(std::move(pos_col), /*slot_id=*/1);

    auto st = op->push_chunk(&_state, chunk);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.message().find("binary column") != std::string::npos) << st.message();
}

// The _pos key column must be FixedLengthColumn<int64_t>; a wrong type must be
// rejected with a diagnosable error.
TEST_F(EnforceUniqueRowLocatorOperatorTest, RowPosColumnTypeMismatchRejected) {
    auto factory = make_factory();
    auto op = factory.create(1, 0);

    // slot 1 (_pos) is wrongly a binary column instead of int64.
    auto chunk = std::make_shared<Chunk>();
    auto file_col = BinaryColumn::create();
    file_col->append("file1.parquet");
    chunk->append_column(std::move(file_col), /*slot_id=*/0);
    auto wrong_pos = BinaryColumn::create();
    wrong_pos->append("not-an-int");
    chunk->append_column(std::move(wrong_pos), /*slot_id=*/1);

    auto st = op->push_chunk(&_state, chunk);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.message().find("FixedLengthColumn<int64_t>") != std::string::npos) << st.message();
}

// The factory requires exactly two key slot ids; other arities must be rejected
// in prepare().
TEST_F(EnforceUniqueRowLocatorOperatorTest, FactoryPrepareRejectsWrongKeyCount) {
    EnforceUniqueRowLocatorOperatorFactory one_key(/*id=*/0, /*plan_node_id=*/0, std::vector<int32_t>{0});
    EXPECT_FALSE(one_key.prepare(&_state).ok());

    EnforceUniqueRowLocatorOperatorFactory three_keys(/*id=*/0, /*plan_node_id=*/0, std::vector<int32_t>{0, 1, 2});
    EXPECT_FALSE(three_keys.prepare(&_state).ok());

    auto two_keys = make_factory();
    EXPECT_OK(two_keys.prepare(&_state));
    two_keys.close(&_state);
}

// ---------------------------------------------------------------------------
// EnforceUniqueRowLocatorNode (ExecNode) tests
// ---------------------------------------------------------------------------

// A minimal child node whose decompose_to_pipeline yields an empty operator
// chain, so the EnforceUniqueRowLocatorNode under test simply appends its own
// operator factory on top.
class EnforceUniqueChildNode final : public ExecNode {
public:
    EnforceUniqueChildNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs) {}

    StatusOr<pipeline::OpFactories> decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override {
        // A plain source (no partition exprs, could_local_shuffle()==true) mimics a
        // post-probe passthrough where the join-key partitioning has been lost.
        return pipeline::OpFactories{std::make_shared<EmptySetOperatorFactory>(context->next_operator_id(), id())};
    }
};

class EnforceUniqueRowLocatorNodeTest : public ::testing::Test {
protected:
    void SetUp() override {
        TDescriptorTable t_desc_table;
        // ExecNode builds a RowDescriptor from row_tuples and requires at least one
        // tuple, so register a single tuple (id 1) holding the two row-locator slots.
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 1;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
        // _file (slot 10, VARCHAR) and _pos (slot 11, BIGINT): the node stores these
        // slot ids and the operator resolves them from each chunk's slot-id map.
        addSlot(t_desc_table, /*slot_id=*/10, /*parent_tuple=*/1, TPrimitiveType::VARCHAR);
        addSlot(t_desc_table, /*slot_id=*/11, /*parent_tuple=*/1, TPrimitiveType::BIGINT);
        ASSERT_OK(DescriptorTbl::create(&_state, &_pool, t_desc_table, &_desc_tbl, config::vector_chunk_size));
        _fragment_ctx = std::make_shared<pipeline::FragmentContext>();
        // The fragment context mirrors the real pipeline-builder setup.
        auto fragment_state = std::make_shared<RuntimeState>();
        fragment_state->set_desc_tbl(_desc_tbl);
        _fragment_ctx->set_runtime_state(std::move(fragment_state));
    }

    static void addSlot(TDescriptorTable& t_desc_table, int slot_id, int parent_tuple, TPrimitiveType::type ptype) {
        TTypeNode type_node;
        type_node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(ptype);
        scalar_type.__set_len(16);
        type_node.__set_scalar_type(scalar_type);
        TTypeDesc type_desc;
        type_desc.types.push_back(type_node);

        TSlotDescriptor slot;
        slot.id = slot_id;
        slot.parent = parent_tuple;
        slot.slotType = type_desc;
        slot.__set_isMaterialized(true);
        t_desc_table.slotDescriptors.push_back(slot);
    }

    TPlanNode make_tnode(int node_id, TPlanNodeType::type type, int64_t limit) {
        TPlanNode tnode;
        tnode.node_id = node_id;
        tnode.node_type = type;
        tnode.limit = limit;
        tnode.num_children = 0;
        tnode.row_tuples = std::vector<TTupleId>{1};
        tnode.nullable_tuples = std::vector<bool>{false};
        return tnode;
    }

    EnforceUniqueRowLocatorNode make_node(const std::vector<int32_t>& slot_ids, int64_t limit = -1) {
        TPlanNode tnode = make_tnode(/*node_id=*/1, TPlanNodeType::ENFORCE_UNIQUE_ROW_LOCATOR_NODE, limit);
        tnode.__isset.enforce_unique_row_locator_node = true;
        tnode.enforce_unique_row_locator_node.__set_unique_key_slot_ids(slot_ids);
        return EnforceUniqueRowLocatorNode(&_pool, tnode, *_desc_tbl);
    }

    EnforceUniqueChildNode* make_child() {
        TPlanNode tnode = make_tnode(/*node_id=*/2, TPlanNodeType::EXCHANGE_NODE, /*limit=*/-1);
        return _pool.add(new EnforceUniqueChildNode(&_pool, tnode, *_desc_tbl));
    }

    pipeline::PipelineBuilderContext make_context(size_t dop = 1) {
        return pipeline::PipelineBuilderContext(_fragment_ctx.get(), dop, /*sink_dop=*/1);
    }

    RuntimeState _state;
    ObjectPool _pool;
    DescriptorTbl* _desc_tbl = nullptr;
    std::shared_ptr<pipeline::FragmentContext> _fragment_ctx;
};

// The constructor must read the unique key slot ids from the thrift node.
TEST_F(EnforceUniqueRowLocatorNodeTest, ReadsSlotIdsFromThrift) {
    EnforceUniqueRowLocatorNode node = make_node({10, 11});
    ASSERT_OK(node.init(make_tnode(1, TPlanNodeType::ENFORCE_UNIQUE_ROW_LOCATOR_NODE, -1), &_state));
    EXPECT_EQ(node._unique_key_slot_ids, (std::vector<SlotId>{10, 11}));
    node.close(&_state);
}

// decompose_to_pipeline must append an EnforceUniqueRowLocatorOperatorFactory on
// top of the child's operator chain (and interpolate no exchange).
TEST_F(EnforceUniqueRowLocatorNodeTest, DecomposeAppendsEnforceOperator) {
    EnforceUniqueRowLocatorNode node = make_node({10, 11});
    node._children.push_back(make_child());

    auto context = make_context();
    ASSIGN_OR_ABORT(auto ops, node.decompose_to_pipeline(&context));
    ASSERT_FALSE(ops.empty());
    auto* enforce = dynamic_cast<EnforceUniqueRowLocatorOperatorFactory*>(ops.back().get());
    EXPECT_NE(enforce, nullptr);
    node.close(&_state);
}

// A node with a LIMIT must append a LimitOperatorFactory above the check.
TEST_F(EnforceUniqueRowLocatorNodeTest, DecomposeWithLimitAppendsLimitOperator) {
    EnforceUniqueRowLocatorNode node = make_node({10, 11}, /*limit=*/5);
    node._children.push_back(make_child());

    auto context = make_context();
    ASSIGN_OR_ABORT(auto ops, node.decompose_to_pipeline(&context));
    ASSERT_GE(ops.size(), 2);
    // The enforce operator sits below the trailing limit operator.
    bool has_enforce = false;
    for (auto& op : ops) {
        if (dynamic_cast<EnforceUniqueRowLocatorOperatorFactory*>(op.get()) != nullptr) {
            has_enforce = true;
        }
    }
    EXPECT_TRUE(has_enforce);
    node.close(&_state);
}

// MERGE correctness depends on the merge join preserving local key distribution.
// EnforceUniqueRowLocatorNode itself must not add a row-locator local shuffle:
// NOT-MATCHED insert rows have NULL row-locator keys and should not be funneled by
// a duplicate-check partition key they never use.
TEST_F(EnforceUniqueRowLocatorNodeTest, DecomposeDoesNotInterpolateRowLocatorShuffleUnderParallelism) {
    EnforceUniqueRowLocatorNode node = make_node({10, 11});
    node._children.push_back(make_child());

    auto context = make_context(/*dop=*/4);
    ASSIGN_OR_ABORT(auto ops, node.decompose_to_pipeline(&context));
    ASSERT_EQ(ops.size(), 2);
    EXPECT_NE(dynamic_cast<EmptySetOperatorFactory*>(ops.front().get()), nullptr);
    EXPECT_NE(dynamic_cast<EnforceUniqueRowLocatorOperatorFactory*>(ops.back().get()), nullptr);
    node.close(&_state);
}

} // namespace starrocks::pipeline
