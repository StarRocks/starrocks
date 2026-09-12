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

#include "exec/pipeline/nljoin/nljoin_probe_operator.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "common/status.h"
#include "exec/pipeline/nljoin/nljoin_context.h"
#include "exec/pipeline/nljoin/spillable_nljoin_probe_operator.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"
#include "util/slice.h"

// The test object library is compiled with -fno-access-control, so the test can reach the
// private members (_init_output_chunk, _curr_build_chunk, NLJoinContext::_build_chunks) directly.

namespace starrocks::pipeline {

class NLJoinProbeOperatorTest : public testing::Test {
protected:
    // Build a single INT SlotDescriptor with a controlled nullable flag (the direct
    // (id, name, type) ctor always marks the slot nullable, so go through TSlotDescriptor).
    SlotDescriptor* make_int_slot(int id, bool nullable) {
        TSlotDescriptor t;
        t.id = id;
        t.parent = 0;
        TTypeDesc type;
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::INT);
        node.__set_scalar_type(scalar_type);
        type.types.push_back(node);
        t.__set_slotType(type);
        t.__set_colName("c" + std::to_string(id));
        t.__set_slotIdx(id);
        t.__set_isMaterialized(true);
        t.__set_isNullable(nullable);
        return _pool.add(new SlotDescriptor(t));
    }

    ChunkPtr make_one_int_column_chunk(SlotId slot_id, bool nullable) {
        auto chunk = std::make_shared<Chunk>();
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), nullable);
        col->append_default();
        chunk->append_column(std::move(col), slot_id);
        return chunk;
    }

    // A VARCHAR SlotDescriptor. VARCHAR maps to a uint32-offset BinaryColumn, which is exactly the
    // column type whose offsets the overflow guard protects.
    SlotDescriptor* make_varchar_slot(int id, bool nullable) {
        TSlotDescriptor t;
        t.id = id;
        t.parent = 0;
        TTypeDesc type;
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::VARCHAR);
        scalar_type.__set_len(1024 * 1024 * 1024);
        node.__set_scalar_type(scalar_type);
        type.types.push_back(node);
        t.__set_slotType(type);
        t.__set_colName("v" + std::to_string(id));
        t.__set_slotIdx(id);
        t.__set_isMaterialized(true);
        t.__set_isNullable(nullable);
        return _pool.add(new SlotDescriptor(t));
    }

    // A non-nullable INT column with `num_rows` default (0) values.
    ChunkPtr make_int_chunk(SlotId slot_id, size_t num_rows) {
        auto chunk = std::make_shared<Chunk>();
        auto col = Int32Column::create();
        col->append_default(num_rows);
        chunk->append_column(std::move(col), slot_id);
        return chunk;
    }

    // A non-nullable VARCHAR/binary column holding one row per value.
    ChunkPtr make_varchar_chunk(SlotId slot_id, const std::vector<std::string>& values) {
        auto chunk = std::make_shared<Chunk>();
        auto col = BinaryColumn::create();
        for (const auto& v : values) {
            col->append_datum(Slice(v));
        }
        chunk->append_column(std::move(col), slot_id);
        return chunk;
    }

    // A single VARCHAR value large enough that repeating it across `fanout` rows crosses the 4GB
    // (2^32) BinaryColumn offset limit: 2MB * 2100 > 2^32.
    static std::string big_value() { return std::string(2u * 1024 * 1024, 'x'); }
    static constexpr size_t kOverflowFanout = 2100;

    std::shared_ptr<NLJoinContext> make_context() {
        NLJoinContextParams params;
        params.plan_node_id = 1;
        params.rf_hub = nullptr;
        return std::make_shared<NLJoinContext>(std::move(params));
    }

    // A factory is only needed because the Operator base ctor uses the factory as its
    // OperatorRuntimeAccess (DCHECK'd non-null). _init_output_chunk never touches it.
    std::unique_ptr<NLJoinProbeOperatorFactory> make_factory(const std::shared_ptr<NLJoinContext>& ctx,
                                                             TJoinOp::type join_op) {
        return std::make_unique<NLJoinProbeOperatorFactory>(0, 1, _empty_record_desc, _empty_record_desc, "",
                                                            std::vector<ExprContext*>{}, std::vector<ExprContext*>{},
                                                            std::map<SlotId, ExprContext*>{},
                                                            std::shared_ptr<NLJoinContext>(ctx), join_op);
    }

    ObjectPool _pool;
    RecordDescriptor _empty_record_desc;
    std::vector<ExprContext*> _no_exprs;
    std::map<SlotId, ExprContext*> _no_common_exprs;
    std::string _no_sql;
};

// Regression for the NLJoin build-side nullability crash: when the build chunk's runtime
// column is nullable but its slot descriptor is non-nullable, _init_output_chunk must create
// the build-side output column as nullable so the later append does not mismatch -- even when
// _curr_build_chunk has already advanced to nullptr after the last build chunk is consumed.
// It must read the nullability from a stable build chunk (get_build_chunk(0)), not from the
// transient _curr_build_chunk.
TEST_F(NLJoinProbeOperatorTest, BuildColumnFollowsRuntimeNullabilityOnReentry) {
    // probe slot (id=0) and build slot (id=1), both DECLARED non-nullable.
    SlotDescriptor* probe_slot = make_int_slot(0, /*nullable=*/false);
    SlotDescriptor* build_slot = make_int_slot(1, /*nullable=*/false);
    std::vector<SlotDescriptor*> col_types = {probe_slot, build_slot};

    auto ctx = make_context();
    // The build chunk's runtime column is nullable (more nullable than the slot descriptor).
    ctx->_build_chunks.push_back(make_one_int_column_chunk(build_slot->id(), /*nullable=*/true));

    auto factory = make_factory(ctx, TJoinOp::RIGHT_OUTER_JOIN);
    NLJoinProbeOperator op(factory.get(), /*id=*/0, /*plan_node_id=*/1, /*driver_sequence=*/0,
                           TJoinOp::RIGHT_OUTER_JOIN, _no_sql, _no_exprs, _no_exprs, _no_common_exprs, col_types,
                           /*probe_column_count=*/1, ctx);

    // Reproduce the crash-trigger state: the last build chunk was just consumed.
    op._curr_build_chunk = nullptr;
    op._probe_chunk = nullptr;

    ChunkPtr out = op._init_output_chunk(4096);

    // Build-side output column follows the actual (nullable) build data, not the non-nullable slot.
    EXPECT_TRUE(out->is_column_nullable(build_slot->id()));
}

// When no build chunk exists, the build-column nullability falls back to the slot descriptor.
TEST_F(NLJoinProbeOperatorTest, BuildColumnFollowsSlotWhenNoBuildChunk) {
    SlotDescriptor* probe_slot = make_int_slot(0, /*nullable=*/false);
    SlotDescriptor* build_slot = make_int_slot(1, /*nullable=*/false);
    std::vector<SlotDescriptor*> col_types = {probe_slot, build_slot};

    auto ctx = make_context(); // no build chunks

    auto factory = make_factory(ctx, TJoinOp::INNER_JOIN);
    NLJoinProbeOperator op(factory.get(), /*id=*/0, /*plan_node_id=*/1, /*driver_sequence=*/0, TJoinOp::INNER_JOIN,
                           _no_sql, _no_exprs, _no_exprs, _no_common_exprs, col_types,
                           /*probe_column_count=*/1, ctx);
    op._curr_build_chunk = nullptr;
    op._probe_chunk = nullptr;

    ChunkPtr out = op._init_output_chunk(4096);

    // Non-nullable slot, no build chunk, inner join -> non-nullable build-side output column.
    EXPECT_FALSE(out->is_column_nullable(build_slot->id()));
}

// ---------------------------------------------------------------------------
// NLJoinProber permute overflow guard (spillable NestLoop path).
// A large VARCHAR repeated across a big fan-out would push the output BinaryColumn past its uint32
// (4GB) offset range; the guard must stop before the offsets wrap. 2MB * 2100 > 2^32.
// ---------------------------------------------------------------------------

// A single large probe row repeated across a big build chunk cannot fit into an empty output chunk,
// so the prober reports a recoverable CapacityLimitExceed instead of wrapping the offsets.
TEST_F(NLJoinProbeOperatorTest, ProberSingleRowOverflowReturnsError) {
    std::vector<SlotDescriptor*> col_types = {make_varchar_slot(0, false), make_int_slot(1, false)};

    RuntimeState state{TQueryGlobals()};
    state.set_chunk_size(1 << 20);
    RuntimeProfile profile("nljoin");

    NLJoinProber prober(TJoinOp::INNER_JOIN, _no_exprs, _no_exprs, _no_common_exprs, col_types,
                        /*probe_column_count=*/1);
    ASSERT_TRUE(prober.prepare(&state, &profile).ok());
    ASSERT_TRUE(prober.push_probe_chunk(make_varchar_chunk(0, {big_value()})).ok());

    auto res = prober.probe_chunk(&state, make_int_chunk(1, kOverflowFanout));
    ASSERT_FALSE(res.ok());
    EXPECT_TRUE(res.status().is_capacity_limit_exceeded());
}

// When rows are already buffered, hitting the limit on a later probe row breaks early and emits what
// has been permuted so far (the query keeps making progress) rather than failing.
TEST_F(NLJoinProbeOperatorTest, ProberBreaksEarlyWhenRowsBuffered) {
    std::vector<SlotDescriptor*> col_types = {make_varchar_slot(0, false), make_int_slot(1, false)};

    RuntimeState state{TQueryGlobals()};
    state.set_chunk_size(1 << 20);
    RuntimeProfile profile("nljoin");

    NLJoinProber prober(TJoinOp::INNER_JOIN, _no_exprs, _no_exprs, _no_common_exprs, col_types,
                        /*probe_column_count=*/1);
    ASSERT_TRUE(prober.prepare(&state, &profile).ok());
    // Row 0 is tiny (permutes fine); row 1 is large and would overflow, so the permute stops after row 0.
    ASSERT_TRUE(prober.push_probe_chunk(make_varchar_chunk(0, {"a", big_value()})).ok());

    auto res = prober.probe_chunk(&state, make_int_chunk(1, kOverflowFanout));
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(kOverflowFanout, res.value()->num_rows());
}

// A permute that stays well under the limit produces the full cross product.
TEST_F(NLJoinProbeOperatorTest, ProberSmallPermuteSucceeds) {
    std::vector<SlotDescriptor*> col_types = {make_varchar_slot(0, false), make_int_slot(1, false)};

    RuntimeState state{TQueryGlobals()};
    state.set_chunk_size(1 << 20);
    RuntimeProfile profile("nljoin");

    NLJoinProber prober(TJoinOp::INNER_JOIN, _no_exprs, _no_exprs, _no_common_exprs, col_types,
                        /*probe_column_count=*/1);
    ASSERT_TRUE(prober.prepare(&state, &profile).ok());
    ASSERT_TRUE(prober.push_probe_chunk(make_varchar_chunk(0, {"p", "q"})).ok());

    auto res = prober.probe_chunk(&state, make_int_chunk(1, 4));
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(8, res.value()->num_rows()); // 2 probe rows x 4 build rows
}

// ---------------------------------------------------------------------------
// NLJoinProbeOperator inner-join permute overflow guard.
// ---------------------------------------------------------------------------

// base-right permute (probe rows <= build rows): a single large probe value repeated across the
// build chunk overflows an empty output chunk -> recoverable error.
TEST_F(NLJoinProbeOperatorTest, InnerJoinBaseRightOverflowReturnsError) {
    std::vector<SlotDescriptor*> col_types = {make_varchar_slot(0, false), make_int_slot(1, false)};
    auto ctx = make_context();
    ctx->_build_chunks.push_back(make_int_chunk(1, kOverflowFanout));

    auto factory = make_factory(ctx, TJoinOp::INNER_JOIN);
    NLJoinProbeOperator op(factory.get(), 0, 1, 0, TJoinOp::INNER_JOIN, _no_sql, _no_exprs, _no_exprs, _no_common_exprs,
                           col_types, /*probe_column_count=*/1, ctx);

    op._probe_chunk = make_varchar_chunk(0, {big_value()});
    op._curr_build_chunk = ctx->get_build_chunk(0);
    op._curr_build_chunk_index = 0;
    op._probe_row_current = 0;
    op._build_row_current = 0;

    auto res = op._permute_chunk_for_inner_join(1 << 20);
    ASSERT_FALSE(res.ok());
    EXPECT_TRUE(res.status().is_capacity_limit_exceeded());
}

// base-left permute (probe rows > build rows) that stays under the limit: full cross product.
TEST_F(NLJoinProbeOperatorTest, InnerJoinBaseLeftSmallPermuteSucceeds) {
    std::vector<SlotDescriptor*> col_types = {make_int_slot(0, false), make_varchar_slot(1, false)};
    auto ctx = make_context();
    ctx->_build_chunks.push_back(make_varchar_chunk(1, {"a", "b"}));

    auto factory = make_factory(ctx, TJoinOp::INNER_JOIN);
    NLJoinProbeOperator op(factory.get(), 0, 1, 0, TJoinOp::INNER_JOIN, _no_sql, _no_exprs, _no_exprs, _no_common_exprs,
                           col_types, /*probe_column_count=*/1, ctx);

    op._probe_chunk = make_int_chunk(0, 3);
    op._curr_build_chunk = ctx->get_build_chunk(0);
    op._curr_build_chunk_index = 0;
    op._probe_row_current = 0;
    op._build_row_current = 0;

    auto res = op._permute_chunk_for_inner_join(1 << 20);
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(6, res.value()->num_rows()); // 3 probe rows x 2 build rows
}

// base-right permute that stays under the limit: full cross product.
TEST_F(NLJoinProbeOperatorTest, InnerJoinBaseRightSmallPermuteSucceeds) {
    std::vector<SlotDescriptor*> col_types = {make_varchar_slot(0, false), make_int_slot(1, false)};
    auto ctx = make_context();
    ctx->_build_chunks.push_back(make_int_chunk(1, 3));

    auto factory = make_factory(ctx, TJoinOp::INNER_JOIN);
    NLJoinProbeOperator op(factory.get(), 0, 1, 0, TJoinOp::INNER_JOIN, _no_sql, _no_exprs, _no_exprs, _no_common_exprs,
                           col_types, /*probe_column_count=*/1, ctx);

    op._probe_chunk = make_varchar_chunk(0, {"p", "q"});
    op._curr_build_chunk = ctx->get_build_chunk(0);
    op._curr_build_chunk_index = 0;
    op._probe_row_current = 0;
    op._build_row_current = 0;

    auto res = op._permute_chunk_for_inner_join(1 << 20);
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(6, res.value()->num_rows()); // 2 probe rows x 3 build rows
}

// ---------------------------------------------------------------------------
// NLJoinProbeOperator other-join (LEFT SEMI/ANTI) permute overflow guard.
// These paths require >= 2 build chunks. A real profile counter is wired because the permute path
// updates it; without ENABLE_COUNTERS the macro would still create a live counter.
// ---------------------------------------------------------------------------

// On the last build chunk the operator must permute; if the single probe row overflows an empty
// output chunk it returns a recoverable error rather than wrapping the offsets.
TEST_F(NLJoinProbeOperatorTest, OtherJoinLastBuildChunkOverflowReturnsError) {
    std::vector<SlotDescriptor*> col_types = {make_varchar_slot(0, false), make_int_slot(1, false)};
    auto ctx = make_context();
    ctx->_build_chunks.push_back(make_int_chunk(1, 2));               // build chunk 0
    ctx->_build_chunks.push_back(make_int_chunk(1, kOverflowFanout)); // build chunk 1 (last)

    auto factory = make_factory(ctx, TJoinOp::LEFT_SEMI_JOIN);
    NLJoinProbeOperator op(factory.get(), 0, 1, 0, TJoinOp::LEFT_SEMI_JOIN, _no_sql, _no_exprs, _no_exprs,
                           _no_common_exprs, col_types, /*probe_column_count=*/1, ctx);
    RuntimeProfile profile("nljoin");
    op._permute_rows_counter = ADD_COUNTER((&profile), "PermuteRows", TUnit::UNIT);

    op._probe_chunk = make_varchar_chunk(0, {big_value()});
    op._curr_build_chunk_index = 1; // last build chunk
    op._curr_build_chunk = ctx->get_build_chunk(1);
    op._probe_row_current = 0;
    op._probe_row_finished = false;
    op._probe_row_matched = false;

    auto res = op._permute_chunk_for_other_join(1 << 20);
    ASSERT_FALSE(res.ok());
    EXPECT_TRUE(res.status().is_capacity_limit_exceeded());
}

// While accumulating build chunks for one probe row, the operator emits the buffered rows before an
// append that would cross the limit (progress instead of overflow).
TEST_F(NLJoinProbeOperatorTest, OtherJoinBreaksEarlyWhenRowsBuffered) {
    std::vector<SlotDescriptor*> col_types = {make_varchar_slot(0, false), make_int_slot(1, false)};
    auto ctx = make_context();
    ctx->_build_chunks.push_back(make_int_chunk(1, 1));               // build chunk 0 (permutes fine)
    ctx->_build_chunks.push_back(make_int_chunk(1, kOverflowFanout)); // build chunk 1 (would overflow)

    auto factory = make_factory(ctx, TJoinOp::LEFT_SEMI_JOIN);
    NLJoinProbeOperator op(factory.get(), 0, 1, 0, TJoinOp::LEFT_SEMI_JOIN, _no_sql, _no_exprs, _no_exprs,
                           _no_common_exprs, col_types, /*probe_column_count=*/1, ctx);
    RuntimeProfile profile("nljoin");
    op._permute_rows_counter = ADD_COUNTER((&profile), "PermuteRows", TUnit::UNIT);

    op._probe_chunk = make_varchar_chunk(0, {big_value()});
    op._curr_build_chunk_index = 0; // not the last build chunk
    op._curr_build_chunk = ctx->get_build_chunk(0);
    op._probe_row_current = 0;
    op._probe_row_finished = false;
    op._probe_row_matched = false;

    auto res = op._permute_chunk_for_other_join(1 << 20);
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(1, res.value()->num_rows()); // only build chunk 0 (1 row) permuted before the limit
}

// A single overflowing probe row against a non-last build chunk with an empty output -> error.
TEST_F(NLJoinProbeOperatorTest, OtherJoinAccumulateOverflowReturnsError) {
    std::vector<SlotDescriptor*> col_types = {make_varchar_slot(0, false), make_int_slot(1, false)};
    auto ctx = make_context();
    ctx->_build_chunks.push_back(make_int_chunk(1, kOverflowFanout)); // build chunk 0 (would overflow)
    ctx->_build_chunks.push_back(make_int_chunk(1, 2));               // build chunk 1

    auto factory = make_factory(ctx, TJoinOp::LEFT_SEMI_JOIN);
    NLJoinProbeOperator op(factory.get(), 0, 1, 0, TJoinOp::LEFT_SEMI_JOIN, _no_sql, _no_exprs, _no_exprs,
                           _no_common_exprs, col_types, /*probe_column_count=*/1, ctx);
    RuntimeProfile profile("nljoin");
    op._permute_rows_counter = ADD_COUNTER((&profile), "PermuteRows", TUnit::UNIT);

    op._probe_chunk = make_varchar_chunk(0, {big_value()});
    op._curr_build_chunk_index = 0; // not the last build chunk
    op._curr_build_chunk = ctx->get_build_chunk(0);
    op._probe_row_current = 0;
    op._probe_row_finished = false;
    op._probe_row_matched = false;

    auto res = op._permute_chunk_for_other_join(1 << 20);
    ASSERT_FALSE(res.ok());
    EXPECT_TRUE(res.status().is_capacity_limit_exceeded());
}

// A small other-join permute across multiple build chunks stays under the limit and accumulates all rows.
TEST_F(NLJoinProbeOperatorTest, OtherJoinSmallPermuteAccumulatesAllBuildChunks) {
    std::vector<SlotDescriptor*> col_types = {make_varchar_slot(0, false), make_int_slot(1, false)};
    auto ctx = make_context();
    ctx->_build_chunks.push_back(make_int_chunk(1, 2)); // build chunk 0
    ctx->_build_chunks.push_back(make_int_chunk(1, 3)); // build chunk 1

    auto factory = make_factory(ctx, TJoinOp::LEFT_SEMI_JOIN);
    NLJoinProbeOperator op(factory.get(), 0, 1, 0, TJoinOp::LEFT_SEMI_JOIN, _no_sql, _no_exprs, _no_exprs,
                           _no_common_exprs, col_types, /*probe_column_count=*/1, ctx);
    RuntimeProfile profile("nljoin");
    op._permute_rows_counter = ADD_COUNTER((&profile), "PermuteRows", TUnit::UNIT);

    op._probe_chunk = make_varchar_chunk(0, {"p"});
    op._curr_build_chunk_index = 0;
    op._curr_build_chunk = ctx->get_build_chunk(0);
    op._probe_row_current = 0;
    op._probe_row_finished = false;
    op._probe_row_matched = false;

    auto res = op._permute_chunk_for_other_join(1 << 20);
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(5, res.value()->num_rows()); // 1 probe row x (2 + 3) build rows
}

// The last-build-chunk permute path (single probe row) under the limit produces one chunk.
TEST_F(NLJoinProbeOperatorTest, OtherJoinLastBuildChunkSmallPermuteSucceeds) {
    std::vector<SlotDescriptor*> col_types = {make_varchar_slot(0, false), make_int_slot(1, false)};
    auto ctx = make_context();
    ctx->_build_chunks.push_back(make_int_chunk(1, 2)); // build chunk 0
    ctx->_build_chunks.push_back(make_int_chunk(1, 3)); // build chunk 1 (last)

    auto factory = make_factory(ctx, TJoinOp::LEFT_SEMI_JOIN);
    NLJoinProbeOperator op(factory.get(), 0, 1, 0, TJoinOp::LEFT_SEMI_JOIN, _no_sql, _no_exprs, _no_exprs,
                           _no_common_exprs, col_types, /*probe_column_count=*/1, ctx);
    RuntimeProfile profile("nljoin");
    op._permute_rows_counter = ADD_COUNTER((&profile), "PermuteRows", TUnit::UNIT);

    op._probe_chunk = make_varchar_chunk(0, {"p"});
    op._curr_build_chunk_index = 1; // last build chunk
    op._curr_build_chunk = ctx->get_build_chunk(1);
    op._probe_row_current = 0;
    op._probe_row_finished = false;
    op._probe_row_matched = false;

    auto res = op._permute_chunk_for_other_join(1 << 20);
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(3, res.value()->num_rows()); // last build chunk has 3 rows
}

} // namespace starrocks::pipeline
