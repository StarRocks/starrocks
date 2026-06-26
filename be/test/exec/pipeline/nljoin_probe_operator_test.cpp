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

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exec/pipeline/nljoin/nljoin_context.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "types/logical_type.h"

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
        return std::make_unique<NLJoinProbeOperatorFactory>(0, 1, _empty_row_desc, _empty_row_desc, _empty_row_desc, "",
                                                            std::vector<ExprContext*>{}, std::vector<ExprContext*>{},
                                                            std::map<SlotId, ExprContext*>{},
                                                            std::shared_ptr<NLJoinContext>(ctx), join_op);
    }

    ObjectPool _pool;
    RowDescriptor _empty_row_desc;
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

} // namespace starrocks::pipeline
