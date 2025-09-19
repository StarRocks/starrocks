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

#include <memory>

#include "exec/chunk_buffer_memory_manager.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/query_context.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "types/logical_type.h"

namespace starrocks::pipeline {

class LocalBucketShuffleTest : public ::testing::Test {
public:
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();

        _query_context = std::make_shared<QueryContext>();
        _query_context->set_exec_env(_exec_env);
        _query_context->init_mem_tracker(-1, GlobalEnv::GetInstance()->process_mem_tracker());

        TQueryOptions query_options;
        query_options.batch_size = 4096;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(_fragment_id, query_options, query_globals, _exec_env);
        _runtime_state->set_query_ctx(_query_context.get());
        _runtime_state->init_instance_mem_tracker();

        _chunk_buffer_memory_manager = std::make_shared<ChunkBufferMemoryManager>(4, 1024 * 1024);
        _source_op_factory = std::make_unique<LocalExchangeSourceOperatorFactory>(0, 1, _chunk_buffer_memory_manager);
        _source_op_factory->set_runtime_state(_runtime_state.get());
        for (size_t i = 0; i < _dop; i++) {
            _sources.emplace_back(_source_op_factory->create(_dop, i));
        }
    }

protected:
    TExpr _create_slot_ref_expr(int slot_id) {
        std::vector<TExprNode> nodes;
        TExprNode node1;
        node1.node_type = TExprNodeType::SLOT_REF;
        node1.type = gen_type_desc(TPrimitiveType::INT);
        node1.num_children = 0;
        TSlotRef t_slot_ref = TSlotRef();
        t_slot_ref.slot_id = slot_id;
        t_slot_ref.tuple_id = 0;
        node1.__set_slot_ref(t_slot_ref);
        node1.is_nullable = true;
        nodes.emplace_back(node1);

        TExpr t_expr;
        t_expr.nodes = nodes;
        return t_expr;
    }

    std::shared_ptr<ChunkBufferMemoryManager> _chunk_buffer_memory_manager;
    std::unique_ptr<LocalExchangeSourceOperatorFactory> _source_op_factory;
    TUniqueId _fragment_id;
    ExecEnv* _exec_env;
    std::shared_ptr<QueryContext> _query_context;
    std::shared_ptr<RuntimeState> _runtime_state;
    ObjectPool _object_pool;
    std::vector<OperatorPtr> _sources;
    size_t _dop = 3;
};

TEST_F(LocalBucketShuffleTest, test_local_bucket_shuffle) {
    std::vector<TExpr> t_conjuncts;
    t_conjuncts.emplace_back(_create_slot_ref_expr(1));
    t_conjuncts.emplace_back(_create_slot_ref_expr(2));

    std::vector<ExprContext*> partition_exprs;
    Expr::create_expr_trees(&_object_pool, t_conjuncts, &partition_exprs, nullptr);
    Expr::prepare(partition_exprs, _runtime_state.get());
    Expr::open(partition_exprs, _runtime_state.get());

    TBucketProperty bucket_property = TBucketProperty();
    bucket_property.bucket_func = TBucketFunction::MURMUR3_X86_32;
    bucket_property.bucket_num = 2;
    auto bucket_properies = std::vector<TBucketProperty>();
    bucket_properies.emplace_back(bucket_property);
    bucket_properies.emplace_back(bucket_property);

    auto partitioner =
            make_unique<ShufflePartitioner>(_source_op_factory.get(), TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED,
                                            partition_exprs, bucket_properies);
    auto chunk = std::make_shared<Chunk>();

    auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
    col->append_datum(Datum(1));
    col->append_datum(Datum(2));
    col->append_datum(Datum(3));
    col->append_nulls(1);
    col->append_datum(Datum(4));
    col->append_nulls(1);
    chunk->append_column(std::move(col), 1);

    auto col_s = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
    std::vector<Slice> strings{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}};
    col_s->append_strings(strings.data(), strings.size());
    col_s->append_nulls(1);
    chunk->append_column(std::move(col_s), 2);

    std::vector<uint32_t> partition_indexes(chunk->num_rows());
    ASSERT_OK(partitioner->partition_chunk(chunk, _dop, partition_indexes));
    std::vector<uint32_t> indexes{5, 0, 3, 1, 2, 4};
    for (size_t i = 0; i < partition_indexes.size(); i++) {
        ASSERT_EQ(indexes[i], partition_indexes[i]);
    }
    std::vector<uint32_t> part_len{1, 2, 3};
    for (size_t i = 0; i < _dop; i++) {
        ASSERT_EQ(part_len[i], partitioner->partition_end_offset(i) - partitioner->partition_begin_offset(i));
    }
}

} // namespace starrocks::pipeline
