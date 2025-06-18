#include "exec/pipeline/table_function_operator.h"

#include "exec/chunk_buffer_memory_manager.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/query_context.h"
#include "gtest/gtest.h"

namespace starrocks::pipeline {
class TableFunctionOperatorTest : public testing::Test {
public:
    TableFunctionOperatorTest() : _runtime_state(TQueryGlobals()) {}

protected:
    void SetUp() override;

private:
    RuntimeState _runtime_state;
    std::unique_ptr<QueryContext> _query_ctx = std::make_unique<QueryContext>();
    ObjectPool _object_pool;
    DescriptorTbl* _desc_tbl = nullptr;
    TPlanNode _tnode;
};

class Counter {
public:
    void process_push(const ChunkPtr& chunk) {
        std::lock_guard<std::mutex> l(_mutex);
        ++_push_chunk_num;
        _push_chunk_row_num += chunk->num_rows();
    }

    void process_pull(const ChunkPtr& chunk) {
        std::lock_guard<std::mutex> l(_mutex);
        ++_pull_chunk_num;
        _pull_chunk_row_num += chunk->num_rows();
    }

    size_t push_chunk_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _push_chunk_num;
    }

    size_t pull_chunk_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _pull_chunk_num;
    }

    size_t push_chunk_row_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _push_chunk_row_num;
    }

    size_t pull_chunk_row_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _pull_chunk_row_num;
    }

private:
    std::mutex _mutex;
    size_t _push_chunk_num = 0;
    size_t _pull_chunk_num = 0;
    size_t _push_chunk_row_num = 0;
    size_t _pull_chunk_row_num = 0;
};

using CounterPtr = std::shared_ptr<Counter>;

class TestNormalOperatorFactory final : public OperatorFactory {
public:
    TestNormalOperatorFactory(int32_t id, int32_t plan_node_id, CounterPtr counter, TPlanNode* tnode)
            : OperatorFactory(id, "test_normal", plan_node_id), _counter(std::move(counter)), _tnode(tnode) {}

    ~TestNormalOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TableFunctionOperator>(this, _id, _plan_node_id, driver_sequence, *_tnode);
    }

private:
    CounterPtr _counter;
    TPlanNode* _tnode = nullptr;
};

void TableFunctionOperatorTest::SetUp() {
    _runtime_state.set_query_ctx(_query_ctx.get());

    TTableDescriptor t_table_desc;
    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;

    TDescriptorTable t_desc_table;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    TTupleDescriptor t_tuple_desc;
    t_tuple_desc.id = 1;
    t_desc_table.tupleDescriptors.push_back(t_tuple_desc);

    // ARRAY<int>
    TTypeDesc array_ttype;
    {
        array_ttype.__isset.types = true;
        array_ttype.types.resize(2);
        array_ttype.types[0].__set_type(TTypeNodeType::ARRAY);
        array_ttype.types[1].__set_type(TTypeNodeType::SCALAR);
        array_ttype.types[1].__set_scalar_type(TScalarType());
        array_ttype.types[1].scalar_type.__set_type(TPrimitiveType::INT);
        array_ttype.types[1].scalar_type.__set_len(0);
    }

    // int
    TTypeDesc int_ttype;
    {
        int_ttype.__isset.types = true;
        int_ttype.types.emplace_back();
        int_ttype.types.back().__set_type(TTypeNodeType::SCALAR);
        int_ttype.types.back().__set_scalar_type(TScalarType());
        int_ttype.types.back().scalar_type.__set_type(TPrimitiveType::INT);
    }

    for (int i = 0; i < 3; i++) {
        TSlotDescriptor slot_desc;
        slot_desc.id = 2 + i;

        slot_desc.parent = 1;
        slot_desc.slotType = int_ttype;
        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    ASSERT_TRUE(
            DescriptorTbl::create(&_runtime_state, &_object_pool, t_desc_table, &_desc_tbl, config::vector_chunk_size)
                    .ok());
    _runtime_state.set_desc_tbl(_desc_tbl);

    _tnode.node_id = 1;
    _tnode.node_type = TPlanNodeType::TABLE_FUNCTION_NODE;
    _tnode.num_children = 1;

    _tnode.row_tuples.push_back(1);

    TExprNode expr_node;
    expr_node.__isset.fn = true;
    expr_node.fn.name.function_name = "unnest";
    expr_node.fn.arg_types.push_back(array_ttype);
    expr_node.fn.table_fn.ret_types.push_back(int_ttype);

    _tnode.table_function_node.table_function.nodes.push_back(expr_node);

    _tnode.table_function_node.__isset.param_columns = true;
    _tnode.table_function_node.param_columns.emplace_back(1);

    _tnode.table_function_node.__isset.outer_columns = true;
    _tnode.table_function_node.outer_columns.emplace_back(2);

    _tnode.table_function_node.__isset.fn_result_columns = true;
    _tnode.table_function_node.fn_result_columns.emplace_back(3);
}

TEST_F(TableFunctionOperatorTest, check_mem_leak) {
    CounterPtr counter_ptr = std::make_shared<Counter>();
    TestNormalOperatorFactory factory(1, 1, counter_ptr, &_tnode);
    TableFunctionOperator op(&factory, 1, 1, 0, _tnode);
    ASSERT_TRUE(op.prepare(&_runtime_state).ok());
    op.close(&_runtime_state);
}

TEST_F(TableFunctionOperatorTest, key_partition_exchanger) {
    auto pseudo_plan_node_id = -200;
    auto mem_mgr = std::make_shared<ChunkBufferMemoryManager>(4096, 134217728);

    ChunkPtr chunk = std::make_shared<Chunk>();
    auto c = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true);
    c->append_datum(Datum(1));
    c->append_datum(Datum(2));
    c->append_datum(Datum(1));
    c->append_datum(Datum(2));
    auto null_datum = Datum();
    null_datum.set_null();
    c->append_datum(null_datum);
    c->append_datum(null_datum);
    chunk->append_column(std::move(c), 0);
    ObjectPool _pool;
    auto _runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));

    std::vector<TExprNode> nodes;
    TExprNode node1;
    node1.node_type = TExprNodeType::SLOT_REF;
    node1.type = gen_type_desc(TPrimitiveType::INT);
    node1.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = 0;
    t_slot_ref.tuple_id = 0;
    node1.__set_slot_ref(t_slot_ref);
    node1.is_nullable = true;
    nodes.emplace_back(node1);

    TExpr t_expr;
    t_expr.nodes = nodes;

    std::vector<TExpr> t_conjuncts;
    t_conjuncts.emplace_back(t_expr);
    std::vector<ExprContext*> expr_ctxs;
    Expr::create_expr_trees(&_pool, t_conjuncts, &expr_ctxs, nullptr);
    Expr::prepare(expr_ctxs, _runtime_state);
    Expr::open(expr_ctxs, _runtime_state);

    auto local_exchange_source = std::make_shared<LocalExchangeSourceOperatorFactory>(1, pseudo_plan_node_id, mem_mgr);
    local_exchange_source->set_degree_of_parallelism(1);
    auto source = local_exchange_source->create(1, 1);
    auto local_exchange = std::make_shared<KeyPartitionExchanger>(mem_mgr, local_exchange_source.get(), expr_ctxs, 1,
                                                                  std::vector<std::string>{"identity"});

    local_exchange->accept(chunk, 0);

    auto local_exchange2 = std::make_shared<KeyPartitionExchanger>(mem_mgr, local_exchange_source.get(), expr_ctxs, 1,
                                                                   std::vector<std::string>{});
    local_exchange2->accept(chunk, 0);
}

} // namespace starrocks::pipeline
