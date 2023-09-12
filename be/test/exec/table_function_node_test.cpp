#include "exec/table_function_node.h"

#include "gtest/gtest.h"

namespace starrocks {
class TableFunctionNodeTest : public testing::Test {
public:
    TableFunctionNodeTest() : _runtime_state(TQueryGlobals()) {}

protected:
    void SetUp() override;

private:
    RuntimeState _runtime_state;
    ObjectPool _object_pool;
    DescriptorTbl* _desc_tbl = nullptr;
    TPlanNode _tnode;
};

void TableFunctionNodeTest::SetUp() {
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

    TTypeDesc type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::INT);
        scalar_type.__set_len(0);
        node.__set_scalar_type(scalar_type);
        type.types.push_back(node);
    }

    for (int i = 0; i < 3; i++) {
        TSlotDescriptor slot_desc;
        slot_desc.id = 2 + i;

        slot_desc.parent = 1;
        slot_desc.slotType = type;
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
    _tnode.nullable_tuples.push_back(false);
}

TEST_F(TableFunctionNodeTest, close_after_not_init) {
    TableFunctionNode table_function_node(&_object_pool, _tnode, *_desc_tbl);
    table_function_node.close(&_runtime_state);
}
} // namespace starrocks
