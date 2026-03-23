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

#include "exec/repeat_node.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/config_exec_fwd.h"
#include "common/object_pool.h"
#include "exec/exec_node.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/user_function_cache.h"

namespace starrocks {

class MockExchangeNode : public ExecNode {
public:
    MockExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs) {}

    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override {
        if (times == 0) {
            ++times;
            auto first_column = FixedLengthColumn<int32_t>::create();
            first_column->append(0);
            first_column->append(1);
            first_column->append(2);

            auto second_column = FixedLengthColumn<int32_t>::create();
            second_column->append(0);
            second_column->append(11);
            second_column->append(22);

            auto result_chunk = std::make_shared<Chunk>();
            result_chunk->append_column(std::move(first_column), 0);
            result_chunk->append_column(std::move(second_column), 1);

            *chunk = std::move(result_chunk);
        } else {
            ++times;
            *eos = true;
        }

        return Status::OK();
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override { return Status::OK(); }

    Status prepare(RuntimeState* state) override { return Status::OK(); }

    Status open(RuntimeState* state) override { return Status::OK(); }

    void close(RuntimeState* state) override {}

private:
    int times{0};
};

class RepeatNodeTest : public testing::Test {
public:
    RepeatNodeTest() : _runtime_state(TQueryGlobals()) {}

protected:
    void SetUp() override {
        TDescriptorTable t_desc_table;

        // table descriptors
        TTableDescriptor t_table_desc;

        t_table_desc.id = 0;
        t_table_desc.tableType = TTableType::MYSQL_TABLE;
        t_table_desc.numCols = 0;
        t_table_desc.numClusteringCols = 0;
        t_desc_table.tableDescriptors.push_back(t_table_desc);
        t_desc_table.__isset.tableDescriptors = true;

        {
            // TTupleDescriptor source
            TTupleDescriptor t_tuple_desc;
            // tuple_id for _output_tuple_id used in RepeatNode
            t_tuple_desc.id = 1;
            t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
        }

        {
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::VARCHAR);
                scalar_type.__set_len(10);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }

            for (int i = 0; i < 3; ++i) {
                TSlotDescriptor slot_desc;
                // slot_ids for
                // grouping_id and virtual columns of grouping()/grouping_id() functions
                slot_desc.id = 2 + i;

                // tuple_id use in RepeatNode, slots is included in this tuple.
                slot_desc.parent = 1;
                slot_desc.slotType = type;
                t_desc_table.slotDescriptors.push_back(slot_desc);
            }
        }

        ASSERT_TRUE(
                DescriptorTbl::create(&_runtime_state, &_obj_pool, t_desc_table, &_desc_tbl, config::vector_chunk_size)
                        .ok());

        _runtime_state.set_desc_tbl(_desc_tbl);

        // Node Id
        _tnode.node_id = 1;
        _tnode.node_type = TPlanNodeType::REPEAT_NODE;
        _tnode.num_children = 1;

        // slot_id_set_list for the data colums that coply from chunk.
        {
            // (NULL, NULL)
            std::set<SlotId> first_repeat;
            _tnode.repeat_node.slot_id_set_list.push_back(first_repeat);

            // (c1, NULL)
            std::set<SlotId> second_repeat;
            second_repeat.insert(0);
            _tnode.repeat_node.slot_id_set_list.push_back(second_repeat);

            // (c1, c2)
            std::set<SlotId> third_repeat;
            third_repeat.insert(0);
            third_repeat.insert(1);
            _tnode.repeat_node.slot_id_set_list.push_back(third_repeat);
        }

        //_all_slot_ids for the all columns
        {
            _tnode.repeat_node.all_slot_ids.insert(0);
            _tnode.repeat_node.all_slot_ids.insert(1);
        }

        //repeat_id_list repeat_ids is the same with grouping_list[0].
        {
            _tnode.repeat_node.repeat_id_list.push_back(3);
            _tnode.repeat_node.repeat_id_list.push_back(1);
            _tnode.repeat_node.repeat_id_list.push_back(0);
        }

        //grouping_list
        {
            // list for repeat_ids.
            std::vector<int64_t> one;
            one.push_back(3);
            one.push_back(1);
            one.push_back(0);

            // list for gourping()'s virtual colummn's slot_id.
            std::vector<int64_t> grouping_column;
            grouping_column.push_back(1);
            grouping_column.push_back(0);
            grouping_column.push_back(0);

            // list for gourping_id()'s virtual colummn's slot_id.
            std::vector<int64_t> grouping_id_column;
            grouping_id_column.push_back(3);
            grouping_id_column.push_back(1);
            grouping_id_column.push_back(0);

            _tnode.repeat_node.grouping_list.push_back(one);
            _tnode.repeat_node.grouping_list.push_back(grouping_column);
            _tnode.repeat_node.grouping_list.push_back(grouping_id_column);
        }

        _tnode.repeat_node.output_tuple_id = 1;

        _tnode.row_tuples.push_back(1);
    }

    void TearDown() override {}

private:
    RuntimeState _runtime_state;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    TPlanNode _tnode;
};

} // namespace starrocks
