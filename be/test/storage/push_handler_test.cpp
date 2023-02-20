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

#include "storage/push_handler.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/schema.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks {

class PushHandlerTest : public testing::Test {
public:
    PushHandlerTest() { _init(); }

private:
    void _init();
    Schema _create_schema();
    void _create_src_dst_tuple(TDescriptorTable& t_desc_table);
    void _create_expr_info();
    TDescriptorTable _init_desc_table();

    TDescriptorTable _t_desc_table;
    TBrokerScanRangeParams _params;
};

Schema PushHandlerTest::_create_schema() {
    Fields fields;
    fields.emplace_back(std::make_shared<Field>(0, "k1_int", get_type_info(TYPE_INT), true));
    fields.emplace_back(std::make_shared<Field>(1, "k2_smallint", get_type_info(TYPE_SMALLINT), true));
    fields.emplace_back(std::make_shared<Field>(2, "k3_varchar", get_type_info(TYPE_VARCHAR), true));
    fields.emplace_back(std::make_shared<Field>(3, "k4_bigint", get_type_info(TYPE_BIGINT), true));
    fields.back()->set_aggregate_method(STORAGE_AGGREGATE_SUM);
    return Schema(fields);
}

void PushHandlerTest::_create_src_dst_tuple(TDescriptorTable& t_desc_table) {
    int slot_id = 0;
    // 0 is dest, 1 is src
    for (int i = 0; i < 2; ++i) {
        {
            //k1_int
            TSlotDescriptor slot_desc;

            slot_desc.id = slot_id++;
            slot_desc.parent = i;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::INT);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = -1;
            slot_desc.byteOffset = 4;
            slot_desc.nullIndicatorByte = 0;
            slot_desc.nullIndicatorBit = 1;
            slot_desc.colName = "k1_int";
            slot_desc.slotIdx = 1;
            slot_desc.isMaterialized = true;

            t_desc_table.slotDescriptors.push_back(slot_desc);
        }
        {
            // k2_smallint
            TSlotDescriptor slot_desc;

            slot_desc.id = slot_id++;
            slot_desc.parent = i;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::SMALLINT);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = -1;
            slot_desc.byteOffset = 2;
            slot_desc.nullIndicatorByte = 0;
            slot_desc.nullIndicatorBit = 0;
            slot_desc.colName = "k2_smallint";
            slot_desc.slotIdx = 0;
            slot_desc.isMaterialized = true;

            t_desc_table.slotDescriptors.push_back(slot_desc);
        }
        {
            //k3_varchar
            TSlotDescriptor slot_desc;

            slot_desc.id = slot_id++;
            slot_desc.parent = i;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::VARCHAR);
                scalar_type.__set_len(50);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = -1;
            slot_desc.byteOffset = 16;
            slot_desc.nullIndicatorByte = 0;
            slot_desc.nullIndicatorBit = 3;
            slot_desc.colName = "k3_varchar";
            slot_desc.slotIdx = 3;
            slot_desc.isMaterialized = true;

            t_desc_table.slotDescriptors.push_back(slot_desc);
        }
        {
            // v_bigint
            TSlotDescriptor slot_desc;

            slot_desc.id = slot_id++;
            slot_desc.parent = i;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::BIGINT);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = -1;
            slot_desc.byteOffset = 8;
            slot_desc.nullIndicatorByte = 0;
            slot_desc.nullIndicatorBit = 2;
            slot_desc.colName = "v_bigint";
            slot_desc.slotIdx = 2;
            slot_desc.isMaterialized = true;

            t_desc_table.slotDescriptors.push_back(slot_desc);
        }

        t_desc_table.__isset.slotDescriptors = true;
        {
            // TTupleDescriptor
            TTupleDescriptor t_tuple_desc;
            t_tuple_desc.id = i;
            t_tuple_desc.byteSize = 32;
            t_tuple_desc.numNullBytes = 1;
            t_tuple_desc.numNullSlots = 4;
            t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
        }
    }
}

TDescriptorTable PushHandlerTest::_init_desc_table() {
    TDescriptorTable t_desc_table;
    _create_src_dst_tuple(t_desc_table);
    return t_desc_table;
}

void PushHandlerTest::_create_expr_info() {
    {
        // k1_int
        TTypeDesc int_type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::INT);
            node.__set_scalar_type(scalar_type);
            int_type.types.push_back(node);
        }

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = int_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = 4;
        slot_ref.slot_ref.tuple_id = 1;
        slot_ref.__set_is_nullable(true);

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(0, expr);
        _params.src_slot_ids.push_back(4);
    }
    {
        // k2_smallint
        TTypeDesc smallint_type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::SMALLINT);
            node.__set_scalar_type(scalar_type);
            smallint_type.types.push_back(node);
        }

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = smallint_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = 5;
        slot_ref.slot_ref.tuple_id = 1;
        slot_ref.__set_is_nullable(true);

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(1, expr);
        _params.src_slot_ids.push_back(5);
    }
    {
        // k3_varchar
        TTypeDesc varchar_type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(50);
            node.__set_scalar_type(scalar_type);
            varchar_type.types.push_back(node);
        }

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = 6;
        slot_ref.slot_ref.tuple_id = 1;
        slot_ref.__set_is_nullable(true);

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(2, expr);
        _params.src_slot_ids.push_back(6);
    }
    {
        // v_bigint
        TTypeDesc bigint_type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT);
            node.__set_scalar_type(scalar_type);
            bigint_type.types.push_back(node);
        }

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = bigint_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = 7;
        slot_ref.slot_ref.tuple_id = 1;
        slot_ref.__set_is_nullable(true);

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(3, expr);
        _params.src_slot_ids.push_back(7);
    }

    _params.__set_dest_tuple_id(0);
    _params.__set_src_tuple_id(1);
}

void PushHandlerTest::_init() {
    _create_expr_info();
    _t_desc_table = _init_desc_table();
}

TEST_F(PushHandlerTest, PushBrokerReaderNormal) {
    config::vector_chunk_size = 4096;
    TBrokerScanRange broker_scan_range;
    broker_scan_range.params = _params;
    TBrokerRangeDesc range;
    range.start_offset = 0;
    range.size = LONG_MAX;
    range.format_type = TFileFormatType::FORMAT_PARQUET;
    range.splittable = false;
    range.path = "./be/test/storage/test_data/push_broker_reader.parquet";
    range.file_type = TFileType::FILE_LOCAL;
    broker_scan_range.ranges.push_back(range);

    TPushReq request;
    request.__set_desc_tbl(_t_desc_table);

    // data
    // k1_int k2_smallint varchar bigint
    // 0           0       a0      0
    // 0           2       a1      3
    // 1           4       a2      6
    PushBrokerReader reader;
    Schema schema = _create_schema();
    reader.init(broker_scan_range, request);
    ChunkPtr chunk = ChunkHelper::new_chunk(schema, 0);

    // next chunk
    reader.next_chunk(&chunk);
    ASSERT_FALSE(reader.eof());
    ASSERT_EQ(3, chunk->num_rows());
    // line 1
    ASSERT_EQ(0, chunk->get_column_by_index(0)->get(0).get_int32());
    ASSERT_EQ(0, chunk->get_column_by_index(1)->get(0).get_int16());
    ASSERT_EQ("a0", chunk->get_column_by_index(2)->get(0).get_slice().to_string());
    ASSERT_EQ(0, chunk->get_column_by_index(3)->get(0).get_int64());
    // line 2
    ASSERT_EQ(0, chunk->get_column_by_index(0)->get(1).get_int32());
    ASSERT_EQ(2, chunk->get_column_by_index(1)->get(1).get_int16());
    ASSERT_EQ("a1", chunk->get_column_by_index(2)->get(1).get_slice().to_string());
    ASSERT_EQ(3, chunk->get_column_by_index(3)->get(1).get_int64());
    // line 3
    ASSERT_EQ(1, chunk->get_column_by_index(0)->get(2).get_int32());
    ASSERT_EQ(4, chunk->get_column_by_index(1)->get(2).get_int16());
    ASSERT_EQ("a2", chunk->get_column_by_index(2)->get(2).get_slice().to_string());
    ASSERT_EQ(6, chunk->get_column_by_index(3)->get(2).get_int64());

    // eof
    reader.next_chunk(&chunk);
    ASSERT_TRUE(reader.eof());

    reader.close();
}
} // namespace starrocks
