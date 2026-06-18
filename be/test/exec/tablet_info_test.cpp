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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/exec/tablet_info_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/tablet_info.h"

#include <gtest/gtest.h>

#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

class OlapTablePartitionParamTest : public testing::Test {
public:
    OlapTablePartitionParamTest() = default;
    ~OlapTablePartitionParamTest() override = default;
    void SetUp() override {}
};

TOlapTableSchemaParam get_schema(TDescriptorTable* desc_tbl) {
    TOlapTableSchemaParam t_schema_param;
    t_schema_param.db_id = 1;
    t_schema_param.table_id = 2;
    t_schema_param.version = 0;

    // descriptor
    {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).column_name("c1").column_pos(1).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("c2").column_pos(2).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder().string_type(20).column_name("c3").column_pos(3).build());

        tuple_builder.build(&dtb);

        *desc_tbl = dtb.desc_tbl();
        t_schema_param.slot_descs = desc_tbl->slotDescriptors;
        t_schema_param.tuple_desc = desc_tbl->tupleDescriptors[0];
    }
    // index
    t_schema_param.indexes.resize(2);
    t_schema_param.indexes[0].id = 4;
    t_schema_param.indexes[0].columns = {"c1", "c2", "c3"};
    t_schema_param.indexes[1].id = 5;
    t_schema_param.indexes[1].columns = {"c1", "c3"};

    return t_schema_param;
}

TEST_F(OlapTablePartitionParamTest, to_protobuf) {
    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    auto st = schema->init(t_schema);
    ASSERT_TRUE(st.ok());
    POlapTableSchemaParam pschema;
    schema->to_protobuf(&pschema);
    {
        std::shared_ptr<OlapTableSchemaParam> schema2(new OlapTableSchemaParam());
        auto st = schema2->init(pschema);
        ASSERT_TRUE(st.ok());

        ASSERT_STREQ(schema->debug_string().c_str(), schema2->debug_string().c_str());
    }
}

TEST_F(OlapTablePartitionParamTest, unknown_distributed_col) {
    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    auto st = schema->init(t_schema);
    ASSERT_TRUE(st.ok());

    // (-oo, 10] | [10.50) | [60, +oo)
    TOlapTablePartitionParam t_partition_param;
    t_partition_param.db_id = 1;
    t_partition_param.table_id = 2;
    t_partition_param.version = 0;
    t_partition_param.__set_distributed_columns({"c4"});
    t_partition_param.partitions.resize(1);
    t_partition_param.partitions[0].id = 10;
    t_partition_param.partitions[0].indexes.resize(2);
    t_partition_param.partitions[0].indexes[0].index_id = 4;
    t_partition_param.partitions[0].indexes[0].tablet_ids = {21};
    t_partition_param.partitions[0].indexes[1].index_id = 5;

    OlapTablePartitionParam part(schema, t_partition_param);
    st = part.init(nullptr);
    ASSERT_FALSE(st.ok());
}

TEST_F(OlapTablePartitionParamTest, bad_index) {
    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    auto st = schema->init(t_schema);
    ASSERT_TRUE(st.ok());

    {
        // (-oo, 10] | [10.50) | [60, +oo)
        TOlapTablePartitionParam t_partition_param;
        t_partition_param.db_id = 1;
        t_partition_param.table_id = 2;
        t_partition_param.version = 0;
        t_partition_param.__set_distributed_columns({"c1", "c3"});
        t_partition_param.partitions.resize(1);
        t_partition_param.partitions[0].id = 10;
        t_partition_param.partitions[0].indexes.resize(1);
        t_partition_param.partitions[0].indexes[0].index_id = 4;
        t_partition_param.partitions[0].indexes[0].tablet_ids = {21};

        OlapTablePartitionParam part(schema, t_partition_param);
        st = part.init(nullptr);
        ASSERT_FALSE(st.ok());
    }
    {
        // (-oo, 10] | [10.50) | [60, +oo)
        TOlapTablePartitionParam t_partition_param;
        t_partition_param.db_id = 1;
        t_partition_param.table_id = 2;
        t_partition_param.version = 0;
        t_partition_param.__set_partition_column("c4");
        t_partition_param.__set_distributed_columns({"c1", "c3"});
        t_partition_param.partitions.resize(1);
        t_partition_param.partitions[0].id = 10;
        t_partition_param.partitions[0].indexes.resize(2);
        t_partition_param.partitions[0].indexes[0].index_id = 4;
        t_partition_param.partitions[0].indexes[0].tablet_ids = {21};
        t_partition_param.partitions[0].indexes[1].index_id = 6;

        OlapTablePartitionParam part(schema, t_partition_param);
        st = part.init(nullptr);
        ASSERT_FALSE(st.ok());
    }
}

TEST_F(OlapTablePartitionParamTest, tableLoacation) {
    TOlapTableLocationParam tparam;
    tparam.tablets.resize(1);
    tparam.tablets[0].tablet_id = 1;
    OlapTableLocationParam location(tparam);
    {
        auto loc = location.find_tablet(1);
        ASSERT_TRUE(loc != nullptr);
    }
    {
        auto loc = location.find_tablet(2);
        ASSERT_TRUE(loc == nullptr);
    }
}

TEST_F(OlapTablePartitionParamTest, NodesInfo) {
    TNodesInfo tinfo;
    tinfo.nodes.resize(1);
    tinfo.nodes[0].id = 1;
    StarRocksNodesInfo nodes(tinfo);
    {
        auto node = nodes.find_node(1);
        ASSERT_TRUE(node != nullptr);
    }
    {
        auto node = nodes.find_node(2);
        ASSERT_TRUE(node == nullptr);
    }
}

// Build a single slot-ref TExpr referencing slot 0 of tuple 0, typed INT.
static TExpr make_slot_ref_texpr() {
    TExpr texpr;
    TExprNode node;
    node.node_type = TExprNodeType::SLOT_REF;
    node.type = gen_type_desc(TPrimitiveType::INT);
    node.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = 0;
    t_slot_ref.tuple_id = 0;
    node.__set_slot_ref(t_slot_ref);
    node.is_nullable = true;
    texpr.nodes.emplace_back(node);
    return texpr;
}

static RuntimeState* make_runtime_state(ObjectPool* pool) {
    TUniqueId query_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    TUniqueId fragment_id;
    auto* exec_env = ExecEnv::GetInstance();
    auto* state = pool->add(new RuntimeState(query_id, fragment_id, query_options, query_globals, exec_env));
    state->init_mem_trackers(query_id);
    return state;
}

TEST_F(OlapTablePartitionParamTest, per_index_distributed_exprs_parsed) {
    ObjectPool pool;
    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    t_schema.indexes[0].__set_distributed_exprs({make_slot_ref_texpr()});

    auto* state = make_runtime_state(&pool);
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    auto st = schema->init(t_schema, state);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // index_id 4 == indexes[0]
    const OlapTableIndexSchema* idx = nullptr;
    for (auto* i : schema->indexes()) {
        if (i->index_id == 4) {
            idx = i;
        }
    }
    ASSERT_TRUE(idx != nullptr);
    ASSERT_TRUE(idx->has_distributed_exprs);
    ASSERT_EQ(1u, idx->distributed_expr_ctxs.size());
}

TEST_F(OlapTablePartitionParamTest, per_index_distributed_exprs_unset_falls_back) {
    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    // no distributed_exprs set on any index
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    auto st = schema->init(t_schema, nullptr);
    ASSERT_TRUE(st.ok()) << st.to_string();

    for (auto* idx : schema->indexes()) {
        ASSERT_FALSE(idx->has_distributed_exprs);
        ASSERT_TRUE(idx->distributed_expr_ctxs.empty());
    }
}

TEST_F(OlapTablePartitionParamTest, per_index_distributed_exprs_nonempty_requires_state) {
    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    t_schema.indexes[0].__set_distributed_exprs({make_slot_ref_texpr()});

    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    auto st = schema->init(t_schema, nullptr);
    ASSERT_FALSE(st.ok());
}

TEST_F(OlapTablePartitionParamTest, removePartition) {
    MutableColumns columns;
    for (size_t i = 0; i < 2; i++) {
        auto column = FixedLengthColumn<int32_t>::create();
        for (int j = 0; j < 5; j++) {
            column->append(j);
        }
        columns.emplace_back(column);
    }
    ChunkRow row1(&columns, 0);
    ChunkRow row2(&columns, 1);
    ChunkRow row3(&columns, 2);

    OlapTablePartition partition1;
    partition1.id = 10;

    OlapTablePartition partition2;
    partition2.id = 11;
    partition2.end_key = row1;

    OlapTablePartition partition3;
    partition3.id = 12;
    partition3.in_keys.push_back(row2);

    TDescriptorTable t_desc_tbl;
    auto t_schema = get_schema(&t_desc_tbl);
    std::shared_ptr<OlapTableSchemaParam> schema(new OlapTableSchemaParam());
    TOlapTablePartitionParam t_partition_param;
    OlapTablePartitionParam part(schema, t_partition_param);

    ASSERT_TRUE(part.test_add_partitions(&partition1).ok());
    ASSERT_TRUE(part.remove_partitions({10}).ok());

    ASSERT_TRUE(part.test_add_partitions(&partition2).ok());
    ASSERT_TRUE(part.test_add_partitions(&partition3).ok());
    ASSERT_TRUE(part.remove_partitions({11, 12}).ok());
}

} // namespace starrocks
