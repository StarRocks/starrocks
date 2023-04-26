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
    t_partition_param.partitions[0].num_buckets = 1;
    t_partition_param.partitions[0].indexes.resize(2);
    t_partition_param.partitions[0].indexes[0].index_id = 4;
    t_partition_param.partitions[0].indexes[0].tablets = {21};
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
        t_partition_param.partitions[0].num_buckets = 1;
        t_partition_param.partitions[0].indexes.resize(1);
        t_partition_param.partitions[0].indexes[0].index_id = 4;
        t_partition_param.partitions[0].indexes[0].tablets = {21};

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
        t_partition_param.partitions[0].num_buckets = 1;
        t_partition_param.partitions[0].indexes.resize(2);
        t_partition_param.partitions[0].indexes[0].index_id = 4;
        t_partition_param.partitions[0].indexes[0].tablets = {21};
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

} // namespace starrocks
