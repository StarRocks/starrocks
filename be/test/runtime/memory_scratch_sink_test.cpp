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
//   https://github.com/apache/incubator-doris/blob/master/be/test/runtime/memory_scratch_sink_test.cpp

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

#include "runtime/memory_scratch_sink.h"

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/visitor.h>
#include <arrow/visitor_inline.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "column/chunk.h"
#include "common/config.h"
#include "common/logging.h"
#include "exec/csv_scanner.h"
#include "exprs/expr.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/runtime_state.h"
#include "storage/options.h"
#include "testutil/desc_tbl_builder.h"
#include "types/logical_type.h"
#include "util/blocking_queue.hpp"
#include "util/logging.h"

namespace starrocks {

class MemoryScratchSinkTest : public testing::Test {
public:
    MemoryScratchSinkTest() {
        {
            TExpr expr;
            {
                // first int_column
                TExprNode node;
                node.node_type = TExprNodeType::SLOT_REF;
                node.num_children = 0;
                TSlotRef slot_ref;
                slot_ref.__set_slot_id(0);
                slot_ref.__set_tuple_id(0);

                ::starrocks::TTypeDesc desc;
                TTypeNode type_node;
                type_node.__set_type(TTypeNodeType::type::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type::INT);
                type_node.__set_scalar_type(scalar_type);
                desc.__set_types({type_node});

                node.__set_slot_ref(slot_ref);
                node.__set_type(desc);

                expr.nodes.push_back(node);
            }
            _exprs.push_back(expr);
        }
    }

    ~MemoryScratchSinkTest() override { delete _state; }

    void SetUp() override {
        config::periodic_counter_update_period_ms = 500;
        _default_storage_root_path = config::storage_root_path;
        config::storage_root_path = "./data";

        system("mkdir -p ./test_run/output/");
        system("pwd");
        system("cp -r ./be/test/runtime/test_data/ ./test_run/.");

        init();
    }

    void TearDown() override {
        _obj_pool.clear();
        system("rm -rf ./test_run");
        config::storage_root_path = _default_storage_root_path;
    }

    std::unique_ptr<CSVScanner> create_csv_scanner(const std::vector<TypeDescriptor>& types,
                                                   const std::vector<TBrokerRangeDesc>& ranges,
                                                   const string& multi_row_delimiter = "\n",
                                                   const string& multi_column_separator = "|") {
        /// Init DescriptorTable
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        for (auto& t : types) {
            TSlotDescriptorBuilder slot_desc_builder;
            slot_desc_builder.type(t).length(t.len).precision(t.precision).scale(t.scale).nullable(true);
            tuple_desc_builder.add_slot(slot_desc_builder.build());
        }
        tuple_desc_builder.build(&desc_tbl_builder);

        RuntimeState* state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr));
        DescriptorTbl* desc_tbl = nullptr;
        Status st = DescriptorTbl::create(state, &_obj_pool, desc_tbl_builder.desc_tbl(), &desc_tbl,
                                          config::vector_chunk_size);
        CHECK(st.ok()) << st.to_string();

        /// Init RuntimeState
        state->set_desc_tbl(desc_tbl);
        state->init_instance_mem_tracker();

        /// TBrokerScanRangeParams
        TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
        params->__set_multi_row_delimiter(multi_row_delimiter);
        params->__set_multi_column_separator(multi_column_separator);
        params->strict_mode = true;
        params->dest_tuple_id = 0;
        params->src_tuple_id = 0;
        for (int i = 0; i < types.size(); i++) {
            params->expr_of_dest_slot[i] = TExpr();
            params->expr_of_dest_slot[i].nodes.emplace_back(TExprNode());
            params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
            params->expr_of_dest_slot[i].nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
            params->expr_of_dest_slot[i].nodes[0].__set_is_nullable(true);
            params->expr_of_dest_slot[i].nodes[0].__set_slot_ref(TSlotRef());
            params->expr_of_dest_slot[i].nodes[0].slot_ref.__set_slot_id(i);
            params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
        }

        for (int i = 0; i < types.size(); i++) {
            params->src_slot_ids.emplace_back(i);
        }

        RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));

        ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

        TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;
        return std::make_unique<CSVScanner>(state, profile, *broker_scan_range, counter);
    }

    void init();
    void init_desc_tbl();
    void init_runtime_state();

private:
    ObjectPool _obj_pool;
    ExecEnv* _exec_env = nullptr;
    // std::vector<TExpr> _exprs;
    TDescriptorTable _t_desc_table;
    RuntimeState* _state = nullptr;
    TPlanNode _tnode;
    RowDescriptor* _row_desc = nullptr;
    TMemoryScratchSink _tsink;
    DescriptorTbl* _desc_tbl = nullptr;
    std::vector<TExpr> _exprs;
    std::string _default_storage_root_path;
};

void MemoryScratchSinkTest::init() {
    _exec_env = ExecEnv::GetInstance();
    init_runtime_state();
    init_desc_tbl();
}

void MemoryScratchSinkTest::init_runtime_state() {
    TQueryOptions query_options;
    query_options.batch_size = 1024;
    TUniqueId query_id;
    query_id.lo = 10;
    query_id.hi = 100;
    _state = new RuntimeState(query_id, query_options, TQueryGlobals(), _exec_env);
    _state->init_instance_mem_tracker();
    _state->set_desc_tbl(_desc_tbl);
    _state->init_mem_trackers(TUniqueId());
}

void MemoryScratchSinkTest::init_desc_tbl() {
    // TTableDescriptor
    TTableDescriptor t_table_desc;
    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_table_desc.olapTable.tableName = "test";
    t_table_desc.tableName = "test_table_name";
    t_table_desc.dbName = "test_db_name";
    t_table_desc.__isset.olapTable = true;

    _t_desc_table.tableDescriptors.push_back(t_table_desc);
    _t_desc_table.__isset.tableDescriptors = true;

    // TSlotDescriptor
    std::vector<TSlotDescriptor> slot_descs;
    int offset = 1;
    int i = 0;
    // int_column
    {
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(i);
        t_slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::INT));
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(-1);
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);
        t_slot_desc.__set_colName("second_column");
        t_slot_desc.__set_parent(0);

        slot_descs.push_back(t_slot_desc);
        offset += sizeof(int32_t);
    }
    _t_desc_table.__set_slotDescriptors(slot_descs);

    // TTupleDescriptor
    TTupleDescriptor t_tuple_desc;
    t_tuple_desc.id = 0;
    t_tuple_desc.byteSize = offset;
    t_tuple_desc.numNullBytes = 1;
    t_tuple_desc.tableId = 0;
    t_tuple_desc.__isset.tableId = true;
    _t_desc_table.tupleDescriptors.push_back(t_tuple_desc);

    DescriptorTbl::create(_state, &_obj_pool, _t_desc_table, &_desc_tbl, config::vector_chunk_size);

    std::vector<TTupleId> row_tids;
    row_tids.push_back(0);

    std::vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);
    _row_desc = _obj_pool.add(new RowDescriptor(*_desc_tbl, row_tids, nullable_tuples));

    // node
    _tnode.node_id = 0;
    _tnode.node_type = TPlanNodeType::CSV_SCAN_NODE;
    _tnode.num_children = 0;
    _tnode.limit = -1;
    _tnode.row_tuples.push_back(0);
    _tnode.nullable_tuples.push_back(false);
}

TEST_F(MemoryScratchSinkTest, work_flow_normal) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range_one;
    range_one.__set_path("./be/test/runtime/test_data/csv_data");
    range_one.__set_start_offset(0);
    range_one.__set_num_of_columns_from_file(types.size());
    ranges.push_back(range_one);

    auto scanner = create_csv_scanner(types, ranges);
    EXPECT_NE(scanner, nullptr);

    auto st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    MemoryScratchSink sink(*_row_desc, _exprs, _tsink);
    TDataSink data_sink;
    data_sink.memory_scratch_sink = _tsink;
    ASSERT_TRUE(sink.init(data_sink, nullptr).ok());
    ASSERT_TRUE(sink.prepare(_state).ok());

    auto maybe_chunk = scanner->get_next();
    ASSERT_TRUE(maybe_chunk.ok());
    auto chunk = maybe_chunk.value();
    int num = chunk->num_rows();

    ASSERT_EQ(6, num);
    ASSERT_TRUE(sink.send_chunk(_state, chunk.get()).ok());
    ASSERT_TRUE(sink.close(_state, Status::OK()).ok());
}

} // namespace starrocks
