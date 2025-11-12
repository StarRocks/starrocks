// Copyright 2021-present StarRocks, Inc. All rights reserved.  //
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

#include "runtime/iceberg_table_sink.h"

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "exec/pipeline/empty_set_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "runtime/descriptor_helper.h"
#include "testutil/assert.h"

namespace starrocks {

class IcebergTableSinkTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>());
        _runtime_state = _fragment_context->runtime_state();
    }

    void TearDown() override {}

    ObjectPool _pool;
    std::shared_ptr<pipeline::FragmentContext> _fragment_context;
    RuntimeState* _runtime_state;
};

TEST_F(IcebergTableSinkTest, decompose_to_pipeline) {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    auto slot1 = slot_desc_builder.type(LogicalType::TYPE_INT).column_name("c1").column_pos(0).nullable(true).build();
    TTupleDescriptorBuilder tuple_desc_builder;
    tuple_desc_builder.add_slot(slot1);
    tuple_desc_builder.build(&table_desc_builder);
    DescriptorTbl* tbl = nullptr;
    EXPECT_OK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size));
    _runtime_state->set_desc_tbl(tbl);

    TIcebergTable t_iceberg_table;

    TColumn t_column;
    t_column.__set_column_name("c1");
    t_iceberg_table.__set_columns({t_column});

    TSortOrder sort_order;
    sort_order.__set_sort_key_idxes({0});
    sort_order.__set_is_ascs({true});
    sort_order.__set_is_null_firsts({true});
    t_iceberg_table.__set_sort_order(sort_order);

    TTableDescriptor tdesc;
    tdesc.__set_icebergTable(t_iceberg_table);

    IcebergTableDescriptor* ice_table_desc = _pool.add(new IcebergTableDescriptor(tdesc, &_pool));
    tbl->get_tuple_descriptor(0)->set_table_desc(ice_table_desc);
    tbl->_tbl_desc_map[0] = ice_table_desc;

    auto context = std::make_shared<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1, false);

    TDataSink data_sink;
    TIcebergTableSink iceberg_table_sink;
    data_sink.iceberg_table_sink = iceberg_table_sink;

    std::vector<starrocks::TExpr> exprs = {};
    IcebergTableSink sink(&_pool, exprs);
    auto connector = connector::ConnectorManager::default_instance()->get(connector::Connector::ICEBERG);
    auto sink_provider = connector->create_data_sink_provider();
    pipeline::OpFactories prev_operators{std::make_shared<pipeline::EmptySetOperatorFactory>(1, 1)};

    EXPECT_OK(sink.decompose_to_pipeline(prev_operators, data_sink, context.get()));

    pipeline::Pipeline* pl = const_cast<pipeline::Pipeline*>(context->last_pipeline());
    pipeline::OperatorFactory* op_factory = pl->sink_operator_factory();
    auto connector_sink_factory = dynamic_cast<pipeline::ConnectorSinkOperatorFactory*>(op_factory);
    auto sink_ctx = dynamic_cast<connector::IcebergChunkSinkContext*>(connector_sink_factory->_sink_context.get());
    EXPECT_EQ(sink_ctx->sort_ordering->sort_key_idxes.size(), 1);
    EXPECT_EQ(sink_ctx->sort_ordering->sort_descs.descs.size(), 1);
}

} // namespace starrocks
