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

#include "exec/data_sinks/pipeline_sink_builder.h"

#include <gtest/gtest.h>

#include "exec/pipeline/empty_set_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_execution_params.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/sink/blackhole_table_sink_operator.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime/query_context.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

struct PipelineSinkTestRequest {
    explicit PipelineSinkTestRequest(const TDataSink& sink, int32_t pipeline_sink_dop) {
        TPlanFragmentExecParams params;
        params.__set_sender_id(0);
        params.__set_pipeline_sink_dop(pipeline_sink_dop);

        TPlanFragment fragment;
        fragment.__set_output_sink(sink);

        common.__set_params(params);
        common.__set_fragment(fragment);
        unique.__set_params(params);
        unique.__set_fragment(fragment);
        unique.__set_backend_num(0);
        unique.__set_pipeline_dop(1);
    }

    TExecPlanFragmentParams common;
    TExecPlanFragmentParams unique;
};

} // namespace

class PipelineSinkBuilderTest : public ::testing::Test {
protected:
    Status build(const TDataSink& sink, size_t upstream_dop = 1, int32_t pipeline_sink_dop = 1) {
        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _query_context = std::make_shared<pipeline::QueryContext>();
        auto runtime_state = std::make_shared<RuntimeState>();
        runtime_state->set_query_ctx(_query_context.get(), &_query_context->query_runtime_state(),
                                     _query_context->object_pool());
        _fragment_context->set_runtime_state(std::move(runtime_state));
        _context = std::make_unique<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1);

        PipelineSinkTestRequest request(sink, pipeline_sink_dop);
        pipeline::UnifiedExecPlanFragmentParams unified(request.common, request.unique);
        auto source = std::make_shared<pipeline::EmptySetOperatorFactory>(0, 0);
        source->set_degree_of_parallelism(upstream_dop);
        pipeline::OpFactories upstream{std::move(source)};
        return PipelineSinkBuilder::build(_context.get(), std::move(upstream), unified, _row_desc);
    }

    RowDescriptor _row_desc;
    std::shared_ptr<pipeline::QueryContext> _query_context;
    std::shared_ptr<pipeline::FragmentContext> _fragment_context;
    std::unique_ptr<pipeline::PipelineBuilderContext> _context;
};

TEST_F(PipelineSinkBuilderTest, BuildsNoopSinkWithoutLegacyDataSink) {
    TDataSink sink;
    sink.__set_type(TDataSinkType::NOOP_SINK);

    ASSERT_TRUE(build(sink).ok());
    auto* pipeline = const_cast<pipeline::Pipeline*>(_context->last_pipeline());
    auto* factory = pipeline->sink_operator_factory();
    EXPECT_NE(dynamic_cast<pipeline::NoopSinkOperatorFactory*>(factory), nullptr);
}

TEST_F(PipelineSinkBuilderTest, ExplicitlyRejectsUnsupportedPipelineTypes) {
    for (auto type : {TDataSinkType::SCHEMA_TABLE_SINK, TDataSinkType::DATA_SPLIT_SINK}) {
        TDataSink sink;
        sink.__set_type(type);
        EXPECT_TRUE(build(sink).is_not_supported()) << type;
    }
}

TEST_F(PipelineSinkBuilderTest, ReportsUnknownSinkTypeByValue) {
    TDataSink sink;
    sink.__set_type(static_cast<TDataSinkType::type>(999));
    auto status = build(sink);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.message().find("999"), std::string::npos);
}

TEST_F(PipelineSinkBuilderTest, RejectsMissingSinkDescriptors) {
    for (auto type :
         {TDataSinkType::RESULT_SINK, TDataSinkType::DATA_STREAM_SINK, TDataSinkType::MULTI_CAST_DATA_STREAM_SINK,
          TDataSinkType::SPLIT_DATA_STREAM_SINK, TDataSinkType::OLAP_TABLE_SINK, TDataSinkType::MULTI_OLAP_TABLE_SINK,
          TDataSinkType::EXPORT_SINK, TDataSinkType::MYSQL_TABLE_SINK, TDataSinkType::MEMORY_SCRATCH_SINK,
          TDataSinkType::DICTIONARY_CACHE_SINK, TDataSinkType::HIVE_TABLE_SINK,
          TDataSinkType::TABLE_FUNCTION_TABLE_SINK}) {
        TDataSink sink;
        sink.__set_type(type);

        auto status = build(sink);
        EXPECT_FALSE(status.ok()) << type;
        EXPECT_NE(status.message().find("Missing"), std::string::npos) << type << ": " << status;
    }
}

TEST_F(PipelineSinkBuilderTest, RejectsMalformedConnectorSinkDescriptors) {
    THiveTableSink hive;
    hive.__set_data_column_names({"c1"});
    TDataSink hive_sink;
    hive_sink.__set_type(TDataSinkType::HIVE_TABLE_SINK);
    hive_sink.__set_hive_table_sink(hive);
    auto hive_status = build(hive_sink);
    EXPECT_FALSE(hive_status.ok());
    EXPECT_NE(hive_status.message().find("more data columns than output expressions"), std::string::npos);

    TTableFunctionTableSink table_function;
    TDataSink table_function_sink;
    table_function_sink.__set_type(TDataSinkType::TABLE_FUNCTION_TABLE_SINK);
    table_function_sink.__set_table_function_table_sink(table_function);
    auto table_function_status = build(table_function_sink);
    EXPECT_FALSE(table_function_status.ok());
    EXPECT_NE(table_function_status.message().find("Missing table function target table"), std::string::npos);
}

TEST_F(PipelineSinkBuilderTest, DispatchesIcebergSinksByPlatform) {
    for (auto type : {TDataSinkType::ICEBERG_TABLE_SINK, TDataSinkType::ICEBERG_DELETE_SINK,
                      TDataSinkType::ICEBERG_ROW_DELTA_SINK}) {
        TDataSink sink;
        sink.__set_type(type);
        auto status = build(sink);
#ifdef __APPLE__
        EXPECT_TRUE(status.is_not_supported()) << type << ": " << status;
#else
        EXPECT_FALSE(status.ok()) << type;
        EXPECT_NE(status.message().find("Missing iceberg table sink"), std::string::npos) << type << ": " << status;
#endif
    }
}

TEST_F(PipelineSinkBuilderTest, RejectsMismatchedStreamSinkCounts) {
    TMultiCastDataStreamSink multicast;
    multicast.__set_sinks({TDataStreamSink()});
    multicast.__set_destinations({});
    TDataSink multicast_sink;
    multicast_sink.__set_type(TDataSinkType::MULTI_CAST_DATA_STREAM_SINK);
    multicast_sink.__set_multi_cast_stream_sink(multicast);
    auto multicast_status = build(multicast_sink);
    EXPECT_FALSE(multicast_status.ok());
    EXPECT_NE(multicast_status.message().find("counts do not match"), std::string::npos);

    TSplitDataStreamSink split;
    split.__set_sinks({TDataStreamSink()});
    split.__set_destinations({});
    TDataSink split_sink;
    split_sink.__set_type(TDataSinkType::SPLIT_DATA_STREAM_SINK);
    split_sink.__set_split_stream_sink(split);
    auto split_status = build(split_sink);
    EXPECT_FALSE(split_status.ok());
    EXPECT_NE(split_status.message().find("counts do not match"), std::string::npos);
}

TEST_F(PipelineSinkBuilderTest, RejectsInvalidSinkDop) {
    TOlapTableSink olap;
    TDataSink olap_sink;
    olap_sink.__set_type(TDataSinkType::OLAP_TABLE_SINK);
    olap_sink.__set_olap_table_sink(olap);
    auto olap_status = build(olap_sink, 1, 0);
    EXPECT_FALSE(olap_status.ok());
    EXPECT_NE(olap_status.message().find("DOP must be positive"), std::string::npos);

    TMemoryScratchSink memory_scratch;
    TDataSink memory_sink;
    memory_sink.__set_type(TDataSinkType::MEMORY_SCRATCH_SINK);
    memory_sink.__set_memory_scratch_sink(memory_scratch);
    auto memory_status = build(memory_sink, 2);
    EXPECT_FALSE(memory_status.ok());
    EXPECT_NE(memory_status.message().find("requires DOP 1"), std::string::npos);
}

TEST_F(PipelineSinkBuilderTest, BuildsBlackholeSinkAndMarksFinalSink) {
    TDataSink sink;
    sink.__set_type(TDataSinkType::BLACKHOLE_TABLE_SINK);

    ASSERT_TRUE(build(sink).ok());
    auto* pipeline = const_cast<pipeline::Pipeline*>(_context->last_pipeline());
    auto* factory = pipeline->sink_operator_factory();
    EXPECT_NE(dynamic_cast<pipeline::BlackHoleTableSinkOperatorFactory*>(factory), nullptr);
    EXPECT_TRUE(_query_context->is_final_sink());
}

} // namespace starrocks
