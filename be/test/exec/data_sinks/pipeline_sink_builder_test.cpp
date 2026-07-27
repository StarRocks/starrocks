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
#include "exec/runtime/pipeline.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

struct PipelineSinkTestRequest {
    explicit PipelineSinkTestRequest(const TDataSink& sink) {
        TPlanFragmentExecParams params;
        params.__set_sender_id(0);

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
    Status build(const TDataSink& sink) {
        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>());
        _context = std::make_unique<pipeline::PipelineBuilderContext>(_fragment_context.get(), 1, 1);

        PipelineSinkTestRequest request(sink);
        pipeline::UnifiedExecPlanFragmentParams unified(request.common, request.unique);
        pipeline::OpFactories upstream{std::make_shared<pipeline::EmptySetOperatorFactory>(0, 0)};
        return PipelineSinkBuilder::build(_context.get(), std::move(upstream), unified, _row_desc);
    }

    RowDescriptor _row_desc;
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

} // namespace starrocks
