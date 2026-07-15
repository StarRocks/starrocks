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

#include "data_sink/pipeline/pipeline_sink_dispatcher.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "exec/pipeline/fragment_execution_params.h"
#include "exec/runtime/query_context.h"
#include "runtime/descriptors.h"

namespace starrocks::pipeline {
namespace {

class TestPipelineSinkGraph final : public PipelineSinkGraph {};

struct PipelineSinkTestRequest {
    explicit PipelineSinkTestRequest(TDataSinkType::type type) {
        TDataSink sink;
        sink.__set_type(type);

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

StatusOr<std::shared_ptr<const PipelineSinkRegistry>> make_registry(TDataSinkType::type selected_type,
                                                                    SinkRole selected_role,
                                                                    PipelineSinkBuildFn selected_build) {
    PipelineSinkRegistryBuilder builder;
    for (const auto& [value, name] : _TDataSinkType_VALUES_TO_NAMES) {
        auto type = static_cast<TDataSinkType::type>(value);
        PipelineSinkProvider provider{
                .type = type,
                .name = name,
                .role = SinkRole::INTERMEDIATE,
                .build = [](PipelineSinkBuildContext&&) { return Status::OK(); },
        };
        if (type == selected_type) {
            provider.role = selected_role;
            provider.build = selected_build;
        }
        RETURN_IF_ERROR(builder.add(std::move(provider)));
    }
    return builder.freeze();
}

TEST(PipelineSinkDispatcherTest, InvokesIntermediateProviderWithBuildContext) {
    TestPipelineSinkGraph graph;
    RowDescriptor row_desc;
    PipelineSinkTestRequest test_request(TDataSinkType::DATA_STREAM_SINK);
    UnifiedExecPlanFragmentParams request(test_request.common, test_request.unique);
    QueryContext query_context;
    bool invoked = false;

    auto registry = make_registry(TDataSinkType::DATA_STREAM_SINK, SinkRole::INTERMEDIATE,
                                  [&](PipelineSinkBuildContext&& context) {
                                      invoked = true;
                                      EXPECT_EQ(&graph, &context.graph);
                                      EXPECT_EQ(&request, &context.request);
                                      EXPECT_EQ(&row_desc, &context.row_desc);
                                      EXPECT_EQ(2, context.upstream.size());
                                      return Status::OK();
                                  });
    ASSERT_OK(registry.status());

    OpFactories upstream(2);
    PipelineSinkBuildContext context(graph, std::move(upstream), request, row_desc);
    ASSERT_OK(PipelineSinkDispatcher::build(*registry.value(), TDataSinkType::DATA_STREAM_SINK, query_context,
                                            std::move(context)));
    EXPECT_TRUE(invoked);
    EXPECT_FALSE(query_context.is_final_sink());
}

TEST(PipelineSinkDispatcherTest, MarksFinalSinkBeforeInvocation) {
    TestPipelineSinkGraph graph;
    RowDescriptor row_desc;
    PipelineSinkTestRequest test_request(TDataSinkType::RESULT_SINK);
    UnifiedExecPlanFragmentParams request(test_request.common, test_request.unique);
    QueryContext query_context;
    bool invoked = false;

    auto registry = make_registry(TDataSinkType::RESULT_SINK, SinkRole::FINAL, [&](PipelineSinkBuildContext&&) {
        invoked = true;
        EXPECT_TRUE(query_context.is_final_sink());
        return Status::OK();
    });
    ASSERT_OK(registry.status());

    PipelineSinkBuildContext context(graph, {}, request, row_desc);
    ASSERT_OK(PipelineSinkDispatcher::build(*registry.value(), TDataSinkType::RESULT_SINK, query_context,
                                            std::move(context)));
    EXPECT_TRUE(invoked);
    EXPECT_TRUE(query_context.is_final_sink());
}

TEST(PipelineSinkDispatcherTest, PropagatesProviderFailureAfterFinalMark) {
    TestPipelineSinkGraph graph;
    RowDescriptor row_desc;
    PipelineSinkTestRequest test_request(TDataSinkType::RESULT_SINK);
    UnifiedExecPlanFragmentParams request(test_request.common, test_request.unique);
    QueryContext query_context;

    auto registry = make_registry(TDataSinkType::RESULT_SINK, SinkRole::FINAL, [&](PipelineSinkBuildContext&&) {
        EXPECT_TRUE(query_context.is_final_sink());
        return Status::InternalError("injected provider failure");
    });
    ASSERT_OK(registry.status());

    PipelineSinkBuildContext context(graph, {}, request, row_desc);
    auto status = PipelineSinkDispatcher::build(*registry.value(), TDataSinkType::RESULT_SINK, query_context,
                                                std::move(context));
    EXPECT_TRUE(status.is_internal_error()) << status;
    EXPECT_EQ("injected provider failure", status.message());
    EXPECT_TRUE(query_context.is_final_sink());
}

TEST(PipelineSinkDispatcherTest, RejectsUnknownTypeWithoutInvokingProvider) {
    TestPipelineSinkGraph graph;
    RowDescriptor row_desc;
    PipelineSinkTestRequest test_request(TDataSinkType::RESULT_SINK);
    UnifiedExecPlanFragmentParams request(test_request.common, test_request.unique);
    QueryContext query_context;
    bool invoked = false;

    auto registry = make_registry(TDataSinkType::RESULT_SINK, SinkRole::FINAL, [&](PipelineSinkBuildContext&&) {
        invoked = true;
        return Status::OK();
    });
    ASSERT_OK(registry.status());

    PipelineSinkBuildContext context(graph, {}, request, row_desc);
    auto status = PipelineSinkDispatcher::build(*registry.value(), static_cast<TDataSinkType::type>(999), query_context,
                                                std::move(context));
    EXPECT_TRUE(status.is_internal_error()) << status;
    EXPECT_NE(status.message().find("999"), std::string_view::npos);
    EXPECT_FALSE(invoked);
    EXPECT_FALSE(query_context.is_final_sink());
}

} // namespace
} // namespace starrocks::pipeline
