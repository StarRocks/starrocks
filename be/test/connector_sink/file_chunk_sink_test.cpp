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

#include "connector/file_chunk_sink.h"

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "connector/connector_chunk_sink.h"
#include "connector/sink_memory_manager.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/file_writer.h"
#include "formats/utils.h"
#include "testutil/assert.h"
#include "util/defer_op.h"

namespace starrocks::connector {
namespace {

using CommitResult = formats::FileWriter::CommitResult;
using WriterAndStream = formats::WriterAndStream;
using ::testing::Return;
using ::testing::ByMove;
using ::testing::_;

class FileChunkSinkTest : public ::testing::Test {
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

class MockFileWriterFactory : public formats::FileWriterFactory {
public:
    MOCK_METHOD(Status, init, (), (override));
    MOCK_METHOD(StatusOr<formats::WriterAndStream>, create, (const std::string&), (const override));
};

TEST_F(FileChunkSinkTest, test_callback) {
    {
        std::vector<std::string> partition_column_names = {"k1"};
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators =
                ColumnSlotIdEvaluator::from_types({TypeDescriptor::from_logical_type(TYPE_VARCHAR)});
        auto mock_writer_factory = std::make_unique<MockFileWriterFactory>();
        auto location_provider = std::make_unique<LocationProvider>("base_path", "ffffff", 0, 0, "parquet");
        auto sink = std::make_unique<FileChunkSink>(partition_column_names, std::move(partition_column_evaluators),
                                                    std::move(location_provider), std::move(mock_writer_factory), 100,
                                                    _runtime_state);
        sink->callback_on_commit(CommitResult{
                .io_status = Status::OK(),
                .format = formats::PARQUET,
                .file_statistics =
                        {
                                .record_count = 100,
                        },
                .location = "path/to/directory/data.parquet",
        });

        EXPECT_EQ(_runtime_state->num_rows_load_sink(), 100);
    }
}

TEST_F(FileChunkSinkTest, test_factory) {
    FileChunkSinkProvider provider;

    {
        auto sink_ctx = std::make_shared<connector::FileChunkSinkContext>();
        sink_ctx->path = "/path/to/directory/";
        sink_ctx->column_names = {"k1", "k2"};
        sink_ctx->partition_column_indices = {0};
        sink_ctx->executor = nullptr;
        sink_ctx->format = formats::PARQUET; // iceberg sink only supports parquet
        sink_ctx->compression_type = TCompressionType::NO_COMPRESSION;
        sink_ctx->options = {}; // default for now
        sink_ctx->max_file_size = 1 << 30;
        sink_ctx->column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_VARCHAR), TypeDescriptor::from_logical_type(TYPE_INT)});
        sink_ctx->fragment_context = _fragment_context.get();
        auto sink = provider.create_chunk_sink(sink_ctx, 0).value();
        SinkOperatorMemoryManager mm;
        sink->set_operator_mem_mgr(&mm);
        EXPECT_OK(sink->init());
    }

    {
        auto sink_ctx = std::make_shared<connector::FileChunkSinkContext>();
        sink_ctx->path = "/path/to/directory/";
        sink_ctx->column_names = {"k1", "k2"};
        sink_ctx->partition_column_indices = {0};
        sink_ctx->executor = nullptr;
        sink_ctx->format = formats::PARQUET;
        sink_ctx->compression_type = TCompressionType::NO_COMPRESSION;
        sink_ctx->options = {};
        sink_ctx->max_file_size = 1 << 30;
        sink_ctx->column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_VARCHAR), TypeDescriptor::from_logical_type(TYPE_INT)});
        sink_ctx->fragment_context = _fragment_context.get();
        auto sink = provider.create_chunk_sink(sink_ctx, 0).value();
        SinkOperatorMemoryManager mm;
        sink->set_operator_mem_mgr(&mm);
        EXPECT_OK(sink->init());
    }

    {
        auto sink_ctx = std::make_shared<connector::FileChunkSinkContext>();
        sink_ctx->path = "/path/to/directory/";
        sink_ctx->column_names = {"k1", "k2"};
        sink_ctx->partition_column_indices = {0};
        sink_ctx->executor = nullptr;
        sink_ctx->format = "unknown";
        sink_ctx->compression_type = TCompressionType::NO_COMPRESSION;
        sink_ctx->options = {}; // default for now
        sink_ctx->max_file_size = 1 << 30;
        sink_ctx->column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_VARCHAR), TypeDescriptor::from_logical_type(TYPE_INT)});
        sink_ctx->fragment_context = _fragment_context.get();
        auto sink = provider.create_chunk_sink(sink_ctx, 0).value();
        SinkOperatorMemoryManager mm;
        sink->set_operator_mem_mgr(&mm);
        EXPECT_ERROR(sink->init());
    }
}

} // namespace
} // namespace starrocks::connector
