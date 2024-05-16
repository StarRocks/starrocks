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
#include "exec/pipeline/fragment_context.h"
#include "formats/file_writer.h"
#include "formats/utils.h"
#include "testutil/assert.h"
#include "util/defer_op.h"

namespace starrocks::connector {
namespace {

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
    MOCK_METHOD(StatusOr<std::shared_ptr<formats::FileWriter>>, create, (const std::string&), (const override));
};

class MockFileWriter : public formats::FileWriter {
public:
    MOCK_METHOD(Status, init, (), (override));
    MOCK_METHOD(int64_t, get_written_bytes, (), (override));
    MOCK_METHOD(std::future<Status>, write, (ChunkPtr), (override));
    MOCK_METHOD(std::future<CommitResult>, commit, (), (override));
};

using ::testing::Return;
using ::testing::ByMove;
using Futures = connector::ConnectorChunkSink::Futures;
using CommitResult = formats::FileWriter::CommitResult;
using ::testing::_;

TEST_F(FileChunkSinkTest, test_unpartitioned_sink) {
    {
        std::vector<std::string> partition_column_names = {};
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators = {};
        auto mock_file_writer = std::make_shared<MockFileWriter>();
        EXPECT_CALL(*mock_file_writer, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer, write(_)).WillOnce(Return(ByMove(make_ready_future(Status::OK()))));
        auto mock_file_writer_factory = std::make_unique<MockFileWriterFactory>();
        EXPECT_CALL(*mock_file_writer_factory, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer_factory, create(_)).WillOnce(Return(ByMove(mock_file_writer)));
        auto location_provider = std::make_unique<LocationProvider>("base_path", "ffffff", 0, 0, "parquet");
        auto sink = std::make_unique<FileChunkSink>(partition_column_names, std::move(partition_column_evaluators),
                                                    std::move(location_provider), std::move(mock_file_writer_factory),
                                                    100, _runtime_state);

        EXPECT_OK(sink->init());
        auto chunk = std::make_shared<Chunk>();
        auto futures = sink->add(chunk);
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().add_chunk_futures.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().add_chunk_futures[0]));
        EXPECT_OK(futures.value().add_chunk_futures[0].get());
    }

    {
        std::vector<std::string> partition_column_names = {};
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators = {};
        auto mock_file_writer1 = std::make_shared<MockFileWriter>();
        EXPECT_CALL(*mock_file_writer1, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer1, get_written_bytes()).WillOnce(Return(100));
        EXPECT_CALL(*mock_file_writer1, write(_)).WillOnce(Return(ByMove(make_ready_future(Status::OK()))));
        EXPECT_CALL(*mock_file_writer1, commit())
                .WillOnce(Return(ByMove(make_ready_future(CommitResult{.io_status = Status::OK()}))));
        auto mock_file_writer2 = std::make_shared<MockFileWriter>();
        EXPECT_CALL(*mock_file_writer2, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer2, write(_)).WillOnce(Return(ByMove(make_ready_future(Status::OK()))));
        EXPECT_CALL(*mock_file_writer2, commit())
                .WillOnce(Return(ByMove(make_ready_future(CommitResult{.io_status = Status::OK()}))));
        auto mock_file_writer_factory = std::make_unique<MockFileWriterFactory>();
        EXPECT_CALL(*mock_file_writer_factory, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer_factory, create(_))
                .WillOnce(Return(ByMove(mock_file_writer1)))
                .WillOnce(Return(ByMove(mock_file_writer2)));
        auto location_provider = std::make_unique<LocationProvider>("base_path", "ffffff", 0, 0, "parquet");
        auto sink = std::make_unique<FileChunkSink>(partition_column_names, std::move(partition_column_evaluators),
                                                    std::move(location_provider), std::move(mock_file_writer_factory),
                                                    100, _runtime_state);

        EXPECT_OK(sink->init());
        auto futures = sink->add(nullptr);
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().commit_file_futures.size(), 0);
        EXPECT_EQ(futures.value().add_chunk_futures.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().add_chunk_futures[0]));
        EXPECT_OK(futures.value().add_chunk_futures[0].get());

        futures = sink->add(nullptr);
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().commit_file_futures.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().commit_file_futures[0]));
        EXPECT_OK(futures.value().commit_file_futures[0].get().io_status);
        EXPECT_EQ(futures.value().add_chunk_futures.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().add_chunk_futures[0]));
        EXPECT_OK(futures.value().add_chunk_futures[0].get());

        futures = sink->finish();
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().commit_file_futures.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().commit_file_futures[0]));
        EXPECT_OK(futures.value().commit_file_futures[0].get().io_status);
    }
}

TEST_F(FileChunkSinkTest, test_partitioned_sink) {
    {
        std::vector<std::string> partition_column_names = {"k1"};
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators =
                ColumnSlotIdEvaluator::from_types({TypeDescriptor::from_logical_type(TYPE_VARCHAR)});
        auto mock_file_writer = std::make_shared<MockFileWriter>();
        EXPECT_CALL(*mock_file_writer, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer, write(_)).WillOnce(Return(ByMove(make_ready_future(Status::OK()))));
        auto mock_file_writer_factory = std::make_unique<MockFileWriterFactory>();
        EXPECT_CALL(*mock_file_writer_factory, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer_factory, create(_)).WillOnce(Return(ByMove(mock_file_writer)));
        auto location_provider = std::make_unique<LocationProvider>("base_path", "ffffff", 0, 0, "parquet");
        auto sink = std::make_unique<FileChunkSink>(partition_column_names, std::move(partition_column_evaluators),
                                                    std::move(location_provider), std::move(mock_file_writer_factory),
                                                    100, _runtime_state);

        EXPECT_OK(sink->init());
        auto chunk = std::make_shared<Chunk>();
        {
            auto partition_column = BinaryColumn::create();
            partition_column->append("hello");
            chunk->append_column(partition_column, chunk->num_columns());
        }
        auto futures = sink->add(chunk);
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().add_chunk_futures.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().add_chunk_futures[0]));
        EXPECT_OK(futures.value().add_chunk_futures[0].get());
    }

    {
        std::vector<std::string> partition_column_names = {"k1"};
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators =
                ColumnSlotIdEvaluator::from_types({TypeDescriptor::from_logical_type(TYPE_VARCHAR)});
        auto mock_file_writer1 = std::make_shared<MockFileWriter>();
        EXPECT_CALL(*mock_file_writer1, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer1, write(_)).WillOnce(Return(ByMove(make_ready_future(Status::OK()))));
        EXPECT_CALL(*mock_file_writer1, commit())
                .WillOnce(Return(ByMove(make_ready_future(CommitResult{.io_status = Status::OK()}))));
        auto mock_file_writer2 = std::make_shared<MockFileWriter>();
        EXPECT_CALL(*mock_file_writer2, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer2, write(_)).WillOnce(Return(ByMove(make_ready_future(Status::OK()))));
        EXPECT_CALL(*mock_file_writer2, commit())
                .WillOnce(Return(ByMove(make_ready_future(CommitResult{.io_status = Status::OK()}))));
        auto mock_file_writer_factory = std::make_unique<MockFileWriterFactory>();
        EXPECT_CALL(*mock_file_writer_factory, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer_factory, create(_))
                .WillOnce(Return(ByMove(mock_file_writer1)))
                .WillOnce(Return(ByMove(mock_file_writer2)));
        auto location_provider = std::make_unique<LocationProvider>("base_path", "ffffff", 0, 0, "parquet");
        auto sink = std::make_unique<FileChunkSink>(partition_column_names, std::move(partition_column_evaluators),
                                                    std::move(location_provider), std::move(mock_file_writer_factory),
                                                    100, _runtime_state);

        EXPECT_OK(sink->init());
        auto chunk = std::make_shared<Chunk>();
        {
            auto partition_column = BinaryColumn::create();
            partition_column->append("hello");
            chunk->append_column(partition_column, 0);
        }
        auto futures = sink->add(chunk);
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().commit_file_futures.size(), 0);
        EXPECT_EQ(futures.value().add_chunk_futures.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().add_chunk_futures[0]));
        EXPECT_OK(futures.value().add_chunk_futures[0].get());

        chunk = std::make_shared<Chunk>();
        {
            auto partition_column = BinaryColumn::create();
            partition_column->append("world");
            chunk->append_column(partition_column, 0);
        }
        futures = sink->add(chunk);
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().commit_file_futures.size(), 0);
        EXPECT_EQ(futures.value().add_chunk_futures.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().add_chunk_futures[0]));
        EXPECT_OK(futures.value().add_chunk_futures[0].get());

        futures = sink->finish();
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().commit_file_futures.size(), 2);
        EXPECT_TRUE(is_ready(futures.value().commit_file_futures[0]));
        EXPECT_TRUE(is_ready(futures.value().commit_file_futures[1]));
        EXPECT_OK(futures.value().commit_file_futures[0].get().io_status);
        EXPECT_OK(futures.value().commit_file_futures[1].get().io_status);
    }
}

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
        sink->callback_on_success()(CommitResult{
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
        EXPECT_ERROR(sink->init());
    }
}

} // namespace
} // namespace starrocks::connector
