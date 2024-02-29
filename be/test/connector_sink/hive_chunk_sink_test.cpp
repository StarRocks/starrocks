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

#include "connector_sink/hive_chunk_sink.h"

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "connector_sink/connector_chunk_sink.h"
#include "formats/file_writer.h"
#include "formats/utils.h"
#include "testutil/assert.h"
#include "util/defer_op.h"

namespace starrocks::connector {
namespace {

class HiveChunkSinkTest : public ::testing::Test {
protected:
    void SetUp() override { _runtime_state = _pool.add(new RuntimeState); }

    void TearDown() override {}

    ObjectPool _pool;
    RuntimeState* _runtime_state;
};

class MockFileWriterFactory : public formats::FileWriterFactory {
public:
    MOCK_METHOD(StatusOr<std::shared_ptr<formats::FileWriter>>, create, (const std::string&), (override));
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

TEST_F(HiveChunkSinkTest, test_unpartitioned_sink) {
    {
        std::vector<std::string> partition_column_names = {};
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators = {};
        auto mock_file_writer = std::make_shared<MockFileWriter>();
        EXPECT_CALL(*mock_file_writer, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer, get_written_bytes()).WillOnce(Return(0));
        EXPECT_CALL(*mock_file_writer, write(_)).WillOnce(Return(ByMove(make_ready_future(Status::OK()))));
        auto mock_file_writer_factory = std::make_unique<MockFileWriterFactory>();
        EXPECT_CALL(*mock_file_writer_factory, create(_)).WillOnce(Return(ByMove(mock_file_writer)));
        auto location_provider = std::make_unique<LocationProvider>("base_path", "ffffff", 0, 0, "parquet");
        auto sink = std::make_unique<HiveChunkSink>(partition_column_names, std::move(partition_column_evaluators),
                                                    std::move(location_provider), std::move(mock_file_writer_factory),
                                                    100, _runtime_state);

        EXPECT_OK(sink->init());
        auto chunk = std::make_shared<Chunk>();
        auto futures = sink->add(chunk);
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().add_chunk_future.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().add_chunk_future[0]));
        EXPECT_OK(futures.value().add_chunk_future[0].get());
    }

    {
        std::vector<std::string> partition_column_names = {};
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators = {};
        auto mock_file_writer1 = std::make_shared<MockFileWriter>();
        EXPECT_CALL(*mock_file_writer1, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer1, get_written_bytes()).WillOnce(Return(0)).WillOnce(Return(100));
        EXPECT_CALL(*mock_file_writer1, write(_)).WillOnce(Return(ByMove(make_ready_future(Status::OK()))));
        EXPECT_CALL(*mock_file_writer1, commit())
                .WillOnce(Return(ByMove(make_ready_future(CommitResult{.io_status = Status::OK()}))));
        auto mock_file_writer2 = std::make_shared<MockFileWriter>();
        EXPECT_CALL(*mock_file_writer2, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer2, write(_)).WillOnce(Return(ByMove(make_ready_future(Status::OK()))));
        EXPECT_CALL(*mock_file_writer2, commit())
                .WillOnce(Return(ByMove(make_ready_future(CommitResult{.io_status = Status::OK()}))));
        auto mock_file_writer_factory = std::make_unique<MockFileWriterFactory>();
        EXPECT_CALL(*mock_file_writer_factory, create(_))
                .WillOnce(Return(ByMove(mock_file_writer1)))
                .WillOnce(Return(ByMove(mock_file_writer2)));
        auto location_provider = std::make_unique<LocationProvider>("base_path", "ffffff", 0, 0, "parquet");
        auto sink = std::make_unique<HiveChunkSink>(partition_column_names, std::move(partition_column_evaluators),
                                                    std::move(location_provider), std::move(mock_file_writer_factory),
                                                    100, _runtime_state);

        EXPECT_OK(sink->init());
        auto futures = sink->add(nullptr);
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().commit_file_future.size(), 0);
        EXPECT_EQ(futures.value().add_chunk_future.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().add_chunk_future[0]));
        EXPECT_OK(futures.value().add_chunk_future[0].get());

        futures = sink->add(nullptr);
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().commit_file_future.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().commit_file_future[0]));
        EXPECT_OK(futures.value().commit_file_future[0].get().io_status);
        EXPECT_EQ(futures.value().add_chunk_future.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().add_chunk_future[0]));
        EXPECT_OK(futures.value().add_chunk_future[0].get());

        futures = sink->finish();
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().commit_file_future.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().commit_file_future[0]));
        EXPECT_OK(futures.value().commit_file_future[0].get().io_status);
    }
}

TEST_F(HiveChunkSinkTest, test_partitioned_sink) {
    {
        std::vector<std::string> partition_column_names = {"k1"};
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators =
                ColumnSlotIdEvaluator::from_types({TypeDescriptor::from_logical_type(TYPE_VARCHAR)});
        auto mock_file_writer = std::make_shared<MockFileWriter>();
        EXPECT_CALL(*mock_file_writer, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer, get_written_bytes()).WillOnce(Return(0));
        EXPECT_CALL(*mock_file_writer, write(_)).WillOnce(Return(ByMove(make_ready_future(Status::OK()))));
        auto mock_file_writer_factory = std::make_unique<MockFileWriterFactory>();
        EXPECT_CALL(*mock_file_writer_factory, create(_)).WillOnce(Return(ByMove(mock_file_writer)));
        auto location_provider = std::make_unique<LocationProvider>("base_path", "ffffff", 0, 0, "parquet");
        auto sink = std::make_unique<HiveChunkSink>(partition_column_names, std::move(partition_column_evaluators),
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
        EXPECT_EQ(futures.value().add_chunk_future.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().add_chunk_future[0]));
        EXPECT_OK(futures.value().add_chunk_future[0].get());
    }

    {
        std::vector<std::string> partition_column_names = {"k1"};
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators =
                ColumnSlotIdEvaluator::from_types({TypeDescriptor::from_logical_type(TYPE_VARCHAR)});
        auto mock_file_writer1 = std::make_shared<MockFileWriter>();
        EXPECT_CALL(*mock_file_writer1, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer1, get_written_bytes()).WillOnce(Return(0));
        EXPECT_CALL(*mock_file_writer1, write(_)).WillOnce(Return(ByMove(make_ready_future(Status::OK()))));
        EXPECT_CALL(*mock_file_writer1, commit())
                .WillOnce(Return(ByMove(make_ready_future(CommitResult{.io_status = Status::OK()}))));
        auto mock_file_writer2 = std::make_shared<MockFileWriter>();
        EXPECT_CALL(*mock_file_writer2, init()).WillOnce(Return(Status::OK()));
        EXPECT_CALL(*mock_file_writer2, get_written_bytes()).WillOnce(Return(0));
        EXPECT_CALL(*mock_file_writer2, write(_)).WillOnce(Return(ByMove(make_ready_future(Status::OK()))));
        EXPECT_CALL(*mock_file_writer2, commit())
                .WillOnce(Return(ByMove(make_ready_future(CommitResult{.io_status = Status::OK()}))));
        auto mock_file_writer_factory = std::make_unique<MockFileWriterFactory>();
        EXPECT_CALL(*mock_file_writer_factory, create(_))
                .WillOnce(Return(ByMove(mock_file_writer1)))
                .WillOnce(Return(ByMove(mock_file_writer2)));
        auto location_provider = std::make_unique<LocationProvider>("base_path", "ffffff", 0, 0, "parquet");
        auto sink = std::make_unique<HiveChunkSink>(partition_column_names, std::move(partition_column_evaluators),
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
        EXPECT_EQ(futures.value().commit_file_future.size(), 0);
        EXPECT_EQ(futures.value().add_chunk_future.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().add_chunk_future[0]));
        EXPECT_OK(futures.value().add_chunk_future[0].get());

        chunk = std::make_shared<Chunk>();
        {
            auto partition_column = BinaryColumn::create();
            partition_column->append("world");
            chunk->append_column(partition_column, 0);
        }
        futures = sink->add(chunk);
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().commit_file_future.size(), 0);
        EXPECT_EQ(futures.value().add_chunk_future.size(), 1);
        EXPECT_TRUE(is_ready(futures.value().add_chunk_future[0]));
        EXPECT_OK(futures.value().add_chunk_future[0].get());

        futures = sink->finish();
        EXPECT_TRUE(futures.ok());
        EXPECT_EQ(futures.value().commit_file_future.size(), 2);
        EXPECT_TRUE(is_ready(futures.value().commit_file_future[0]));
        EXPECT_TRUE(is_ready(futures.value().commit_file_future[1]));
        EXPECT_OK(futures.value().commit_file_future[0].get().io_status);
        EXPECT_OK(futures.value().commit_file_future[1].get().io_status);
    }
}

} // namespace
} // namespace starrocks::connector
