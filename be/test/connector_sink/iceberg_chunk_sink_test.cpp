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

#include "connector/iceberg_chunk_sink.h"

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
#include "testutil/scoped_updater.h"
#include "util/defer_op.h"
#include "util/integer_util.h"

namespace starrocks::connector {
namespace {

using CommitResult = formats::FileWriter::CommitResult;
using WriterAndStream = formats::WriterAndStream;
using Stream = io::AsyncFlushOutputStream;
using ::testing::Return;
using ::testing::ByMove;
using ::testing::_;

class IcebergChunkSinkTest : public ::testing::Test {
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
    MOCK_METHOD(StatusOr<WriterAndStream>, create, (const std::string&), (const override));
};

class MockWriter : public formats::FileWriter {
public:
    MOCK_METHOD(Status, init, (), (override));
    MOCK_METHOD(int64_t, get_written_bytes, (), (override));
    MOCK_METHOD(int64_t, get_allocated_bytes, (), (override));
    MOCK_METHOD(int64_t, get_flush_batch_size, (), (override));
    MOCK_METHOD(Status, write, (Chunk * chunk), (override));
    MOCK_METHOD(CommitResult, commit, (), (override));
};

class MockFile : public WritableFile {
public:
    MOCK_METHOD(Status, append, (const Slice& data), (override));
    MOCK_METHOD(Status, appendv, (const Slice* data, size_t cnt), (override));
    MOCK_METHOD(Status, pre_allocate, (uint64_t size), (override));
    MOCK_METHOD(Status, close, (), (override));
    MOCK_METHOD(Status, flush, (FlushMode mode), (override));
    MOCK_METHOD(Status, sync, (), (override));
    MOCK_METHOD(uint64_t, size, (), (const, override));
    MOCK_METHOD(const std::string&, filename, (), (const, override));
};

class MockPoller : public AsyncFlushStreamPoller {
public:
    MOCK_METHOD(void, enqueue, (std::shared_ptr<Stream> stream), (override));
};

TEST_F(IcebergChunkSinkTest, test_callback) {
    {
        std::vector<std::string> partition_column_names = {"k1"};
        std::vector<std::string> transform = {"identity"};
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators =
                ColumnSlotIdEvaluator::from_types({TypeDescriptor::from_logical_type(TYPE_VARCHAR)});
        auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
        auto location_provider = std::make_shared<LocationProvider>("base_path", "ffffff", 0, 0, "parquet");
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        EXPECT_CALL(*mock_writer_factory, create(::testing::_))
                .WillRepeatedly(::testing::Return(ByMove(StatusOr<WriterAndStream>(std::move(ws)))));

        auto partition_chunk_writer_ctx = std::make_shared<BufferPartitionChunkWriterContext>(
                BufferPartitionChunkWriterContext{mock_writer_factory, location_provider, 100, false});
        auto partition_chunk_writer_factory =
                std::make_unique<BufferPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
        auto sink = std::make_unique<connector::IcebergChunkSink>(
                partition_column_names, transform, std::move(partition_column_evaluators),
                std::move(partition_chunk_writer_factory), _runtime_state);
        auto poller = MockPoller();
        sink->set_io_poller(&poller);

        Columns partition_key_columns;
        ChunkPtr chunk = std::make_shared<Chunk>();
        std::vector<ChunkExtraColumnsMeta> extra_metas;
        std::string tmp = "abc";
        Datum datum;
        datum.set_slice(tmp);
        auto res = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
        res->append_datum(datum);
        auto ptr = ConstColumn::create(std::move(res), 1);
        partition_key_columns.emplace_back(ptr);
        extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_VARCHAR_DESC, true /*useless*/, true /*useless*/});

        auto chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(extra_metas, std::move(partition_key_columns));
        // Unlock during merging partition chunks into a full chunk.
        chunk->set_extra_data(chunk_extra_data);
        auto ret = sink->add(chunk);
        EXPECT_EQ(ret.ok(), true);
        sink->callback_on_commit(CommitResult{
                .io_status = Status::OK(),
                .format = formats::PARQUET,
                .file_statistics =
                        {
                                .record_count = 100,
                        },
                .location = "path/to/directory/data.parquet",
        }
                                         .set_extra_data("0"));
        sink->set_status(Status::OK());

        EXPECT_EQ(sink->is_finished(), true);
        EXPECT_EQ(sink->status().ok(), true);
        EXPECT_EQ(_runtime_state->num_rows_load_sink(), 100);
    }
}

TEST_F(IcebergChunkSinkTest, test_factory) {
    SCOPED_UPDATE(bool, config::enable_connector_sink_spill, false);
    IcebergChunkSinkProvider provider;

    {
        auto sink_ctx = std::make_shared<connector::IcebergChunkSinkContext>();
        sink_ctx->path = "/path/to/directory/";
        sink_ctx->column_names = {"k1", "k2"};
        sink_ctx->executor = nullptr;
        sink_ctx->format = formats::PARQUET; // iceberg sink only supports parquet
        sink_ctx->compression_type = TCompressionType::NO_COMPRESSION;
        sink_ctx->options = {}; // default for now
        sink_ctx->parquet_field_ids = {};
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
        auto sink_ctx = std::make_shared<connector::IcebergChunkSinkContext>();
        sink_ctx->path = "/path/to/directory/";
        sink_ctx->column_names = {"k1", "k2"};
        sink_ctx->executor = nullptr;
        sink_ctx->format = "unknown";
        sink_ctx->compression_type = TCompressionType::NO_COMPRESSION;
        sink_ctx->options = {};
        sink_ctx->parquet_field_ids = {};
        sink_ctx->max_file_size = 1 << 30;
        sink_ctx->column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_VARCHAR), TypeDescriptor::from_logical_type(TYPE_INT)});
        sink_ctx->fragment_context = _fragment_context.get();
        auto sink = provider.create_chunk_sink(sink_ctx, 0).value();
        SinkOperatorMemoryManager mm;
        sink->set_operator_mem_mgr(&mm);
        EXPECT_ERROR(sink->init()); // format is not supported
    }
}

TEST_F(IcebergChunkSinkTest, test_utils) {
    int128_t val = 123;
    std::string str128 = integer_to_string(val);
    EXPECT_EQ("123", str128);
    str128 = integer_to_string(-val);
    EXPECT_EQ("-123", str128);

    auto format_dec = HiveUtils::format_decimal_value<int32_t>(123, 2);
    EXPECT_EQ("1.23", format_dec.value());
    format_dec = HiveUtils::format_decimal_value<int32_t>(123, 4);
    EXPECT_EQ("0.0123", format_dec.value());
    EXPECT_ERROR(HiveUtils::format_decimal_value<int32_t>(123, -1));

    {
        Columns partition_key_columns;
        ChunkPtr chunk = std::make_shared<Chunk>();
        std::vector<ChunkExtraColumnsMeta> extra_metas;
        {
            Datum datum;
            datum.set_int64(23);
            auto res = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_BIGINT_DESC, true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int64(40);
            auto res = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_BIGINT_DESC, true /*useless*/, true /*useless*/});
        }
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_BIGINT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)});
        auto chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(extra_metas, std::move(partition_key_columns));
        // Unlock during merging partition chunks into a full chunk.
        chunk->set_extra_data(chunk_extra_data);
        std::vector<int8_t> field_is_null;
        auto ret = HiveUtils::iceberg_make_partition_name({"k1", "k2"}, partition_column_evaluators, {"day", "hour"},
                                                          chunk.get(), true, field_is_null);

        EXPECT_EQ("k1=1970-01-24/k2=1970-01-02-16/", ret.value());
        ret = HiveUtils::iceberg_make_partition_name({"k1", "k2"}, partition_column_evaluators, {"year", "month"},
                                                     chunk.get(), true, field_is_null);
        EXPECT_EQ("k1=1993/k2=1973-05/", ret.value());
    }

    {
        Columns partition_key_columns;
        ChunkPtr chunk = std::make_shared<Chunk>();
        std::string tmp = "abc";
        std::vector<ChunkExtraColumnsMeta> extra_metas;
        {
            Datum datum;
            datum.set_slice(tmp);
            auto res = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_VARCHAR_DESC, true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_date(DateValue::create(1999, 12, 31));
            auto res = ColumnHelper::create_column(TYPE_DATE_DESC, true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_DATE_DESC, true /*useless*/, true /*useless*/});
        }

        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_VARCHAR), TypeDescriptor::from_logical_type(TYPE_DATE)});

        auto chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(extra_metas, std::move(partition_key_columns));
        // Unlock during merging partition chunks into a full chunk.
        chunk->set_extra_data(chunk_extra_data);
        std::vector<int8_t> field_is_null;
        auto ret = HiveUtils::iceberg_make_partition_name({"k1", "k2"}, partition_column_evaluators,
                                                          {"identity", "truncate"}, chunk.get(), true, field_is_null);
        EXPECT_EQ("k1=abc/k2=1999-12-31/", ret.value());
    }

    {
        Columns partition_key_columns;
        ChunkPtr chunk = std::make_shared<Chunk>();
        std::string tmp = "abc";
        auto time = TimestampValue::create(2023, 10, 31, 12, 0, 0);
        std::vector<ChunkExtraColumnsMeta> extra_metas;
        {
            Datum datum;
            datum.set_slice(tmp);
            auto res = ColumnHelper::create_column(TYPE_VARBINARY_DESC, true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_VARBINARY_DESC, true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_slice(tmp);
            auto res = ColumnHelper::create_column(TypeDescriptor::create_char_type(10), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(
                    ChunkExtraColumnsMeta{TypeDescriptor::create_char_type(10), true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_timestamp(time);
            auto res = ColumnHelper::create_column(TYPE_DATETIME_DESC, true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_DATETIME_DESC, true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int8(12);
            auto res = ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_TINYINT), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TypeDescriptor(LogicalType::TYPE_TINYINT), true /*useless*/,
                                                        true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int16(33);
            auto res = ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_SMALLINT), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TypeDescriptor(LogicalType::TYPE_SMALLINT), true /*useless*/,
                                                        true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int32(33);
            auto res = ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_INT), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(
                    ChunkExtraColumnsMeta{TypeDescriptor(LogicalType::TYPE_INT), true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int64(33);
            auto res = ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_BIGINT), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TypeDescriptor(LogicalType::TYPE_BIGINT), true /*useless*/,
                                                        true /*useless*/});
        }

        auto chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(extra_metas, std::move(partition_key_columns));
        // Unlock during merging partition chunks into a full chunk.
        chunk->set_extra_data(chunk_extra_data);
        std::vector<int8_t> field_is_null;
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_VARBINARY), TypeDescriptor::from_logical_type(TYPE_CHAR),
                 TypeDescriptor::from_logical_type(TYPE_DATETIME), TypeDescriptor::from_logical_type(TYPE_TINYINT),
                 TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_BIGINT)});
        auto ret = HiveUtils::iceberg_make_partition_name(
                {"k1", "k2", "k3", "k4", "k5", "k6", "k7"}, partition_column_evaluators,
                {"bucket", "identity", "truncate", "truncate", "truncate", "truncate", "truncate"}, chunk.get(), true,
                field_is_null);
        EXPECT_EQ("k1=YWJj/k2=abc%20%20%20%20%20%20%20/k3=2023-10-31%2012%3A00%3A00/k4=12/k5=33/k6=33/k7=33/",
                  ret.value());
    }

    {
        Columns partition_key_columns;
        ChunkPtr chunk = std::make_shared<Chunk>();
        std::vector<ChunkExtraColumnsMeta> extra_metas;
        {
            Datum datum;
            datum.set_int32(1234);
            auto res = ColumnHelper::create_column(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 7, 2), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 7, 2),
                                                        true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int64(3344);
            auto res = ColumnHelper::create_column(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 10, 2), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 10, 2),
                                                        true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int128(7893);
            auto res = ColumnHelper::create_column(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 2), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 2),
                                                        true /*useless*/, true /*useless*/});
        }

        auto chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(extra_metas, std::move(partition_key_columns));
        // Unlock during merging partition chunks into a full chunk.
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_DECIMAL32), TypeDescriptor::from_logical_type(TYPE_DECIMAL64),
                 TypeDescriptor::from_logical_type(TYPE_DECIMAL128)});
        chunk->set_extra_data(chunk_extra_data);
        std::vector<int8_t> field_is_null;
        auto ret = HiveUtils::iceberg_make_partition_name({"k1", "k2", "k3"}, partition_column_evaluators,
                                                          {"identity", "truncate", "bucket"}, chunk.get(), true,
                                                          field_is_null);
        EXPECT_EQ("k1=12.34/k2=33.44/k3=78.93/", ret.value());
    }
}

} // namespace
} // namespace starrocks::connector
