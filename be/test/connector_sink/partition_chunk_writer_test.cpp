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

#include "connector/partition_chunk_writer.h"

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "connector/connector_chunk_sink.h"
#include "connector/iceberg_chunk_sink.h"
#include "connector/sink_memory_manager.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/file_writer.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/utils.h"
#include "testutil/assert.h"
#include "util/await.h"
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

class PartitionChunkWriterTest : public ::testing::Test {
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

class WriterHelper {
public:
    static WriterHelper* instance() {
        static WriterHelper helper;
        return &helper;
    }

    Status write(Chunk* chunk) {
        if (!_tmp_chunk) {
            _tmp_chunk = chunk->clone_empty();
        }
        _tmp_chunk->append(*chunk, 0, chunk->num_rows());
        ;
        return Status::OK();
    }

    int64_t commit() {
        if (!_tmp_chunk) {
            return 0;
        }
        ChunkPtr result_chunk = _tmp_chunk->clone_empty(_tmp_chunk->num_rows());
        result_chunk->append(*_tmp_chunk, 0, _tmp_chunk->num_rows());
        _result_chunks.push_back(result_chunk);
        size_t num_rows = result_chunk->num_rows();
        _tmp_chunk.reset();
        return num_rows;
    }

    void reset() {
        if (_tmp_chunk) {
            _tmp_chunk.reset();
        }
        _result_chunks.clear();
    }

    int64_t written_bytes() { return _tmp_chunk ? _tmp_chunk->bytes_usage() : 0; }

    int64_t written_rows() { return _tmp_chunk != nullptr ? _tmp_chunk->num_rows() : 0; }

    int64_t result_rows() {
        int64_t num_rows = 0;
        for (auto& chunk : _result_chunks) {
            num_rows += chunk->num_rows();
        }
        return num_rows;
    }

    std::vector<ChunkPtr>& result_chunks() { return _result_chunks; }

private:
    WriterHelper() {}

    ChunkPtr _tmp_chunk = nullptr;
    std::vector<ChunkPtr> _result_chunks;
};

class MockWriter : public formats::FileWriter {
public:
    MockWriter() {}
    MOCK_METHOD(Status, init, (), (override));
    MOCK_METHOD(int64_t, get_allocated_bytes, (), (override));

    int64_t get_written_bytes() override { return WriterHelper::instance()->written_bytes(); }

    Status write(Chunk* chunk) override { return WriterHelper::instance()->write(chunk); }

    int64_t get_flush_batch_size() override { return _flush_batch_size; }

    void set_flush_batch_size(int64_t flush_batch_size) { _flush_batch_size = flush_batch_size; }

    CommitResult commit() override {
        size_t num_rows = WriterHelper::instance()->commit();
        CommitResult commit_result = {
                .io_status = Status::OK(),
                .format = formats::PARQUET,
                .file_statistics =
                        {
                                .record_count = static_cast<int64_t>(num_rows),
                        },
                .location = "path/to/directory/data.parquet",
        };
        return commit_result;
    }

private:
    int64_t _flush_batch_size = 128L * 1024 * 1024; // 128MB
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

TEST_F(PartitionChunkWriterTest, buffer_partition_chunk_writer) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto writer_helper = WriterHelper::instance();
    {
        writer_helper->reset();
        // Create partition writer
        auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
        auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
        EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
            WriterAndStream ws;
            ws.writer = std::make_unique<MockWriter>();
            ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
            return ws;
        });

        auto partition_chunk_writer_ctx = std::make_shared<BufferPartitionChunkWriterContext>(
                BufferPartitionChunkWriterContext{mock_writer_factory, location_provider, 100, false});
        auto partition_chunk_writer_factory =
                std::make_unique<BufferPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
        std::vector<int8_t> partition_field_null_list;
        auto partition_writer = partition_chunk_writer_factory->create("c1", partition_field_null_list);
        bool commited = false;
        auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
        auto poller = MockPoller();
        partition_writer->set_io_poller(&poller);
        partition_writer->set_commit_callback(commit_callback);
        EXPECT_OK(partition_writer->init());

        // Create a chunk
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 1);
        chunk->get_column_by_index(0)->append_datum(Slice("aaa"));

        // Write chunk
        auto ret = partition_writer->write(chunk);
        EXPECT_EQ(ret.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 1);
        EXPECT_EQ(writer_helper->result_rows(), 0);

        // Flush chunk
        ret = partition_writer->flush();
        EXPECT_EQ(ret.ok(), true);
        ret = partition_writer->wait_flush();
        EXPECT_EQ(ret.ok(), true);
        EXPECT_EQ(commited, true);
        EXPECT_EQ(partition_writer->is_finished(), true);
        EXPECT_EQ(partition_writer->get_written_bytes(), 0);
        EXPECT_EQ(partition_writer->get_flushable_bytes(), 0);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 1);
    }
    std::filesystem::remove_all(fs_base_path);
}

TEST_F(PartitionChunkWriterTest, spill_partition_chunk_writer) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    // Create partition writer
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{mock_writer_factory, location_provider, 100, false, nullptr,
                                             _fragment_context.get(), tuple_desc, nullptr, nullptr});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // Normal write and flush to file
    {
        writer_helper->reset();
        // Create a chunk
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 1);
        chunk->get_column_by_index(0)->append_datum(Slice("aaa"));

        // Write chunk
        auto ret = partition_writer->write(chunk);
        EXPECT_EQ(ret.ok(), true);
        EXPECT_GT(partition_writer->get_written_bytes(), 0);
        EXPECT_EQ(partition_writer->get_flushable_bytes(), chunk->bytes_usage());

        // Flush chunk
        ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        EXPECT_EQ(commited, true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 1);
    }

    // Write and spill
    {
        // Reset states
        writer_helper->reset();
        commited = false;
        status = Status::OK();

        // Create a chunk
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 1);
        chunk->get_column_by_index(0)->append_datum(Slice("aaa"));

        for (size_t i = 0; i < 3; ++i) {
            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);

            // Flush chunk
            EXPECT_GT(partition_writer->get_flushable_bytes(), 0);
            ret = partition_writer->flush();
            EXPECT_EQ(ret.ok(), true);
            ret = partition_writer->wait_flush();
            EXPECT_EQ(ret.ok(), true);
            Awaitility()
                    .timeout(3 * 1000 * 1000) // 3s
                    .interval(300 * 1000)     // 300ms
                    .until([partition_writer]() {
                        return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
                    });

            EXPECT_EQ(partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed), 0);
            EXPECT_EQ(status.ok(), true);
        }

        // Merge spill blocks
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        Awaitility()
                .timeout(3 * 1000 * 1000) // 3s
                .interval(300 * 1000)     // 300ms
                .until([&commited]() { return commited; });

        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(partition_writer->is_finished(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 3);
    }

    std::filesystem::remove_all(fs_base_path);
}

TEST_F(PartitionChunkWriterTest, sort_column_asc) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    // Create partition writer
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0};
    sort_ordering->sort_descs.descs.emplace_back(true, false);
    const size_t max_file_size = 1073741824; // 1GB
    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{mock_writer_factory, location_provider, max_file_size, false, nullptr,
                                             _fragment_context.get(), tuple_desc, nullptr, sort_ordering});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // Normal write and flush to file
    {
        writer_helper->reset();

        for (size_t i = 0; i < 3; ++i) {
            // Create a chunk
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
            std::string suffix = std::to_string(3 - i);
            chunk->get_column_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("aaa" + suffix));

            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);
        }

        // Flush chunks directly
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 9);
        EXPECT_EQ(writer_helper->result_chunks().size(), 1);

        // Check the result order
        auto result_chunk = writer_helper->result_chunks()[0];
        auto column = result_chunk->get_column_by_index(0);
        std::string last_row;
        for (size_t i = 0; i < column->size(); ++i) {
            std::string cur_row = column->get(i).get_slice().to_string();
            LOG(INFO) << "(" << i << "): " << cur_row;
            if (!last_row.empty()) {
                EXPECT_GT(cur_row, last_row);
            }
            last_row = cur_row;
        }
    }

    // Write and spill multiple chunks
    {
        writer_helper->reset();
        commited = false;
        status = Status::OK();

        for (size_t i = 0; i < 3; ++i) {
            // Create a chunk
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
            std::string suffix = std::to_string(3 - i);
            chunk->get_column_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("aaa" + suffix));

            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);

            // Flush chunk
            ret = partition_writer->flush();
            EXPECT_EQ(ret.ok(), true);
            ret = partition_writer->wait_flush();
            EXPECT_EQ(ret.ok(), true);
            Awaitility()
                    .timeout(3 * 1000 * 1000) // 3s
                    .interval(300 * 1000)     // 300ms
                    .until([partition_writer]() {
                        return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
                    });

            EXPECT_EQ(partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed), 0);
            EXPECT_EQ(status.ok(), true);
        }

        // Merge spill blocks
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        Awaitility()
                .timeout(3 * 1000 * 1000) // 3s
                .interval(300 * 1000)     // 300ms
                .until([&commited]() { return commited; });

        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 9);
        EXPECT_EQ(writer_helper->result_chunks().size(), 1);

        // Check the result order
        auto result_chunk = writer_helper->result_chunks()[0];
        auto column = result_chunk->get_column_by_index(0);
        std::string last_row;
        for (size_t i = 0; i < column->size(); ++i) {
            std::string cur_row = column->get(i).get_slice().to_string();
            LOG(INFO) << "(" << i << "): " << cur_row;
            if (!last_row.empty()) {
                EXPECT_GT(cur_row, last_row);
            }
            last_row = cur_row;
        }
    }

    std::filesystem::remove_all(fs_base_path);
}

TEST_F(PartitionChunkWriterTest, sort_column_desc) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    // Create partition writer
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0};
    sort_ordering->sort_descs.descs.emplace_back(false, false);
    const size_t max_file_size = 1073741824; // 1GB
    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{mock_writer_factory, location_provider, max_file_size, false, nullptr,
                                             _fragment_context.get(), tuple_desc, nullptr, sort_ordering});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // Normal write and flush to file
    {
        writer_helper->reset();

        for (size_t i = 0; i < 3; ++i) {
            // Create a chunk
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
            std::string suffix = std::to_string(3 - i);
            chunk->get_column_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("aaa" + suffix));

            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);
        }

        // Flush chunks directly
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 9);
        EXPECT_EQ(writer_helper->result_chunks().size(), 1);

        // Check the result order
        auto result_chunk = writer_helper->result_chunks()[0];
        auto column = result_chunk->get_column_by_index(0);
        std::string last_row;
        for (size_t i = 0; i < column->size(); ++i) {
            std::string cur_row = column->get(i).get_slice().to_string();
            LOG(INFO) << "(" << i << "): " << cur_row;
            if (!last_row.empty()) {
                EXPECT_LT(cur_row, last_row);
            }
            last_row = cur_row;
        }
    }

    // Write and spill multiple chunks
    {
        writer_helper->reset();
        commited = false;
        status = Status::OK();

        for (size_t i = 0; i < 3; ++i) {
            // Create a chunk
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
            std::string suffix = std::to_string(3 - i);
            chunk->get_column_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("aaa" + suffix));

            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);

            // Flush chunk
            ret = partition_writer->flush();
            EXPECT_EQ(ret.ok(), true);
            ret = partition_writer->wait_flush();
            EXPECT_EQ(ret.ok(), true);
            Awaitility()
                    .timeout(3 * 1000 * 1000) // 3s
                    .interval(300 * 1000)     // 300ms
                    .until([partition_writer]() {
                        return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
                    });

            EXPECT_EQ(partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed), 0);
            EXPECT_EQ(status.ok(), true);
        }

        // Merge spill blocks
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        Awaitility()
                .timeout(3 * 1000 * 1000) // 3s
                .interval(300 * 1000)     // 300ms
                .until([&commited]() { return commited; });

        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 9);
        EXPECT_EQ(writer_helper->result_chunks().size(), 1);

        // Check the result order
        auto result_chunk = writer_helper->result_chunks()[0];
        auto column = result_chunk->get_column_by_index(0);
        std::string last_row;
        for (size_t i = 0; i < column->size(); ++i) {
            std::string cur_row = column->get(i).get_slice().to_string();
            LOG(INFO) << "(" << i << "): " << cur_row;
            if (!last_row.empty()) {
                EXPECT_LT(cur_row, last_row);
            }
            last_row = cur_row;
        }
    }

    std::filesystem::remove_all(fs_base_path);
}

TEST_F(PartitionChunkWriterTest, sort_multiple_columns) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {"c2", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    // Create partition writer
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0, 1};
    sort_ordering->sort_descs.descs.emplace_back(true, false);
    sort_ordering->sort_descs.descs.emplace_back(false, false);
    const size_t max_file_size = 1073741824; // 1GB
    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{mock_writer_factory, location_provider, max_file_size, false, nullptr,
                                             _fragment_context.get(), tuple_desc, nullptr, sort_ordering});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // Write and spill multiple chunks
    {
        writer_helper->reset();

        for (size_t i = 0; i < 3; ++i) {
            // Create a chunk
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
            std::string suffix = std::to_string(3 - i);
            chunk->get_column_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_by_index(1)->append_datum(Slice("222" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_by_index(1)->append_datum(Slice("111" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_by_index(1)->append_datum(Slice("222" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_by_index(1)->append_datum(Slice("111" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("aaa" + suffix));
            chunk->get_column_by_index(1)->append_datum(Slice("222" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("aaa" + suffix));
            chunk->get_column_by_index(1)->append_datum(Slice("111" + suffix));

            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);

            // Flush chunk
            ret = partition_writer->flush();
            EXPECT_EQ(ret.ok(), true);
            ret = partition_writer->wait_flush();
            EXPECT_EQ(ret.ok(), true);
            Awaitility()
                    .timeout(3 * 1000 * 1000) // 3s
                    .interval(300 * 1000)     // 300ms
                    .until([partition_writer]() {
                        return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
                    });

            EXPECT_EQ(partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed), 0);
            EXPECT_EQ(status.ok(), true);
        }

        // Merge spill blocks
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        Awaitility()
                .timeout(3 * 1000 * 1000) // 3s
                .interval(300 * 1000)     // 300ms
                .until([&commited]() { return commited; });

        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 18);
        EXPECT_EQ(writer_helper->result_chunks().size(), 1);

        // Check the result order
        auto result_chunk = writer_helper->result_chunks()[0];
        auto c1 = result_chunk->get_column_by_index(0);
        auto c2 = result_chunk->get_column_by_index(1);
        std::string c1_last_row;
        std::string c2_last_row;
        for (size_t i = 0; i < c1->size(); ++i) {
            std::string c1_cur_row = c1->get(i).get_slice().to_string();
            std::string c2_cur_row = c2->get(i).get_slice().to_string();
            LOG(INFO) << "(" << i << "): " << c1_cur_row << ", " << c2_cur_row;
            if (!c1_last_row.empty()) {
                EXPECT_GE(c1_cur_row, c1_last_row);
            }
            if (!c2_last_row.empty() && c1_cur_row == c1_last_row) {
                EXPECT_LE(c2_cur_row, c2_last_row);
            }
            c1_last_row = c1_cur_row;
            c2_last_row = c2_cur_row;
        }
    }

    std::filesystem::remove_all(fs_base_path);
}

TEST_F(PartitionChunkWriterTest, sort_column_with_schema_chunk) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    Fields fields;
    for (auto& slot : tuple_desc->slots()) {
        TypeDescriptor type_desc = slot->type();
        TypeInfoPtr type_info = get_type_info(type_desc.type, type_desc.precision, type_desc.scale);
        auto field = std::make_shared<Field>(slot->id(), slot->col_name(), type_info, slot->is_nullable());
        fields.push_back(field);
    }
    auto schema = std::make_shared<Schema>(std::move(fields), KeysType::DUP_KEYS, std::vector<uint32_t>(), nullptr);

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    // Create partition writer
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0};
    sort_ordering->sort_descs.descs.emplace_back(true, false);
    const size_t max_file_size = 1073741823; // 1GB
    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{mock_writer_factory, location_provider, max_file_size, false, nullptr,
                                             _fragment_context.get(), tuple_desc, nullptr, sort_ordering});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // Write and spill multiple chunks
    {
        writer_helper->reset();

        for (size_t i = 0; i < 3; ++i) {
            // Create a chunk
            ChunkPtr chunk = ChunkHelper::new_chunk(*schema, 3);
            std::string suffix = std::to_string(3 - i);
            chunk->get_column_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_by_index(0)->append_datum(Slice("aaa" + suffix));

            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);

            // Flush chunk
            ret = partition_writer->flush();
            EXPECT_EQ(ret.ok(), true);
            ret = partition_writer->wait_flush();
            EXPECT_EQ(ret.ok(), true);
            Awaitility()
                    .timeout(3 * 1000 * 1000) // 3s
                    .interval(300 * 1000)     // 300ms
                    .until([partition_writer]() {
                        return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
                    });

            EXPECT_EQ(partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed), 0);
            EXPECT_EQ(status.ok(), true);
        }

        // Merge spill blocks
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        Awaitility()
                .timeout(3 * 1000 * 1000) // 3s
                .interval(300 * 1000)     // 300ms
                .until([&commited]() { return commited; });

        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 9);
        EXPECT_EQ(writer_helper->result_chunks().size(), 1);

        // Check the result order
        auto result_chunk = writer_helper->result_chunks()[0];
        auto column = result_chunk->get_column_by_index(0);
        std::string last_row;
        for (size_t i = 0; i < column->size(); ++i) {
            std::string cur_row = column->get(i).get_slice().to_string();
            LOG(INFO) << "(" << i << "): " << cur_row;
            if (!last_row.empty()) {
                EXPECT_GT(cur_row, last_row);
            }
            last_row = cur_row;
        }
    }

    std::filesystem::remove_all(fs_base_path);
}

} // namespace
} // namespace starrocks::connector
