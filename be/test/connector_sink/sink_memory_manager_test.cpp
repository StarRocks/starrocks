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

#include "connector/sink_memory_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "base/testutil/assert.h"
#include "base/utility/integer_util.h"
#include "connector/connector_chunk_sink.h"
#include "connector/partition_chunk_writer.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/file_writer.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/utils.h"

namespace starrocks::connector {
namespace {

using Stream = io::AsyncFlushOutputStream;

class SinkMemoryManagerTest : public ::testing::Test {
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

class MockPartitionChunkWriter : public PartitionChunkWriter {
public:
    MockPartitionChunkWriter(std::string partition, std::vector<int8_t> partition_field_null_list,
                             const std::shared_ptr<PartitionChunkWriterContext>& ctx)
            : PartitionChunkWriter(std::move(partition), std::move(partition_field_null_list), ctx) {}

    MOCK_METHOD(Status, init, (), (override));
    MOCK_METHOD(Status, write, (const starrocks::ChunkPtr&), (override));
    MOCK_METHOD(Status, finish, (), (override));
    MOCK_METHOD(Status, wait_flush, (), (override));
    MOCK_METHOD(bool, is_finished, (), (override));
    MOCK_METHOD(int64_t, get_written_bytes, (), (override));

    int64_t get_flushable_bytes() override { return _flushable_bytes; }

    Status flush() override {
        _flushed = true;
        return Status::OK();
    }

    bool is_flushed() { return _flushed; }

    void set_flushable_bytes(int64_t bytes) { _flushable_bytes = bytes; }

private:
    bool _flushed = false;
    int64_t _flushable_bytes = 0;
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

    const std::string& filename() const override { return _filename; }

private:
    std::string _filename = "mock_filename";
};

TEST_F(SinkMemoryManagerTest, kill_victim) {
    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {"c2", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto partition_chunk_writer_ctx =
            std::make_shared<SpillPartitionChunkWriterContext>(SpillPartitionChunkWriterContext{
                    nullptr, nullptr, 1024, false, nullptr, _fragment_context.get(), tuple_desc, nullptr});

    auto sink_mem_mgr = std::make_shared<SinkOperatorMemoryManager>();
    std::vector<PartitionChunkWriterPtr> writers;

    std::vector<int8_t> partition_field_null_list = {};
    const int64_t max_flush_bytes = 9;
    for (size_t i = 0; i < 5; ++i) {
        std::string partition = std::to_string(i);
        auto writer = std::make_shared<MockPartitionChunkWriter>(partition, partition_field_null_list,
                                                                 partition_chunk_writer_ctx);
        writer->set_flushable_bytes(i);
        auto out_stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        writer->_out_stream = std::move(out_stream);
        writers.push_back(writer);
    }
    for (size_t i = max_flush_bytes; i >= 5; --i) {
        std::string partition = std::to_string(i);
        auto writer = std::make_shared<MockPartitionChunkWriter>(partition, partition_field_null_list,
                                                                 partition_chunk_writer_ctx);
        writer->set_flushable_bytes(i);
        auto out_stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        writer->_out_stream = std::move(out_stream);
        writers.push_back(writer);
    }

    auto commit_callback = [this](const CommitResult& r) {};
    sink_mem_mgr->init(&writers, nullptr, commit_callback);

    EXPECT_TRUE(sink_mem_mgr->kill_victim());
    for (auto& writer : writers) {
        auto mock_writer = std::dynamic_pointer_cast<MockPartitionChunkWriter>(writer);
        if (mock_writer->get_flushable_bytes() == max_flush_bytes) {
            EXPECT_TRUE(mock_writer->is_flushed());
        } else {
            EXPECT_FALSE(mock_writer->is_flushed());
        }
    }
}

TEST_F(SinkMemoryManagerTest, init_with_vector) {
    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {"c2", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto partition_chunk_writer_ctx =
            std::make_shared<SpillPartitionChunkWriterContext>(SpillPartitionChunkWriterContext{
                    nullptr, nullptr, 1024, false, nullptr, _fragment_context.get(), tuple_desc, nullptr});

    auto sink_mem_mgr = std::make_shared<SinkOperatorMemoryManager>();
    std::vector<PartitionChunkWriterPtr> writers;

    std::vector<int8_t> partition_field_null_list = {};
    for (size_t i = 0; i < 3; ++i) {
        std::string partition = std::to_string(i);
        auto writer = std::make_shared<MockPartitionChunkWriter>(partition, partition_field_null_list,
                                                                 partition_chunk_writer_ctx);
        writer->set_flushable_bytes(100);
        auto out_stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        writer->_out_stream = std::move(out_stream);
        writers.push_back(writer);
    }

    auto commit_callback = [this](const CommitResult& r) {};
    Status status = sink_mem_mgr->init(&writers, nullptr, commit_callback);
    EXPECT_OK(status);
    EXPECT_EQ(sink_mem_mgr->update_writer_occupied_memory(), 300);
}

TEST_F(SinkMemoryManagerTest, kill_victim_selects_max_flushable_bytes) {
    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {"c2", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto partition_chunk_writer_ctx =
            std::make_shared<SpillPartitionChunkWriterContext>(SpillPartitionChunkWriterContext{
                    nullptr, nullptr, 1024, false, nullptr, _fragment_context.get(), tuple_desc, nullptr});

    auto sink_mem_mgr = std::make_shared<SinkOperatorMemoryManager>();
    std::vector<PartitionChunkWriterPtr> writers;

    std::vector<int8_t> partition_field_null_list = {};
    std::vector<int64_t> flushable_bytes = {10, 50, 30, 80, 20};
    for (size_t i = 0; i < flushable_bytes.size(); ++i) {
        std::string partition = std::to_string(i);
        auto writer = std::make_shared<MockPartitionChunkWriter>(partition, partition_field_null_list,
                                                                 partition_chunk_writer_ctx);
        writer->set_flushable_bytes(flushable_bytes[i]);
        auto out_stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        writer->_out_stream = std::move(out_stream);
        writers.push_back(writer);
    }

    auto commit_callback = [this](const CommitResult& r) {};
    sink_mem_mgr->init(&writers, nullptr, commit_callback);

    EXPECT_TRUE(sink_mem_mgr->kill_victim());
    for (auto& writer : writers) {
        auto mock_writer = std::dynamic_pointer_cast<MockPartitionChunkWriter>(writer);
        if (mock_writer->get_flushable_bytes() == 80) {
            EXPECT_TRUE(mock_writer->is_flushed());
        } else {
            EXPECT_FALSE(mock_writer->is_flushed());
        }
    }
}

TEST_F(SinkMemoryManagerTest, update_writer_occupied_memory) {
    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {"c2", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto partition_chunk_writer_ctx =
            std::make_shared<SpillPartitionChunkWriterContext>(SpillPartitionChunkWriterContext{
                    nullptr, nullptr, 1024, false, nullptr, _fragment_context.get(), tuple_desc, nullptr});

    auto sink_mem_mgr = std::make_shared<SinkOperatorMemoryManager>();
    std::vector<PartitionChunkWriterPtr> writers;

    std::vector<int8_t> partition_field_null_list = {};
    std::vector<int64_t> flushable_bytes = {100, 200, 300, 400, 500};
    for (size_t i = 0; i < flushable_bytes.size(); ++i) {
        std::string partition = std::to_string(i);
        auto writer = std::make_shared<MockPartitionChunkWriter>(partition, partition_field_null_list,
                                                                 partition_chunk_writer_ctx);
        writer->set_flushable_bytes(flushable_bytes[i]);
        auto out_stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        writer->_out_stream = std::move(out_stream);
        writers.push_back(writer);
    }

    auto commit_callback = [this](const CommitResult& r) {};
    sink_mem_mgr->init(&writers, nullptr, commit_callback);

    int64_t total_memory = sink_mem_mgr->update_writer_occupied_memory();
    EXPECT_EQ(total_memory, 1500);
    EXPECT_EQ(sink_mem_mgr->writer_occupied_memory(), 1500);
}

TEST_F(SinkMemoryManagerTest, iceberg_delete_sink_scenario) {
    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {"c2", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto partition_chunk_writer_ctx =
            std::make_shared<SpillPartitionChunkWriterContext>(SpillPartitionChunkWriterContext{
                    nullptr, nullptr, 1024, false, nullptr, _fragment_context.get(), tuple_desc, nullptr});

    auto sink_mem_mgr = std::make_shared<SinkOperatorMemoryManager>();
    std::vector<PartitionChunkWriterPtr> writers;

    std::vector<int8_t> partition_field_null_list = {};
    for (size_t i = 0; i < 10; ++i) {
        std::string partition = "partition_" + std::to_string(i);
        auto writer = std::make_shared<MockPartitionChunkWriter>(partition, partition_field_null_list,
                                                                 partition_chunk_writer_ctx);
        writer->set_flushable_bytes(1000 + i * 100);
        auto out_stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        writer->_out_stream = std::move(out_stream);
        writers.push_back(writer);
    }

    auto commit_callback = [this](const CommitResult& r) {};
    sink_mem_mgr->init(&writers, nullptr, commit_callback);

    int64_t total_memory = sink_mem_mgr->update_writer_occupied_memory();
    EXPECT_EQ(total_memory, 14500);

    EXPECT_TRUE(sink_mem_mgr->kill_victim());
    int64_t max_flushable = 1900;
    for (auto& writer : writers) {
        auto mock_writer = std::dynamic_pointer_cast<MockPartitionChunkWriter>(writer);
        if (mock_writer->get_flushable_bytes() == max_flushable) {
            EXPECT_TRUE(mock_writer->is_flushed());
        } else {
            EXPECT_FALSE(mock_writer->is_flushed());
        }
    }
}

} // namespace
} // namespace starrocks::connector
