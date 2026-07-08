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
#include "exec/exec_env.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/file_writer.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/utils.h"

namespace starrocks::connector {
namespace {

using Stream = formats::AsyncFlushOutputStream;

class SinkMemoryManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>());
        _runtime_state = _fragment_context->runtime_state();
        auto* exec_env = ExecEnv::GetInstance();
        _runtime_state->set_exec_env(exec_env);
        _runtime_state->set_query_execution_services(&exec_env->query_execution_services());
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
        if (_clear_flushable_on_flush) {
            _flushable_bytes = 0;
        }
        return Status::OK();
    }

    bool is_flushed() { return _flushed; }

    void set_flushable_bytes(int64_t bytes) { _flushable_bytes = bytes; }

    void clear_flushable_on_flush() { _clear_flushable_on_flush = true; }

private:
    bool _flushed = false;
    bool _clear_flushable_on_flush = false;
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
                    {nullptr, nullptr, 1024, false}, nullptr, _fragment_context.get(), nullptr, tuple_desc, nullptr});

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

    sink_mem_mgr->init(&writers, nullptr);

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
                    {nullptr, nullptr, 1024, false}, nullptr, _fragment_context.get(), nullptr, tuple_desc, nullptr});

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

    Status status = sink_mem_mgr->init(&writers, nullptr);
    EXPECT_OK(status);
    EXPECT_EQ(sink_mem_mgr->update_writer_occupied_memory(), 300);
}

TEST_F(SinkMemoryManagerTest, kill_victim_selects_max_flushable_bytes) {
    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {"c2", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto partition_chunk_writer_ctx =
            std::make_shared<SpillPartitionChunkWriterContext>(SpillPartitionChunkWriterContext{
                    {nullptr, nullptr, 1024, false}, nullptr, _fragment_context.get(), nullptr, tuple_desc, nullptr});

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

    sink_mem_mgr->init(&writers, nullptr);

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
                    {nullptr, nullptr, 1024, false}, nullptr, _fragment_context.get(), nullptr, tuple_desc, nullptr});

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

    sink_mem_mgr->init(&writers, nullptr);

    int64_t total_memory = sink_mem_mgr->update_writer_occupied_memory();
    EXPECT_EQ(total_memory, 1500);
    EXPECT_EQ(sink_mem_mgr->writer_occupied_memory(), 1500);
}

TEST_F(SinkMemoryManagerTest, registered_children_participate_in_total_occupied_memory) {
    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {"c2", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto partition_chunk_writer_ctx =
            std::make_shared<SpillPartitionChunkWriterContext>(SpillPartitionChunkWriterContext{
                    {nullptr, nullptr, 1024, false}, nullptr, _fragment_context.get(), nullptr, tuple_desc, nullptr});

    MemTracker query_pool_tracker(MemTrackerType::QUERY_POOL, 1000, "registered_children_pool");
    MemTracker query_tracker(MemTrackerType::QUERY, -1, "registered_children_query");
    query_pool_tracker.consume(950);
    SinkMemoryManager manager(&query_pool_tracker, &query_tracker);

    auto child1 = std::make_unique<SinkOperatorMemoryManager>();
    auto* child1_ptr = child1.get();
    ASSERT_EQ(manager.register_child_manager(std::move(child1)), child1_ptr);

    auto child2 = std::make_unique<SinkOperatorMemoryManager>();
    auto* child2_ptr = child2.get();
    ASSERT_EQ(manager.register_child_manager(std::move(child2)), child2_ptr);

    std::vector<int8_t> partition_field_null_list = {};
    std::vector<PartitionChunkWriterPtr> child1_writers;
    auto child1_writer =
            std::make_shared<MockPartitionChunkWriter>("child1", partition_field_null_list, partition_chunk_writer_ctx);
    child1_writer->set_flushable_bytes(10);
    child1_writer->clear_flushable_on_flush();
    child1_writer->_out_stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
    child1_writers.push_back(child1_writer);

    std::vector<PartitionChunkWriterPtr> child2_writers;
    auto child2_writer =
            std::make_shared<MockPartitionChunkWriter>("child2", partition_field_null_list, partition_chunk_writer_ctx);
    child2_writer->set_flushable_bytes(100);
    child2_writers.push_back(child2_writer);

    formats::AsyncFlushStreamPoller child1_poller;
    formats::AsyncFlushStreamPoller child2_poller;
    ASSERT_OK(child1_ptr->init(&child1_writers, &child1_poller));
    ASSERT_OK(child2_ptr->init(&child2_writers, &child2_poller));
    EXPECT_EQ(child2_ptr->update_writer_occupied_memory(), 100);

    EXPECT_TRUE(manager.can_accept_more_input(child1_ptr));
    EXPECT_TRUE(child1_writer->is_flushed());
}

TEST_F(SinkMemoryManagerTest, iceberg_delete_sink_scenario) {
    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {"c2", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto partition_chunk_writer_ctx =
            std::make_shared<SpillPartitionChunkWriterContext>(SpillPartitionChunkWriterContext{
                    {nullptr, nullptr, 1024, false}, nullptr, _fragment_context.get(), nullptr, tuple_desc, nullptr});

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

    sink_mem_mgr->init(&writers, nullptr);

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
