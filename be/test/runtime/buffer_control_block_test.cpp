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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/runtime/buffer_control_block_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/buffer_control_block.h"

#include <arrow/api.h>
#include <gtest/gtest.h>
#include <pthread.h>

#include <future>

#include "gen_cpp/InternalService_types.h"

namespace starrocks {

class BufferControlBlockTest : public testing::Test {
public:
    BufferControlBlockTest() = default;
    ~BufferControlBlockTest() override = default;

protected:
    void SetUp() override {}

private:
};

TEST_F(BufferControlBlockTest, init_normal) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());
}

TEST_F(BufferControlBlockTest, add_one_get_one) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    std::unique_ptr<TFetchDataResult> add_result(new TFetchDataResult());
    add_result->result_batch.rows.emplace_back("hello test");
    ASSERT_TRUE(control_block.add_batch(add_result).ok());

    TFetchDataResult get_result;
    ASSERT_TRUE(control_block.get_batch(&get_result).ok());
    ASSERT_FALSE(get_result.eos);
    ASSERT_EQ(1U, get_result.result_batch.rows.size());
    ASSERT_STREQ("hello test", get_result.result_batch.rows[0].c_str());
}

TEST_F(BufferControlBlockTest, get_one_after_close) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    control_block.close(Status::OK());
    TFetchDataResult get_result;
    ASSERT_TRUE(control_block.get_batch(&get_result).ok());
    ASSERT_TRUE(get_result.eos);
}

TEST_F(BufferControlBlockTest, get_add_after_cancel) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    control_block.cancel();
    std::unique_ptr<TFetchDataResult> add_result(new TFetchDataResult());
    add_result->result_batch.rows.emplace_back("hello test");
    ASSERT_FALSE(control_block.add_batch(add_result).ok());

    TFetchDataResult get_result;
    ASSERT_FALSE(control_block.get_batch(&get_result).ok());
}

void* cancel_thread(void* param) {
    auto* control_block = static_cast<BufferControlBlock*>(param);
    sleep(1);
    control_block->cancel();
    return nullptr;
}

TEST_F(BufferControlBlockTest, add_then_cancel) {
    // only can add one batch
    BufferControlBlock control_block(TUniqueId(), 1);
    ASSERT_TRUE(control_block.init().ok());

    // add_batch in main thread -> cancel in cancel_thread -> add_batch in main thread
    std::promise<void> p1, p2;
    std::future<void> f1 = p1.get_future(), f2 = p2.get_future();
    auto cancel_thread = std::thread([&]() {
        f1.wait();
        control_block.cancel();
        p2.set_value();
    });

    {
        std::unique_ptr<TFetchDataResult> add_result(new TFetchDataResult());
        add_result->result_batch.rows.emplace_back("hello test1");
        add_result->result_batch.rows.emplace_back("hello test2");
        ASSERT_TRUE(control_block.add_batch(add_result).ok());
        p1.set_value();
    }
    {
        f2.wait();
        std::unique_ptr<TFetchDataResult> add_result(new TFetchDataResult());
        add_result->result_batch.rows.emplace_back("hello test1");
        add_result->result_batch.rows.emplace_back("hello test2");
        ASSERT_FALSE(control_block.add_batch(add_result).ok());
    }

    TFetchDataResult get_result;
    ASSERT_FALSE(control_block.get_batch(&get_result).ok());

    cancel_thread.join();
}

TEST_F(BufferControlBlockTest, get_then_cancel) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    pthread_t id;
    pthread_create(&id, nullptr, cancel_thread, &control_block);

    // get block until cancel
    TFetchDataResult get_result;
    ASSERT_FALSE(control_block.get_batch(&get_result).ok());

    pthread_join(id, nullptr);
}

void* add_thread(void* param) {
    auto* control_block = static_cast<BufferControlBlock*>(param);
    sleep(1);
    {
        std::unique_ptr<TFetchDataResult> add_result(new TFetchDataResult());
        add_result->result_batch.rows.emplace_back("hello test1");
        add_result->result_batch.rows.emplace_back("hello test2");
        control_block->add_batch(add_result);
    }
    return nullptr;
}

TEST_F(BufferControlBlockTest, get_then_add) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    pthread_t id;
    pthread_create(&id, nullptr, add_thread, &control_block);

    // get block until a batch add
    TFetchDataResult get_result;
    ASSERT_TRUE(control_block.get_batch(&get_result).ok());
    ASSERT_FALSE(get_result.eos);
    ASSERT_EQ(2U, get_result.result_batch.rows.size());
    ASSERT_STREQ("hello test1", get_result.result_batch.rows[0].c_str());
    ASSERT_STREQ("hello test2", get_result.result_batch.rows[1].c_str());

    pthread_join(id, nullptr);
}

void* close_thread(void* param) {
    auto* control_block = static_cast<BufferControlBlock*>(param);
    sleep(1);
    control_block->close(Status::OK());
    return nullptr;
}

TEST_F(BufferControlBlockTest, get_then_close) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    pthread_t id;
    pthread_create(&id, nullptr, close_thread, &control_block);

    // get block until a batch add
    TFetchDataResult get_result;
    ASSERT_TRUE(control_block.get_batch(&get_result).ok());
    ASSERT_TRUE(get_result.eos);
    ASSERT_EQ(0U, get_result.result_batch.rows.size());

    pthread_join(id, nullptr);
}

TEST_F(BufferControlBlockTest, is_full_arrow_batch_queue) {
    BufferControlBlock control_block(TUniqueId(), 1);
    ASSERT_TRUE(control_block.init().ok());

    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.Resize(4097).ok());
    for (int i = 0; i < 4097; ++i) {
        ASSERT_TRUE(builder.Append(i).ok());
    }

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    auto schema = arrow::schema({arrow::field("int_col", arrow::int32())});
    auto record_batch = arrow::RecordBatch::Make(schema, 4097, {array});
    ASSERT_TRUE(control_block.add_arrow_batch(record_batch).ok());

    ASSERT_TRUE(control_block.is_full());
}

TEST_F(BufferControlBlockTest, get_arrow_batch_simple) {
    BufferControlBlock control_block(TUniqueId(), 1024);
    ASSERT_TRUE(control_block.init().ok());

    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.Append(100).ok());
    ASSERT_TRUE(builder.Append(200).ok());

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    auto schema = arrow::schema({arrow::field("int_col", arrow::int32())});
    auto record_batch = arrow::RecordBatch::Make(schema, 2, {array});

    ASSERT_TRUE(control_block.add_arrow_batch(record_batch).ok());

    std::shared_ptr<arrow::RecordBatch> result;
    Status st = control_block.get_arrow_batch(&result);

    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(result != nullptr);
    ASSERT_EQ(result->num_rows(), 2);
    ASSERT_EQ(result->num_columns(), 1);

    auto int_array = std::static_pointer_cast<arrow::Int32Array>(result->column(0));
    ASSERT_EQ(int_array->Value(0), 100);
    ASSERT_EQ(int_array->Value(1), 200);
}

} // namespace starrocks

/* vim: set ts=4 sw=4 sts=4 tw=100 noet: */
