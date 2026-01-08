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

#include "exec/pipeline/set/raw_values_source_operator.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {

class RawValuesSourceOperatorTest : public ::testing::Test {
public:
    void SetUp() override {
        _runtime_state.set_chunk_size(4096);
        _obj_pool = std::make_unique<ObjectPool>();
        
        // Create a simple slot descriptor for testing
        TSlotDescriptor t_slot_desc;
        t_slot_desc.__set_id(0);
        t_slot_desc.__set_slotType(TypeDescriptor(TYPE_BIGINT).to_thrift());
        t_slot_desc.__set_columnPos(0);
        t_slot_desc.__set_byteOffset(0);
        t_slot_desc.__set_nullIndicatorByte(0);
        t_slot_desc.__set_nullIndicatorBit(0);
        t_slot_desc.__set_colName("value");
        t_slot_desc.__set_slotIdx(0);
        t_slot_desc.__set_isMaterialized(true);
        
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.__set_id(0);
        t_tuple_desc.__set_byteSize(8);
        t_tuple_desc.__set_numNullBytes(0);
        
        auto* tuple_desc = _obj_pool->add(new TupleDescriptor(t_tuple_desc));
        auto* slot_desc = _obj_pool->add(new SlotDescriptor(t_slot_desc));
        slot_desc->set_parent(tuple_desc);
        tuple_desc->add_slot(slot_desc);
        
        _slot_descriptors.push_back(slot_desc);
    }

    void TearDown() override {}

protected:
    RuntimeState _runtime_state;
    std::unique_ptr<ObjectPool> _obj_pool;
    std::vector<SlotDescriptor*> _slot_descriptors;
};

// Test set_finished with long values
TEST_F(RawValuesSourceOperatorTest, test_set_finished_with_long_values) {
    // Create test data
    std::vector<int64_t> long_values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<std::string> string_values;
    
    TypeDescriptor type_desc(TYPE_BIGINT);
    
    RawValuesSourceOperatorFactory factory(1, 1, _slot_descriptors, type_desc, 
                                          std::move(long_values), std::move(string_values));
    
    auto op = factory.create(1, 0);
    ASSERT_TRUE(op != nullptr);
    
    // Prepare the operator
    ASSERT_OK(op->prepare(&_runtime_state));
    
    // Verify operator is not finished initially
    EXPECT_TRUE(op->has_output());
    EXPECT_FALSE(op->is_finished());
    
    // Call set_finished
    ASSERT_OK(op->set_finished(&_runtime_state));
    
    // After set_finished, the operator should report as finished
    EXPECT_FALSE(op->has_output());
    EXPECT_TRUE(op->is_finished());
    
    // Clean up
    op->close(&_runtime_state);
}

// Test set_finished with string values
TEST_F(RawValuesSourceOperatorTest, test_set_finished_with_string_values) {
    // Create test data
    std::vector<int64_t> long_values;
    std::vector<std::string> string_values = {"a", "b", "c", "d", "e"};
    
    TypeDescriptor type_desc(TYPE_VARCHAR);
    type_desc.len = 65535;
    
    RawValuesSourceOperatorFactory factory(1, 1, _slot_descriptors, type_desc,
                                          std::move(long_values), std::move(string_values));
    
    auto op = factory.create(1, 0);
    ASSERT_TRUE(op != nullptr);
    
    // Prepare the operator
    ASSERT_OK(op->prepare(&_runtime_state));
    
    // Verify operator is not finished initially
    EXPECT_TRUE(op->has_output());
    EXPECT_FALSE(op->is_finished());
    
    // Call set_finished
    ASSERT_OK(op->set_finished(&_runtime_state));
    
    // After set_finished, the operator should report as finished
    EXPECT_FALSE(op->has_output());
    EXPECT_TRUE(op->is_finished());
    
    // Clean up
    op->close(&_runtime_state);
}

// Test set_finished after operator is already finished
TEST_F(RawValuesSourceOperatorTest, test_set_finished_after_completion) {
    // Create test data with a small number of values
    std::vector<int64_t> long_values = {1, 2, 3};
    std::vector<std::string> string_values;
    
    TypeDescriptor type_desc(TYPE_BIGINT);
    
    RawValuesSourceOperatorFactory factory(1, 1, _slot_descriptors, type_desc,
                                          std::move(long_values), std::move(string_values));
    
    auto op = factory.create(1, 0);
    ASSERT_TRUE(op != nullptr);
    
    // Prepare the operator
    ASSERT_OK(op->prepare(&_runtime_state));
    
    // Pull all chunks until finished
    while (op->has_output()) {
        auto chunk_or = op->pull_chunk(&_runtime_state);
        ASSERT_OK(chunk_or.status());
    }
    
    // Verify operator is finished
    EXPECT_FALSE(op->has_output());
    EXPECT_TRUE(op->is_finished());
    
    // Call set_finished even when already finished (should be idempotent)
    ASSERT_OK(op->set_finished(&_runtime_state));
    
    // Verify state is still finished
    EXPECT_FALSE(op->has_output());
    EXPECT_TRUE(op->is_finished());
    
    // Clean up
    op->close(&_runtime_state);
}

// Test set_finished with multiple drivers (DOP > 1)
TEST_F(RawValuesSourceOperatorTest, test_set_finished_with_multiple_drivers) {
    // Create test data
    std::vector<int64_t> long_values;
    for (int64_t i = 0; i < 100; ++i) {
        long_values.push_back(i);
    }
    std::vector<std::string> string_values;
    
    TypeDescriptor type_desc(TYPE_BIGINT);
    
    RawValuesSourceOperatorFactory factory(1, 1, _slot_descriptors, type_desc,
                                          std::move(long_values), std::move(string_values));
    
    // Create multiple operators (simulating DOP = 4)
    int degree_of_parallelism = 4;
    std::vector<OperatorPtr> operators;
    
    for (int i = 0; i < degree_of_parallelism; ++i) {
        auto op = factory.create(degree_of_parallelism, i);
        ASSERT_TRUE(op != nullptr);
        ASSERT_OK(op->prepare(&_runtime_state));
        operators.push_back(op);
    }
    
    // Call set_finished on each operator
    for (auto& op : operators) {
        EXPECT_TRUE(op->has_output());
        EXPECT_FALSE(op->is_finished());
        ASSERT_OK(op->set_finished(&_runtime_state));
        // After set_finished, operator should be finished
        EXPECT_FALSE(op->has_output());
        EXPECT_TRUE(op->is_finished());
    }
    
    // Clean up all operators
    for (auto& op : operators) {
        op->close(&_runtime_state);
    }
}

// Test set_finished can be called multiple times (idempotent)
TEST_F(RawValuesSourceOperatorTest, test_set_finished_idempotent) {
    // Create test data
    std::vector<int64_t> long_values = {1, 2, 3, 4, 5};
    std::vector<std::string> string_values;
    
    TypeDescriptor type_desc(TYPE_BIGINT);
    
    RawValuesSourceOperatorFactory factory(1, 1, _slot_descriptors, type_desc,
                                          std::move(long_values), std::move(string_values));
    
    auto op = factory.create(1, 0);
    ASSERT_TRUE(op != nullptr);
    
    // Prepare the operator
    ASSERT_OK(op->prepare(&_runtime_state));
    
    // Call set_finished multiple times
    ASSERT_OK(op->set_finished(&_runtime_state));
    EXPECT_FALSE(op->has_output());
    EXPECT_TRUE(op->is_finished());
    
    ASSERT_OK(op->set_finished(&_runtime_state));
    EXPECT_FALSE(op->has_output());
    EXPECT_TRUE(op->is_finished());
    
    ASSERT_OK(op->set_finished(&_runtime_state));
    EXPECT_FALSE(op->has_output());
    EXPECT_TRUE(op->is_finished());
    
    // Clean up
    op->close(&_runtime_state);
}

} // namespace starrocks::pipeline
