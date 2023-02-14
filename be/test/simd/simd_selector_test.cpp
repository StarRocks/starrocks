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

#include <limits>

#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "gtest/gtest.h"
#include "simd/selector.h"
#include "testutil/parallel_test.h"
#include "types/logical_type.h"
#include "util/value_generator.h"

namespace starrocks {

template <LogicalType TYPE, template <typename T> class Gen1, template <typename T> class Gen2,
          template <typename T> class Gen3, int TEST_SIZE>
void test_simd_select_if() {
    static_assert(isArithmeticLT<TYPE>, "Now Select IF only support Arithmetic TYPE");

    using SelectorContainer = typename BooleanColumn::Container;
    using RuntimeContainer = typename RunTimeColumnType<TYPE>::Container;

    using SelectorDataGenerator = Gen1<SelectorContainer::value_type>;
    using FirstValueGenerator = Gen2<typename RuntimeContainer::value_type>;
    using SecondValueGenerator = Gen3<typename RuntimeContainer::value_type>;

    using DstValueGenerator = AlwaysZeroGenerator<typename RuntimeContainer::value_type>;

    using SelectorInit = ContainerIniter<SelectorDataGenerator, SelectorContainer, TEST_SIZE>;
    using Input1Init = ContainerIniter<FirstValueGenerator, RuntimeContainer, TEST_SIZE>;
    using Input2Init = ContainerIniter<SecondValueGenerator, RuntimeContainer, TEST_SIZE>;

    // Test Var Var
    {
        SelectorContainer selector;
        RuntimeContainer dst;
        RuntimeContainer input1;
        RuntimeContainer input2;

        // init data
        SelectorInit::init(selector);
        Input1Init::init(input1);
        Input2Init::init(input2);
        ContainerIniter<DstValueGenerator, RuntimeContainer, TEST_SIZE>::init(dst);

        SIMD_selector<TYPE>::select_if(selector.data(), dst, input1, input2);

        for (int i = 0; i < TEST_SIZE; ++i) {
            auto res = selector[i] ? input1[i] : input2[i];
            ASSERT_EQ(dst[i], res);
        }
    }
    // Test Var Const
    {
        SelectorContainer selector;
        RuntimeContainer dst;
        RuntimeContainer input1;
        RunTimeCppType<TYPE> input2;

        SelectorInit::init(selector);
        Input1Init::init(input1);
        input2 = SecondValueGenerator::next_value();
        ContainerIniter<DstValueGenerator, RuntimeContainer, TEST_SIZE>::init(dst);

        SIMD_selector<TYPE>::select_if(selector.data(), dst, input1, input2);

        for (int i = 0; i < TEST_SIZE; ++i) {
            auto res = selector[i] ? input1[i] : input2;
            ASSERT_EQ(dst[i], res);
        }
    }
    // Test Const Var
    {
        SelectorContainer selector;
        RuntimeContainer dst;
        RunTimeCppType<TYPE> input1;
        RuntimeContainer input2;

        SelectorInit::init(selector);
        input1 = FirstValueGenerator::next_value();
        Input2Init::init(input2);
        ContainerIniter<DstValueGenerator, RuntimeContainer, TEST_SIZE>::init(dst);

        SIMD_selector<TYPE>::select_if(selector.data(), dst, input1, input2);

        for (int i = 0; i < TEST_SIZE; ++i) {
            auto res = selector[i] ? input1 : input2[i];
            ASSERT_EQ(dst[i], res);
        }
    }
    // Test Const Const
    {
        SelectorContainer selector;
        RuntimeContainer dst;
        RunTimeCppType<TYPE> input1;
        RunTimeCppType<TYPE> input2;

        SelectorInit::init(selector);
        input1 = SecondValueGenerator::next_value();
        input2 = SecondValueGenerator::next_value();
        ContainerIniter<DstValueGenerator, RuntimeContainer, TEST_SIZE>::init(dst);

        SIMD_selector<TYPE>::select_if(selector.data(), dst, input1, input2);

        for (int i = 0; i < TEST_SIZE; ++i) {
            auto res = selector[i] ? input1 : input2;
            ASSERT_EQ(dst[i], res);
        }
    }
}

template <class T>
using SelectorRandomGen = RandomGenerator<T, 2>;

template <class T>
using ValueRandomGen = RandomGenerator<T, 65535>;

template <LogicalType TYPE, int TEST_SIZE>
bool test_simd_select_if_wrapper() {
    test_simd_select_if<TYPE, AlwaysZeroGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, TEST_SIZE>();
    test_simd_select_if<TYPE, AlwaysOneGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, TEST_SIZE>();
    test_simd_select_if<TYPE, SelectorRandomGen, ValueRandomGen, ValueRandomGen, TEST_SIZE>();
    return true;
}

template <LogicalType... TYPE>
bool test_simd_select_if_all() {
    constexpr int chunk_size = 4095;
    return (... && test_simd_select_if_wrapper<TYPE, chunk_size>());
}

PARALLEL_TEST(SIMDSelectorTest, SelectorTest) {
    constexpr int BATCH_SIZE = 4095;
    {
        test_simd_select_if<TYPE_BOOLEAN, AlwaysZeroGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_BOOLEAN, AlwaysOneGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_BOOLEAN, SelectorRandomGen, SelectorRandomGen, SelectorRandomGen, BATCH_SIZE>();
    }
    // clang-format off
    test_simd_select_if_all<TYPE_TINYINT,
                            TYPE_SMALLINT, 
                            TYPE_INT, 
                            TYPE_BIGINT, 
                            TYPE_LARGEINT, 
                            TYPE_DECIMAL32,
                            TYPE_DECIMAL64, 
                            TYPE_DECIMAL128, 
                            TYPE_FLOAT, 
                            TYPE_DOUBLE>();
    // clang-format on
}
} // namespace starrocks
