// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "gtest/gtest.h"
#include "runtime/primitive_type.h"
#include "simd/selector.h"
#include "testutil/parallel_test.h"
#include "util/value_generator.h"

namespace starrocks::vectorized {

template <PrimitiveType TYPE, template <typename T> class Gen1, template <typename T> class Gen2,
          template <typename T> class Gen3, int TEST_SIZE>
void test_simd_select_if() {
    static_assert(isArithmeticPT<TYPE>, "Now Select IF only support Arithmetic TYPE");

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
using ValueRandomGen = RandomGenerator<T, 128>;

PARALLEL_TEST(SIMDSelectorTest, SelectorTest) {
    constexpr int BATCH_SIZE = 4095;
    {
        test_simd_select_if<TYPE_TINYINT, AlwaysZeroGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_TINYINT, AlwaysOneGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_TINYINT, SelectorRandomGen, SelectorRandomGen, SelectorRandomGen, BATCH_SIZE>();
    }
    {
        test_simd_select_if<TYPE_INT, AlwaysZeroGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_INT, AlwaysOneGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_INT, SelectorRandomGen, SelectorRandomGen, SelectorRandomGen, BATCH_SIZE>();
    }
    {
        test_simd_select_if<TYPE_BIGINT, AlwaysZeroGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_BIGINT, AlwaysOneGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_BIGINT, SelectorRandomGen, SelectorRandomGen, SelectorRandomGen, BATCH_SIZE>();
    }
    {
        test_simd_select_if<TYPE_FLOAT, AlwaysZeroGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_FLOAT, AlwaysOneGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_FLOAT, SelectorRandomGen, SelectorRandomGen, SelectorRandomGen, BATCH_SIZE>();
    }
    {
        test_simd_select_if<TYPE_DOUBLE, AlwaysZeroGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_DOUBLE, AlwaysOneGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_DOUBLE, SelectorRandomGen, SelectorRandomGen, SelectorRandomGen, BATCH_SIZE>();
    }
    {
        test_simd_select_if<TYPE_LARGEINT, AlwaysZeroGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_LARGEINT, AlwaysOneGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_LARGEINT, SelectorRandomGen, SelectorRandomGen, SelectorRandomGen, BATCH_SIZE>();
    }
    {
        test_simd_select_if<TYPE_DECIMAL32, AlwaysZeroGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_DECIMAL32, AlwaysOneGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_DECIMAL32, SelectorRandomGen, SelectorRandomGen, SelectorRandomGen, BATCH_SIZE>();
    }
    {
        test_simd_select_if<TYPE_DECIMAL64, AlwaysZeroGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_DECIMAL64, AlwaysOneGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_DECIMAL64, SelectorRandomGen, SelectorRandomGen, SelectorRandomGen, BATCH_SIZE>();
    }
    {
        test_simd_select_if<TYPE_DECIMAL128, AlwaysZeroGenerator, AlwaysOneGenerator, AlwaysZeroGenerator,
                            BATCH_SIZE>();
        test_simd_select_if<TYPE_DECIMAL128, AlwaysOneGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, BATCH_SIZE>();
        test_simd_select_if<TYPE_DECIMAL128, SelectorRandomGen, SelectorRandomGen, SelectorRandomGen, BATCH_SIZE>();
    }
}
} // namespace starrocks::vectorized