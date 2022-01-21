// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <cstdint>

#include "column/vectorized_fwd.h"
#include "gtest/gtest.h"
#include "runtime/primitive_type.h"
#include "simd/mulselector.h"
#include "testutil/parallel_test.h"
#include "util/value_generator.h"

namespace starrocks::vectorized {

template <PrimitiveType TYPE, int CASE_SIZE, int TEST_SIZE>
void test_simd_multi_select_if() {
    static_assert(isArithmeticPT<TYPE>, "Now Select IF only support Arithmetic TYPE");

    using SelectVec = typename SIMD_muti_selector<TYPE>::SelectVec;
    using Container = typename SIMD_muti_selector<TYPE>::Container;
    using SelectorContainer = typename BooleanColumn::Container;
    using RuntimeCppType = typename SIMD_muti_selector<TYPE>::CppType;

    {
        SelectorContainer selectors[CASE_SIZE];
        Container data_valus[CASE_SIZE + 1];
        Container dst;

        SelectVec select_vecs[CASE_SIZE];
        Container* select_lists[CASE_SIZE + 1];

        using SelectorInit = ContainerIniter<RandomGenerator<uint8_t, 2>, SelectorContainer, TEST_SIZE>;
        using DataInit = ContainerIniter<RandomGenerator<RuntimeCppType, 128>, Container, TEST_SIZE>;

        // Init TEST Value
        for (int i = 0; i < CASE_SIZE; ++i) {
            SelectorInit::init(selectors[i]);
            select_vecs[i] = selectors[i].data();
        }
        for (int i = 0; i < CASE_SIZE + 1; ++i) {
            DataInit::init(data_valus[i]);
            select_lists[i] = &data_valus[i];
        }
        DataInit::init(dst);

        SIMD_muti_selector<TYPE>::multi_select_if(select_vecs, CASE_SIZE, dst, select_lists, CASE_SIZE + 1);

        for (int i = 0; i < TEST_SIZE; i++) {
            int index = 0;
            while (index < CASE_SIZE && !select_vecs[index][i]) {
                index++;
            }
            auto result = (*select_lists[index])[i];
            ASSERT_EQ(result, dst[i]);
        }
    }
}

template <PrimitiveType TYPE, int CASE_SIZE, int TEST_SIZE>
bool test_function_wrapper() {
    test_simd_multi_select_if<TYPE, CASE_SIZE, TEST_SIZE>();
    return true;
}

template <PrimitiveType... TYPE>
bool test_all() {
    constexpr int chunk_size = 4095;

    return (... && test_function_wrapper<TYPE, 1, chunk_size>()) &&
           (... && test_function_wrapper<TYPE, 2, chunk_size>()) &&
           (... && test_function_wrapper<TYPE, 4, chunk_size>()) &&
           (... && test_function_wrapper<TYPE, 8, chunk_size>());
}

PARALLEL_TEST(SIMDMultiSelectorTest, TestVarVar) {
    // clang-format off
    test_all<TYPE_TINYINT, 
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
} // namespace starrocks::vectorized