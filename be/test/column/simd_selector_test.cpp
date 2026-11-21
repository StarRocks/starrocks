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

#include "column/simd_selector.h"

#include <cstring>
#include <limits>
#include <vector>

#include "base/testutil/parallel_test.h"
#include "column/runtime_type_traits.h"
#include "column/simd_mulselector.h"
#include "column/vectorized_fwd.h"
#include "gtest/gtest.h"
#include "types/logical_type.h"
#include "types/value_generator.h"

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

template <int TEST_SIZE, LogicalType... TYPE>
bool test_simd_select_if_for_size() {
    test_simd_select_if<TYPE_BOOLEAN, AlwaysZeroGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, TEST_SIZE>();
    test_simd_select_if<TYPE_BOOLEAN, AlwaysOneGenerator, AlwaysOneGenerator, AlwaysZeroGenerator, TEST_SIZE>();
    test_simd_select_if<TYPE_BOOLEAN, SelectorRandomGen, SelectorRandomGen, SelectorRandomGen, TEST_SIZE>();
    return (... && test_simd_select_if_wrapper<TYPE, TEST_SIZE>());
}

template <LogicalType TYPE, int TEST_SIZE>
void test_simd_multi_select_boundary_for_size() {
    using SelectVec = typename SIMD_muti_selector<TYPE>::SelectVec;
    using Container = typename SIMD_muti_selector<TYPE>::Container;
    using SelectorContainer = typename BooleanColumn::Container;
    using CppType = typename SIMD_muti_selector<TYPE>::CppType;

    constexpr int kWhenBranches = 4;
    constexpr int kListSize = kWhenBranches + 1;

    // Pattern 0: All ELSE (select_nibble_mask == 0)
    // Pattern 1: Single winner (single_winner >= 0)
    // Pattern 2: Sub-chunk disjoint winners (0 or 1 active winner per sub-chunk)
    // Pattern 3: Alternating mixed winners across all lanes
    for (int pat = 0; pat < 4; ++pat) {
        for (int const_mode = 0; const_mode < 3; ++const_mode) {
            SelectorContainer selectors[kWhenBranches];
            SelectVec select_vecs[kWhenBranches];
            for (int b = 0; b < kWhenBranches; ++b) {
                selectors[b].resize(TEST_SIZE);
                for (int r = 0; r < TEST_SIZE; ++r) {
                    if (pat == 0) {
                        selectors[b][r] = 0;
                    } else if (pat == 1) {
                        selectors[b][r] = (b == 2) ? 1 : 0;
                    } else if (pat == 2) {
                        // Partition 4-row sub-chunks across branches 0, 1, 2, and ELSE
                        int sub = (r / 4) % 4;
                        selectors[b][r] = (b == sub) ? 1 : 0;
                    } else {
                        selectors[b][r] = (((r + b) % kListSize) == b) ? 1 : 0;
                    }
                }
                select_vecs[b] = selectors[b].data();
            }

            Container branches[kListSize];
            const Container* select_lists[kListSize];
            std::vector<bool> is_const(kListSize, false);
            for (int i = 0; i < kListSize; ++i) {
                is_const[i] = (const_mode == 1) || (const_mode == 2 && (i % 2 == 1));
                branches[i].resize(is_const[i] ? 1 : TEST_SIZE);
                for (size_t r = 0; r < branches[i].size(); ++r) {
                    branches[i][r] = RandomGenerator<CppType, 128>::next_value();
                }
                select_lists[i] = &branches[i];
            }

            Container dst(TEST_SIZE);
            Container expected(TEST_SIZE);
            std::memset(dst.data(), 0xCD, TEST_SIZE * sizeof(CppType));
            for (int r = 0; r < TEST_SIZE; ++r) {
                int idx = 0;
                while (idx < kWhenBranches && !select_vecs[idx][r]) {
                    ++idx;
                }
                expected[r] = is_const[idx] ? (*select_lists[idx])[0] : (*select_lists[idx])[r];
            }

            SIMD_muti_selector<TYPE>::multi_select_if(select_vecs, kWhenBranches, dst, select_lists, kListSize,
                                                      is_const, TEST_SIZE);
            ASSERT_EQ(0, std::memcmp(expected.data(), dst.data(), TEST_SIZE * sizeof(CppType)));
        }
    }
}

template <int TEST_SIZE, LogicalType... TYPE>
void test_simd_multi_select_boundary_all() {
    (test_simd_multi_select_boundary_for_size<TYPE, TEST_SIZE>(), ...);
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

PARALLEL_TEST(SIMDSelectorTest, ExactMultiplesOf16Boundary) {
    // Explicit boundary coverage for exact multiples of 16 (N = 16, N = 32).
    // clang-format off
    test_simd_select_if_for_size<16,
                                 TYPE_TINYINT,
                                 TYPE_SMALLINT,
                                 TYPE_INT,
                                 TYPE_BIGINT,
                                 TYPE_LARGEINT,
                                 TYPE_DECIMAL32,
                                 TYPE_DECIMAL64,
                                 TYPE_DECIMAL128,
                                 TYPE_FLOAT,
                                 TYPE_DOUBLE>();
    test_simd_select_if_for_size<32,
                                 TYPE_TINYINT,
                                 TYPE_SMALLINT,
                                 TYPE_INT,
                                 TYPE_BIGINT,
                                 TYPE_LARGEINT,
                                 TYPE_DECIMAL32,
                                 TYPE_DECIMAL64,
                                 TYPE_DECIMAL128,
                                 TYPE_FLOAT,
                                 TYPE_DOUBLE>();
    test_simd_multi_select_boundary_all<16,
                                        TYPE_TINYINT,
                                        TYPE_SMALLINT,
                                        TYPE_INT,
                                        TYPE_BIGINT,
                                        TYPE_DECIMAL32,
                                        TYPE_DECIMAL64,
                                        TYPE_FLOAT,
                                        TYPE_DOUBLE,
                                        TYPE_DATE,
                                        TYPE_DATETIME>();
    test_simd_multi_select_boundary_all<32,
                                        TYPE_TINYINT,
                                        TYPE_SMALLINT,
                                        TYPE_INT,
                                        TYPE_BIGINT,
                                        TYPE_DECIMAL32,
                                        TYPE_DECIMAL64,
                                        TYPE_FLOAT,
                                        TYPE_DOUBLE,
                                        TYPE_DATE,
                                        TYPE_DATETIME>();
    // clang-format on
}
} // namespace starrocks
