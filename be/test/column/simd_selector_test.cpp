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

#include <cstdint>
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

template <LogicalType TYPE, int CASE_SIZE>
void run_exact_batch_multiple_selector_tests_for_arity() {
    using SelectVec = typename SIMD_muti_selector<TYPE>::SelectVec;
    using Container = typename SIMD_muti_selector<TYPE>::Container;
    using SelectorContainer = typename BooleanColumn::Container;
    using CppType = RunTimeCppType<TYPE>;

    const uint8_t truthy_bytes[3] = {0x02, 0x80, 0xff};
    const uint8_t mixed_truthy[5] = {0x01, 0x02, 0x7f, 0x80, 0xff};

    // Pattern 0: all-zero
    // Pattern 1: all-nonzero (branch 0 wins every row with non-canonical truthy bytes 0x02, 0x80, 0xFF)
    // Pattern 2: single-winner branch (only last WHEN branch is non-zero with 0x02, 0x80, 0xFF)
    // Pattern 3: single-winner row per 32-row batch (lane 0 in batch 0, lane 31 in batch 1)
    // Pattern 4: mixed selector pattern (and batch 0 all-zero -> batch 1 all-nonzero for N = 64)
    for (int n : {32, 64}) {
        for (int pat : {0, 1, 2, 3, 4}) {
            for (int const_mode : {0, 1, 2}) {
                SelectorContainer selectors[CASE_SIZE];
                Container inputs[CASE_SIZE + 1];
                Container dst(n);
                Container expected(n);
                SelectVec select_vecs[CASE_SIZE];
                const Container* select_lists[CASE_SIZE + 1];
                std::vector<bool> is_const(CASE_SIZE + 1, false);

                for (int b = 0; b < CASE_SIZE; ++b) {
                    selectors[b].resize(n, 0);
                    for (int r = 0; r < n; ++r) {
                        if (pat == 0) {
                            selectors[b][r] = 0x00;
                        } else if (pat == 1) {
                            selectors[b][r] =
                                    (b == 0) ? truthy_bytes[r % 3] : (((r + b) & 1) ? truthy_bytes[b % 3] : 0);
                        } else if (pat == 2) {
                            selectors[b][r] = (b == CASE_SIZE - 1) ? truthy_bytes[r % 3] : 0x00;
                        } else if (pat == 3) {
                            int batch = r / 32;
                            int lane = r % 32;
                            int target_lane = (batch == 0) ? 0 : 31;
                            int target_branch = batch % CASE_SIZE;
                            selectors[b][r] =
                                    (lane == target_lane && b == target_branch) ? truthy_bytes[(batch + b) % 3] : 0x00;
                        } else {
                            if (n == 64 && r >= 32 && b == 0) {
                                selectors[b][r] = truthy_bytes[r % 3];
                            } else {
                                int winner = r % (CASE_SIZE + 1);
                                if (b == winner) {
                                    selectors[b][r] = mixed_truthy[(r + b) % 5];
                                } else if (b > winner && ((r + b) % 2 == 0)) {
                                    selectors[b][r] = (r & 1) ? 0x80 : 0xff;
                                } else {
                                    selectors[b][r] = 0x00;
                                }
                            }
                        }
                    }
                    select_vecs[b] = selectors[b].data();
                }

                for (int i = 0; i < CASE_SIZE + 1; ++i) {
                    is_const[i] = (const_mode == 1) ? (i % 2 == 1) : (const_mode == 2);
                    if (is_const[i]) {
                        inputs[i].resize(1);
                        inputs[i][0] = RandomGenerator<CppType, 128>::next_value();
                    } else {
                        inputs[i].resize(n);
                        for (int r = 0; r < n; ++r) {
                            inputs[i][r] = RandomGenerator<CppType, 128>::next_value();
                        }
                    }
                    select_lists[i] = &inputs[i];
                }

                std::memset(dst.data(), 0xCD, n * sizeof(CppType));
                for (int r = 0; r < n; ++r) {
                    int idx = 0;
                    while (idx < CASE_SIZE && !select_vecs[idx][r]) {
                        ++idx;
                    }
                    expected[r] = is_const[idx] ? (*select_lists[idx])[0] : (*select_lists[idx])[r];
                }

                SIMD_muti_selector<TYPE>::multi_select_if(select_vecs, CASE_SIZE, dst, select_lists, CASE_SIZE + 1,
                                                          is_const, n);
                ASSERT_EQ(0, std::memcmp(dst.data(), expected.data(), n * sizeof(CppType)))
                        << "type=" << static_cast<int>(TYPE) << " arity=" << CASE_SIZE << " n=" << n << " pat=" << pat
                        << " const_mode=" << const_mode;

                if constexpr (CASE_SIZE == 1 && isArithmeticLT<TYPE>) {
                    if (const_mode == 0) {
                        Container dst_if(n);
                        std::memset(dst_if.data(), 0xCD, n * sizeof(CppType));
                        SIMD_selector<TYPE>::select_if(selectors[0].data(), dst_if, inputs[0], inputs[1]);
                        ASSERT_EQ(0, std::memcmp(dst_if.data(), expected.data(), n * sizeof(CppType)))
                                << "SIMD_selector var-var type=" << static_cast<int>(TYPE) << " n=" << n
                                << " pat=" << pat;
                    } else if (const_mode == 2) {
                        Container dst_if(n);
                        std::memset(dst_if.data(), 0xCD, n * sizeof(CppType));
                        SIMD_selector<TYPE>::select_if(selectors[0].data(), dst_if, inputs[0][0], inputs[1][0]);
                        ASSERT_EQ(0, std::memcmp(dst_if.data(), expected.data(), n * sizeof(CppType)))
                                << "SIMD_selector const-const type=" << static_cast<int>(TYPE) << " n=" << n
                                << " pat=" << pat;
                    }
                }
            }
        }
    }
}

template <LogicalType TYPE>
void run_exact_batch_multiple_selector_tests() {
    run_exact_batch_multiple_selector_tests_for_arity<TYPE, 1>();
    run_exact_batch_multiple_selector_tests_for_arity<TYPE, 2>();
    run_exact_batch_multiple_selector_tests_for_arity<TYPE, 4>();
    run_exact_batch_multiple_selector_tests_for_arity<TYPE, 8>();
}

PARALLEL_TEST(SIMDSelectorTest, ExactBatchMultiplesAndNonCanonicalPatterns) {
    run_exact_batch_multiple_selector_tests<TYPE_BOOLEAN>();
    run_exact_batch_multiple_selector_tests<TYPE_TINYINT>();
    run_exact_batch_multiple_selector_tests<TYPE_SMALLINT>();
    run_exact_batch_multiple_selector_tests<TYPE_INT>();
    run_exact_batch_multiple_selector_tests<TYPE_BIGINT>();
    run_exact_batch_multiple_selector_tests<TYPE_FLOAT>();
    run_exact_batch_multiple_selector_tests<TYPE_DOUBLE>();
    run_exact_batch_multiple_selector_tests<TYPE_DATE>();
    run_exact_batch_multiple_selector_tests<TYPE_DATETIME>();
}
} // namespace starrocks
