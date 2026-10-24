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

#include "column/simd_mulselector.h"

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "base/testutil/parallel_test.h"
#include "column/vectorized_fwd.h"
#include "gtest/gtest.h"
#include "types/logical_type.h"
#include "types/value_generator.h"

namespace starrocks {

enum class SelectorPattern {
    kRandom01 = 0,
    kAllFalse = 1,
    kFirstWhenAllTrue = 2,
    kSingleLaterWhenAllTrue = 3,
    kTwoWhenFullPartitionNoElse = 4,
    kMultiBatchTransitions = 5,
    kDefensivePositiveBytes = 6,
};

enum class ConstMode {
    kAllVar = 0,
    kAllConst = 1,
    kAlternatingConstVar = 2,
    kVarThenConstElse = 3,
    kConstThenVarElse = 4,
};

template <LogicalType TYPE>
void inject_special_values(typename SIMD_muti_selector<TYPE>::Container& container, int branch_idx) {
    if constexpr (TYPE == TYPE_DOUBLE) {
        const double custom_nan = []() {
            uint64_t bits = 0x7ff8000000000001ULL;
            double v = 0.0;
            std::memcpy(&v, &bits, sizeof(double));
            return v;
        }();
        const double neg_zero = -0.0;
        const double pos_inf = std::numeric_limits<double>::infinity();
        const double neg_inf = -std::numeric_limits<double>::infinity();
        if (container.size() == 1) {
            if (branch_idx % 4 == 0) {
                container[0] = custom_nan;
            } else if (branch_idx % 4 == 1) {
                container[0] = neg_zero;
            } else if (branch_idx % 4 == 2) {
                container[0] = pos_inf;
            }
            return;
        }
        for (size_t i = 0; i < container.size(); ++i) {
            if (i % 17 == 0) {
                container[i] = custom_nan;
            } else if (i % 19 == 0) {
                container[i] = neg_zero;
            } else if (i % 23 == 0) {
                container[i] = pos_inf;
            } else if (i % 29 == 0) {
                container[i] = neg_inf;
            }
        }
    }
}

template <LogicalType TYPE, int CASE_SIZE>
void run_simd_multi_select_case(int test_size, SelectorPattern pattern, ConstMode const_mode) {
    static_assert(isArithmeticLT<TYPE> || lt_is_date_or_datetime<TYPE>,
                  "Now Select IF only support Arithmetic, DATE, or DATETIME TYPE");

    using SelectVec = typename SIMD_muti_selector<TYPE>::SelectVec;
    using Container = typename SIMD_muti_selector<TYPE>::Container;
    using SelectorContainer = typename BooleanColumn::Container;
    using RuntimeCppType = typename SIMD_muti_selector<TYPE>::CppType;

    SelectorContainer selectors[CASE_SIZE];
    Container data_valus[CASE_SIZE + 1];
    Container dst;
    Container expected;

    SelectVec select_vecs[CASE_SIZE];
    const Container* select_lists[CASE_SIZE + 1];

    std::vector<bool> is_const(CASE_SIZE + 1, false);
    std::string const_mask_str;
    for (int i = 0; i < CASE_SIZE + 1; ++i) {
        if (const_mode == ConstMode::kAllConst) {
            is_const[i] = true;
        } else if (const_mode == ConstMode::kAlternatingConstVar) {
            is_const[i] = (i % 2 == 1);
        } else if (const_mode == ConstMode::kVarThenConstElse) {
            is_const[i] = (i == CASE_SIZE);
        } else if (const_mode == ConstMode::kConstThenVarElse) {
            is_const[i] = (i < CASE_SIZE);
        }
        const_mask_str.push_back(is_const[i] ? '1' : '0');
    }

    // Initialize selectors with a single resize from empty.
    for (int b = 0; b < CASE_SIZE; ++b) {
        selectors[b].resize(test_size);
        for (int r = 0; r < test_size; ++r) {
            uint8_t val = 0;
            switch (pattern) {
            case SelectorPattern::kRandom01:
                val = RandomGenerator<uint8_t, 2>::next_value();
                break;
            case SelectorPattern::kAllFalse:
                val = 0;
                break;
            case SelectorPattern::kFirstWhenAllTrue:
                val = (b == 0) ? 1 : RandomGenerator<uint8_t, 2>::next_value();
                break;
            case SelectorPattern::kSingleLaterWhenAllTrue:
                val = (b == CASE_SIZE - 1) ? 1 : 0;
                break;
            case SelectorPattern::kTwoWhenFullPartitionNoElse:
                if (b == 0) {
                    val = static_cast<uint8_t>((r & 1) == 0 ? 1 : 0);
                } else if (b == 1) {
                    val = static_cast<uint8_t>((r & 1) == 1 ? 1 : 0);
                } else {
                    val = 0;
                }
                break;
            case SelectorPattern::kMultiBatchTransitions: {
                int chunk_idx = (r / 16) % 4;
                if (chunk_idx == 0) {
                    val = 0; // All ELSE
                } else if (chunk_idx == 1) {
                    val = (b == CASE_SIZE - 1) ? 1 : 0; // Single winner
                } else if (chunk_idx == 2) {
                    if (CASE_SIZE >= 2) {
                        val = (b == (r & 1)) ? 1 : 0; // Partitioned across WHEN 0 and WHEN 1, zero ELSE
                    } else {
                        val = 1;
                    }
                } else {
                    val = static_cast<uint8_t>(((r + b) % 3) == 0 ? 1 : 0); // General overlapping
                }
                break;
            }
            case SelectorPattern::kDefensivePositiveBytes: {
                const uint8_t pos_bytes[3] = {0x01, 0x02, 0x7f};
                bool match = (((r + b) % (CASE_SIZE + 1)) == b);
                val = match ? pos_bytes[(r + b) % 3] : 0x00;
                break;
            }
            }
            selectors[b][r] = val;
        }
        select_vecs[b] = selectors[b].data();
    }

    // Initialize THEN/ELSE data containers with a single resize from empty.
    // Whenever is_const[i] == true, size the container to 1 from empty so ASAN places a redzone right after index 0.
    for (int i = 0; i < CASE_SIZE + 1; ++i) {
        if (is_const[i]) {
            data_valus[i].resize(1);
            data_valus[i][0] = RandomGenerator<RuntimeCppType, 128>::next_value();
        } else {
            data_valus[i].resize(test_size);
            for (int r = 0; r < test_size; ++r) {
                data_valus[i][r] = RandomGenerator<RuntimeCppType, 128>::next_value();
            }
        }
        inject_special_values<TYPE>(data_valus[i], i);
        select_lists[i] = &data_valus[i];
    }

    // Size dst and expected with a single resize from empty, and poison dst with 0xCD.
    dst.resize(test_size);
    expected.resize(test_size);
    if (test_size > 0) {
        std::memset(dst.data(), 0xCD, test_size * sizeof(RuntimeCppType));
    }

    for (int r = 0; r < test_size; ++r) {
        int index = 0;
        while (index < CASE_SIZE && !select_vecs[index][r]) {
            index++;
        }
        expected[r] = is_const[index] ? (*select_lists[index])[0] : (*select_lists[index])[r];
    }

    SIMD_muti_selector<TYPE>::multi_select_if(select_vecs, CASE_SIZE, dst, select_lists, CASE_SIZE + 1, is_const,
                                              test_size);

    std::string ctx = "type=" + std::to_string(static_cast<int>(TYPE)) + " arity=" + std::to_string(CASE_SIZE) +
                      " n=" + std::to_string(test_size) + " pat=" + std::to_string(static_cast<int>(pattern)) +
                      " const_mask=" + const_mask_str;
    ASSERT_EQ(static_cast<size_t>(test_size), dst.size()) << ctx;
    if (test_size > 0) {
        ASSERT_EQ(0, std::memcmp(expected.data(), dst.data(), test_size * sizeof(RuntimeCppType))) << ctx;
    }
}

template <LogicalType TYPE, int CASE_SIZE, int TEST_SIZE, bool IS_CONST>
void test_simd_multi_select_if() {
    run_simd_multi_select_case<TYPE, CASE_SIZE>(TEST_SIZE, SelectorPattern::kRandom01,
                                                IS_CONST ? ConstMode::kAllConst : ConstMode::kAllVar);
}

template <LogicalType TYPE, int CASE_SIZE, int TEST_SIZE>
bool test_function_wrapper() {
    test_simd_multi_select_if<TYPE, CASE_SIZE, TEST_SIZE, false>();
    test_simd_multi_select_if<TYPE, CASE_SIZE, TEST_SIZE, true>();
    return true;
}

template <LogicalType TYPE, int CASE_SIZE>
void test_arity_matrix() {
    const int row_counts[] = {0, 1, 15, 16, 17, 31, 32, 33, 64, 4095, 4096};
    const SelectorPattern patterns[] = {
            SelectorPattern::kRandom01,
            SelectorPattern::kAllFalse,
            SelectorPattern::kFirstWhenAllTrue,
            SelectorPattern::kSingleLaterWhenAllTrue,
            SelectorPattern::kTwoWhenFullPartitionNoElse,
            SelectorPattern::kMultiBatchTransitions,
            SelectorPattern::kDefensivePositiveBytes,
    };
    const ConstMode const_modes[] = {
            ConstMode::kAllVar,           ConstMode::kAllConst,         ConstMode::kAlternatingConstVar,
            ConstMode::kVarThenConstElse, ConstMode::kConstThenVarElse,
    };

    for (int n : row_counts) {
        for (SelectorPattern pat : patterns) {
            if (n < 4095) {
                for (ConstMode cm : const_modes) {
                    run_simd_multi_select_case<TYPE, CASE_SIZE>(n, pat, cm);
                }
            } else {
                // Full 4095/4096 runs across representative const modes to keep test runtime fast.
                run_simd_multi_select_case<TYPE, CASE_SIZE>(n, pat, ConstMode::kAllVar);
                run_simd_multi_select_case<TYPE, CASE_SIZE>(n, pat, ConstMode::kAllConst);
                run_simd_multi_select_case<TYPE, CASE_SIZE>(n, pat, ConstMode::kAlternatingConstVar);
            }
        }
    }
}

template <LogicalType TYPE>
bool test_type_matrix() {
    test_arity_matrix<TYPE, 1>();
    test_arity_matrix<TYPE, 2>();
    test_arity_matrix<TYPE, 3>();
    test_arity_matrix<TYPE, 4>();
    test_arity_matrix<TYPE, 8>();
    test_arity_matrix<TYPE, 9>(); // Direct-caller guard test (> kMaxSimdWhenSize = 8)
    return true;
}

template <LogicalType... TYPE>
bool test_all() {
    return (... && test_type_matrix<TYPE>());
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
            TYPE_DOUBLE,
            TYPE_DATE,
            TYPE_DATETIME>();
    // clang-format on
}
} // namespace starrocks
