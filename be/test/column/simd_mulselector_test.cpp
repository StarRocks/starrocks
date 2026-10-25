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
            uint64_t bits = 0x7ff8000012345678ULL;
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
            } else {
                container[0] = neg_inf;
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
    } else if constexpr (TYPE == TYPE_FLOAT) {
        const float custom_nan = []() {
            uint32_t bits = 0x7fc01234U;
            float v = 0.0f;
            std::memcpy(&v, &bits, sizeof(float));
            return v;
        }();
        const float neg_zero = -0.0f;
        const float pos_inf = std::numeric_limits<float>::infinity();
        const float neg_inf = -std::numeric_limits<float>::infinity();
        if (container.size() == 1) {
            if (branch_idx % 4 == 0) {
                container[0] = custom_nan;
            } else if (branch_idx % 4 == 1) {
                container[0] = neg_zero;
            } else if (branch_idx % 4 == 2) {
                container[0] = pos_inf;
            } else {
                container[0] = neg_inf;
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
void run_simd_multi_select_case(int row_size, SelectorPattern pattern, ConstMode const_mode) {
    static_assert(isArithmeticLT<TYPE> || lt_is_date_or_datetime<TYPE>,
                  "Now Select IF only support Arithmetic, DATE, or DATETIME TYPE");

    using SelectVec = typename SIMD_muti_selector<TYPE>::SelectVec;
    using Container = typename SIMD_muti_selector<TYPE>::Container;
    using SelectorContainer = typename BooleanColumn::Container;
    using RuntimeCppType = typename SIMD_muti_selector<TYPE>::CppType;

    SelectorContainer selectors[CASE_SIZE];
    Container data_valus[CASE_SIZE + 1];
    Container dst;
    Container dst2;

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

    // Initialise selectors with a single resize from empty.
    for (int b = 0; b < CASE_SIZE; ++b) {
        selectors[b].resize(row_size);
        for (int r = 0; r < row_size; ++r) {
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

    // Initialise THEN/ELSE data containers with a single resize from empty.
    // Whenever is_const[i] == true, size the container to 1 from empty so ASAN places a redzone right after index 0.
    for (int i = 0; i < CASE_SIZE + 1; ++i) {
        if (is_const[i]) {
            data_valus[i].resize(1);
            data_valus[i][0] = RandomGenerator<RuntimeCppType, 128>::next_value();
        } else {
            data_valus[i].resize(row_size);
            for (int r = 0; r < row_size; ++r) {
                data_valus[i][r] = RandomGenerator<RuntimeCppType, 128>::next_value();
            }
        }
        inject_special_values<TYPE>(data_valus[i], i);
        select_lists[i] = &data_valus[i];
    }

    // Size dst and dst2 with a single resize from empty, and poison both with 0xCD.
    dst.resize(row_size);
    dst2.resize(row_size);
    if (row_size > 0) {
        std::memset(dst.data(), 0xCD, row_size * sizeof(RuntimeCppType));
        std::memset(dst2.data(), 0xCD, row_size * sizeof(RuntimeCppType));
    }

    for (int r = 0; r < row_size; ++r) {
        int index = 0;
        while (index < CASE_SIZE && !select_vecs[index][r]) {
            index++;
        }
        dst2[r] = is_const[index] ? (*select_lists[index])[0] : (*select_lists[index])[r];
    }

    SIMD_muti_selector<TYPE>::multi_select_if(select_vecs, CASE_SIZE, dst, select_lists, CASE_SIZE + 1, is_const,
                                              row_size);

    std::string ctx = "type=" + std::to_string(static_cast<int>(TYPE)) + " arity=" + std::to_string(CASE_SIZE) +
                      " n=" + std::to_string(row_size) + " pat=" + std::to_string(static_cast<int>(pattern)) +
                      " const_mask=" + const_mask_str;
    ASSERT_EQ(static_cast<size_t>(row_size), dst.size()) << ctx;
    if (row_size > 0) {
        ASSERT_EQ(0, std::memcmp(dst.data(), dst2.data(), row_size * sizeof(RuntimeCppType))) << ctx;
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
    const int row_counts[] = {0, 1, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 1024, 4095, 4096};
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
            if (n < 1024) {
                for (ConstMode cm : const_modes) {
                    run_simd_multi_select_case<TYPE, CASE_SIZE>(n, pat, cm);
                }
            } else {
                // Full 1024/4095/4096 runs across representative const modes to keep test runtime fast.
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

template <LogicalType TYPE, int CASE_SIZE>
void run_non_canonical_selector_bytes_case() {
    using SelectVec = typename SIMD_muti_selector<TYPE>::SelectVec;
    using Container = typename SIMD_muti_selector<TYPE>::Container;
    using SelectorContainer = typename BooleanColumn::Container;
    using RuntimeCppType = typename SIMD_muti_selector<TYPE>::CppType;

    constexpr int row_size = 97;
    const uint8_t non_zero_bytes[5] = {0x01, 0x02, 0x7f, 0x80, 0xff};

    for (bool alternating_const : {false, true}) {
        SelectorContainer selectors[CASE_SIZE];
        Container data_values[CASE_SIZE + 1];
        Container dst;
        Container dst2;
        SelectVec select_vecs[CASE_SIZE];
        const Container* select_lists[CASE_SIZE + 1];
        std::vector<bool> is_const(CASE_SIZE + 1, false);

        for (int b = 0; b < CASE_SIZE; ++b) {
            selectors[b].resize(row_size, 0);
            for (int r = 0; r < row_size; ++r) {
                if (r >= 32 && r < 64 && b == 0) {
                    // Batch 1 (rows 32..63): all-true branch 0 with non-canonical truthy bytes including 0x80 and 0xff.
                    selectors[b][r] = non_zero_bytes[r % 5];
                } else {
                    int winner = r % (CASE_SIZE + 1);
                    if (b == winner) {
                        selectors[b][r] = non_zero_bytes[(r + b) % 5];
                    } else if (b > winner && ((r + b) % 2 == 0)) {
                        // Later overshadowed branch also uses 0x80 / 0xff to test priority masking.
                        selectors[b][r] = (r & 1) ? 0x80 : 0xff;
                    } else {
                        selectors[b][r] = 0x00;
                    }
                }
            }
            select_vecs[b] = selectors[b].data();
        }

        for (int i = 0; i < CASE_SIZE + 1; ++i) {
            is_const[i] = alternating_const && (i % 2 == 1);
            if (is_const[i]) {
                data_values[i].resize(1);
                data_values[i][0] = RandomGenerator<RuntimeCppType, 128>::next_value();
            } else {
                data_values[i].resize(row_size);
                for (int r = 0; r < row_size; ++r) {
                    data_values[i][r] = RandomGenerator<RuntimeCppType, 128>::next_value();
                }
            }
            select_lists[i] = &data_values[i];
        }

        dst.resize(row_size);
        dst2.resize(row_size);
        std::memset(dst.data(), 0xCD, row_size * sizeof(RuntimeCppType));
        std::memset(dst2.data(), 0xCD, row_size * sizeof(RuntimeCppType));

        for (int r = 0; r < row_size; ++r) {
            int idx = 0;
            while (idx < CASE_SIZE && !select_vecs[idx][r]) {
                idx++;
            }
            dst2[r] = is_const[idx] ? (*select_lists[idx])[0] : (*select_lists[idx])[r];
        }

        SIMD_muti_selector<TYPE>::multi_select_if(select_vecs, CASE_SIZE, dst, select_lists, CASE_SIZE + 1, is_const,
                                                  row_size);
        ASSERT_EQ(0, std::memcmp(dst.data(), dst2.data(), row_size * sizeof(RuntimeCppType)))
                << "type=" << static_cast<int>(TYPE) << " arity=" << CASE_SIZE
                << " alternating_const=" << alternating_const;
    }
}

template <LogicalType TYPE>
void run_non_canonical_for_type() {
    run_non_canonical_selector_bytes_case<TYPE, 1>();
    run_non_canonical_selector_bytes_case<TYPE, 2>();
    run_non_canonical_selector_bytes_case<TYPE, 4>();
    run_non_canonical_selector_bytes_case<TYPE, 8>();
}

template <LogicalType TYPE, int CASE_SIZE>
void run_bit31_and_subchunk_divergence_case() {
    using SelectVec = typename SIMD_muti_selector<TYPE>::SelectVec;
    using Container = typename SIMD_muti_selector<TYPE>::Container;
    using SelectorContainer = typename BooleanColumn::Container;
    using RuntimeCppType = typename SIMD_muti_selector<TYPE>::CppType;

    constexpr int row_size = 65;
    for (bool alternating_const : {false, true}) {
        SelectorContainer selectors[CASE_SIZE];
        Container data_values[CASE_SIZE + 1];
        Container dst;
        Container dst2;
        SelectVec select_vecs[CASE_SIZE];
        const Container* select_lists[CASE_SIZE + 1];
        std::vector<bool> is_const(CASE_SIZE + 1, false);

        for (int b = 0; b < CASE_SIZE; ++b) {
            selectors[b].resize(row_size, 0);
            for (int r = 0; r < row_size; ++r) {
                int lane = r % 32;
                if (b == 0) {
                    if (lane == 31) {
                        selectors[b][r] = 1;
                    } else if (lane >= 16 && lane <= 30) {
                        selectors[b][r] = 1;
                    } else if (lane >= 8 && lane <= 15) {
                        selectors[b][r] = static_cast<uint8_t>(lane & 1);
                    } else {
                        selectors[b][r] = 0;
                    }
                } else {
                    // In rows 0..7 and even lanes of 8..15, later branches or ELSE win;
                    // also set 1s in lanes 16..31 to verify earlier branch 0 priority over later branches.
                    if (lane < 8) {
                        selectors[b][r] = static_cast<uint8_t>((lane % CASE_SIZE) == b ? 1 : 0);
                    } else if (lane < 16) {
                        selectors[b][r] = static_cast<uint8_t>(((lane + b) % 3 == 0) ? 1 : 0);
                    } else {
                        selectors[b][r] = 1;
                    }
                }
            }
            select_vecs[b] = selectors[b].data();
        }

        for (int i = 0; i < CASE_SIZE + 1; ++i) {
            is_const[i] = alternating_const && (i % 2 == 1);
            if (is_const[i]) {
                data_values[i].resize(1);
                data_values[i][0] = RandomGenerator<RuntimeCppType, 128>::next_value();
            } else {
                data_values[i].resize(row_size);
                for (int r = 0; r < row_size; ++r) {
                    data_values[i][r] = RandomGenerator<RuntimeCppType, 128>::next_value();
                }
            }
            select_lists[i] = &data_values[i];
        }

        dst.resize(row_size);
        dst2.resize(row_size);
        std::memset(dst.data(), 0xCD, row_size * sizeof(RuntimeCppType));
        std::memset(dst2.data(), 0xCD, row_size * sizeof(RuntimeCppType));

        for (int r = 0; r < row_size; ++r) {
            int idx = 0;
            while (idx < CASE_SIZE && !select_vecs[idx][r]) {
                idx++;
            }
            dst2[r] = is_const[idx] ? (*select_lists[idx])[0] : (*select_lists[idx])[r];
        }

        SIMD_muti_selector<TYPE>::multi_select_if(select_vecs, CASE_SIZE, dst, select_lists, CASE_SIZE + 1, is_const,
                                                  row_size);
        ASSERT_EQ(0, std::memcmp(dst.data(), dst2.data(), row_size * sizeof(RuntimeCppType)))
                << "type=" << static_cast<int>(TYPE) << " arity=" << CASE_SIZE
                << " alternating_const=" << alternating_const;
    }
}

template <LogicalType TYPE>
void run_bit31_for_type() {
    run_bit31_and_subchunk_divergence_case<TYPE, 2>();
    run_bit31_and_subchunk_divergence_case<TYPE, 4>();
    run_bit31_and_subchunk_divergence_case<TYPE, 8>();
}

template <LogicalType TYPE>
void run_first_when_all_true_and_exceed_max_when_for_type() {
    using SelectVec = typename SIMD_muti_selector<TYPE>::SelectVec;
    using Container = typename SIMD_muti_selector<TYPE>::Container;
    using SelectorContainer = typename BooleanColumn::Container;
    using RuntimeCppType = typename SIMD_muti_selector<TYPE>::CppType;

    constexpr int row_size = 97;

    // 1. win_bits[0] == 0xffffffffu across 3 consecutive 32-row batches (row_size = 97) with non-const ELSE,
    // plus a mixed batch pattern (batch 0 all WHEN 0, batch 1 all ELSE, batch 2 all WHEN 0) to verify
    // non-const ELSE pointer advancement across all-true WHEN batches.
    for (bool all_three_batches_when0 : {true, false}) {
        constexpr int kArity = 4;
        SelectorContainer selectors[kArity];
        Container data_values[kArity + 1];
        Container dst;
        Container dst2;
        SelectVec select_vecs[kArity];
        const Container* select_lists[kArity + 1];
        std::vector<bool> is_const(kArity + 1, false);

        for (int b = 0; b < kArity; ++b) {
            selectors[b].resize(row_size, 0);
            for (int r = 0; r < row_size; ++r) {
                if (all_three_batches_when0) {
                    selectors[b][r] = (b == 0) ? 1 : static_cast<uint8_t>(r & 1);
                } else {
                    int batch = r / 32;
                    if (batch == 1) {
                        selectors[b][r] = 0; // All ELSE in batch 1
                    } else {
                        selectors[b][r] = (b == 0) ? 1 : 0;
                    }
                }
            }
            select_vecs[b] = selectors[b].data();
        }

        for (int i = 0; i < kArity + 1; ++i) {
            data_values[i].resize(row_size);
            for (int r = 0; r < row_size; ++r) {
                data_values[i][r] = RandomGenerator<RuntimeCppType, 128>::next_value();
            }
            select_lists[i] = &data_values[i];
        }

        dst.resize(row_size);
        dst2.resize(row_size);
        std::memset(dst.data(), 0xCD, row_size * sizeof(RuntimeCppType));
        std::memset(dst2.data(), 0xCD, row_size * sizeof(RuntimeCppType));

        for (int r = 0; r < row_size; ++r) {
            int idx = 0;
            while (idx < kArity && !select_vecs[idx][r]) {
                idx++;
            }
            dst2[r] = (*select_lists[idx])[r];
        }

        SIMD_muti_selector<TYPE>::multi_select_if(select_vecs, kArity, dst, select_lists, kArity + 1, is_const,
                                                  row_size);
        ASSERT_EQ(0, std::memcmp(dst.data(), dst2.data(), row_size * sizeof(RuntimeCppType)))
                << "type=" << static_cast<int>(TYPE) << " all_three_batches_when0=" << all_three_batches_when0;
    }

    // 2. select_vec_size = 9 (> kMaxSimdWhenSize) with both canonical {0, 1} and non-canonical {0, 0x80, 0xff} bytes.
    for (bool use_non_canonical : {false, true}) {
        constexpr int kArity = 9;
        SelectorContainer selectors[kArity];
        Container data_values[kArity + 1];
        Container dst;
        Container dst2;
        SelectVec select_vecs[kArity];
        const Container* select_lists[kArity + 1];
        std::vector<bool> is_const(kArity + 1, false);

        for (int b = 0; b < kArity; ++b) {
            selectors[b].resize(row_size, 0);
            for (int r = 0; r < row_size; ++r) {
                uint8_t truthy = use_non_canonical ? ((r & 1) ? 0x80 : 0xff) : 0x01;
                if (r < 32 && b == 0) {
                    // Batch 0: all-true branch 0 to exercise early break in fallback load loop.
                    selectors[b][r] = truthy;
                } else {
                    int winner = r % (kArity + 1);
                    selectors[b][r] = (b == winner) ? truthy : 0x00;
                }
            }
            select_vecs[b] = selectors[b].data();
        }

        for (int i = 0; i < kArity + 1; ++i) {
            is_const[i] = (i == 3 || i == 7);
            if (is_const[i]) {
                data_values[i].resize(1);
                data_values[i][0] = RandomGenerator<RuntimeCppType, 128>::next_value();
            } else {
                data_values[i].resize(row_size);
                for (int r = 0; r < row_size; ++r) {
                    data_values[i][r] = RandomGenerator<RuntimeCppType, 128>::next_value();
                }
            }
            select_lists[i] = &data_values[i];
        }

        dst.resize(row_size);
        dst2.resize(row_size);
        std::memset(dst.data(), 0xCD, row_size * sizeof(RuntimeCppType));
        std::memset(dst2.data(), 0xCD, row_size * sizeof(RuntimeCppType));

        for (int r = 0; r < row_size; ++r) {
            int idx = 0;
            while (idx < kArity && !select_vecs[idx][r]) {
                idx++;
            }
            dst2[r] = is_const[idx] ? (*select_lists[idx])[0] : (*select_lists[idx])[r];
        }

        SIMD_muti_selector<TYPE>::multi_select_if(select_vecs, kArity, dst, select_lists, kArity + 1, is_const,
                                                  row_size);
        ASSERT_EQ(0, std::memcmp(dst.data(), dst2.data(), row_size * sizeof(RuntimeCppType)))
                << "type=" << static_cast<int>(TYPE) << " arity=9 use_non_canonical=" << use_non_canonical;
    }
}

PARALLEL_TEST(SIMDMultiSelectorTest, TestVarVar) {
    // clang-format off
    test_all<TYPE_BOOLEAN,
            TYPE_TINYINT, 
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

PARALLEL_TEST(SIMDMultiSelectorTest, TestNonCanonicalSelectorBytes) {
    run_non_canonical_for_type<TYPE_BOOLEAN>();
    run_non_canonical_for_type<TYPE_SMALLINT>();
    run_non_canonical_for_type<TYPE_INT>();
    run_non_canonical_for_type<TYPE_BIGINT>();
    run_non_canonical_for_type<TYPE_DATE>();
    run_non_canonical_for_type<TYPE_DATETIME>();
}

PARALLEL_TEST(SIMDMultiSelectorTest, TestBit31SignExtensionAndSubChunkDivergence) {
    run_bit31_for_type<TYPE_SMALLINT>();
    run_bit31_for_type<TYPE_INT>();
    run_bit31_for_type<TYPE_BIGINT>();
    run_bit31_for_type<TYPE_DATE>();
    run_bit31_for_type<TYPE_DATETIME>();
}

PARALLEL_TEST(SIMDMultiSelectorTest, TestFirstWhenAllTrueAndExceedMaxWhenFallback) {
    run_first_when_all_true_and_exceed_max_when_for_type<TYPE_BOOLEAN>();
    run_first_when_all_true_and_exceed_max_when_for_type<TYPE_SMALLINT>();
    run_first_when_all_true_and_exceed_max_when_for_type<TYPE_INT>();
    run_first_when_all_true_and_exceed_max_when_for_type<TYPE_BIGINT>();
    run_first_when_all_true_and_exceed_max_when_for_type<TYPE_DATE>();
    run_first_when_all_true_and_exceed_max_when_for_type<TYPE_DATETIME>();
}

} // namespace starrocks
