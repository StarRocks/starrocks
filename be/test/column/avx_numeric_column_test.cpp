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

#ifdef __AVX2__

#include "column/avx_numeric_column.h"

#include "butil/time.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace starrocks {

static int max_size = 20480 * 2;

TEST(AvxTest, seqFilterTest) {
    std::vector<int32_t> data;
    std::vector<uint8_t> filter;

    for (int i = 0; i < max_size / 2; ++i) {
        data.push_back(i);
        filter.push_back(0);
    }

    for (int i = max_size / 2; i < max_size; ++i) {
        data.push_back(i);
        filter.push_back(1);
    }

    AvxNumericColumn column;
    std::vector<int32_t> result;

    butil::Timer timer;
    timer.start();
    column.filter(data, filter, 0, result);

    timer.stop();

    ASSERT_EQ(max_size / 2, result.size());
    LOG(INFO) << "result size: " << result.size();

    for (int j = 0; j < result.size(); ++j) {
        ASSERT_EQ(j + max_size / 2, result[j]);
    }

    LOG(INFO) << "test seq cost: " << timer.n_elapsed() << " ns";
}

TEST(AvxTest, seqProgmaFilterTest) {
    std::vector<int32_t> data;
    std::vector<uint8_t> filter;

    for (int i = 0; i < max_size / 2; ++i) {
        data.push_back(i);
        filter.push_back(0);
    }

    for (int i = max_size / 2; i < max_size; ++i) {
        data.push_back(i);
        filter.push_back(1);
    }

    AvxNumericColumn column;
    std::vector<int32_t> result;

    butil::Timer timer;
    timer.start();
    column.progma_filter(data, filter, 0, result);

    timer.stop();

    ASSERT_EQ(max_size / 2, result.size());
    LOG(INFO) << "result size: " << result.size();

    for (int j = 0; j < result.size(); ++j) {
        ASSERT_EQ(j + max_size / 2, result[j]);
    }

    LOG(INFO) << "test seq cost: " << timer.n_elapsed() << " ns";
}

TEST(AvxTest, seqAvxFilterTest) {
    std::vector<int32_t> data;
    std::vector<uint8_t> filter;

    for (int i = 0; i < max_size / 2; ++i) {
        data.push_back(i);
        filter.push_back(0);
    }

    for (int i = max_size / 2; i < max_size; ++i) {
        data.push_back(i);
        filter.push_back(1);
    }

    AvxNumericColumn column;
    std::vector<int32_t> result;

    butil::Timer timer;
    timer.start();
    column.avx256_filter(data, filter, 0, result);

    timer.stop();

    ASSERT_EQ(max_size / 2, result.size());

    for (int j = 0; j < result.size(); ++j) {
        ASSERT_EQ(j + max_size / 2, result[j]);
    }

    LOG(INFO) << "test seq cost: " << timer.n_elapsed() << " ns";
}

TEST(AvxTest, randomFilterTest) {
    std::vector<int32_t> data;
    std::vector<uint8_t> filter;

    for (int i = 0; i < max_size; ++i) {
        data.push_back(i);
        filter.push_back(i % 2);
    }

    AvxNumericColumn column;
    std::vector<int32_t> result;

    butil::Timer timer;
    timer.start();
    column.filter(data, filter, 0, result);

    timer.stop();

    ASSERT_EQ(max_size / 2, result.size());
    LOG(INFO) << "result size: " << result.size();

    for (int j = 0; j < result.size(); ++j) {
        ASSERT_EQ(j * 2 + 1, result[j]);
    }

    LOG(INFO) << "test random cost: " << timer.n_elapsed() << " ns";
}

TEST(AvxTest, randomProgmaFilterTest) {
    std::vector<int32_t> data;
    std::vector<uint8_t> filter;

    for (int i = 0; i < max_size; ++i) {
        data.push_back(i);
        filter.push_back(i % 2);
    }

    AvxNumericColumn column;
    std::vector<int32_t> result;

    butil::Timer timer;
    timer.start();
    column.progma_filter(data, filter, 0, result);

    timer.stop();

    ASSERT_EQ(max_size / 2, result.size());
    LOG(INFO) << "result size: " << result.size();

    for (int j = 0; j < result.size(); ++j) {
        ASSERT_EQ(j * 2 + 1, result[j]);
    }

    LOG(INFO) << "test random cost: " << timer.n_elapsed() << " ns";
}

TEST(AvxTest, randomAvxFilterTest) {
    std::vector<int32_t> data;
    std::vector<uint8_t> filter;

    for (int i = 0; i < max_size; ++i) {
        data.push_back(i);
        filter.push_back(i % 2);
    }

    AvxNumericColumn column;
    std::vector<int32_t> result;

    butil::Timer timer;
    timer.start();
    column.avx256_filter(data, filter, 0, result);

    timer.stop();

    ASSERT_EQ(max_size / 2, result.size());

    for (int j = 0; j < result.size(); ++j) {
        ASSERT_EQ(j * 2 + 1, result[j]);
    }

    LOG(INFO) << "test random cost: " << timer.n_elapsed() << " ns";
}

TEST(AvxTest, simd) {
    std::vector<uint8_t> filter;

    std::stringstream s;
    for (int i = 0; i < 16; ++i) {
        filter.push_back(1);
    }

    for (int i = 0; i < 15; ++i) {
        filter.push_back(0);
    }

    filter.push_back(1);
    LOG(WARNING) << "Filter: " << s.str();

    int offset = 0;
    // !important: filter must be an uint8_t container
    const uint8_t* f_data = filter.data();

    int simd_bits = 256;
    int batch_nums = simd_bits / (8 * sizeof(uint8_t));

    __m256i all0 = _mm256_setzero_si256();

    while (offset + batch_nums <= filter.size()) {
        __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(f_data + offset));
        uint32_t mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(f, all0));

        if (mask == 0) {
            // all no hit, pass
            LOG(WARNING) << "NO HIT";
        } else if (mask == 0xffffffff) {
            // all hit, copy all
            LOG(WARNING) << "ALL HIT";
        } else {
            int start = __builtin_ctz(mask);
            int end = __builtin_clz(mask);
            int ffs = __builtin_ffs(mask);

            LOG(WARNING) << "mask: " << mask;
            LOG(WARNING) << "start: " << start;
            LOG(WARNING) << "end: " << end;
            LOG(WARNING) << "ffs: " << ffs;
        }

        offset += batch_nums;
    }
}

} // namespace starrocks

#endif
