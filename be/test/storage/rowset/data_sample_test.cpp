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

#include "storage/rowset/data_sample.h"

#include <gtest/gtest.h>

#include <random>

#include "storage/olap_common.h"
#include "storage/range.h"

namespace starrocks {

TEST(DataSampleTest, test_block_sample) {
    // Test cases for different probability percentages
    std::vector<int64_t> test_percentages = {0, 1, 2, 10, 100};
    std::map<int64_t, std::string> expected = {
            {1, "([55296,56320), [93184,94208))"},
            {2, "([55296,56320), [93184,94208))"},
            {10, "([17408,18432), [31744,32768), [34816,35840), [55296,56320), [93184,94208), [95232,97280))"},

    };
    for (auto percentage : test_percentages) {
        int64_t random_seed = 123;
        auto data_sample = DataSample::make_block_sample(percentage, random_seed, 1024, 100'000);
        ASSERT_NE(data_sample, nullptr);

        OlapReaderStatistics stats;
        auto sampled_ranges = data_sample->sample(&stats);
        if (percentage == 0 || percentage == 100) {
            ASSERT_FALSE(sampled_ranges.ok());
        } else {
            ASSERT_TRUE(sampled_ranges.ok());
            EXPECT_LE(sampled_ranges.value().size(), (size_t)(percentage / 100.0 * 100'000));
            if (expected.contains(percentage)) {
                EXPECT_EQ(expected[percentage], sampled_ranges.value().to_string());
            }
        }
    }
}

TEST(DataSampleTest, test_page_sample) {
    std::vector<int64_t> test_percentages = {0, 1, 2, 10, 100};
    std::map<int64_t, std::string> expected = {
            {1, "([55296,56320), [93184,94208))"},
            {2, "([55296,56320), [93184,94208))"},
            {10,
             "([17408,18432), [31744,32768), [34816,35840), [55296,56320), [93184,94208), [95232,97280), "
             "[102400,103424), [115712,116736))"},
    };
    for (auto percentage : test_percentages) {
        int64_t random_seed = 123;
        auto page_indexer = [](size_t page_index) {
            return std::make_pair(page_index * 1024, (page_index + 1) * 1024);
        };
        auto data_sample = DataSample::make_page_sample(percentage, random_seed, 128, std::move(page_indexer));
        ASSERT_NE(data_sample, nullptr);

        OlapReaderStatistics stats;
        auto sampled_ranges = data_sample->sample(&stats);
        if (percentage == 0 || percentage == 100) {
            ASSERT_FALSE(sampled_ranges.ok());
        } else {
            ASSERT_TRUE(sampled_ranges.ok());
            EXPECT_LE(sampled_ranges.value().size(), (size_t)(percentage / 100.0 * 100'000));
            if (expected.contains(percentage)) {
                EXPECT_EQ(expected[percentage], sampled_ranges.value().to_string()) << "Percent " << percentage;
            }
        }
    }
}

class PageSampleTestSuite : public testing::Test {
protected:
    void SetUp() override {
        page_indexer = [=](size_t page_index) {
            return std::make_pair(page_index * kPageSize, (page_index + 1) * kPageSize);
        };
        stats = OlapReaderStatistics();
    }

    StatusOr<RowIdSparseRange> run(const std::shared_ptr<SortableZoneMap>& zonemap_ptr) {
        data_sample =
                DataSample::make_page_sample(probability_percent, random_seed, kNumPages, std::move(page_indexer));
        if (zonemap_ptr) {
            data_sample->with_zonemap(zonemap_ptr);
        }

        auto result = data_sample->sample(&stats);
        if (result.ok()) {
            // validate page address
            for (size_t i = 0; i < result.value().size(); i++) {
                auto& range = result.value()[i];
                rowid_t begin = range.begin();
                rowid_t end = range.begin();
                EXPECT_TRUE(0 <= begin && begin <= kNumPages * kPageSize);
                EXPECT_TRUE(0 <= end && end <= kNumPages * kPageSize);
            }
        }

        return result;
    }

protected:
    size_t kNumPages = 128;
    size_t kPageSize = 1024;
    int64_t probability_percent = 5;
    int64_t random_seed = 123;
    std::function<std::pair<int64_t, int64_t>(size_t)> page_indexer;
    OlapReaderStatistics stats;
    std::unique_ptr<PageDataSample> data_sample;
};

TEST_F(PageSampleTestSuite, test_page_sample_without_zonemap) {
    auto sampled_ranges = run(nullptr);

    ASSERT_TRUE(sampled_ranges.ok());
    ASSERT_GT(sampled_ranges.value().size(), 0);
    EXPECT_EQ("([31744,32768), [55296,56320), [93184,94208), [96256,97280), [102400,103424))",
              sampled_ranges.value().to_string());
}

TEST_F(PageSampleTestSuite, test_page_sample_with_uniform_zonemap) {
    std::vector<ZoneMapDetail> zonemap;
    for (size_t i = 0; i < kNumPages; i++) {
        zonemap.emplace_back(Datum((int32_t)i * 100), Datum((int32_t)(i + 1) * 100), false);
    }
    std::shuffle(zonemap.begin(), zonemap.end(), std::mt19937(random_seed));
    auto zonemap_ptr = std::make_shared<SortableZoneMap>(TYPE_INT, std::move(zonemap));

    auto sampled_ranges = run(zonemap_ptr);

    EXPECT_EQ(1, stats.sample_build_histogram_count);
    EXPECT_TRUE(zonemap_ptr->is_diverse());
    EXPECT_EQ("zonemap: [0,100],[100,200],[200,300],[300,400],[400,500],[500,600],[600,700],[700,800],[800,900],[90",
              zonemap_ptr->zonemap_string().substr(0, 100));
    EXPECT_EQ(R"(histogram: [0,2100]: (52,23,109,119,61,48,20,36,90,0,43,58,69,27,10,16,50,34,9,6,93)
[2100,4200]: (28,99,65,49,13,47,98,83,57,85,81,124,66,4,44,76,19,46,101,14,116)
[4200,6300]: (110,86,75,105,15,55,114,82,87,25,111,8,84,79,22,62,35,2,103,1,106)
[6300,8400]: (80,120,92,29,78,12,117,5,68,38,59,53,100,40,74,118,88,51,31,108,60)
[8400,10500]: (72,121,89,115,64,42,102,122,56,77,112,94,123,7,41,30,32,63,67,45,37)
[10500,12600]: (104,39,107,24,96,125,126,33,21,113,71,73,97,91,18,11,17,95,127,54,26)
[12600,12800]: (3,70)
)",
              zonemap_ptr->histogram_string());
    ASSERT_TRUE(sampled_ranges.ok());
    ASSERT_GT(sampled_ranges.value().size(), 0);
    EXPECT_EQ(
            "([10240,11264), [18432,19456), [45056,46080), [65536,66560), [69632,70656), [71680,72704), "
            "[116736,117760))",
            sampled_ranges.value().to_string());
}

TEST_F(PageSampleTestSuite, test_page_sample_with_skewed_zonemap) {
    std::vector<ZoneMapDetail> zonemap;
    for (size_t i = 0; i < kNumPages; i++) {
        // most pages have a very small range
        if (i <= kNumPages / 2) {
            zonemap.emplace_back(Datum((int32_t)i * 5), Datum((int32_t)(i + 1) * 5), false);
        } else {
            zonemap.emplace_back(Datum((int32_t)i * 100), Datum((int32_t)(i + 1) * 100), false);
        }
    }
    std::shuffle(zonemap.begin(), zonemap.end(), std::mt19937(random_seed));
    auto zonemap_ptr = std::make_shared<SortableZoneMap>(TYPE_INT, std::move(zonemap));

    auto sampled_ranges = run(zonemap_ptr);
    EXPECT_EQ(1, stats.sample_build_histogram_count);
    EXPECT_TRUE(zonemap_ptr->is_diverse());
    EXPECT_EQ("zonemap: [0,5],[5,10],[10,15],[15,20],[20,25],[25,30],[30,35],[35,40],[40,45],[45,50],[50,55],[55,60",
              zonemap_ptr->zonemap_string().substr(0, 100));
    EXPECT_EQ(
            R"(histogram: [0,160]: (52,23,109,119,61,48,20,36,90,0,43,58,69,27,10,16,50,34,9,6,93,28,99,65,49,13,47,98,83,57,85,81)
[160,320]: (124,66,4,44,76,19,46,101,14,116,110,86,75,105,15,55,114,82,87,25,111,8,84,79,22,62,35,2,103,1,106,80)
[320,6600]: (120,92)
[6600,8800]: (29,78,12,117,5,68,38,59,53,100,40,74,118,88,51,31,108,60,72,121,89,115)
[8800,11000]: (64,42,102,122,56,77,112,94,123,7,41,30,32,63,67,45,37,104,39,107,24,96)
[11000,12800]: (125,126,33,21,113,71,73,97,91,18,11,17,95,127,54,26,3,70)
)",
            zonemap_ptr->histogram_string());

    ASSERT_TRUE(sampled_ranges.ok());
    ASSERT_GT(sampled_ranges.value().size(), 0);
    EXPECT_EQ("([57344,58368), [86016,87040), [97280,98304), [101376,103424), [122880,123904))",
              sampled_ranges.value().to_string());
}

TEST_F(PageSampleTestSuite, test_page_sample_with_normal_zonemap) {
    std::vector<ZoneMapDetail> zonemap;
    std::mt19937 gen(random_seed);
    std::normal_distribution<> distribution(100, 20);
    for (size_t i = 0; i < kNumPages; i++) {
        int min_value = distribution(gen);
        zonemap.emplace_back(Datum(min_value * 100), Datum((min_value + 1) * 100), false);
    }
    std::shuffle(zonemap.begin(), zonemap.end(), std::mt19937(random_seed));
    auto zonemap_ptr = std::make_shared<SortableZoneMap>(TYPE_INT, std::move(zonemap));

    auto sampled_ranges = run(zonemap_ptr);
    EXPECT_EQ(1, stats.sample_build_histogram_count);
    EXPECT_TRUE(zonemap_ptr->is_diverse());
    EXPECT_EQ("zonemap: [2400,2500],[3800,3900],[4600,4700],[5200,5300],[5600,5700],[5800,5900],[6100,6200],[6100,6",
              zonemap_ptr->zonemap_string().substr(0, 100));
    EXPECT_EQ(
            R"(histogram: [2400,3900]: (70,124)
[4600,5300]: (91,30)
[5600,6400]: (100,104,103,28,106)
[6400,7100]: (96,4,36,82,8,39,32,105,46,43)
[7000,7900]: (22,17,21,38,64,47,54,35,117,27)
[7900,8700]: (0,13,79,50,31,99,97,11)
[8700,9400]: (123,98,122,52,12,9,59,75,84,118,25,121,107,120,116,26,83)
[9300,10000]: (115,114,57,60,90,1,42,7,34,93,86,40,19)
[9900,10600]: (48,101,73,62,113,71,74,78,72,2,80,125,88,67,111,94)
[10500,11200]: (92,18,112,55,5,89,24,127,95,37)
[11200,11900]: (45,44,77,87,63,3,68,15,85,69,53,29,102,108)
[11900,12600]: (51,119,66,58,81,109,16,33)
[12600,13400]: (65,110,61,20,14,23)
[13300,14100]: (6,10,49,126,56,76)
[14600,14700]: (41)
)",
            zonemap_ptr->histogram_string());

    ASSERT_TRUE(sampled_ranges.ok());
    ASSERT_GT(sampled_ranges.value().size(), 0);
    EXPECT_EQ(
            "([3072,4096), [5120,6144), [8192,9216), [16384,17408), [21504,22528), [23552,24576), [30720,31744), "
            "[41984,43008), [50176,51200), [95232,96256), [101376,102400), [106496,107520), [120832,121856), "
            "[126976,129024))",
            sampled_ranges.value().to_string());
}

TEST_F(PageSampleTestSuite, test_page_sample_with_overlapped_zonemap) {
    std::vector<ZoneMapDetail> zonemap;
    for (size_t i = 0; i < kNumPages; i++) {
        int min_value = i;
        zonemap.emplace_back(Datum(min_value), Datum(min_value + 100), false);
    }
    std::shuffle(zonemap.begin(), zonemap.end(), std::mt19937(random_seed));
    auto zonemap_ptr = std::make_shared<SortableZoneMap>(TYPE_INT, std::move(zonemap));

    auto sampled_ranges = run(zonemap_ptr);

    EXPECT_FALSE(zonemap_ptr->is_diverse());
    EXPECT_EQ(0, stats.sample_build_histogram_count);
    EXPECT_EQ("zonemap: [0,100],[1,101],[2,102],[3,103],[4,104],[5,105],[6,106],[7,107],[8,108],[9,109],[10,110],[1",
              zonemap_ptr->zonemap_string().substr(0, 100));
    ASSERT_TRUE(sampled_ranges.ok());
    ASSERT_GT(sampled_ranges.value().size(), 0);
    EXPECT_EQ("([31744,32768), [55296,56320), [93184,94208), [96256,97280), [102400,103424))",
              sampled_ranges.value().to_string());
}

} // namespace starrocks