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

#include "base/stats/ndv_estimator.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(NDVEstimatorTest, InvalidSampleRatioReturnsZero) {
    DUJ1Estimator duj1;
    LinearEstimator linear;
    PolynomialEstimator poly;
    GEEEstimator gee;

    EXPECT_EQ(0, duj1.estimate(100, 10, 5, -0.5));
    EXPECT_EQ(0, linear.estimate(100, 10, 5, -0.5));
    EXPECT_EQ(0, poly.estimate(100, 10, 5, -0.5));
    EXPECT_EQ(0, gee.estimate(100, 10, 5, -0.5));
}

TEST(NDVEstimatorTest, SampleRatioClampedToOne) {
    DUJ1Estimator duj1;
    LinearEstimator linear;
    PolynomialEstimator poly;
    GEEEstimator gee;

    EXPECT_EQ(10, duj1.estimate(100, 10, 5, 2.0));
    EXPECT_EQ(10, linear.estimate(100, 10, 5, 2.0));
    EXPECT_EQ(10, poly.estimate(100, 10, 5, 2.0));
    EXPECT_EQ(10, gee.estimate(100, 10, 5, 2.0));
}

} // namespace starrocks
