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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/bit_util_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/bit_util.h"

#include <gtest/gtest.h>

#include <boost/utility.hpp>
#include <cstdio>
#include <iostream>

namespace starrocks {

TEST(BitUtil, Ceil) {
    EXPECT_EQ(BitUtil::ceil(0, 1), 0);
    EXPECT_EQ(BitUtil::ceil(1, 1), 1);
    EXPECT_EQ(BitUtil::ceil(1, 2), 1);
    EXPECT_EQ(BitUtil::ceil(1, 8), 1);
    EXPECT_EQ(BitUtil::ceil(7, 8), 1);
    EXPECT_EQ(BitUtil::ceil(8, 8), 1);
    EXPECT_EQ(BitUtil::ceil(9, 8), 2);
}

TEST(BitUtil, Popcount) {
    EXPECT_EQ(BitUtil::popcount(BOOST_BINARY(0 1 0 1 0 1 0 1)), 4);
    EXPECT_EQ(BitUtil::popcount_no_hw(BOOST_BINARY(0 1 0 1 0 1 0 1)), 4);
    EXPECT_EQ(BitUtil::popcount(BOOST_BINARY(1 1 1 1 0 1 0 1)), 6);
    EXPECT_EQ(BitUtil::popcount_no_hw(BOOST_BINARY(1 1 1 1 0 1 0 1)), 6);
    EXPECT_EQ(BitUtil::popcount(BOOST_BINARY(1 1 1 1 1 1 1 1)), 8);
    EXPECT_EQ(BitUtil::popcount_no_hw(BOOST_BINARY(1 1 1 1 1 1 1 1)), 8);
    EXPECT_EQ(BitUtil::popcount(0), 0);
    EXPECT_EQ(BitUtil::popcount_no_hw(0), 0);
}

TEST(BitUtil, RoundUp) {
    EXPECT_EQ(BitUtil::round_up_numi32(1), 1);
    EXPECT_EQ(BitUtil::round_up_numi32(0), 0);
}

TEST(BitUtil, RoundDown) {
    EXPECT_EQ(BitUtil::RoundDownToPowerOf2(7, 4), 4);
}

} // namespace starrocks
