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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/mysql_row_buffer.cpp

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

#include "util/ndv_estimator.h"

#include <cmath>
#include <stdexcept>
#include <unordered_map>

namespace starrocks {

namespace {

enum class NDVEstimatorType { DUJ1, LINEAR, POLYNOMIAL, GEE };

std::unordered_map<std::string, NDVEstimatorType> estimator_name_to_type = {
        {"DUJ1", NDVEstimatorType::DUJ1},
        {"LINEAR", NDVEstimatorType::LINEAR},
        {"POLYNOMIAL", NDVEstimatorType::POLYNOMIAL},
        {"GEE", NDVEstimatorType::GEE}};

} // namespace

int64_t DUJ1Estimator::estimate(int64_t sample_row, int64_t sample_distinct, int64_t count_once, double sample_ratio) {
    return static_cast<int64_t>(sample_row * sample_distinct / (sample_row - count_once + count_once * sample_ratio));
}

int64_t LinearEstimator::estimate(int64_t /*sample_row*/, int64_t sample_distinct, int64_t /*count_once*/,
                                  double sample_ratio) {
    return static_cast<int64_t>(sample_distinct / sample_ratio);
}

int64_t PolynomialEstimator::estimate(int64_t /*sample_row*/, int64_t sample_distinct, int64_t /*count_once*/,
                                      double sample_ratio) {
    return static_cast<int64_t>(sample_distinct / (1 - std::pow((1 - sample_ratio), 3)));
}

int64_t GEEEstimator::estimate(int64_t sample_row, int64_t sample_distinct, int64_t count_once, double sample_ratio) {
    return static_cast<int64_t>(sample_distinct + (std::sqrt(1 / sample_ratio) - 1) * count_once);
}

NDVEstimatorPtr NDVEstimatorBuilder::build(const std::string& name) {
    auto it = estimator_name_to_type.find(name);
    if (it == estimator_name_to_type.end()) {
        throw std::runtime_error("ndv_estimator: unknown estimator type.");
    }

    switch (it->second) {
    case NDVEstimatorType::DUJ1:
        return std::make_unique<DUJ1Estimator>();
    case NDVEstimatorType::LINEAR:
        return std::make_unique<LinearEstimator>();
    case NDVEstimatorType::POLYNOMIAL:
        return std::make_unique<PolynomialEstimator>();
    case NDVEstimatorType::GEE:
        return std::make_unique<GEEEstimator>();
    }

    throw std::runtime_error("ndv_estimator: unknown estimator type.");
}

} // namespace starrocks