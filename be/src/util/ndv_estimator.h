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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/mysql_row_buffer.h

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

#pragma once

#include <memory>
#include <string>

namespace starrocks {

class NDVEstimator {
public:
    virtual ~NDVEstimator() {};

    virtual int64_t estimate(int64_t sample_row, int64_t sample_distinct, int64_t count_once, double sample_ratio) = 0;
};

using NDVEstimatorPtr = std::unique_ptr<NDVEstimator>;

class DUJ1Estimator final : public NDVEstimator {
public:
    DUJ1Estimator() = default;
    ~DUJ1Estimator() override  = default;

    int64_t estimate(int64_t sample_row, int64_t sample_distinct, int64_t count_once, double sample_ratio) override;
};

class LinearEstimator final : public NDVEstimator {
public:
    LinearEstimator() = default;
    ~LinearEstimator() override = default;

    int64_t estimate(int64_t /*sample_row*/, int64_t sample_distinct, int64_t /*count_once*/, double sample_ratio) override;
};

class PolynomialEstimator final : public NDVEstimator {
public:
    PolynomialEstimator() = default;
    ~PolynomialEstimator() override = default;

    int64_t estimate(int64_t /*sample_row*/, int64_t sample_distinct, int64_t /*count_once*/, double sample_ratio) override;
};

class GEEEstimator final : public NDVEstimator {
public:
    GEEEstimator() = default;
    ~GEEEstimator() override = default;

    int64_t estimate(int64_t sample_row, int64_t sample_distinct, int64_t count_once, double sample_ratio) override;
};

class NDVEstimatorBuilder {
public:
    NDVEstimatorBuilder() = default;
    ~NDVEstimatorBuilder() = default;

    static NDVEstimatorPtr build(const std::string& name);
};

}