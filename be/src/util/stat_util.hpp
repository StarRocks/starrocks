// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/stat_util.hpp

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

#include <math.h>

namespace starrocks {

class StatUtil {
public:
    // Computes mean and standard deviation
    template <typename T>
    static void compute_mean_stddev(const T* values, int N, double* mean, double* stddev) {
        *mean = 0;
        *stddev = 0;

        for (int i = 0; i < N; ++i) {
            *mean += values[i];
        }

        *mean /= N;

        for (int i = 0; i < N; ++i) {
            double d = values[i] - *mean;
            *stddev += d * d;
        }

        *stddev /= N;
        *stddev = sqrt(*stddev);
    }
};

}
