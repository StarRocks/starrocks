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

package com.starrocks.memory.estimate;

import com.google.common.base.Preconditions;

public class StringEstimator implements CustomEstimator {
    @Override
    public long estimate(Object obj) {
        Preconditions.checkArgument(obj instanceof String);
        String str = (String) obj;
        // String: shallow size + array header (16 bytes) + character data
        // Java 9+ uses byte[] with compact strings (Latin-1: 1 byte per char, otherwise 2 bytes)
        // Here we use a conservative estimate of 1 byte per character for most cases
        return Estimator.shallow(str) + Estimator.ARRAY_HEADER_SIZE + str.length();
    }
}
