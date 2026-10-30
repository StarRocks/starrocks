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

package com.starrocks.example.udf;

/**
 * Example UDF with integer varargs to sum numbers.
 * This demonstrates support for variable argument UDFs with numeric types.
 */
public class UDFVarargsInt {
    /**
     * Sum a variable number of integers.
     * @param values Variable number of integer arguments
     * @return Sum of all values, or null if any input is null
     */
    public Integer evaluate(Integer... values) {
        if (values == null || values.length == 0) {
            return 0;
        }
        int sum = 0;
        for (Integer value : values) {
            if (value == null) {
                return null;
            }
            sum += value;
        }
        return sum;
    }
}
