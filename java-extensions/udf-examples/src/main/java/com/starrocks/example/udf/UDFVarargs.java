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
 * Example UDF with varargs to concatenate strings.
 * This demonstrates support for variable argument UDFs.
 */
public class UDFVarargs {
    /**
     * Concatenate multiple strings with a separator.
     * @param args Variable number of string arguments
     * @return Concatenated string with space separator
     */
    public String evaluate(String... args) {
        if (args == null || args.length == 0) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            if (args[i] != null) {
                if (i > 0) {
                    result.append(" ");
                }
                result.append(args[i]);
            }
        }
        return result.toString();
    }
}
