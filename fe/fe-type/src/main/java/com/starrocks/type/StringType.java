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

package com.starrocks.type;

public class StringType extends ScalarType {
    // Longest supported VARCHAR and CHAR, chosen to match Hive.
    public static final int DEFAULT_STRING_LENGTH = 65533;
    public static final int MAX_STRING_LENGTH = 1048576;

    public static final ScalarType DEFAULT_STRING = new StringType(DEFAULT_STRING_LENGTH);
    public static StringType STRING = new StringType(MAX_STRING_LENGTH);

    public StringType(int len) {
        super(PrimitiveType.VARCHAR);
        setLength(len);
    }
}
