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

import com.starrocks.common.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TypeFactoryTest {

    @Test
    void testInferenceAndGenericStringLengthsDoNotFollowOlapMax() {
        int savedMaxVarcharLength = Config.max_varchar_length;
        StringType savedString = StringType.STRING;
        try {
            // Model a future OLAP limit close to 2 GiB before TypeFactory is first used by this test.
            Config.max_varchar_length = Integer.MAX_VALUE - 1;

            Assertions.assertEquals(Integer.MAX_VALUE - 1, TypeFactory.getOlapMaxVarcharLength());
            Assertions.assertEquals(StringType.MAX_STRING_LENGTH,
                    TypeFactory.getOlapVarcharInferenceLength());
            Assertions.assertSame(savedString, StringType.STRING);
            Assertions.assertEquals(StringType.MAX_STRING_LENGTH, StringType.STRING.getLength());
        } finally {
            Config.max_varchar_length = savedMaxVarcharLength;
            StringType.STRING = savedString;
        }
    }
}
