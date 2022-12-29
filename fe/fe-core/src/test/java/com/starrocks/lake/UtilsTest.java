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

package com.starrocks.lake;

import org.junit.Assert;
import org.junit.Test;

import static com.starrocks.lake.Utils.formatQueryInstant;

public class UtilsTest {

    @Test
    public void testFormatQueryInstant() throws Exception {
        String queryInstant1 = "2022-12-12 16:15:41.122";
        String queryInstant2 = "20221212161542";
        String queryInstant3 = "2022-12-12";

        Assert.assertEquals("20221212161541122", formatQueryInstant(queryInstant1));
        Assert.assertEquals("20221212161542", formatQueryInstant(queryInstant2));
        Assert.assertEquals("20221212000000000", formatQueryInstant(queryInstant3));
        Assert.assertThrows("Unsupported query instant time format",
                IllegalArgumentException.class,
                () -> formatQueryInstant("2022"));
    }
}
