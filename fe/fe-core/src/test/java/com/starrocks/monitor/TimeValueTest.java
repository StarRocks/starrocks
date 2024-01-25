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
package com.starrocks.monitor;

import com.starrocks.monitor.unit.TimeValue;
import org.junit.Assert;
import org.junit.Test;

public class TimeValueTest {
    @Test
    public void testGetMilliseconds() {
        Assert.assertEquals(0, TimeValue.parseTimeValue("0ms").getMillis());
        Assert.assertEquals(0, TimeValue.parseTimeValue("0s").getMillis());
        Assert.assertEquals(0, TimeValue.parseTimeValue("0m").getMillis());
        Assert.assertEquals(1011, TimeValue.parseTimeValue("1011ms").getMillis());
        Assert.assertEquals(1011000, TimeValue.parseTimeValue("1011s").getMillis());
        Assert.assertEquals(60000, TimeValue.parseTimeValue("1m").getMillis());
        Assert.assertEquals(600000, TimeValue.parseTimeValue("10m").getMillis());
    }
}
