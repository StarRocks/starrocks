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

package com.starrocks.datacache;

import org.junit.Assert;
import org.junit.Test;

public class DataCachePopulateModeTest {

    @Test
    public void testModeName() {
        Assert.assertEquals("always", DataCachePopulateMode.ALWAYS.modeName());
        Assert.assertEquals("never", DataCachePopulateMode.NEVER.modeName());
        Assert.assertEquals("auto", DataCachePopulateMode.AUTO.modeName());
    }

    @Test
    public void testFromName() {
        Assert.assertEquals(DataCachePopulateMode.AUTO, DataCachePopulateMode.fromName("Auto"));
        Assert.assertEquals(DataCachePopulateMode.NEVER, DataCachePopulateMode.fromName("NEVER"));
        Assert.assertEquals(DataCachePopulateMode.ALWAYS, DataCachePopulateMode.fromName("always"));
        Assert.assertThrows(IllegalArgumentException.class, () -> DataCachePopulateMode.fromName("other"));
    }
}
