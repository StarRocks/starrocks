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

package com.starrocks.catalog;

import org.junit.Assert;
import org.junit.Test;

public class MetaVersionTest {

    @Test
    public void testCompatible() {
        Assert.assertTrue(MetaVersion.isCompatible(3, 3));
        Assert.assertTrue(MetaVersion.isCompatible(2, 3));
        Assert.assertTrue(MetaVersion.isCompatible(4, 3));
        Assert.assertFalse(MetaVersion.isCompatible(5, 3));
    }
}
