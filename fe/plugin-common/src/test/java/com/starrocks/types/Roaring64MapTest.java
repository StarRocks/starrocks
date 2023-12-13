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

package com.starrocks.types;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class Roaring64MapTest {
    static BitmapValue largeBitmap;

    @BeforeClass
    public static void beforeClass() throws Exception {
        largeBitmap = new BitmapValue();
        for (long i = 0; i < 20; i++) {
            largeBitmap.add(i);
        }
    }

    @Test
    public void testSerializeToString() {
        Assert.assertEquals("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19", largeBitmap.serializeToString());
    }
}
