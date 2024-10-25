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

public class ColumnIdTest {

    @Test
    public void testColumnId() {
        ColumnId columnIdA = ColumnId.create("a");

        Assert.assertTrue(columnIdA.equals(columnIdA));
        Assert.assertTrue(columnIdA.equals(ColumnId.create("a")));
        Assert.assertFalse(columnIdA.equals(ColumnId.create("A")));
        Assert.assertFalse(columnIdA.equals(ColumnId.create("b")));

        Assert.assertTrue(columnIdA.equalsIgnoreCase(columnIdA));
        Assert.assertTrue(columnIdA.equalsIgnoreCase(ColumnId.create("a")));
        Assert.assertTrue(columnIdA.equalsIgnoreCase(ColumnId.create("A")));
        Assert.assertFalse(columnIdA.equalsIgnoreCase(ColumnId.create("b")));
    }
}
