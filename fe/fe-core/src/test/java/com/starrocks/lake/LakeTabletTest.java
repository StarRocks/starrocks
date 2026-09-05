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

import com.starrocks.catalog.TabletRange;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LakeTabletTest {

    @Test
    public void testSerialization() {
        LakeTablet tablet = new LakeTablet(10001L);
        tablet.setDataSize(100L);
        tablet.setRowCount(10L);

        String json = GsonUtils.GSON.toJson(tablet);
        LakeTablet deserializedTablet = GsonUtils.GSON.fromJson(json, LakeTablet.class);

        Assertions.assertEquals(tablet.getId(), deserializedTablet.getId());
        Assertions.assertEquals(tablet.getDataSize(true), deserializedTablet.getDataSize(true));
        Assertions.assertEquals(tablet.getRowCount(0), deserializedTablet.getRowCount(0));
        Assertions.assertNull(deserializedTablet.getRange());
    }

    @Test
    public void testDefaultConstructor() {
        LakeTablet tablet = new LakeTablet();
        Assertions.assertNull(tablet.getRange());
    }

    @Test
    public void testRangeSerialization() {
        LakeTablet tablet = new LakeTablet(10002L, new TabletRange());
        String json = GsonUtils.GSON.toJson(tablet);
        LakeTablet deserializedTablet = GsonUtils.GSON.fromJson(json, LakeTablet.class);
        Assertions.assertNotNull(deserializedTablet.getRange());
    }
}
