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

package com.starrocks.common.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class ColocatePropertyInfoTest {

    @Test
    public void testOfGroupNameOnly() {
        ColocatePropertyInfo info = ColocatePropertyInfo.of("group1");
        Assertions.assertNotNull(info);
        Assertions.assertEquals("group1", info.getColocateGroupName());
        Assertions.assertNull(info.getColocateColumnNames());
    }

    @Test
    public void testOfGroupNameWithColumns() {
        ColocatePropertyInfo info = ColocatePropertyInfo.of("group1:tenant_id,created_time");
        Assertions.assertNotNull(info);
        Assertions.assertEquals("group1", info.getColocateGroupName());
        Assertions.assertEquals(Arrays.asList("tenant_id", "created_time"),
                info.getColocateColumnNames());
    }

    @Test
    public void testOfGroupNameWithSingleColumn() {
        ColocatePropertyInfo info = ColocatePropertyInfo.of("grp1:tenant_id");
        Assertions.assertNotNull(info);
        Assertions.assertEquals("grp1", info.getColocateGroupName());
        Assertions.assertEquals(Arrays.asList("tenant_id"), info.getColocateColumnNames());
    }

    @Test
    public void testOfGroupNameWithSpaces() {
        ColocatePropertyInfo info = ColocatePropertyInfo.of("group1: col1 , col2 ");
        Assertions.assertNotNull(info);
        Assertions.assertEquals("group1", info.getColocateGroupName());
        Assertions.assertEquals(Arrays.asList("col1", "col2"), info.getColocateColumnNames());
    }

    @Test
    public void testOfNull() {
        Assertions.assertNull(ColocatePropertyInfo.of(null));
    }

    @Test
    public void testOfEmpty() {
        Assertions.assertNull(ColocatePropertyInfo.of(""));
    }

    @Test
    public void testToStringGroupNameOnly() {
        ColocatePropertyInfo info = new ColocatePropertyInfo("group1", null);
        Assertions.assertEquals("group1", info.toString());
    }

    @Test
    public void testToStringWithColumns() {
        ColocatePropertyInfo info = new ColocatePropertyInfo("group1",
                Arrays.asList("tenant_id", "created_time"));
        Assertions.assertEquals("group1:tenant_id,created_time", info.toString());
    }

    @Test
    public void testOfAndToStringRoundTrip() {
        // Group name only
        String value1 = "group1";
        Assertions.assertEquals(value1, ColocatePropertyInfo.of(value1).toString());

        // Group name with columns
        String value2 = "group1:tenant_id,created_time";
        Assertions.assertEquals(value2, ColocatePropertyInfo.of(value2).toString());
    }

    @Test
    public void testEquals() {
        ColocatePropertyInfo info1 = ColocatePropertyInfo.of("group1:col1,col2");
        ColocatePropertyInfo info2 = ColocatePropertyInfo.of("group1:col1,col2");
        ColocatePropertyInfo info3 = ColocatePropertyInfo.of("group1:col1");
        ColocatePropertyInfo info4 = ColocatePropertyInfo.of("group2:col1,col2");
        ColocatePropertyInfo info5 = ColocatePropertyInfo.of("group1");

        Assertions.assertEquals(info1, info2);
        Assertions.assertNotEquals(info1, info3);
        Assertions.assertNotEquals(info1, info4);
        Assertions.assertNotEquals(info1, info5);
        Assertions.assertNotEquals(info1, null);
    }

    @Test
    public void testHashCode() {
        ColocatePropertyInfo info1 = ColocatePropertyInfo.of("group1:col1,col2");
        ColocatePropertyInfo info2 = ColocatePropertyInfo.of("group1:col1,col2");
        Assertions.assertEquals(info1.hashCode(), info2.hashCode());
    }

    @Test
    public void testGetColocateGroupName() {
        Assertions.assertEquals("group1", ColocatePropertyInfo.getColocateGroupName("group1"));
        Assertions.assertEquals("group1", ColocatePropertyInfo.getColocateGroupName("group1:col1,col2"));
        Assertions.assertEquals("grp1", ColocatePropertyInfo.getColocateGroupName("grp1:tenant_id"));
        Assertions.assertEquals("group1", ColocatePropertyInfo.getColocateGroupName(" group1 : col1,col2"));
        Assertions.assertNull(ColocatePropertyInfo.getColocateGroupName(null));
        Assertions.assertEquals("", ColocatePropertyInfo.getColocateGroupName(""));
    }
}
