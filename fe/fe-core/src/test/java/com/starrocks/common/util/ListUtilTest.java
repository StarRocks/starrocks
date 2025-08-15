// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.util;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ListUtilTest {

    @Test
    public void testSplitBySizeNormal() {
        List<Integer> lists = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7);
        int expectSize = 3;

        List<List<Integer>> splitLists = ListUtil.splitBySize(lists, expectSize);

        Assertions.assertEquals(splitLists.size(), 3);
        Assertions.assertEquals(splitLists.get(0).size(), 3);
        Assertions.assertEquals(splitLists.get(1).size(), 2);
        Assertions.assertEquals(splitLists.get(2).size(), 2);
    }

    @Test
    public void testSplitBySizeNormal2() {
        List<Integer> lists = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7);
        int expectSize = 1;

        List<List<Integer>> splitLists = ListUtil.splitBySize(lists, expectSize);

        Assertions.assertEquals(splitLists.size(), 1);
        Assertions.assertEquals(lists, splitLists.get(0));
    }

    @Test
    public void testSplitBySizeWithLargeExpectSize() {
        List<Integer> lists = Lists.newArrayList(1, 2, 3);
        int expectSize = 10;

        List<List<Integer>> splitLists = ListUtil.splitBySize(lists, expectSize);

        Assertions.assertEquals(splitLists.size(), lists.size());
        Assertions.assertTrue(splitLists.get(0).get(0) == 1);
        Assertions.assertTrue(splitLists.get(1).get(0) == 2);
        Assertions.assertTrue(splitLists.get(2).get(0) == 3);
    }

    @Test
    public void testSplitBySizeWithEmptyList() {
        List<Integer> lists = Lists.newArrayList();
        int expectSize = 10;

        List<List<Integer>> splitLists = ListUtil.splitBySize(lists, expectSize);

        Assertions.assertEquals(splitLists.size(), lists.size());
    }

    @Test
    public void testSplitBySizeWithNullList() {
        List<Integer> lists = null;
        int expectSize = 10;

        Throwable exception = assertThrows(NullPointerException.class, () -> ListUtil.splitBySize(lists, expectSize));
        assertThat(exception.getMessage(), containsString("list must not be null"));
    }

    @Test
    public void testSplitBySizeWithNegativeSize() {
        List<Integer> lists = Lists.newArrayList(1, 2, 3);
        int expectSize = -1;

        Throwable exception = assertThrows(IllegalArgumentException.class, () -> ListUtil.splitBySize(lists, expectSize));
        assertThat(exception.getMessage(), containsString("expectedSize must larger than 0"));
    }
}
