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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/common/util/ListComparatorTest.java

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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ListComparatorTest {

    List<List<Comparable>> listCollection;

    @BeforeEach
    public void setUp() {
        listCollection = new LinkedList<List<Comparable>>();
    }

    private void printCollection() {
        System.out.println("LIST:");
        for (List<Comparable> list : listCollection) {
            for (Comparable comparable : list) {
                System.out.print(comparable + " | ");
            }
            System.out.println();
        }
        System.out.println("END LIST\n");
    }

    @Test
    public void test_1() {
        // 1, 200, "bcd", 2000
        // 1, 200, "abc"
        List<Comparable> list1 = new LinkedList<Comparable>();
        list1.add(Long.valueOf(1));
        list1.add(Long.valueOf(200));
        list1.add("bcd");
        list1.add(Long.valueOf(1000));
        listCollection.add(list1);

        List<Comparable> list2 = new LinkedList<Comparable>();
        list2.add(Long.valueOf(1));
        list2.add(Long.valueOf(200));
        list2.add("abc");
        listCollection.add(list2);

        ListComparator<List<Comparable>> comparator = new ListComparator<>(new OrderByPair(1, false),
                new OrderByPair(2, false));
        Collections.sort(listCollection, comparator);

        Assertions.assertEquals(list2, listCollection.get(0));
    }

    @Test
    public void test_2() {
        // 1, 200, "abc", 1000
        // 1, 200, "abc"
        List<Comparable> list1 = new LinkedList<Comparable>();
        list1.add(Long.valueOf(1));
        list1.add(Long.valueOf(200));
        list1.add("abc");
        list1.add(Long.valueOf(1000));
        listCollection.add(list1);

        List<Comparable> list2 = new LinkedList<Comparable>();
        list2.add(Long.valueOf(1));
        list2.add(Long.valueOf(200));
        list2.add("abc");
        listCollection.add(list2);

        ListComparator<List<Comparable>> comparator = new ListComparator<>(new OrderByPair(1, false),
                new OrderByPair(2, false));
        Collections.sort(listCollection, comparator);

        Assertions.assertEquals(list2, listCollection.get(0));
    }

    @Test
    public void test_3() {
        assertThrows(ClassCastException.class, () -> {
            // 1, 200, "abc", 2000
            // 1, 200, "abc", "bcd"
            List<Comparable> list1 = new LinkedList<Comparable>();
            list1.add(Long.valueOf(1));
            list1.add(Long.valueOf(200));
            list1.add("abc");
            list1.add(Long.valueOf(2000));
            listCollection.add(list1);

            List<Comparable> list2 = new LinkedList<Comparable>();
            list2.add(Long.valueOf(1));
            list2.add(Long.valueOf(200));
            list2.add("abc");
            list2.add("bcd");
            listCollection.add(list2);

            ListComparator<List<Comparable>> comparator = new ListComparator<>(new OrderByPair(1, false),
                    new OrderByPair(3, false));
            Collections.sort(listCollection, comparator);
            Assertions.fail();
        });
    }

    @Test
    public void test_4() {
        // 1, 200, "bb", 2000
        // 1, 300, "aa"
        List<Comparable> list1 = new LinkedList<Comparable>();
        list1.add(Long.valueOf(1));
        list1.add(Long.valueOf(200));
        list1.add("bb");
        list1.add(Long.valueOf(1000));
        listCollection.add(list1);

        List<Comparable> list2 = new LinkedList<Comparable>();
        list2.add(Long.valueOf(1));
        list2.add(Long.valueOf(300));
        list2.add("aa");
        listCollection.add(list2);

        ListComparator<List<Comparable>> comparator = new ListComparator<>(new OrderByPair(2, false),
                new OrderByPair(1, false));
        Collections.sort(listCollection, comparator);

        Assertions.assertEquals(list2, listCollection.get(0));
    }

    @Test
    public void test_5() {
        // 1, 200, "bb", 2000
        // 1, 100, "aa"
        // 1, 300, "aa"
        List<Comparable> list1 = new LinkedList<Comparable>();
        list1.add(Long.valueOf(1));
        list1.add(Long.valueOf(200));
        list1.add("bb");
        list1.add(Long.valueOf(1000));
        listCollection.add(list1);

        List<Comparable> list2 = new LinkedList<Comparable>();
        list2.add(Long.valueOf(1));
        list2.add(Long.valueOf(100));
        list2.add("aa");
        listCollection.add(list2);

        List<Comparable> list3 = new LinkedList<Comparable>();
        list3.add(Long.valueOf(1));
        list3.add(Long.valueOf(300));
        list3.add("aa");
        listCollection.add(list3);

        ListComparator<List<Comparable>> comparator = new ListComparator<>(new OrderByPair(2, false),
                new OrderByPair(1, true));
        Collections.sort(listCollection, comparator);

        Assertions.assertEquals(list3, listCollection.get(0));
    }

}
