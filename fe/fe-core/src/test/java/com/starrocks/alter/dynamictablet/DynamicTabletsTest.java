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

package com.starrocks.alter.dynamictablet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class DynamicTabletsTest {
    @Test
    public void testSplittingTablets() {
        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
            Assertions.assertTrue(splittingTablets.isEmpty());
            Assertions.assertTrue(splittingTablets.getSplittingTablets().isEmpty());
            Assertions.assertTrue(splittingTablets.getMergingTablets().isEmpty());
            Assertions.assertTrue(splittingTablets.getIdenticalTablets().isEmpty());
            Assertions.assertTrue(splittingTablets.getOldTabletIds().isEmpty());
            Assertions.assertTrue(splittingTablets.getNewTabletIds().isEmpty());
            Assertions.assertEquals(0, splittingTablets.getParallelTablets());
        }

        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    List.of(
                            new SplittingTablet(1L,
                                    List.of(101L,
                                            102L)),
                            new SplittingTablet(2L,
                                    List.of(201L,
                                            202L))),
                    Collections.emptyList(),
                    Collections.emptyList());

            Assertions.assertThrows(IllegalStateException.class,
                    () -> splittingTablets.calcNewVirtualBuckets(List.of(1L, 2L, 3L, 4L, 5L)));
        }

        {
            DynamicTablets identicalTablets = new DynamicTablets(
                    Collections.emptyList(),
                    Collections.emptyList(),
                    List.of(
                            new IdenticalTablet(1, 1L),
                            new IdenticalTablet(2, 2L),
                            new IdenticalTablet(3, 3L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertFalse(identicalTablets.isEmpty());

            Assertions.assertEquals(List.of(
                    1L, 2L, 3L, 4L, 5L),
                    identicalTablets.calcNewVirtualBuckets(List.of(1L, 2L, 3L, 4L, 5L)));
        }

        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    List.of(
                            new SplittingTablet(1L,
                                    List.of(101L,
                                            102L)),
                            new SplittingTablet(2L,
                                    List.of(201L,
                                            202L))),
                    Collections.emptyList(),
                    List.of(
                            new IdenticalTablet(3, 3L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertFalse(splittingTablets.isEmpty());

            Assertions.assertEquals(List.of(
                    101L, 201L, 3L, 4L, 5L,
                    102L, 202L, 3L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(List.of(1L, 2L, 3L, 4L, 5L)));
        }

        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    List.of(
                            new SplittingTablet(1L,
                                    List.of(101L,
                                            102L)),
                            new SplittingTablet(2L,
                                    List.of(201L,
                                            202L))),
                    Collections.emptyList(),
                    List.of(
                            new IdenticalTablet(301, 301L),
                            new IdenticalTablet(302, 302L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 201L, 301L, 4L, 5L,
                    102L, 202L, 302L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(
                            List.of(1L, 2L, 301L, 4L, 5L,
                                    1L, 2L, 302L, 4L, 5L)));
        }

        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    List.of(
                            new SplittingTablet(1L,
                                    List.of(101L,
                                            102L)),
                            new SplittingTablet(2L,
                                    List.of(201L,
                                            202L))),
                    Collections.emptyList(),
                    List.of(
                            new IdenticalTablet(301, 301L),
                            new IdenticalTablet(302, 302L),
                            new IdenticalTablet(303, 303L),
                            new IdenticalTablet(304, 304L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 201L, 301L, 4L, 5L,
                    102L, 202L, 302L, 4L, 5L,
                    101L, 201L, 303L, 4L, 5L,
                    102L, 202L, 304L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(
                            List.of(1L, 2L, 301L, 4L, 5L,
                                    1L, 2L, 302L, 4L, 5L,
                                    1L, 2L, 303L, 4L, 5L,
                                    1L, 2L, 304L, 4L, 5L)));
        }

        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    List.of(
                            new SplittingTablet(2L,
                                    List.of(201L,
                                            202L,
                                            203L,
                                            204L)),
                            new SplittingTablet(3L,
                                    List.of(301L,
                                            302L))),
                    Collections.emptyList(),
                    List.of(
                            new IdenticalTablet(1, 1L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    1L, 201L, 301L, 4L, 5L,
                    1L, 202L, 302L, 4L, 5L,
                    1L, 203L, 301L, 4L, 5L,
                    1L, 204L, 302L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(List.of(1L, 2L, 3L, 4L, 5L)));
        }

        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    List.of(
                            new SplittingTablet(2L,
                                    List.of(201L,
                                            202L,
                                            203L,
                                            204L)),
                            new SplittingTablet(3L,
                                    List.of(301L,
                                            302L))),
                    Collections.emptyList(),
                    List.of(
                            new IdenticalTablet(101, 101L),
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 201L, 301L, 4L, 5L,
                    102L, 202L, 302L, 4L, 5L,
                    101L, 203L, 301L, 4L, 5L,
                    102L, 204L, 302L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(
                            List.of(101L, 2L, 3L, 4L, 5L,
                                    102L, 2L, 3L, 4L, 5L)));
        }

        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    List.of(
                            new SplittingTablet(2L,
                                    List.of(201L,
                                            202L,
                                            203L,
                                            204L)),
                            new SplittingTablet(3L,
                                    List.of(301L,
                                            302L))),
                    Collections.emptyList(),
                    List.of(
                            new IdenticalTablet(101, 101L),
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(103, 103L),
                            new IdenticalTablet(104, 104L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 201L, 301L, 4L, 5L,
                    102L, 202L, 302L, 4L, 5L,
                    103L, 203L, 301L, 4L, 5L,
                    104L, 204L, 302L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(
                            List.of(101L, 2L, 3L, 4L, 5L,
                                    102L, 2L, 3L, 4L, 5L,
                                    103L, 2L, 3L, 4L, 5L,
                                    104L, 2L, 3L, 4L, 5L)));
        }

        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    List.of(
                            new SplittingTablet(2L,
                                    List.of(201L,
                                            202L,
                                            203L,
                                            204L)),
                            new SplittingTablet(3L,
                                    List.of(301L,
                                            302L))),
                    Collections.emptyList(),
                    List.of(
                            new IdenticalTablet(101, 101L),
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(103, 103L),
                            new IdenticalTablet(104, 104L),
                            new IdenticalTablet(105, 105L),
                            new IdenticalTablet(106, 106L),
                            new IdenticalTablet(107, 107L),
                            new IdenticalTablet(108, 108L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 201L, 301L, 4L, 5L,
                    102L, 202L, 302L, 4L, 5L,
                    103L, 203L, 301L, 4L, 5L,
                    104L, 204L, 302L, 4L, 5L,
                    105L, 201L, 301L, 4L, 5L,
                    106L, 202L, 302L, 4L, 5L,
                    107L, 203L, 301L, 4L, 5L,
                    108L, 204L, 302L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(
                            List.of(101L, 2L, 3L, 4L, 5L,
                                    102L, 2L, 3L, 4L, 5L,
                                    103L, 2L, 3L, 4L, 5L,
                                    104L, 2L, 3L, 4L, 5L,
                                    105L, 2L, 3L, 4L, 5L,
                                    106L, 2L, 3L, 4L, 5L,
                                    107L, 2L, 3L, 4L, 5L,
                                    108L, 2L, 3L, 4L, 5L)));
        }

        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    List.of(
                            new SplittingTablet(101L,
                                    List.of(10101L,
                                            10102L,
                                            10103L,
                                            10104L)),
                            new SplittingTablet(2L,
                                    List.of(201L,
                                            202L,
                                            203L,
                                            204L))),
                    Collections.emptyList(),
                    List.of(
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(3, 3L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    10101L, 201L, 3L, 4L, 5L,
                    102L, 202L, 3L, 4L, 5L,
                    10102L, 203L, 3L, 4L, 5L,
                    102L, 204L, 3L, 4L, 5L,
                    10103L, 201L, 3L, 4L, 5L,
                    102L, 202L, 3L, 4L, 5L,
                    10104L, 203L, 3L, 4L, 5L,
                    102L, 204L, 3L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(List.of(
                            101L, 2L, 3L, 4L, 5L,
                            102L, 2L, 3L, 4L, 5L)));
        }

        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    List.of(
                            new SplittingTablet(101L,
                                    List.of(10101L,
                                            10102L,
                                            10103L,
                                            10104L)),
                            new SplittingTablet(2L,
                                    List.of(201L,
                                            202L,
                                            203L,
                                            204L))),
                    Collections.emptyList(),
                    List.of(
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(301, 301L),
                            new IdenticalTablet(302, 302L),
                            new IdenticalTablet(303, 303L),
                            new IdenticalTablet(304, 304L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    10101L, 201L, 301L, 4L, 5L,
                    102L, 202L, 302L, 4L, 5L,
                    10102L, 203L, 303L, 4L, 5L,
                    102L, 204L, 304L, 4L, 5L,
                    10103L, 201L, 301L, 4L, 5L,
                    102L, 202L, 302L, 4L, 5L,
                    10104L, 203L, 303L, 4L, 5L,
                    102L, 204L, 304L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(List.of(
                            101L, 2L, 301L, 4L, 5L,
                            102L, 2L, 302L, 4L, 5L,
                            101L, 2L, 303L, 4L, 5L,
                            102L, 2L, 304L, 4L, 5L)));
        }

        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    List.of(
                            new SplittingTablet(101L,
                                    List.of(10101L,
                                            10102L,
                                            10103L,
                                            10104L)),
                            new SplittingTablet(2L,
                                    List.of(201L,
                                            202L,
                                            203L,
                                            204L))),
                    Collections.emptyList(),
                    List.of(
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(301, 301L),
                            new IdenticalTablet(302, 302L),
                            new IdenticalTablet(303, 303L),
                            new IdenticalTablet(304, 304L),
                            new IdenticalTablet(305, 305L),
                            new IdenticalTablet(306, 306L),
                            new IdenticalTablet(307, 307L),
                            new IdenticalTablet(308, 308L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    10101L, 201L, 301L, 4L, 5L,
                    102L, 202L, 302L, 4L, 5L,
                    10102L, 203L, 303L, 4L, 5L,
                    102L, 204L, 304L, 4L, 5L,
                    10103L, 201L, 305L, 4L, 5L,
                    102L, 202L, 306L, 4L, 5L,
                    10104L, 203L, 307L, 4L, 5L,
                    102L, 204L, 308L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(List.of(
                            101L, 2L, 301L, 4L, 5L,
                            102L, 2L, 302L, 4L, 5L,
                            101L, 2L, 303L, 4L, 5L,
                            102L, 2L, 304L, 4L, 5L,
                            101L, 2L, 305L, 4L, 5L,
                            102L, 2L, 306L, 4L, 5L,
                            101L, 2L, 307L, 4L, 5L,
                            102L, 2L, 308L, 4L, 5L)));

        }

        {
            DynamicTablets splittingTablets = new DynamicTablets(
                    List.of(
                            new SplittingTablet(101L,
                                    List.of(10101L,
                                            10102L,
                                            10103L,
                                            10104L)),
                            new SplittingTablet(2L,
                                    List.of(201L,
                                            202L,
                                            203L,
                                            204L))),
                    Collections.emptyList(),
                    List.of(
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(301, 301L),
                            new IdenticalTablet(302, 302L),
                            new IdenticalTablet(303, 303L),
                            new IdenticalTablet(304, 304L),
                            new IdenticalTablet(305, 305L),
                            new IdenticalTablet(306, 306L),
                            new IdenticalTablet(307, 307L),
                            new IdenticalTablet(308, 308L),
                            new IdenticalTablet(309, 309L),
                            new IdenticalTablet(310, 310L),
                            new IdenticalTablet(311, 311L),
                            new IdenticalTablet(312, 312L),
                            new IdenticalTablet(313, 313L),
                            new IdenticalTablet(314, 314L),
                            new IdenticalTablet(315, 315L),
                            new IdenticalTablet(316, 316L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    10101L, 201L, 301L, 4L, 5L,
                    102L, 202L, 302L, 4L, 5L,
                    10102L, 203L, 303L, 4L, 5L,
                    102L, 204L, 304L, 4L, 5L,
                    10103L, 201L, 305L, 4L, 5L,
                    102L, 202L, 306L, 4L, 5L,
                    10104L, 203L, 307L, 4L, 5L,
                    102L, 204L, 308L, 4L, 5L,
                    10101L, 201L, 309L, 4L, 5L,
                    102L, 202L, 310L, 4L, 5L,
                    10102L, 203L, 311L, 4L, 5L,
                    102L, 204L, 312L, 4L, 5L,
                    10103L, 201L, 313L, 4L, 5L,
                    102L, 202L, 314L, 4L, 5L,
                    10104L, 203L, 315L, 4L, 5L,
                    102L, 204L, 316L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(List.of(
                            101L, 2L, 301L, 4L, 5L,
                            102L, 2L, 302L, 4L, 5L,
                            101L, 2L, 303L, 4L, 5L,
                            102L, 2L, 304L, 4L, 5L,
                            101L, 2L, 305L, 4L, 5L,
                            102L, 2L, 306L, 4L, 5L,
                            101L, 2L, 307L, 4L, 5L,
                            102L, 2L, 308L, 4L, 5L,
                            101L, 2L, 309L, 4L, 5L,
                            102L, 2L, 310L, 4L, 5L,
                            101L, 2L, 311L, 4L, 5L,
                            102L, 2L, 312L, 4L, 5L,
                            101L, 2L, 313L, 4L, 5L,
                            102L, 2L, 314L, 4L, 5L,
                            101L, 2L, 315L, 4L, 5L,
                            102L, 2L, 316L, 4L, 5L)));
        }
    }

    @Test
    public void testMergingTablets() {
        {
            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
            Assertions.assertTrue(mergingTablets.isEmpty());
            Assertions.assertTrue(mergingTablets.getSplittingTablets().isEmpty());
            Assertions.assertTrue(mergingTablets.getMergingTablets().isEmpty());
            Assertions.assertTrue(mergingTablets.getIdenticalTablets().isEmpty());
            Assertions.assertEquals(0, mergingTablets.getParallelTablets());
        }

        {
            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(),
                    List.of(
                            new MergingTablet(
                                    List.of(101L, 102L),
                                    1L),
                            new MergingTablet(
                                    List.of(201L, 202L),
                                    2L)),
                    Collections.emptyList());

            Assertions.assertThrows(IllegalStateException.class,
                    () -> mergingTablets.calcNewVirtualBuckets(List.of(
                            101L, 201L, 3L, 4L, 5L,
                            102L, 202L, 3L, 4L, 5L)));
        }

        {
            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(),
                    List.of(
                            new MergingTablet(
                                    List.of(101L, 102L),
                                    1L),
                            new MergingTablet(
                                    List.of(201L, 202L),
                                    2L)),
                    List.of(
                            new IdenticalTablet(3, 3L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertFalse(mergingTablets.isEmpty());

            Assertions.assertEquals(List.of(1L, 2L, 3L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(List.of(
                            101L, 201L, 3L, 4L, 5L,
                            102L, 202L, 3L, 4L, 5L)));
        }

        {

            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(),
                    List.of(
                            new MergingTablet(
                                    List.of(101L, 102L),
                                    1L),
                            new MergingTablet(
                                    List.of(201L, 202L),
                                    2L)),
                    List.of(
                            new IdenticalTablet(301, 301L),
                            new IdenticalTablet(302, 302L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    1L, 2L, 301L, 4L, 5L,
                    1L, 2L, 302L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(
                            List.of(101L, 201L, 301L, 4L, 5L,
                                    102L, 202L, 302L, 4L, 5L)));
        }

        {
            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(),
                    List.of(
                            new MergingTablet(
                                    List.of(101L, 102L),
                                    1L),
                            new MergingTablet(
                                    List.of(201L, 202L),
                                    2L)),
                    List.of(
                            new IdenticalTablet(301, 301L),
                            new IdenticalTablet(302, 302L),
                            new IdenticalTablet(303, 303L),
                            new IdenticalTablet(304, 304L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    1L, 2L, 301L, 4L, 5L,
                    1L, 2L, 302L, 4L, 5L,
                    1L, 2L, 303L, 4L, 5L,
                    1L, 2L, 304L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(
                            List.of(101L, 201L, 301L, 4L, 5L,
                                    102L, 202L, 302L, 4L, 5L,
                                    101L, 201L, 303L, 4L, 5L,
                                    102L, 202L, 304L, 4L, 5L)));
        }

        {
            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(),
                    List.of(
                            new MergingTablet(
                                    List.of(201L, 202L, 203L, 204L),
                                    2L),
                            new MergingTablet(
                                    List.of(301L, 302L),
                                    3L)),
                    List.of(
                            new IdenticalTablet(1, 1L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(1L, 2L, 3L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(List.of(
                            1L, 201L, 301L, 4L, 5L,
                            1L, 202L, 302L, 4L, 5L,
                            1L, 203L, 301L, 4L, 5L,
                            1L, 204L, 302L, 4L, 5L)));
        }

        {
            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(),
                    List.of(
                            new MergingTablet(
                                    List.of(201L, 202L, 203L, 204L),
                                    2L),
                            new MergingTablet(
                                    List.of(301L, 302L),
                                    3L)),
                    List.of(
                            new IdenticalTablet(101, 101L),
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 2L, 3L, 4L, 5L,
                    102L, 2L, 3L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(
                            List.of(101L, 201L, 301L, 4L, 5L,
                                    102L, 202L, 302L, 4L, 5L,
                                    101L, 203L, 301L, 4L, 5L,
                                    102L, 204L, 302L, 4L, 5L)));
        }

        {
            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(),
                    List.of(
                            new MergingTablet(
                                    List.of(201L, 202L, 203L, 204L),
                                    2L),
                            new MergingTablet(
                                    List.of(301L, 302L),
                                    3L)),
                    List.of(
                            new IdenticalTablet(101, 101L),
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(103, 103L),
                            new IdenticalTablet(104, 104L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 2L, 3L, 4L, 5L,
                    102L, 2L, 3L, 4L, 5L,
                    103L, 2L, 3L, 4L, 5L,
                    104L, 2L, 3L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(
                            List.of(101L, 201L, 301L, 4L, 5L,
                                    102L, 202L, 302L, 4L, 5L,
                                    103L, 203L, 301L, 4L, 5L,
                                    104L, 204L, 302L, 4L, 5L)));
        }

        {
            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(),
                    List.of(
                            new MergingTablet(
                                    List.of(201L, 202L, 203L, 204L),
                                    2L),
                            new MergingTablet(
                                    List.of(301L, 302L),
                                    3L)),
                    List.of(
                            new IdenticalTablet(101, 101L),
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(103, 103L),
                            new IdenticalTablet(104, 104L),
                            new IdenticalTablet(105, 105L),
                            new IdenticalTablet(106, 106L),
                            new IdenticalTablet(107, 107L),
                            new IdenticalTablet(108, 108L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 2L, 3L, 4L, 5L,
                    102L, 2L, 3L, 4L, 5L,
                    103L, 2L, 3L, 4L, 5L,
                    104L, 2L, 3L, 4L, 5L,
                    105L, 2L, 3L, 4L, 5L,
                    106L, 2L, 3L, 4L, 5L,
                    107L, 2L, 3L, 4L, 5L,
                    108L, 2L, 3L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(
                            List.of(101L, 201L, 301L, 4L, 5L,
                                    102L, 202L, 302L, 4L, 5L,
                                    103L, 203L, 301L, 4L, 5L,
                                    104L, 204L, 302L, 4L, 5L,
                                    105L, 201L, 301L, 4L, 5L,
                                    106L, 202L, 302L, 4L, 5L,
                                    107L, 203L, 301L, 4L, 5L,
                                    108L, 204L, 302L, 4L, 5L)));
        }

        {
            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(),
                    List.of(
                            new MergingTablet(
                                    List.of(10101L, 10102L, 10103L, 10104L),
                                    101L),
                            new MergingTablet(
                                    List.of(201L, 202L, 203L, 204L),
                                    2L)),
                    List.of(
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(3, 3L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 2L, 3L, 4L, 5L,
                    102L, 2L, 3L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(List.of(
                            10101L, 201L, 3L, 4L, 5L,
                            102L, 202L, 3L, 4L, 5L,
                            10102L, 203L, 3L, 4L, 5L,
                            102L, 204L, 3L, 4L, 5L,
                            10103L, 201L, 3L, 4L, 5L,
                            102L, 202L, 3L, 4L, 5L,
                            10104L, 203L, 3L, 4L, 5L,
                            102L, 204L, 3L, 4L, 5L)));
        }

        {
            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(),
                    List.of(
                            new MergingTablet(
                                    List.of(10101L, 10102L, 10103L, 10104L),
                                    101L),
                            new MergingTablet(
                                    List.of(201L, 202L, 203L, 204L),
                                    2L)),
                    List.of(
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(301, 301L),
                            new IdenticalTablet(302, 302L),
                            new IdenticalTablet(303, 303L),
                            new IdenticalTablet(304, 304L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 2L, 301L, 4L, 5L,
                    102L, 2L, 302L, 4L, 5L,
                    101L, 2L, 303L, 4L, 5L,
                    102L, 2L, 304L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(List.of(
                            10101L, 201L, 301L, 4L, 5L,
                            102L, 202L, 302L, 4L, 5L,
                            10102L, 203L, 303L, 4L, 5L,
                            102L, 204L, 304L, 4L, 5L,
                            10103L, 201L, 301L, 4L, 5L,
                            102L, 202L, 302L, 4L, 5L,
                            10104L, 203L, 303L, 4L, 5L,
                            102L, 204L, 304L, 4L, 5L)));
        }

        {
            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(),
                    List.of(
                            new MergingTablet(
                                    List.of(10101L, 10102L, 10103L, 10104L),
                                    101L),
                            new MergingTablet(
                                    List.of(201L, 202L, 203L, 204L),
                                    2L)),
                    List.of(
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(301, 301L),
                            new IdenticalTablet(302, 302L),
                            new IdenticalTablet(303, 303L),
                            new IdenticalTablet(304, 304L),
                            new IdenticalTablet(305, 305L),
                            new IdenticalTablet(306, 306L),
                            new IdenticalTablet(307, 307L),
                            new IdenticalTablet(308, 308L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 2L, 301L, 4L, 5L,
                    102L, 2L, 302L, 4L, 5L,
                    101L, 2L, 303L, 4L, 5L,
                    102L, 2L, 304L, 4L, 5L,
                    101L, 2L, 305L, 4L, 5L,
                    102L, 2L, 306L, 4L, 5L,
                    101L, 2L, 307L, 4L, 5L,
                    102L, 2L, 308L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(List.of(
                            10101L, 201L, 301L, 4L, 5L,
                            102L, 202L, 302L, 4L, 5L,
                            10102L, 203L, 303L, 4L, 5L,
                            102L, 204L, 304L, 4L, 5L,
                            10103L, 201L, 305L, 4L, 5L,
                            102L, 202L, 306L, 4L, 5L,
                            10104L, 203L, 307L, 4L, 5L,
                            102L, 204L, 308L, 4L, 5L)));
        }

        {
            DynamicTablets mergingTablets = new DynamicTablets(
                    Collections.emptyList(),
                    List.of(
                            new MergingTablet(
                                    List.of(10101L, 10102L, 10103L, 10104L),
                                    101L),
                            new MergingTablet(
                                    List.of(201L, 202L, 203L, 204L),
                                    2L)),
                    List.of(
                            new IdenticalTablet(102, 102L),
                            new IdenticalTablet(301, 301L),
                            new IdenticalTablet(302, 302L),
                            new IdenticalTablet(303, 303L),
                            new IdenticalTablet(304, 304L),
                            new IdenticalTablet(305, 305L),
                            new IdenticalTablet(306, 306L),
                            new IdenticalTablet(307, 307L),
                            new IdenticalTablet(308, 308L),
                            new IdenticalTablet(309, 309L),
                            new IdenticalTablet(310, 310L),
                            new IdenticalTablet(311, 311L),
                            new IdenticalTablet(312, 312L),
                            new IdenticalTablet(313, 313L),
                            new IdenticalTablet(314, 314L),
                            new IdenticalTablet(315, 315L),
                            new IdenticalTablet(316, 316L),
                            new IdenticalTablet(4, 4L),
                            new IdenticalTablet(5, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 2L, 301L, 4L, 5L,
                    102L, 2L, 302L, 4L, 5L,
                    101L, 2L, 303L, 4L, 5L,
                    102L, 2L, 304L, 4L, 5L,
                    101L, 2L, 305L, 4L, 5L,
                    102L, 2L, 306L, 4L, 5L,
                    101L, 2L, 307L, 4L, 5L,
                    102L, 2L, 308L, 4L, 5L,
                    101L, 2L, 309L, 4L, 5L,
                    102L, 2L, 310L, 4L, 5L,
                    101L, 2L, 311L, 4L, 5L,
                    102L, 2L, 312L, 4L, 5L,
                    101L, 2L, 313L, 4L, 5L,
                    102L, 2L, 314L, 4L, 5L,
                    101L, 2L, 315L, 4L, 5L,
                    102L, 2L, 316L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(List.of(
                            10101L, 201L, 301L, 4L, 5L,
                            102L, 202L, 302L, 4L, 5L,
                            10102L, 203L, 303L, 4L, 5L,
                            102L, 204L, 304L, 4L, 5L,
                            10103L, 201L, 305L, 4L, 5L,
                            102L, 202L, 306L, 4L, 5L,
                            10104L, 203L, 307L, 4L, 5L,
                            102L, 204L, 308L, 4L, 5L,
                            10101L, 201L, 309L, 4L, 5L,
                            102L, 202L, 310L, 4L, 5L,
                            10102L, 203L, 311L, 4L, 5L,
                            102L, 204L, 312L, 4L, 5L,
                            10103L, 201L, 313L, 4L, 5L,
                            102L, 202L, 314L, 4L, 5L,
                            10104L, 203L, 315L, 4L, 5L,
                            102L, 204L, 316L, 4L, 5L)));
        }
    }
}
