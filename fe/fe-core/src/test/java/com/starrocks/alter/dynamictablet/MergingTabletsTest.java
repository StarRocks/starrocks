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

import com.starrocks.catalog.LocalTablet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class MergingTabletsTest {
    @Test
    public void testCalcNewVirtualBuckets() {
        {
            MergingTablets mergingTablets = new MergingTablets(Collections.emptyList());
            Assertions.assertTrue(mergingTablets.isEmpty());
            Assertions.assertTrue(mergingTablets.getMergingTablets().isEmpty());
            Assertions.assertNull(mergingTablets.getSplittingTablets());
        }

        {
            MergingTablets mergingTablets = new MergingTablets(List.of(
                    new MergingTablet(
                            List.of(101L, 102L),
                            new LocalTablet(1)),
                    new MergingTablet(
                            List.of(201L, 202L),
                            new LocalTablet(2))));

            Assertions.assertFalse(mergingTablets.isEmpty());

            Assertions.assertEquals(List.of(1L, 2L, 3L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(List.of(
                            101L, 201L, 3L, 4L, 5L,
                            102L, 202L, 3L, 4L, 5L)));

            Assertions.assertEquals(List.of(
                    1L, 2L, 301L, 4L, 5L,
                    1L, 2L, 302L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(
                            List.of(101L, 201L, 301L, 4L, 5L,
                                    102L, 202L, 302L, 4L, 5L)));

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
            MergingTablets mergingTablets = new MergingTablets(List.of(
                    new MergingTablet(
                            List.of(201L, 202L, 203L, 204L),
                            new LocalTablet(2)),
                    new MergingTablet(
                            List.of(301L, 302L),
                            new LocalTablet(3))));

            Assertions.assertEquals(List.of(1L, 2L, 3L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(List.of(
                            1L, 201L, 301L, 4L, 5L,
                            1L, 202L, 302L, 4L, 5L,
                            1L, 203L, 301L, 4L, 5L,
                            1L, 204L, 302L, 4L, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 2L, 3L, 4L, 5L,
                    102L, 2L, 3L, 4L, 5L),
                    mergingTablets.calcNewVirtualBuckets(
                            List.of(101L, 201L, 301L, 4L, 5L,
                                    102L, 202L, 302L, 4L, 5L,
                                    101L, 203L, 301L, 4L, 5L,
                                    102L, 204L, 302L, 4L, 5L)));

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
            MergingTablets mergingTablets = new MergingTablets(List.of(
                    new MergingTablet(
                            List.of(10101L, 10102L, 10103L, 10104L),
                            new LocalTablet(101)),
                    new MergingTablet(
                            List.of(201L, 202L, 203L, 204L),
                            new LocalTablet(2))));

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
