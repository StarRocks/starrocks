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
import java.util.Map;

public class SplittingTabletsTest {
    @Test
    public void testCalcNewVirtualBuckets() {
        {
            SplittingTablets splittingTablets = new SplittingTablets(Collections.emptyMap());
            Assertions.assertTrue(splittingTablets.isEmpty());
            Assertions.assertTrue(splittingTablets.getSplittingTablets().isEmpty());
            Assertions.assertNull(splittingTablets.getMergingTablets());
        }

        {
            SplittingTablets splittingTablets = new SplittingTablets(Map.of(
                    1L, new SplittingTablet(1L,
                            List.of(new LocalTablet(101),
                                    new LocalTablet(102))),
                    2L, new SplittingTablet(2L,
                            List.of(new LocalTablet(201),
                                    new LocalTablet(202)))));

            Assertions.assertFalse(splittingTablets.isEmpty());

            Assertions.assertEquals(List.of(
                    101L, 201L, 3L, 4L, 5L,
                    102L, 202L, 3L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(List.of(1L, 2L, 3L, 4L, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 201L, 301L, 4L, 5L,
                    102L, 202L, 302L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(
                            List.of(1L, 2L, 301L, 4L, 5L,
                                    1L, 2L, 302L, 4L, 5L)));

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
            SplittingTablets splittingTablets = new SplittingTablets(Map.of(
                    2L, new SplittingTablet(2L,
                            List.of(new LocalTablet(201),
                                    new LocalTablet(202),
                                    new LocalTablet(203),
                                    new LocalTablet(204))),
                    3L, new SplittingTablet(3L,
                            List.of(new LocalTablet(301),
                                    new LocalTablet(302)))));

            Assertions.assertEquals(List.of(
                    1L, 201L, 301L, 4L, 5L,
                    1L, 202L, 302L, 4L, 5L,
                    1L, 203L, 301L, 4L, 5L,
                    1L, 204L, 302L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(List.of(1L, 2L, 3L, 4L, 5L)));

            Assertions.assertEquals(List.of(
                    101L, 201L, 301L, 4L, 5L,
                    102L, 202L, 302L, 4L, 5L,
                    101L, 203L, 301L, 4L, 5L,
                    102L, 204L, 302L, 4L, 5L),
                    splittingTablets.calcNewVirtualBuckets(
                            List.of(101L, 2L, 3L, 4L, 5L,
                                    102L, 2L, 3L, 4L, 5L)));

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
            SplittingTablets splittingTablets = new SplittingTablets(Map.of(
                    101L, new SplittingTablet(101L,
                            List.of(new LocalTablet(10101),
                                    new LocalTablet(10102),
                                    new LocalTablet(10103),
                                    new LocalTablet(10104))),
                    2L, new SplittingTablet(2L,
                            List.of(new LocalTablet(201),
                                    new LocalTablet(202),
                                    new LocalTablet(203),
                                    new LocalTablet(204)))));

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
}
