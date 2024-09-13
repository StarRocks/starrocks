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

package com.starrocks.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starrocks.sql.common.PListCell;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class PListCellTest {
    @Test
    public void testSerializeDeserialize() {
        // one value with one partition column
        {
            PListCell s1 = new PListCell(ImmutableList.of(ImmutableList.of("2024-01-01")));
            String ser = s1.serialize();
            System.out.println(ser);
            PListCell s2 = PListCell.deserialize(ser);
            Assert.assertEquals(s1, s2);
        }
        // one value with multi partition columns
        {
            PListCell s1 = new PListCell(ImmutableList.of(ImmutableList.of("beijing", "2024-01-01")));
            String ser = s1.serialize();
            System.out.println(ser);
            PListCell s2 = PListCell.deserialize(ser);
            Assert.assertEquals(s1, s2);
        }
        // multi values with multi partition columns
        {
            PListCell s1 = new PListCell(ImmutableList.of(
                    ImmutableList.of("beijing", "2024-01-01"),
                    ImmutableList.of("shanghai", "2024-01-02")
            ));
            String ser = s1.serialize();
            System.out.println(ser);
            PListCell s2 = PListCell.deserialize(ser);
            Assert.assertEquals(s1, s2);
        }
    }

    @Test
    public void testBatchSerializeDeserialize() {
        // one value with one partition column
        {
            Set<PListCell> s1 =
                    ImmutableSet.of(
                            new PListCell(ImmutableList.of(ImmutableList.of("2024-01-01"))),
                            new PListCell(ImmutableList.of(ImmutableList.of("2024-01-02")))
                    );
            String ser = PListCell.batchSerialize(s1);
            System.out.println(ser);
            Set<PListCell> s2 = PListCell.batchDeserialize(ser);
            Assert.assertEquals(s1, s2);
        }
        // one value with multi partition columns
        {
            Set<PListCell> s1 =
                    ImmutableSet.of(
                            new PListCell(ImmutableList.of(ImmutableList.of("beijing", "2024-01-01"))),
                            new PListCell(ImmutableList.of(ImmutableList.of("beijing", "2024-01-02")))
                    );
            String ser = PListCell.batchSerialize(s1);
            System.out.println(ser);
            Set<PListCell> s2 = PListCell.batchDeserialize(ser);
            Assert.assertEquals(s1, s2);
        }
        // multi values with multi partition columns
        {
            PListCell s1 = new PListCell(ImmutableList.of(
                    ImmutableList.of("beijing", "2024-01-01"),
                    ImmutableList.of("shanghai", "2024-01-02")
            ));
            String ser = s1.serialize();
            System.out.println(ser);
            PListCell s2 = PListCell.deserialize(ser);
            Assert.assertEquals(s1, s2);
        }
        {
            Set<PListCell> s1 =
                    ImmutableSet.of(
                            new PListCell(ImmutableList.of(
                                    ImmutableList.of("beijing", "2024-01-01"),
                                    ImmutableList.of("shanghai", "2024-01-02")
                            )),
                            new PListCell(ImmutableList.of(
                                    ImmutableList.of("beijing", "2024-01-03"),
                                    ImmutableList.of("shanghai", "2024-01-04")
                            ))
                    );
            String ser = PListCell.batchSerialize(s1);
            System.out.println(ser);
            Set<PListCell> s2 = PListCell.batchDeserialize(ser);
            Assert.assertEquals(s1, s2);
        }
    }

    @Test
    public void testPListCellCompare() {
        {
            PListCell c1 = new PListCell(ImmutableList.of(ImmutableList.of("2024-01-01")));
            PListCell c2 = new PListCell(ImmutableList.of(ImmutableList.of("2024-01-01")));
            Assert.assertEquals(0, c1.compareTo(c2));
        }
        {
            PListCell c1 = new PListCell(ImmutableList.of(ImmutableList.of("2024-01-01")));
            PListCell c2 = new PListCell(ImmutableList.of(ImmutableList.of("2024-01-02")));
            Assert.assertEquals(-1, c1.compareTo(c2));
        }
        {
            PListCell c1 = new PListCell(ImmutableList.of(
                    ImmutableList.of("beijing", "2024-01-01"),
                    ImmutableList.of("shanghai", "2024-01-02")
            ));
            PListCell c2 = new PListCell(ImmutableList.of(
                    ImmutableList.of("beijing", "2024-01-01"),
                    ImmutableList.of("shanghai", "2024-01-02")
            ));
            Assert.assertEquals(0, c1.compareTo(c2));
        }
        {
            PListCell c1 = new PListCell(ImmutableList.of(
                    ImmutableList.of("beijing", "2024-01-01"),
                    ImmutableList.of("shanghai", "2024-01-02")
            ));
            PListCell c2 = new PListCell(ImmutableList.of(
                    ImmutableList.of("beijing", "2024-01-03"),
                    ImmutableList.of("shanghai", "2024-01-04")
            ));
            Assert.assertEquals(-2, c1.compareTo(c2));
        }
    }
}
