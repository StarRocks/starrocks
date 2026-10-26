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

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.sql.common.PListCell;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;

public class PListCellTest {
    @Test
    public void testSerializeDeserialize() {
        // one value with one partition column
        {
            PListCell s1 = new PListCell(ImmutableList.of(ImmutableList.of("2024-01-01")));
            String ser = s1.serialize();
            PListCell s2 = PListCell.deserialize(ser);
            Assertions.assertEquals(s1, s2);
        }
        // one value with multi partition columns
        {
            PListCell s1 = new PListCell(ImmutableList.of(ImmutableList.of("beijing", "2024-01-01")));
            String ser = s1.serialize();
            PListCell s2 = PListCell.deserialize(ser);
            Assertions.assertEquals(s1, s2);
        }
        // multi values with multi partition columns
        {
            PListCell s1 = new PListCell(ImmutableList.of(
                    ImmutableList.of("beijing", "2024-01-01"),
                    ImmutableList.of("shanghai", "2024-01-02")
            ));
            String ser = s1.serialize();
            PListCell s2 = PListCell.deserialize(ser);
            Assertions.assertEquals(s1, s2);
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
            Set<PListCell> s2 = PListCell.batchDeserialize(ser);
            Assertions.assertEquals(s1, s2);
        }
        // one value with multi partition columns
        {
            Set<PListCell> s1 =
                    ImmutableSet.of(
                            new PListCell(ImmutableList.of(ImmutableList.of("beijing", "2024-01-01"))),
                            new PListCell(ImmutableList.of(ImmutableList.of("beijing", "2024-01-02")))
                    );
            String ser = PListCell.batchSerialize(s1);
            Set<PListCell> s2 = PListCell.batchDeserialize(ser);
            Assertions.assertEquals(s1, s2);
        }
        // multi values with multi partition columns
        {
            PListCell s1 = new PListCell(ImmutableList.of(
                    ImmutableList.of("beijing", "2024-01-01"),
                    ImmutableList.of("shanghai", "2024-01-02")
            ));
            String ser = s1.serialize();
            PListCell s2 = PListCell.deserialize(ser);
            Assertions.assertEquals(s1, s2);
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
            Set<PListCell> s2 = PListCell.batchDeserialize(ser);
            Assertions.assertEquals(s1, s2);
        }
    }

    @Test
    public void testPListCellCompare() {
        {
            PListCell c1 = new PListCell(ImmutableList.of(ImmutableList.of("2024-01-01")));
            PListCell c2 = new PListCell(ImmutableList.of(ImmutableList.of("2024-01-01")));
            Assertions.assertEquals(0, c1.compareTo(c2));
        }
        {
            PListCell c1 = new PListCell(ImmutableList.of(ImmutableList.of("2024-01-01")));
            PListCell c2 = new PListCell(ImmutableList.of(ImmutableList.of("2024-01-02")));
            Assertions.assertEquals(-1, c1.compareTo(c2));
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
            Assertions.assertEquals(0, c1.compareTo(c2));
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
            Assertions.assertEquals(-2, c1.compareTo(c2));
        }
    }

    @Test
    public void toStringReturnsEmptyStringForEmptyPartitionItems() {
        PListCell cell = new PListCell(ImmutableList.of());
        Assertions.assertEquals("", cell.toString());
    }

    @Test
    public void toStringHandlesPartitionItemsWithinMaxLength() {
        Config.max_mv_task_run_meta_message_values_length = 5;
        PListCell cell = new PListCell(ImmutableList.of(
                ImmutableList.of("item1"),
                ImmutableList.of("item2"),
                ImmutableList.of("item3")
        ));
        Assertions.assertEquals("(item1),(item2),(item3)", cell.toString());
    }

    @Test
    public void toStringHandlesPartitionItemsExceedingMaxLength() {
        Config.max_mv_task_run_meta_message_values_length = 4;
        PListCell cell = new PListCell(ImmutableList.of(
                ImmutableList.of("item1"),
                ImmutableList.of("item2"),
                ImmutableList.of("item3"),
                ImmutableList.of("item4"),
                ImmutableList.of("item5")
        ));
        Assertions.assertEquals("(item1),(item2),...,(item4),(item5)", cell.toString());
        Config.max_mv_task_run_meta_message_values_length = 8;
    }

    @Test
    public void toStringHandlesNullPartitionItemsGracefully() {
        PListCell cell = new PListCell(Lists.newArrayList());
        Assertions.assertEquals("", cell.toString());
    }

    private static Set<PListCell> tenantCells(int count) {
        Set<PListCell> cells = new LinkedHashSet<>();
        for (int i = 0; i < count; i++) {
            cells.add(new PListCell("tenant-" + i + ".com"));
        }
        return cells;
    }

    @Test
    public void testShrinkBatchForDisplayKeepsPrefixAndTotal() {
        String serialized = PListCell.batchSerialize(tenantCells(20));

        String shrunk = PListCell.shrinkBatchForDisplay(serialized, 8);

        JsonObject json = JsonParser.parseString(shrunk).getAsJsonObject();
        Assertions.assertEquals(20, json.get("total").getAsInt());
        Assertions.assertEquals(8, json.getAsJsonArray("data").size());
        Assertions.assertTrue(shrunk.contains("tenant-0.com"), shrunk);
        Assertions.assertFalse(shrunk.contains("tenant-8.com"), shrunk);
    }

    @Test
    public void testShrinkBatchForDisplayLeavesShortOrInvalidInput() {
        String serialized = PListCell.batchSerialize(tenantCells(8));
        Assertions.assertSame(serialized, PListCell.shrinkBatchForDisplay(serialized, 8));
        Assertions.assertNull(PListCell.shrinkBatchForDisplay(null, 8));
        Assertions.assertEquals("", PListCell.shrinkBatchForDisplay("", 8));
        Assertions.assertEquals("not json", PListCell.shrinkBatchForDisplay("not json", 8));

        String shrunk = PListCell.shrinkBatchForDisplay(PListCell.batchSerialize(tenantCells(20)), 8);
        Assertions.assertSame(shrunk, PListCell.shrinkBatchForDisplay(shrunk, 4));
    }

    @Test
    public void testShrinkBatchForDisplayKeepsAtLeastOneCell() {
        String shrunk = PListCell.shrinkBatchForDisplay(PListCell.batchSerialize(tenantCells(3)), 0);

        JsonObject json = JsonParser.parseString(shrunk).getAsJsonObject();
        Assertions.assertEquals(3, json.get("total").getAsInt());
        Assertions.assertEquals(1, json.getAsJsonArray("data").size());
    }

    @Test
    public void testBatchDeserializeRejectsShrunkValues() {
        String shrunk = PListCell.shrinkBatchForDisplay(PListCell.batchSerialize(tenantCells(20)), 8);

        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> PListCell.batchDeserialize(shrunk));
        Assertions.assertTrue(e.getMessage().contains("8 of 20"), e.getMessage());
    }
}
