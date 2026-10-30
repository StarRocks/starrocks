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

package com.starrocks.scheduler.mv.pct;

import com.starrocks.mv.pct.BaseToMVPartitionMapping;
import com.starrocks.sql.common.PCellNone;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class BaseToMVPartitionMappingTest {

    @Test
    public void testEmptyMapping() {
        PCellSortedSet cells = PCellSortedSet.of();
        cells.add(PCellWithName.of("p202401", new PCellNone()));
        BaseToMVPartitionMapping mapping = BaseToMVPartitionMapping.of(cells);

        Assertions.assertSame(cells, mapping.cells());
        Assertions.assertTrue(mapping.sourceNameMapping().isEmpty());
        Assertions.assertTrue(mapping.getSourceNames("p202401").isEmpty());
    }

    @Test
    public void testWithSourceNames() {
        PCellSortedSet cells = PCellSortedSet.of();
        cells.add(PCellWithName.of("p202401", new PCellNone()));
        Map<String, Set<String>> sourceMapping = Map.of(
                "p202401", Set.of("par_date=2024-01-01", "par_date=2024-01-02"));
        BaseToMVPartitionMapping mapping = BaseToMVPartitionMapping.of(cells, sourceMapping);

        Assertions.assertEquals(Set.of("par_date=2024-01-01", "par_date=2024-01-02"),
                mapping.getSourceNames("p202401"));
        Assertions.assertTrue(mapping.getSourceNames("nonexistent").isEmpty());
    }

    @Test
    public void testExtractCells() {
        PCellSortedSet cells1 = PCellSortedSet.of();
        cells1.add(PCellWithName.of("p1", new PCellNone()));
        PCellSortedSet cells2 = PCellSortedSet.of();
        cells2.add(PCellWithName.of("p2", new PCellNone()));

        Map<String, BaseToMVPartitionMapping> map = Map.of(
                "t1", BaseToMVPartitionMapping.of(cells1),
                "t2", BaseToMVPartitionMapping.of(cells2));

        Map<String, PCellSortedSet> extracted = BaseToMVPartitionMapping.extractCells(map);
        Assertions.assertSame(cells1, extracted.get("t1"));
        Assertions.assertSame(cells2, extracted.get("t2"));
    }
}
