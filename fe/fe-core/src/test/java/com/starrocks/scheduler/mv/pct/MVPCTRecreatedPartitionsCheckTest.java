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

import com.google.common.collect.Range;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.TableProperty;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.mv.MVRefreshParams;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MVPCTRecreatedPartitionsCheckTest {
    private static final LocalDate FIRST_DAY = LocalDate.of(2024, 1, 1);

    @Test
    public void testRecreatedCheckLooksUpCoveringAddsWithoutScanningAll() throws Exception {
        int days = 300;
        PCellSortedSet deletes = PCellSortedSet.of();
        PCellSortedSet adds = PCellSortedSet.of();
        Set<String> expected = new HashSet<>();
        for (int day = 0; day < days; day++) {
            deletes.add("p_old_" + day, days(day, day + 1));
            if (day < days / 2) {
                adds.add("p_new_" + day, days(day, day + 1));
                expected.add("p_old_" + day);
            }
        }
        for (int day = days; day < days + days / 2; day++) {
            adds.add("p_new_" + day, days(day, day + 1));
        }

        AtomicInteger intersectionChecks = new AtomicInteger();
        new MockUp<PRangeCell>() {
            @Mock
            public boolean isIntersected(Invocation invocation, PCell other) {
                intersectionChecks.incrementAndGet();
                return invocation.proceed();
            }
        };

        Assertions.assertEquals(expected, recreated(deletes, adds));
        int bound = 4 * (deletes.size() + adds.size());
        Assertions.assertTrue(intersectionChecks.get() <= bound,
                "checked " + intersectionChecks.get() + " pairs, more than " + bound);
    }

    @Test
    public void testDroppedPartitionsCoveredByCoarserAddAreRecreated() throws Exception {
        PCellSortedSet deletes = PCellSortedSet.of();
        for (int day = 0; day < 10; day++) {
            deletes.add("p_day_" + day, days(day, day + 1));
        }
        PCellSortedSet adds = PCellSortedSet.of();
        adds.add("p_first_five_days", days(0, 5));

        Assertions.assertEquals(Set.of("p_day_0", "p_day_1", "p_day_2", "p_day_3", "p_day_4"),
                recreated(deletes, adds));
    }

    @Test
    public void testIntegerKeysOfDifferentWidthsMeet() {
        PCellSortedSet deletes = PCellSortedSet.of();
        for (int value = 0; value < 10; value++) {
            deletes.add("p_bigint_" + value, integers(PrimitiveType.BIGINT, value, value + 1));
        }
        PCellSortedSet adds = PCellSortedSet.of();
        adds.add("p_int_first_five", integers(PrimitiveType.INT, 0, 5));

        Assertions.assertEquals(Set.of("p_bigint_0", "p_bigint_1", "p_bigint_2", "p_bigint_3", "p_bigint_4"),
                recreated(deletes, adds));
    }

    @Test
    public void testNothingIsRecreatedWithoutAdds() throws Exception {
        PCellSortedSet deletes = PCellSortedSet.of();
        deletes.add("p_day_0", days(0, 1));

        Assertions.assertEquals(Set.of(), recreated(deletes, PCellSortedSet.of()));
    }

    private static Set<String> recreated(PCellSortedSet deletes, PCellSortedSet adds) {
        MaterializedView mv = mock(MaterializedView.class);
        when(mv.getTableProperty()).thenReturn(mock(TableProperty.class));
        when(mv.getPartitionInfo()).thenReturn(mock(PartitionInfo.class));
        when(mv.isPartitionedTable()).thenReturn(true);
        MVPCTRefreshRangePartitioner partitioner = new MVPCTRefreshRangePartitioner(mock(MvTaskRunContext.class),
                null, null, mv, new MVRefreshParams(mv, new HashMap<>()));
        Set<String> recreated = new HashSet<>();
        partitioner.setRecreatedPartitionsCheck(partitions -> recreated.addAll(partitions.getPartitionNames()));
        partitioner.checkRecreatedPartitions(deletes, adds);
        return recreated;
    }

    private static PRangeCell integers(PrimitiveType type, long from, long to) {
        return PRangeCell.of(Range.closedOpen(integerKey(type, from), integerKey(type, to)));
    }

    private static PartitionKey integerKey(PrimitiveType type, long value) {
        PartitionKey key = new PartitionKey();
        key.pushColumn(new IntLiteral(value, TypeFactory.createType(type)), type);
        return key;
    }

    private static PRangeCell days(int fromDay, int toDay) throws Exception {
        return PRangeCell.of(Range.closedOpen(PartitionKey.ofDate(FIRST_DAY.plusDays(fromDay)),
                PartitionKey.ofDate(FIRST_DAY.plusDays(toDay))));
    }
}
