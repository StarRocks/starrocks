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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Range;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class RangeDistributionPrunerTest {

    @Test
    public void testSingleColumnEqualityPrune() {
        Column k1 = new Column("k1", IntegerType.INT, false);

        List<Tablet> tablets = Lists.newArrayList();
        tablets.add(createTablet(1L, "0", "9", k1));
        tablets.add(createTablet(2L, "10", "19", k1));
        tablets.add(createTablet(3L, "20", "29", k1));

        PartitionColumnFilter filter = new PartitionColumnFilter();
        filter.setLowerBound(new StringLiteral("15"), true);
        filter.setUpperBound(new StringLiteral("15"), true);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("k1", filter);

        RangeDistributionPruner pruner =
                new RangeDistributionPruner(tablets, Lists.newArrayList(k1), filters);

        Collection<Long> result = pruner.prune();
        Assertions.assertEquals(Collections.singleton(2L), new HashSet<>(result));
    }

    @Test
    public void testNoFilterReturnsAllTablets() {
        Column k1 = new Column("k1", IntegerType.INT, false);

        List<Tablet> tablets = Lists.newArrayList();
        tablets.add(createTablet(1L, "0", "9", k1));
        tablets.add(createTablet(2L, "10", "19", k1));
        tablets.add(createTablet(3L, "20", "29", k1));

        RangeDistributionPruner pruner =
                new RangeDistributionPruner(tablets, Lists.newArrayList(k1), Maps.newHashMap());

        Collection<Long> result = pruner.prune();
        Assertions.assertEquals(
                new HashSet<>(Lists.newArrayList(1L, 2L, 3L)),
                new HashSet<>(result));
    }

    @Test
    public void testInPredicatePrune() {
        Column k1 = new Column("k1", IntegerType.INT, false);

        List<Tablet> tablets = Lists.newArrayList();
        tablets.add(createTablet(1L, "0", "9", k1));
        tablets.add(createTablet(2L, "10", "19", k1));
        tablets.add(createTablet(3L, "20", "29", k1));

        PartitionColumnFilter filter = new PartitionColumnFilter();
        List<LiteralExpr> inValues = Lists.newArrayList();
        inValues.add(new StringLiteral("5"));   // tablet 1
        inValues.add(new StringLiteral("25"));  // tablet 3
        filter.setInPredicateLiterals(inValues);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("k1", filter);

        RangeDistributionPruner pruner =
                new RangeDistributionPruner(tablets, Lists.newArrayList(k1), filters);

        Collection<Long> result = pruner.prune();
        Assertions.assertEquals(
                new HashSet<>(Lists.newArrayList(1L, 3L)),
                new HashSet<>(result));
    }

    private Tablet createTablet(long id, String lower, String upper, Column column) {
        LocalTablet tablet = new LocalTablet(id);
        List<Variant> lowerValues = Lists.newArrayList(Variant.of(column.getType(), lower));
        List<Variant> upperValues = Lists.newArrayList(Variant.of(column.getType(), upper));
        Tuple lowerTuple = new Tuple(lowerValues);
        Tuple upperTuple = new Tuple(upperValues);
        tablet.setRange(Range.of(lowerTuple, upperTuple, true, true));
        return tablet;
    }
}


