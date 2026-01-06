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
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    @Test
    public void testTwoColumnsDateAndIntWithInAndRange() {
        Column dateCol = new Column("d", DateType.DATE, false);
        Column intCol = new Column("k", IntegerType.INT, false);
        List<Column> columns = Lists.newArrayList(dateCol, intCol);

        List<Tablet> tablets = Lists.newArrayList();
        // d = 2024-01-01/02/03/04, k in [0, 99]
        tablets.add(createTablet(1L, Lists.newArrayList("2024-01-01", "0"),
                Lists.newArrayList("2024-01-01", "99"), columns));
        tablets.add(createTablet(2L, Lists.newArrayList("2024-01-02", "0"),
                Lists.newArrayList("2024-01-02", "99"), columns));
        tablets.add(createTablet(3L, Lists.newArrayList("2024-01-03", "0"),
                Lists.newArrayList("2024-01-03", "99"), columns));
        tablets.add(createTablet(4L, Lists.newArrayList("2024-01-04", "0"),
                Lists.newArrayList("2024-01-04", "99"), columns));

        // d in ('2024-01-02', '2024-01-03')
        PartitionColumnFilter dFilter = new PartitionColumnFilter();
        List<LiteralExpr> dInValues = Lists.newArrayList();
        dInValues.add(new StringLiteral("2024-01-02"));
        dInValues.add(new StringLiteral("2024-01-03"));
        dFilter.setInPredicateLiterals(dInValues);

        // and k between 50 and 150
        PartitionColumnFilter kFilter = new PartitionColumnFilter();
        kFilter.setLowerBound(new StringLiteral("50"), true);
        kFilter.setUpperBound(new StringLiteral("150"), true);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("d", dFilter);
        filters.put("k", kFilter);

        RangeDistributionPruner pruner =
                new RangeDistributionPruner(tablets, columns, filters);

        Set<Long> result = pruner.prune().stream().collect(Collectors.toSet());
        Assertions.assertEquals(
                new HashSet<>(Lists.newArrayList(2L, 3L)),
                result);
    }

    @Test
    public void testTwoColumnsStringAndBooleanInPredicate() {
        Column flagCol = new Column("flag", BooleanType.BOOLEAN, false);
        Column nameCol = new Column("name", VarcharType.VARCHAR, false);
        List<Column> columns = Lists.newArrayList(flagCol, nameCol);

        List<Tablet> tablets = Lists.newArrayList();
        // tablet 1: flag = false, name in ['a', 'z']
        tablets.add(createTablet(1L, Lists.newArrayList("false", "a"),
                Lists.newArrayList("false", "z"), columns));
        // tablet 2: flag = true, name in ['a', 'z']
        tablets.add(createTablet(2L, Lists.newArrayList("true", "a"),
                Lists.newArrayList("true", "z"), columns));

        // flag in (true)
        PartitionColumnFilter flagFilter = new PartitionColumnFilter();
        List<LiteralExpr> flagInValues = Lists.newArrayList();
        flagInValues.add(new StringLiteral("true"));
        flagFilter.setInPredicateLiterals(flagInValues);

        // name in ('p', 'q')
        PartitionColumnFilter nameFilter = new PartitionColumnFilter();
        List<LiteralExpr> nameInValues = Lists.newArrayList();
        nameInValues.add(new StringLiteral("p"));
        nameInValues.add(new StringLiteral("q"));
        nameFilter.setInPredicateLiterals(nameInValues);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("flag", flagFilter);
        filters.put("name", nameFilter);

        RangeDistributionPruner pruner =
                new RangeDistributionPruner(tablets, columns, filters);

        Set<Long> result = pruner.prune().stream().collect(Collectors.toSet());
        Assertions.assertEquals(Collections.singleton(2L), result);
    }

    @Test
    public void testPrefixFilterStopsOnMissingSecondColumnFilter() {
        Column dateCol = new Column("d", DateType.DATE, false);
        Column intCol = new Column("k", IntegerType.INT, false);
        List<Column> columns = Lists.newArrayList(dateCol, intCol);

        List<Tablet> tablets = Lists.newArrayList();
        tablets.add(createTablet(1L, Lists.newArrayList("2024-01-01", "0"),
                Lists.newArrayList("2024-01-01", "99"), columns));
        tablets.add(createTablet(2L, Lists.newArrayList("2024-01-02", "0"),
                Lists.newArrayList("2024-01-02", "99"), columns));
        tablets.add(createTablet(3L, Lists.newArrayList("2024-01-03", "0"),
                Lists.newArrayList("2024-01-03", "99"), columns));
        tablets.add(createTablet(4L, Lists.newArrayList("2024-01-04", "0"),
                Lists.newArrayList("2024-01-04", "99"), columns));

        // Only filter on first column d between '2024-01-02' and '2024-01-03'
        PartitionColumnFilter dFilter = new PartitionColumnFilter();
        dFilter.setLowerBound(new StringLiteral("2024-01-02"), true);
        dFilter.setUpperBound(new StringLiteral("2024-01-03"), true);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("d", dFilter);

        RangeDistributionPruner pruner =
                new RangeDistributionPruner(tablets, columns, filters);

        Set<Long> result = pruner.prune().stream().collect(Collectors.toSet());
        Assertions.assertEquals(
                new HashSet<>(Lists.newArrayList(2L, 3L)),
                result);
    }

    @Test
    public void testSingleColumnDatetimeLowerAndUpperBound() {
        Column dt = new Column("dt", DateType.DATETIME, false);

        List<Tablet> tablets = Lists.newArrayList();
        tablets.add(createTablet(1L, "2024-01-01 00:00:00",
                "2024-01-01 23:59:59.999999", dt));
        tablets.add(createTablet(2L, "2024-01-02 00:00:00",
                "2024-01-02 23:59:59.999999", dt));
        tablets.add(createTablet(3L, "2024-01-03 00:00:00",
                "2024-01-03 23:59:59.999999", dt));

        // dt >= '2024-01-02'
        PartitionColumnFilter lowerOnly = new PartitionColumnFilter();
        lowerOnly.setLowerBound(new StringLiteral("2024-01-02 00:00:00"), true);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("dt", lowerOnly);

        RangeDistributionPruner lowerPruner =
                new RangeDistributionPruner(tablets, Lists.newArrayList(dt), filters);
        Set<Long> lowerResult = lowerPruner.prune().stream().collect(Collectors.toSet());
        Assertions.assertEquals(
                new HashSet<>(Lists.newArrayList(2L, 3L)),
                lowerResult);

        // dt <= '2024-01-02 23:59:59.999999'
        PartitionColumnFilter upperOnly = new PartitionColumnFilter();
        upperOnly.setUpperBound(new StringLiteral("2024-01-02 23:59:59.999999"), true);

        Map<String, PartitionColumnFilter> filters2 = Maps.newHashMap();
        filters2.put("dt", upperOnly);

        RangeDistributionPruner upperPruner =
                new RangeDistributionPruner(tablets, Lists.newArrayList(dt), filters2);
        Set<Long> upperResult = upperPruner.prune().stream().collect(Collectors.toSet());
        Assertions.assertEquals(
                new HashSet<>(Lists.newArrayList(1L, 2L)),
                upperResult);
    }

    @Test
    public void testEarlyStopNoPruningWhenComplexTooLarge() {
        Column x = new Column("x", IntegerType.INT, false);
        Column y = new Column("y", IntegerType.INT, false);
        List<Column> columns = Lists.newArrayList(x, y);

        // tablets cover x in {0,1,2}, y in {0,1}
        List<Tablet> tablets = Lists.newArrayList();
        tablets.add(createTablet(1L, Lists.newArrayList("0", "0"),
                Lists.newArrayList("0", "0"), columns));
        tablets.add(createTablet(2L, Lists.newArrayList("0", "1"),
                Lists.newArrayList("0", "1"), columns));
        tablets.add(createTablet(3L, Lists.newArrayList("1", "0"),
                Lists.newArrayList("1", "0"), columns));
        tablets.add(createTablet(4L, Lists.newArrayList("1", "1"),
                Lists.newArrayList("1", "1"), columns));
        tablets.add(createTablet(5L, Lists.newArrayList("2", "0"),
                Lists.newArrayList("2", "0"), columns));
        tablets.add(createTablet(6L, Lists.newArrayList("2", "1"),
                Lists.newArrayList("2", "1"), columns));

        PartitionColumnFilter xFilter = new PartitionColumnFilter();
        List<LiteralExpr> xInValues = Lists.newArrayList();
        xInValues.add(new StringLiteral("0"));
        xInValues.add(new StringLiteral("1"));
        xInValues.add(new StringLiteral("2"));
        xInValues.add(new StringLiteral("3"));
        xInValues.add(new StringLiteral("4"));
        xFilter.setInPredicateLiterals(xInValues);

        PartitionColumnFilter yFilter = new PartitionColumnFilter();
        List<LiteralExpr> yInValues = Lists.newArrayList();
        yInValues.add(new StringLiteral("0"));
        yInValues.add(new StringLiteral("1"));
        yInValues.add(new StringLiteral("2"));
        yInValues.add(new StringLiteral("3"));
        yInValues.add(new StringLiteral("4"));
        yFilter.setInPredicateLiterals(yInValues);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("x", xFilter);
        filters.put("y", yFilter);

        int oldDepth = Config.max_distribution_pruner_recursion_depth;
        try {
            // 5 (x) * 5 (y) = 25 > 3, so pruning should early-stop and return all tablets
            Config.max_distribution_pruner_recursion_depth = 3;
            RangeDistributionPruner pruner =
                    new RangeDistributionPruner(tablets, columns, filters);
            Set<Long> result = pruner.prune().stream().collect(Collectors.toSet());
            Assertions.assertEquals(
                    new HashSet<>(Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L)),
                    result);
        } finally {
            Config.max_distribution_pruner_recursion_depth = oldDepth;
        }
    }

    @Test
    public void testEarlyStopUsesPrefixColumnsOnly() {
        Column x = new Column("x", IntegerType.INT, false);
        Column y = new Column("y", IntegerType.INT, false);
        List<Column> columns = Lists.newArrayList(x, y);

        // tablets cover x in {0,1,2}, y in {0,1}
        List<Tablet> tablets = Lists.newArrayList();
        tablets.add(createTablet(1L, Lists.newArrayList("0", "0"),
                Lists.newArrayList("0", "0"), columns));
        tablets.add(createTablet(2L, Lists.newArrayList("0", "1"),
                Lists.newArrayList("0", "1"), columns));
        tablets.add(createTablet(3L, Lists.newArrayList("1", "0"),
                Lists.newArrayList("1", "0"), columns));
        tablets.add(createTablet(4L, Lists.newArrayList("1", "1"),
                Lists.newArrayList("1", "1"), columns));
        tablets.add(createTablet(5L, Lists.newArrayList("2", "0"),
                Lists.newArrayList("2", "0"), columns));
        tablets.add(createTablet(6L, Lists.newArrayList("2", "1"),
                Lists.newArrayList("2", "1"), columns));

        PartitionColumnFilter xFilter = new PartitionColumnFilter();
        List<LiteralExpr> xInValues = Lists.newArrayList();
        xInValues.add(new StringLiteral("0"));
        xInValues.add(new StringLiteral("1"));
        xFilter.setInPredicateLiterals(xInValues);

        PartitionColumnFilter yFilter = new PartitionColumnFilter();
        List<LiteralExpr> yInValues = Lists.newArrayList();
        yInValues.add(new StringLiteral("0"));
        yInValues.add(new StringLiteral("1"));
        yInValues.add(new StringLiteral("2"));
        yInValues.add(new StringLiteral("3"));
        yFilter.setInPredicateLiterals(yInValues);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("x", xFilter);
        filters.put("y", yFilter);

        int oldDepth = Config.max_distribution_pruner_recursion_depth;
        try {
            // 2 (x) * 4 (y) = 8 > 5, so only first column x is used for pruning
            Config.max_distribution_pruner_recursion_depth = 5;

            RangeDistributionPruner prunerWithEarlyStop =
                    new RangeDistributionPruner(tablets, columns, filters);
            Set<Long> resultWithEarlyStop =
                    prunerWithEarlyStop.prune().stream().collect(Collectors.toSet());

            Map<String, PartitionColumnFilter> prefixFilters = Maps.newHashMap();
            prefixFilters.put("x", xFilter);
            RangeDistributionPruner prefixOnlyPruner =
                    new RangeDistributionPruner(tablets, columns, prefixFilters);
            Set<Long> prefixResult =
                    prefixOnlyPruner.prune().stream().collect(Collectors.toSet());

            Assertions.assertEquals(prefixResult, resultWithEarlyStop);
        } finally {
            Config.max_distribution_pruner_recursion_depth = oldDepth;
        }
    }

    @Test
    public void testInclusiveAggregationPrunesBoundaryTabletWhenAnyColumnExclusive() {
        Column x = new Column("x", IntegerType.INT, false);
        Column y = new Column("y", IntegerType.INT, false);
        List<Column> columns = Lists.newArrayList(x, y);

        // Single tablet whose key range is exactly [(0,0), (0,0)]
        List<Tablet> tablets = Lists.newArrayList();
        tablets.add(createTablet(1L, Lists.newArrayList("0", "0"),
                Lists.newArrayList("0", "0"), columns));

        // Column x has open bounds (0,0) so both lower and upper are exclusive.
        PartitionColumnFilter xFilter = new PartitionColumnFilter();
        xFilter.setLowerBound(new StringLiteral("0"), false);
        xFilter.setUpperBound(new StringLiteral("0"), false);

        // Column y has closed bounds [0,0], so both lower and upper are inclusive.
        PartitionColumnFilter yFilter = new PartitionColumnFilter();
        yFilter.setLowerBound(new StringLiteral("0"), true);
        yFilter.setUpperBound(new StringLiteral("0"), true);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("x", xFilter);
        filters.put("y", yFilter);

        RangeDistributionPruner pruner =
                new RangeDistributionPruner(tablets, columns, filters);

        Set<Long> result = pruner.prune().stream().collect(Collectors.toSet());

        // Now we use allMatch for inclusive flag, so if any column is exclusive, the whole
        // tuple range boundary is exclusive. Since x is exclusive, the boundary tablet is pruned.
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testBinarySearchLowerAndUpperBothNull() {
        Column k1 = new Column("k1", IntegerType.INT, false);
        List<Tablet> tablets = Lists.newArrayList();
        tablets.add(createTablet(1L, "10", "20", k1));
        tablets.add(createTablet(2L, "21", "30", k1));

        // query range [-inf, +inf] should make lower and upper entries both null
        PartitionColumnFilter filter = new PartitionColumnFilter();
        // no bounds set => defaults to min/max
        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("k1", filter);

        RangeDistributionPruner pruner = new RangeDistributionPruner(tablets, Lists.newArrayList(k1), filters);
        Collection<Long> result = pruner.prune();
        Assertions.assertEquals(new HashSet<>(Lists.newArrayList(1L, 2L)), new HashSet<>(result));
    }

    @Test
    public void testBinarySearchLowerNullUpperExists() {
        Column k1 = new Column("k1", IntegerType.INT, false);
        List<Tablet> tablets = Lists.newArrayList();
        tablets.add(createTablet(1L, "10", "20", k1));
        tablets.add(createTablet(2L, "30", "40", k1));
        tablets.add(createTablet(3L, "50", "60", k1));

        // query range [5, 35]
        // lowerEntry (strictly < [5,35]) is null
        // higherEntry (strictly > [5,35]) is Tablet 3 [50, 60]
        // overlap should be {1, 2}
        PartitionColumnFilter filter = new PartitionColumnFilter();
        filter.setLowerBound(new StringLiteral("5"), true);
        filter.setUpperBound(new StringLiteral("35"), true);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("k1", filter);

        RangeDistributionPruner pruner = new RangeDistributionPruner(tablets, Lists.newArrayList(k1), filters);
        Collection<Long> result = pruner.prune();
        Assertions.assertEquals(new HashSet<>(Lists.newArrayList(1L, 2L)), new HashSet<>(result));
    }

    @Test
    public void testBinarySearchLowerExistsUpperNull() {
        Column k1 = new Column("k1", IntegerType.INT, false);
        List<Tablet> tablets = Lists.newArrayList();
        tablets.add(createTablet(1L, "10", "20", k1));
        tablets.add(createTablet(2L, "30", "40", k1));
        tablets.add(createTablet(3L, "50", "60", k1));

        // query range [35, 100]
        // lowerEntry (strictly < [35,100]) is Tablet 1 [10, 20]
        // higherEntry (strictly > [35,100]) is null
        // overlap should be {2, 3}
        PartitionColumnFilter filter = new PartitionColumnFilter();
        filter.setLowerBound(new StringLiteral("35"), true);
        filter.setUpperBound(new StringLiteral("100"), true);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("k1", filter);

        RangeDistributionPruner pruner = new RangeDistributionPruner(tablets, Lists.newArrayList(k1), filters);
        Collection<Long> result = pruner.prune();
        Assertions.assertEquals(new HashSet<>(Lists.newArrayList(2L, 3L)), new HashSet<>(result));
    }

    @Test
    public void testBinarySearchLowerAndUpperBothExist() {
        Column k1 = new Column("k1", IntegerType.INT, false);
        List<Tablet> tablets = Lists.newArrayList();
        tablets.add(createTablet(1L, "10", "20", k1));
        tablets.add(createTablet(2L, "30", "40", k1));
        tablets.add(createTablet(3L, "50", "60", k1));
        tablets.add(createTablet(4L, "70", "80", k1));

        // query range [35, 55]
        // lowerEntry (< [35,55]) is Tablet 1 [10, 20]
        // higherEntry (> [35,55]) is Tablet 4 [70, 80]
        // subMap(T1, T4) should return {2, 3}
        PartitionColumnFilter filter = new PartitionColumnFilter();
        filter.setLowerBound(new StringLiteral("35"), true);
        filter.setUpperBound(new StringLiteral("55"), true);

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("k1", filter);

        RangeDistributionPruner pruner = new RangeDistributionPruner(tablets, Lists.newArrayList(k1), filters);
        Collection<Long> result = pruner.prune();
        Assertions.assertEquals(new HashSet<>(Lists.newArrayList(2L, 3L)), new HashSet<>(result));
    }

    @Test
    public void testConstructorThrowsWhenRangeIsNull() {
        Column k1 = new Column("k1", IntegerType.INT, false);
        LocalTablet tablet = new LocalTablet(1L);
        tablet.setRange(null); // Ensure range is null

        Assertions.assertThrows(IllegalStateException.class, () -> {
            new RangeDistributionPruner(Lists.newArrayList(tablet), Lists.newArrayList(k1), Maps.newHashMap());
        });
    }

    private Tablet createTablet(long id, String lower, String upper, Column column) {
        LocalTablet tablet = new LocalTablet(id);
        List<Variant> lowerValues = Lists.newArrayList(Variant.of(column.getType(), lower));
        List<Variant> upperValues = Lists.newArrayList(Variant.of(column.getType(), upper));
        Tuple lowerTuple = new Tuple(lowerValues);
        Tuple upperTuple = new Tuple(upperValues);
        tablet.setRange(new TabletRange(Range.of(lowerTuple, upperTuple, true, true)));
        return tablet;
    }

    private Tablet createTablet(long id, List<String> lower, List<String> upper, List<Column> columns) {
        LocalTablet tablet = new LocalTablet(id);
        List<Variant> lowerValues = Lists.newArrayList();
        List<Variant> upperValues = Lists.newArrayList();
        for (int i = 0; i < columns.size(); i++) {
            lowerValues.add(Variant.of(columns.get(i).getType(), lower.get(i)));
            upperValues.add(Variant.of(columns.get(i).getType(), upper.get(i)));
        }
        Tuple lowerTuple = new Tuple(lowerValues);
        Tuple upperTuple = new Tuple(upperValues);
        tablet.setRange(new TabletRange(Range.of(lowerTuple, upperTuple, true, true)));
        return tablet;
    }
}


