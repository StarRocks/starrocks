// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.ListPartitionPruner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


public class ListPartitionPrunerTest {
    private Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap;
    private Map<ColumnRefOperator, Set<Long>> columnToNullPartitions;
    private List<ScalarOperator> conjuncts;
    private ColumnRefOperator dateColumn;
    private ColumnRefOperator intColumn;
    private ListPartitionPruner pruner;

    @Before
    public void setUp() throws AnalysisException {
        // date_col=2021-01-01/int_col=0   0
        // date_col=2021-01-01/int_col=1   1
        // date_col=2021-01-01/int_col=2   2
        // date_col=2021-01-02/int_col=0   3
        // date_col=2021-01-02/int_col=1   4
        // date_col=2021-01-02/int_col=2   5
        // date_col=2021-01-03/int_col=0   6
        // date_col=2021-01-03/int_col=1   7
        // date_col=2021-01-03/int_col=2   8
        // date_col=null/int_col=null      9
        dateColumn = new ColumnRefOperator(1, Type.DATE, "date_col", true);
        intColumn = new ColumnRefOperator(2, Type.INT, "int_col", true);

        // column -> partition values
        columnToPartitionValuesMap = Maps.newHashMap();
        TreeMap<LiteralExpr, Set<Long>> datePartitionValuesMap = Maps.newTreeMap();
        columnToPartitionValuesMap.put(dateColumn, datePartitionValuesMap);
        datePartitionValuesMap.put(new DateLiteral(2021, 1, 1), Sets.newHashSet(0L, 1L, 2L));
        datePartitionValuesMap.put(new DateLiteral(2021, 1, 2), Sets.newHashSet(3L, 4L, 5L));
        datePartitionValuesMap.put(new DateLiteral(2021, 1, 3), Sets.newHashSet(6L, 7L, 8L));
        TreeMap<LiteralExpr, Set<Long>> intPartitionValuesMap = Maps.newTreeMap();
        columnToPartitionValuesMap.put(intColumn, intPartitionValuesMap);
        intPartitionValuesMap.put(new IntLiteral(0, Type.INT), Sets.newHashSet(0L, 3L, 6L));
        intPartitionValuesMap.put(new IntLiteral(1, Type.INT), Sets.newHashSet(1L, 4L, 7L));
        intPartitionValuesMap.put(new IntLiteral(2, Type.INT), Sets.newHashSet(2L, 5L, 8L));

        // column -> null partitions
        columnToNullPartitions = Maps.newHashMap();
        columnToNullPartitions.put(dateColumn, Sets.newHashSet(9L));
        columnToNullPartitions.put(intColumn, Sets.newHashSet(9L));

        conjuncts = Lists.newArrayList();
        pruner = new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions, conjuncts);
    }

    @Test
    public void testEmptyConjunctsOrPartitionColumns() throws AnalysisException {
        conjuncts.clear();
        Assert.assertEquals(null, pruner.prune());
        columnToPartitionValuesMap.clear();
        columnToNullPartitions.clear();
        Assert.assertEquals(null, pruner.prune());
    }

    @Test
    public void testBinaryPredicate() throws AnalysisException {
        // date_col = "2021-01-01"
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, dateColumn,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 1, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(0L, 1L, 2L), pruner.prune());
        // date_col < "2021-01-02"
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, dateColumn,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 2, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(0L, 1L, 2L), pruner.prune());
        // date_col >= "2021-01-03" and date_col <= "2021-01-03"
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, dateColumn,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 3, 0, 0, 0))));
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, dateColumn,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 3, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(6L, 7L, 8L), pruner.prune());
        // date_col > "2021-01-03"
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT, dateColumn,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 3, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(), pruner.prune());
        // date_col <= "2020-12-31"
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, dateColumn,
                ConstantOperator.createDate(LocalDateTime.of(2020, 12, 31, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(), pruner.prune());

        // int_col = 0
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, intColumn,
                ConstantOperator.createInt(0)));
        Assert.assertEquals(Lists.newArrayList(0L, 3L, 6L), pruner.prune());
        // int_col >= 1
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, intColumn,
                ConstantOperator.createInt(1)));
        Assert.assertEquals(Lists.newArrayList(1L, 2L, 4L, 5L, 7L, 8L), pruner.prune());
        // int_col < 0
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, intColumn,
                ConstantOperator.createInt(0)));
        Assert.assertEquals(Lists.newArrayList(), pruner.prune());
        // int_col >= 4
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, intColumn,
                ConstantOperator.createInt(4)));
        Assert.assertEquals(Lists.newArrayList(), pruner.prune());

        // date_col >= "2021-01-02" and int_col < 2
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, dateColumn,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 2, 0, 0, 0))));
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, intColumn,
                ConstantOperator.createInt(2)));
        Assert.assertEquals(Lists.newArrayList(3L, 4L, 6L, 7L), pruner.prune());
    }

    @Test
    public void testExternalTableBinaryPredicate() throws AnalysisException {
        // string_col=2021-01-01   1
        // string_col=2021-01-02   2
        // string_col=2021-01-03   3
        // string_col=2021-01-04   4

        ColumnRefOperator stringColumn = new ColumnRefOperator(1, Type.STRING, "string_col", true);

        // column -> partition values
        Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
        TreeMap<LiteralExpr, Set<Long>> stringPartitionValuesMap = Maps.newTreeMap();
        columnToPartitionValuesMap.put(stringColumn, stringPartitionValuesMap);
        stringPartitionValuesMap.put(new StringLiteral("2021-01-01"), Sets.newHashSet(1L));
        stringPartitionValuesMap.put(new StringLiteral("2021-01-02"), Sets.newHashSet(2L));
        stringPartitionValuesMap.put(new StringLiteral("2021-01-03"), Sets.newHashSet(3L));
        stringPartitionValuesMap.put(new StringLiteral("2021-01-04"), Sets.newHashSet(4L));

        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newHashMap();
        columnToNullPartitions.put(stringColumn, Sets.newHashSet(9L));

        List<ScalarOperator> conjuncts = Lists.newArrayList();
        ListPartitionPruner pruner = new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions, conjuncts);
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, stringColumn,
                ConstantOperator.createVarchar("2021-01-02")));
        Assert.assertEquals(Lists.newArrayList(2L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, stringColumn,
                ConstantOperator.createVarchar("2021-01-02")));
        Assert.assertEquals(Lists.newArrayList(2L, 3L, 4L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT,
                new CastOperator(Type.DATE, stringColumn),
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 3, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(1L, 2L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE,
                new CastOperator(Type.DATE, stringColumn),
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 4, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(4L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                new CastOperator(Type.DATE, stringColumn),
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 1, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(1L), pruner.prune());
    }

    @Test
    public void testExternalTableBinaryPredicate2() throws AnalysisException {
        // string_col=01   1
        // string_col=02   2
        // string_col=03   3
        // string_col=1   4
        // string_col=10   5
        // string_col=11   6
        // string_col=12   7
        // string_col=21   8

        ColumnRefOperator stringColumn = new ColumnRefOperator(1, Type.STRING, "string_col", true);

        // column -> partition values
        Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
        TreeMap<LiteralExpr, Set<Long>> stringPartitionValuesMap = Maps.newTreeMap();
        columnToPartitionValuesMap.put(stringColumn, stringPartitionValuesMap);
        stringPartitionValuesMap.put(new StringLiteral("01"), Sets.newHashSet(1L));
        stringPartitionValuesMap.put(new StringLiteral("02"), Sets.newHashSet(2L));
        stringPartitionValuesMap.put(new StringLiteral("03"), Sets.newHashSet(3L));
        stringPartitionValuesMap.put(new StringLiteral("1"), Sets.newHashSet(4L));
        stringPartitionValuesMap.put(new StringLiteral("10"), Sets.newHashSet(5L));
        stringPartitionValuesMap.put(new StringLiteral("11"), Sets.newHashSet(6L));
        stringPartitionValuesMap.put(new StringLiteral("12"), Sets.newHashSet(7L));
        stringPartitionValuesMap.put(new StringLiteral("21"), Sets.newHashSet(8L));

        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newHashMap();
        columnToNullPartitions.put(stringColumn, Sets.newHashSet(9L));

        List<ScalarOperator> conjuncts = Lists.newArrayList();
        ListPartitionPruner pruner = new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions, conjuncts);
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, stringColumn,
                ConstantOperator.createVarchar("01")));
        Assert.assertEquals(Lists.newArrayList(1L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, stringColumn,
                ConstantOperator.createVarchar("1")));
        Assert.assertEquals(Lists.newArrayList(4L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, stringColumn,
                ConstantOperator.createVarchar("03")));
        Assert.assertEquals(Lists.newArrayList(3L, 4L, 5L, 6L, 7L, 8L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, stringColumn,
                ConstantOperator.createVarchar("03")));
        Assert.assertEquals(Lists.newArrayList(1L, 2L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, new CastOperator(Type.INT, stringColumn),
                ConstantOperator.createInt(1)));
        Assert.assertEquals(Lists.newArrayList(1L, 4L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT, new CastOperator(Type.INT, stringColumn),
                ConstantOperator.createInt(3)));
        Assert.assertEquals(Lists.newArrayList(5L, 6L, 7L, 8L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, new CastOperator(Type.INT, stringColumn),
                ConstantOperator.createInt(3)));
        Assert.assertEquals(Lists.newArrayList(1L, 2L, 4L), pruner.prune());
    }

    @Test
    public void testExternalTableBinaryPredicate3() throws AnalysisException {
        // int_col=1   1
        // int_col=2   2
        // int_col=3   3
        // int_col=10   4
        // int_col=11   5
        // int_col=12   6
        // int_col=20   7
        // int_col=21   8

        ColumnRefOperator intColumn = new ColumnRefOperator(1, Type.INT, "int_col", true);

        // column -> partition values
        Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
        TreeMap<LiteralExpr, Set<Long>> intPartitionValuesMap = Maps.newTreeMap();
        columnToPartitionValuesMap.put(intColumn, intPartitionValuesMap);
        intPartitionValuesMap.put(new IntLiteral(1), Sets.newHashSet(1L));
        intPartitionValuesMap.put(new IntLiteral(2), Sets.newHashSet(2L));
        intPartitionValuesMap.put(new IntLiteral(3), Sets.newHashSet(3L));
        intPartitionValuesMap.put(new IntLiteral(10), Sets.newHashSet(4L));
        intPartitionValuesMap.put(new IntLiteral(11), Sets.newHashSet(5L));
        intPartitionValuesMap.put(new IntLiteral(12), Sets.newHashSet(6L));
        intPartitionValuesMap.put(new IntLiteral(20), Sets.newHashSet(7L));
        intPartitionValuesMap.put(new IntLiteral(21), Sets.newHashSet(8L));

        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newHashMap();
        columnToNullPartitions.put(intColumn, Sets.newHashSet(9L));

        List<ScalarOperator> conjuncts = Lists.newArrayList();
        ListPartitionPruner pruner = new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions, conjuncts);
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, intColumn,
                ConstantOperator.createInt(1)));
        Assert.assertEquals(Lists.newArrayList(1L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, intColumn,
                ConstantOperator.createInt(3)));
        Assert.assertEquals(Lists.newArrayList(3L, 4L, 5L, 6L, 7L, 8L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, intColumn,
                ConstantOperator.createInt(3)));
        Assert.assertEquals(Lists.newArrayList(1L, 2L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, new CastOperator(Type.STRING, intColumn),
                ConstantOperator.createVarchar("1")));
        Assert.assertEquals(Lists.newArrayList(1L), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT, new CastOperator(Type.STRING, intColumn),
                ConstantOperator.createVarchar("3")));
        Assert.assertEquals(Lists.newArrayList(), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, new CastOperator(Type.STRING, intColumn),
                ConstantOperator.createVarchar("2")));
        Assert.assertEquals(Lists.newArrayList(1L, 4L, 5L, 6L), pruner.prune());
    }

    @Test
    public void testComplexBinaryPredicate() throws AnalysisException {
        // 2 partition columns
        // int_col1=0/int_col2=10    0
        // int_col1=0/int_col2=11    1
        // int_col1=1/int_col2=10    2
        intColumn = new ColumnRefOperator(2, Type.INT, "int_col", true);
        ColumnRefOperator intCol1 = new ColumnRefOperator(3, Type.INT, "int_col1", true);
        ColumnRefOperator intCol2 = new ColumnRefOperator(4, Type.INT, "int_col2", true);
        ColumnRefOperator intColNotPart = new ColumnRefOperator(5, Type.INT, "int_col_not_part", true);

        // column -> partition values
        columnToPartitionValuesMap = Maps.newHashMap();
        TreeMap<LiteralExpr, Set<Long>> intPartitionValuesMap1 = Maps.newTreeMap();
        columnToPartitionValuesMap.put(intCol1, intPartitionValuesMap1);
        intPartitionValuesMap1.put(new IntLiteral(0, Type.INT), Sets.newHashSet(0L, 1L));
        intPartitionValuesMap1.put(new IntLiteral(1, Type.INT), Sets.newHashSet(2L));
        TreeMap<LiteralExpr, Set<Long>> intPartitionValuesMap2 = Maps.newTreeMap();
        columnToPartitionValuesMap.put(intCol2, intPartitionValuesMap2);
        intPartitionValuesMap2.put(new IntLiteral(10, Type.INT), Sets.newHashSet(0L, 2L));
        intPartitionValuesMap2.put(new IntLiteral(11, Type.INT), Sets.newHashSet(1L));

        // column -> null partitions
        columnToNullPartitions = Maps.newHashMap();
        columnToNullPartitions.put(intCol1, Sets.newHashSet());
        columnToNullPartitions.put(intCol2, Sets.newHashSet());

        Set<Long> allPartitions = Sets.newHashSet(0L, 1L, 2L);
        conjuncts = Lists.newArrayList();
        pruner = new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions, conjuncts);

        // int_col1 + int_col2 = 10
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(
                BinaryPredicateOperator.BinaryType.EQ,
                new CallOperator("add", Type.BIGINT, Lists.newArrayList(intCol1, intCol2)),
                ConstantOperator.createInt(10)));
        Assert.assertEquals(null, pruner.prune());
        // check not eval conjuncts
        List<ScalarOperator> notEvalConjuncts = pruner.getNoEvalConjuncts();
        Assert.assertEquals(1, notEvalConjuncts.size());
        Assert.assertEquals(conjuncts.get(0), notEvalConjuncts.get(0));

        // int_col2 + 1 = 11
        conjuncts.clear();
        notEvalConjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(
                BinaryPredicateOperator.BinaryType.EQ,
                new CallOperator("add", Type.BIGINT, Lists.newArrayList(intCol2, ConstantOperator.createInt(1))),
                ConstantOperator.createInt(11)));
        Assert.assertEquals(null, pruner.prune());
        // check not eval conjuncts
        notEvalConjuncts = pruner.getNoEvalConjuncts();
        Assert.assertEquals(1, notEvalConjuncts.size());
        Assert.assertEquals(conjuncts.get(0), notEvalConjuncts.get(0));

        // int_col2 = 10 and int_col_not_part = 0
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                Lists.newArrayList(intCol2, ConstantOperator.createInt(10))));
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, intColNotPart,
                ConstantOperator.createInt(0)));
        Assert.assertEquals(Lists.newArrayList(0L, 2L), pruner.prune());
    }

    @Test
    public void testInPredicate() throws AnalysisException {
        // date_col in ("2021-01-01")
        conjuncts.clear();
        conjuncts.add(new InPredicateOperator(dateColumn,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 1, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(0L, 1L, 2L), pruner.prune());
        // date_col in ("2021-01-02", "2021-01-03")
        conjuncts.clear();
        conjuncts.add(new InPredicateOperator(dateColumn,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 2, 0, 0, 0)),
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 3, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(3L, 4L, 5L, 6L, 7L, 8L), pruner.prune());
        // date_col in ("2021-01-03", "2021-01-04")
        conjuncts.clear();
        conjuncts.add(new InPredicateOperator(false, Lists.newArrayList(dateColumn,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 3, 0, 0, 0)),
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 4, 0, 0, 0)))));
        Assert.assertEquals(Lists.newArrayList(6L, 7L, 8L), pruner.prune());
        // date_col in ("2021-01-04")
        conjuncts.clear();
        conjuncts.add(new InPredicateOperator(false, Lists.newArrayList(dateColumn,
                ConstantOperator.createDate(LocalDateTime.of(2021, 1, 4, 0, 0, 0)))));
        Assert.assertEquals(Lists.newArrayList(), pruner.prune());

        // int_col in (0)
        conjuncts.clear();
        conjuncts.add(new InPredicateOperator(false, Lists.newArrayList(intColumn, ConstantOperator.createInt(0))));
        Assert.assertEquals(Lists.newArrayList(0L, 3L, 6L), pruner.prune());
        // int_col in (1, 2)
        conjuncts.clear();
        conjuncts.add(new InPredicateOperator(false,
                Lists.newArrayList(intColumn, ConstantOperator.createInt(1), ConstantOperator.createInt(2))));
        Assert.assertEquals(Lists.newArrayList(1L, 2L, 4L, 5L, 7L, 8L), pruner.prune());
        // int_col not in (1, 2)
        conjuncts.clear();
        conjuncts.add(new InPredicateOperator(true,
                Lists.newArrayList(intColumn, ConstantOperator.createInt(1), ConstantOperator.createInt(2))));
        Assert.assertEquals(Lists.newArrayList(0L, 3L, 6L), pruner.prune());
    }

    @Test
    public void testIsNullPredicate() throws AnalysisException {
        List<Long> notNullPartitions = Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);

        // date_col is null
        conjuncts.clear();
        conjuncts.add(new IsNullPredicateOperator(false, dateColumn));
        Assert.assertEquals(Lists.newArrayList(9L), pruner.prune());

        // date_col is not null
        conjuncts.clear();
        conjuncts.add(new IsNullPredicateOperator(true, dateColumn));
        Assert.assertEquals(notNullPartitions, pruner.prune());

        // int_col is null
        conjuncts.clear();
        conjuncts.add(new IsNullPredicateOperator(false, intColumn));
        Assert.assertEquals(Lists.newArrayList(9L), pruner.prune());

        // int_col is not null
        conjuncts.clear();
        conjuncts.add(new IsNullPredicateOperator(true, intColumn));
        Assert.assertEquals(notNullPartitions, pruner.prune());
    }

    @Test
    public void testCompoundPredicate() throws AnalysisException {
        // date_col = "2021-01-01" or int_col = 2
        conjuncts.clear();
        conjuncts.add(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR,
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, dateColumn,
                        ConstantOperator.createDate(LocalDateTime.of(2021, 1, 1, 0, 0, 0))),
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, intColumn,
                        ConstantOperator.createInt(2))));
        Assert.assertEquals(Lists.newArrayList(0L, 1L, 2L, 5L, 8L), pruner.prune());

        // date_col > "2021-01-02" or int_col is null
        conjuncts.clear();
        conjuncts.add(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR,
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT, dateColumn,
                        ConstantOperator.createDate(LocalDateTime.of(2021, 1, 2, 0, 0, 0))),
                new IsNullPredicateOperator(false, intColumn)));
        Assert.assertEquals(Lists.newArrayList(6L, 7L, 8L, 9L), pruner.prune());

        // date_col < "2021-01-02" or (int_col is null or int_col in (0))
        conjuncts.clear();
        conjuncts.add(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR,
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, dateColumn,
                        ConstantOperator.createDate(LocalDateTime.of(2021, 1, 2, 0, 0, 0))),
                new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.OR,
                        new IsNullPredicateOperator(false, intColumn),
                        new InPredicateOperator(false, Lists.newArrayList(intColumn, ConstantOperator.createInt(0)))
                )));
        Assert.assertEquals(Lists.newArrayList(0L, 1L, 2L, 3L, 6L, 9L), pruner.prune());
    }

    @Test
    public void testCastTypePredicate() throws AnalysisException {
        // date_col = "2021-01-01"
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                new CastOperator(Type.DATETIME, dateColumn),
                ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 1, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(0L, 1L, 2L), pruner.prune());
        // date_col < "2021-01-02"
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT,
                new CastOperator(Type.DATETIME, dateColumn),
                ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 2, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(0L, 1L, 2L), pruner.prune());
        // date_col >= "2021-01-03" and date_col <= "2021-01-03"
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE,
                new CastOperator(Type.DATETIME, dateColumn),
                ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 3, 0, 0, 0))));
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE,
                new CastOperator(Type.DATETIME, dateColumn),
                ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 3, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(6L, 7L, 8L), pruner.prune());
        // date_col > "2021-01-03"
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT,
                new CastOperator(Type.DATETIME, dateColumn),
                ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 3, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(), pruner.prune());
        // date_col <= "2020-12-31"
        conjuncts.clear();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE,
                new CastOperator(Type.DATETIME, dateColumn),
                ConstantOperator.createDatetime(LocalDateTime.of(2020, 12, 31, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(), pruner.prune());

        conjuncts.clear();
        conjuncts.add(new InPredicateOperator(new CastOperator(Type.DATETIME, dateColumn),
                ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 1, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(0L, 1L, 2L), pruner.prune());
        // date_col in ("2021-01-02", "2021-01-03")
        conjuncts.clear();
        conjuncts.add(new InPredicateOperator(new CastOperator(Type.DATETIME, dateColumn),
                ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 2, 0, 0, 0)),
                ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 3, 0, 0, 0))));
        Assert.assertEquals(Lists.newArrayList(3L, 4L, 5L, 6L, 7L, 8L), pruner.prune());
        // date_col in ("2021-01-03", "2021-01-04")
        conjuncts.clear();
        conjuncts.add(new InPredicateOperator(false, Lists.newArrayList(new CastOperator(Type.DATETIME, dateColumn),
                ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 3, 0, 0, 0)),
                ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 4, 0, 0, 0)))));
        Assert.assertEquals(Lists.newArrayList(6L, 7L, 8L), pruner.prune());
        // date_col in ("2021-01-04")
        conjuncts.clear();
        conjuncts.add(new InPredicateOperator(false, Lists.newArrayList(new CastOperator(Type.DATETIME, dateColumn),
                ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 4, 0, 0, 0)))));
        Assert.assertEquals(Lists.newArrayList(), pruner.prune());

    }

    @Test
    public void testBinaryPredicateWithEmptyPartition() throws AnalysisException {
        ColumnRefOperator operator = new ColumnRefOperator(5, Type.VARCHAR, "ds", true);
        columnToPartitionValuesMap = Maps.newHashMap();
        columnToPartitionValuesMap.put(operator, new TreeMap<>());
        columnToNullPartitions = new HashMap<>();
        columnToNullPartitions.put(operator, new HashSet<>());
        conjuncts = Lists.newArrayList();
        conjuncts.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT, intColumn,
                ConstantOperator.createInt(1000)));
        pruner = new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions, conjuncts);
        Assert.assertNull(pruner.prune());
    }

    @Test
    public void testInPredicateWithEmptyPartition() throws AnalysisException {
        ColumnRefOperator operator = new ColumnRefOperator(5, Type.VARCHAR, "ds", true);
        columnToPartitionValuesMap = Maps.newHashMap();
        columnToPartitionValuesMap.put(operator, new TreeMap<>());
        columnToNullPartitions = new HashMap<>();
        columnToNullPartitions.put(operator, new HashSet<>());
        conjuncts = Lists.newArrayList();
        conjuncts.add(new InPredicateOperator(false,
                Lists.newArrayList(intColumn, ConstantOperator.createInt(1000), ConstantOperator.createInt(2000))));
        pruner = new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions, conjuncts);
        Assert.assertNull(pruner.prune());
    }
}
