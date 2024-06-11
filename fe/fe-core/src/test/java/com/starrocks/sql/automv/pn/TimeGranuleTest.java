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

package com.starrocks.sql.automv.pn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.column.OriginalColumn;
import com.starrocks.sql.automv.generator.AliasGenerator;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeGranuleTest {
    private final TableName tableName = new TableName("default_catalog", "test_db", "t0");
    private final ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
    private final List<ColumnRefOperator> columnRefs = Arrays.asList(
            new ColumnRefOperator(1, Type.DATE, "eventDate", true),
            new ColumnRefOperator(2, Type.DATETIME, "eventTime", true),
            new ColumnRefOperator(3, Type.VARCHAR, "eventDateS", true),
            new ColumnRefOperator(4, Type.VARCHAR, "eventTimeS", true)
    );

    private final List<Var> vars = columnRefs.stream()
            .map(colRef -> Op.var(colRef.getType(), colRef.getId()))
            .collect(Collectors.toList());

    private final ColumnRefOperator eventDate = columnRefs.get(0);
    private final ColumnRefOperator eventTime = columnRefs.get(1);
    private final ColumnRefOperator eventDateS = columnRefs.get(2);
    private final ColumnRefOperator eventTimeS = columnRefs.get(3);

    private final TieredMap<Integer, GenericColumn> inputColumns = columnRefs.stream()
            .collect(TieredMap.toMap(idConverter::getId,
                    colRef -> GenericColumn.original(tableName, new Column(colRef.getName(), colRef.getType()))));

    private final TieredMap<Integer, ColumnAlias> columnAliases = inputColumns.entrySet().stream()
            .map(e -> Pair.create(e.getKey(), e.getValue().mustCast(OriginalColumn.class)))
            .collect(TieredMap.toMap(
                    p -> p.first,
                    p -> ColumnAlias.of(p.second.getFqTableName().toSql(), p.second.getColumnName())));
    private final Function<Op, String> opToSql = OpUtil.toOpToSqlConverter(columnAliases);
    private final Function<ScalarOperator, Op> opConverter = OpUtil.toOpConverter(idConverter, inputColumns);

    private void testHelper(
            ScalarOperator scalarOperator,
            List<ScalarOperator> existScalarOperator,
            boolean yieldNewColumn,
            String expectTextualNewOp,
            String expectTextualDependentOps,
            boolean debug) {
        ColumnRefToIdConverter newIdConverter = idConverter.duplicate();
        Function<ScalarOperator, Op> newOpConverter = OpUtil.toOpConverter(newIdConverter, inputColumns);
        Op op = newOpConverter.apply(scalarOperator);

        TieredMap<Integer, GenericColumn> alreadyExistingColumns =
                existScalarOperator.stream().collect(TieredMap.toMap(
                        scalarOp -> newIdConverter.nextId(),
                        scalarOp -> GenericColumn.derived(newOpConverter.apply(scalarOp))));
        TieredMap<Integer, GenericColumn> newInputColumns = inputColumns.merge(alreadyExistingColumns);

        TieredMap<StrictOp, Integer> alreadyExists = OpUtil.columnsToStrictOpMap(newInputColumns);

        Optional<OpPlus2> optResult =
                OpUtil.rewriteRollupAbleTimeGranule(OpPlus.of(op, 0), newIdConverter::nextId, alreadyExists);
        Assert.assertTrue(optResult.isPresent());
        OpPlus2 result = optResult.get();
        Assert.assertEquals(!result.getNewColumns().isEmpty(), yieldNewColumn);
        AliasGenerator aliasGen = AliasGenerator.getDefaultAliasGenerator();
        aliasGen.nextAliasIfTableNameAbsent("tmp");

        TieredMap<Integer, GenericColumn> extraColumns = result.getNewColumns();
        TieredMap<Integer, GenericColumn> newColumns = inputColumns.merge(alreadyExistingColumns).merge(extraColumns);
        TieredMap<Integer, ColumnAlias> extraColumnAliases = alreadyExistingColumns.merge(extraColumns).entrySet()
                .stream()
                .sorted(Comparator.comparingInt(Map.Entry::getKey))
                .collect(TieredMap.toMap(Map.Entry::getKey, e -> aliasGen.nextAliasIfColumnNameAbsent(null)));

        TieredMap<Integer, ColumnAlias> newColumnAliases = columnAliases.merge(extraColumnAliases);
        Function<Op, String> newOpToSql = OpUtil.toOpToSqlConverter(newColumnAliases);
        Op newOp = result.getOp().getOp();

        String dependInputColumns = newOp.getIdSet()
                .stream()
                .sorted()
                .map(id -> Pair.create(newColumnAliases.get(id),
                        opToSql.apply(OpUtil.columnToOp(id, newColumns.get(id)))))
                .map(p -> String.format("%s:%s", p.first.getName(), p.second))
                .collect(Collectors.joining(", "));

        String s = newOpToSql.apply(result.getOp().getOp());
        if (debug) {
            System.out.printf("{%s,%s},\n",
                    PrettyPrinter.escapedDoubleQuoted(s).getResult(),
                    PrettyPrinter.escapedDoubleQuoted(dependInputColumns).getResult());
        } else {
            Assert.assertEquals(s, s, expectTextualNewOp);
            Assert.assertEquals(dependInputColumns, dependInputColumns, expectTextualDependentOps);
        }
    }

    private ConstantOperator varchar(String s) {
        return ConstantOperator.createVarchar(s);
    }

    private ConstantOperator integer(int i) {
        return ConstantOperator.createInt(i);
    }

    @Test
    public void testBasicOps() {
        // date(eventDate)
        CallOperator dateFun = new CallOperator(FunctionSet.DATE, Type.DATE, ImmutableList.of(eventDate));
        CallOperator toDateFun = new CallOperator(FunctionSet.TO_DATE, Type.DATE, ImmutableList.of(eventDateS));
        // date_format(eventTime, '%%%d')
        CallOperator dateFormatFun = new CallOperator(FunctionSet.DATE_FORMAT, Type.VARCHAR,
                ImmutableList.of(eventTime, varchar("%%%d")));
        // date_slice(eventDate, INTERVAL 5 day, ceil)
        CallOperator dateSliceFun = new CallOperator(FunctionSet.DATE_SLICE, Type.DATETIME,
                ImmutableList.of(eventDateS, integer(5), varchar("day"), varchar("floor")));
        CallOperator timeSliceFun = new CallOperator(FunctionSet.TIME_SLICE, Type.DATETIME,
                ImmutableList.of(eventDateS, integer(5), varchar("minute"), varchar("ceil")));
        CallOperator dataTruncFun =
                new CallOperator(FunctionSet.DATE_TRUNC, Type.DATE, ImmutableList.of(varchar("day"), eventTimeS));
        CallOperator dayNameFun =
                new CallOperator(FunctionSet.DAYNAME, Type.VARCHAR, ImmutableList.of(eventTime));
        CallOperator dayOfMonthFun =
                new CallOperator(FunctionSet.DAYOFMONTH, Type.TINYINT, ImmutableList.of(eventDate));
        CallOperator dayOfWeekFun =
                new CallOperator(FunctionSet.DAYOFWEEK, Type.INT, ImmutableList.of(eventDateS));
        CallOperator dayOfYearFun =
                new CallOperator(FunctionSet.DAYOFYEAR, Type.INT, ImmutableList.of(eventTimeS));
        CallOperator yearFun =
                new CallOperator(FunctionSet.YEAR, Type.SMALLINT, ImmutableList.of(eventDate));
        CallOperator quarterFun =
                new CallOperator(FunctionSet.QUARTER, Type.TINYINT, ImmutableList.of(eventDate));
        CallOperator monthFun =
                new CallOperator(FunctionSet.MONTH, Type.TINYINT, ImmutableList.of(eventTime));
        CallOperator weekFun =
                new CallOperator(FunctionSet.WEEK, Type.TINYINT, ImmutableList.of(eventTime));
        CallOperator weekOfYearFun =
                new CallOperator(FunctionSet.WEEKOFYEAR, Type.TINYINT, ImmutableList.of(eventTime));
        CallOperator dayFun =
                new CallOperator(FunctionSet.DAY, Type.TINYINT, ImmutableList.of(eventDateS));
        CallOperator hourFun =
                new CallOperator(FunctionSet.HOUR, Type.TINYINT, ImmutableList.of(eventTimeS));
        CallOperator minuteFun =
                new CallOperator(FunctionSet.MINUTE, Type.TINYINT, ImmutableList.of(eventTime));
        CallOperator secondFun =
                new CallOperator(FunctionSet.SECOND, Type.TINYINT, ImmutableList.of(eventTimeS));
        CallOperator lastDayFun =
                new CallOperator(FunctionSet.LAST_DAY, Type.DATE, ImmutableList.of(eventDate));
        CallOperator monthNameFun =
                new CallOperator(FunctionSet.MONTHNAME, Type.VARCHAR, ImmutableList.of(eventTime));
        CallOperator nextDayFun =
                new CallOperator(FunctionSet.NEXT_DAY, Type.DATE, ImmutableList.of(eventDateS, varchar("Monday")));
        CallOperator previousDayFun =
                new CallOperator(FunctionSet.PREVIOUS_DAY, Type.DATE, ImmutableList.of(eventTimeS, varchar("TuesDay")));
        // str2date(column, '%Y-%m-%d %H:%i:%s')
        CallOperator str2DateFun =
                new CallOperator(FunctionSet.STR2DATE, Type.DATE,
                        ImmutableList.of(eventTimeS, varchar("'%Y-%m-%d %H:%i:%s'")));
        CallOperator strToDateFun =
                new CallOperator(FunctionSet.STR_TO_DATE, Type.DATE,
                        ImmutableList.of(eventTimeS, varchar("'%Y-%m-%d %H:%i:%s'")));
        CallOperator toDaysFun =
                new CallOperator(FunctionSet.TO_DAYS, Type.INT, ImmutableList.of(eventTime));

        List<ScalarOperator> timeGranuleFunList = Lists.newArrayList(
                dateFun,
                toDateFun,
                dateFormatFun,
                dateSliceFun,
                timeSliceFun,
                dataTruncFun,
                dayNameFun,
                dayOfMonthFun,
                dayOfWeekFun,
                dayOfYearFun,
                yearFun,
                quarterFun,
                monthFun,
                weekFun,
                weekOfYearFun,
                dayFun,
                hourFun,
                minuteFun,
                secondFun,
                lastDayFun,
                monthNameFun,
                nextDayFun,
                previousDayFun,
                str2DateFun,
                strToDateFun,
                toDaysFun);

        Function<ScalarOperator, ScalarOperator> createEqPred = scalarOp ->
                new BinaryPredicateOperator(BinaryType.EQ, scalarOp, varchar("2024-01-01"));

        String[][] expectResults = new String[][] {
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:date(`test_db`.`t0`.eventDate)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:to_date(`test_db`.`t0`.eventDateS)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:date_format(`test_db`.`t0`.eventTime, \"%%%d\")"},
                {"(tmp._ca0000 = \"2024-01-01\")",
                        "_ca0000:date_slice(`test_db`.`t0`.eventDateS, 5, \"day\", \"floor\")"},
                {"(tmp._ca0000 = \"2024-01-01\")",
                        "_ca0000:time_slice(`test_db`.`t0`.eventDateS, 5, \"minute\", \"ceil\")"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:date_trunc(\"day\", `test_db`.`t0`.eventTimeS)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:dayname(`test_db`.`t0`.eventTime)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:dayofmonth(`test_db`.`t0`.eventDate)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:dayofweek(`test_db`.`t0`.eventDateS)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:dayofyear(`test_db`.`t0`.eventTimeS)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:year(`test_db`.`t0`.eventDate)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:quarter(`test_db`.`t0`.eventDate)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:month(`test_db`.`t0`.eventTime)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:week(`test_db`.`t0`.eventTime)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:weekofyear(`test_db`.`t0`.eventTime)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:day(`test_db`.`t0`.eventDateS)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:hour(`test_db`.`t0`.eventTimeS)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:minute(`test_db`.`t0`.eventTime)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:second(`test_db`.`t0`.eventTimeS)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:last_day(`test_db`.`t0`.eventDate)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:monthname(`test_db`.`t0`.eventTime)"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:next_day(`test_db`.`t0`.eventDateS, \"Monday\")"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:previous_day(`test_db`.`t0`.eventTimeS, \"TuesDay\")"},
                {"(tmp._ca0000 = \"2024-01-01\")",
                        "_ca0000:str2date(`test_db`.`t0`.eventTimeS, \"'%Y-%m-%d %H:%i:%s'\")"},
                {"(tmp._ca0000 = \"2024-01-01\")",
                        "_ca0000:str_to_date(`test_db`.`t0`.eventTimeS, \"'%Y-%m-%d %H:%i:%s'\")"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:to_days(`test_db`.`t0`.eventTime)"},
        };

        for (int i = 0; i < timeGranuleFunList.size(); ++i) {
            ScalarOperator timeGranule = timeGranuleFunList.get(i);
            String expectTextualNewOp = expectResults[i][0];
            String expectTextualDependentOps = expectResults[i][1];
            ScalarOperator eqPred = createEqPred.apply(timeGranule);

            testHelper(eqPred, Collections.emptyList(), true, expectTextualNewOp, expectTextualDependentOps, false);
            testHelper(eqPred, Collections.singletonList(timeGranule), false, expectTextualNewOp,
                    expectTextualDependentOps, false);
        }
    }

    @Test
    public void testComplexOps() {
        List<ScalarOperator> whenThens = Arrays.asList(
                new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, eventTimeS,
                        varchar("XX\\\\d{4}-\\\\d{2}-\\\\d{2}")),
                new CallOperator(FunctionSet.SUBSTR, Type.VARCHAR,
                        ImmutableList.of(eventTimeS, integer(3), integer(10))),
                new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, eventTimeS,
                        varchar("YYYY\\\\d{4}-\\\\d{2}-\\\\d{2}")),
                new CallOperator(FunctionSet.SUBSTR, Type.VARCHAR,
                        ImmutableList.of(eventTimeS, integer(5), integer(10)))
        );
        CaseWhenOperator caseWhen =
                new CaseWhenOperator(Type.VARCHAR, null, eventTimeS, whenThens);
        CastOperator castOp = new CastOperator(Type.DATE, caseWhen);

        CallOperator dataTruncFun =
                new CallOperator(FunctionSet.DATE_TRUNC, Type.DATE, ImmutableList.of(varchar("month"), castOp));

        CallOperator daysAddFun =
                new CallOperator(FunctionSet.DAYS_ADD, Type.DATE, ImmutableList.of(castOp, integer(1)));
        CallOperator concatFun =
                new CallOperator(FunctionSet.CONCAT, Type.VARCHAR, ImmutableList.of(daysAddFun, varchar(" 00:00:00")));
        CallOperator str2DateFun =
                new CallOperator(FunctionSet.STR2DATE, Type.DATE,
                        ImmutableList.of(concatFun, varchar("'%Y-%m-%d %H:%i:%s'")));

        List<ScalarOperator> timeGranuleFunList = Lists.newArrayList(
                dataTruncFun,
                str2DateFun);

        Function<ScalarOperator, ScalarOperator> createEqPred = scalarOp ->
                new BinaryPredicateOperator(BinaryType.EQ, scalarOp, varchar("2024-01-01"));

        String[][] expectResults = new String[][] {
                {"(tmp._ca0000 = \"2024-01-01\")",
                        "_ca0000:date_trunc(\"month\", " +
                                "CAST((CASE WHEN (`test_db`.`t0`.eventTimeS regexp " +
                                "\"XX\\\\\\\\d{4}-\\\\\\\\d{2}-\\\\\\\\d{2}\") THEN " +
                                "substr(`test_db`.`t0`.eventTimeS, 3, 10) " +
                                "WHEN (`test_db`.`t0`.eventTimeS regexp " +
                                "\"YYYY\\\\\\\\d{4}-\\\\\\\\d{2}-\\\\\\\\d{2}\") THEN " +
                                "substr(`test_db`.`t0`.eventTimeS, 5, 10) " +
                                "ELSE `test_db`.`t0`.eventTimeS END) AS date))"},
                {"(tmp._ca0000 = \"2024-01-01\")", "_ca0000:str2date(concat(days_add" +
                        "(CAST((CASE WHEN (`test_db`.`t0`.eventTimeS regexp " +
                        "\"XX\\\\\\\\d{4}-\\\\\\\\d{2}-\\\\\\\\d{2}\") THEN " +
                        "substr(`test_db`.`t0`.eventTimeS, 3, 10) " +
                        "WHEN (`test_db`.`t0`.eventTimeS regexp " +
                        "\"YYYY\\\\\\\\d{4}-\\\\\\\\d{2}-\\\\\\\\d{2}\") THEN " +
                        "substr(`test_db`.`t0`.eventTimeS, 5, 10) " +
                        "ELSE `test_db`.`t0`.eventTimeS END) AS date), 1), \" 00:00:00\"), " +
                        "\"'%Y-%m-%d %H:%i:%s'\")"},
        };

        for (int i = 0; i < timeGranuleFunList.size(); ++i) {
            ScalarOperator timeGranule = timeGranuleFunList.get(i);
            String expectTextualNewOp = expectResults[i][0];
            String expectTextualDependentOps = expectResults[i][1];
            ScalarOperator eqPred = createEqPred.apply(timeGranule);

            testHelper(eqPred, Collections.emptyList(), true, expectTextualNewOp, expectTextualDependentOps, false);
            testHelper(eqPred, Collections.singletonList(timeGranule), false, expectTextualNewOp,
                    expectTextualDependentOps, false);
        }
    }

    private ScalarOperator dateTrunc(String unit, ScalarOperator dt) {
        return new CallOperator(FunctionSet.DATE_TRUNC, dt.getType(), ImmutableList.of(varchar(unit), dt));
    }

    private ScalarOperator str2Date(ScalarOperator s, String format) {
        return new CallOperator(FunctionSet.STR2DATE, Type.DATE, ImmutableList.of(s, varchar(format)));
    }

    private ScalarOperator dt(String s) {
        return ConstantOperator.createDate(LocalDate.parse(s, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay());

    }

    @Test
    public void testGetPartitionByTimeGranule() {
        ColumnRefSet partitionColumnIds = ColumnRefSet.createByIds(inputColumns.keySet());
        ColumnRefSet emptyPartitionColumnIds = ColumnRefSet.of();

        Object[][] testCases = {
                {eventDate, emptyPartitionColumnIds, false},
                {eventTime, emptyPartitionColumnIds, false},
                {eventDateS, emptyPartitionColumnIds, false},
                {eventTimeS, emptyPartitionColumnIds, false},

                {eventDate, partitionColumnIds, true},
                {eventTime, partitionColumnIds, true},
                {eventDateS, partitionColumnIds, false},
                {eventTimeS, partitionColumnIds, false},

                {dateTrunc("day", eventDate), partitionColumnIds, true},
                {dateTrunc("month", eventTime), partitionColumnIds, true},
                {str2Date(eventDateS, "%Y-%m-%d %H:%i:%s"), partitionColumnIds, true},
                {str2Date(eventTimeS, "%Y-%m-%d %H:%i:%s"), partitionColumnIds, true},

                {dateTrunc("day", dt("2024-01-01")), partitionColumnIds, false},
                {str2Date(varchar("2022-01-31 12:40:46"), "%Y-%m-%d %H:%i:%s"), partitionColumnIds, false},

                {dateTrunc("day",
                        new CallOperator(FunctionSet.DAYS_ADD, Type.DATE, ImmutableList.of(eventDate, integer(1)))),
                        partitionColumnIds, false},
                {new CallOperator(FunctionSet.DAYS_SUB, Type.DATE, ImmutableList.of(
                        dateTrunc("month", eventTime), integer(1))), partitionColumnIds, false},
                {str2Date(new CallOperator(FunctionSet.SUBSTR, Type.VARCHAR,
                        ImmutableList.of(eventDateS, integer(4), integer(10))), "%Y-%m-%d %H:%i:%s"),
                        partitionColumnIds, false},
                {new CallOperator(FunctionSet.MONTHS_ADD, Type.DATE,
                        ImmutableList.of(str2Date(eventTimeS, "%Y-%m-%d %H:%i:%s"), integer(1))),
                        partitionColumnIds, false},
        };
        for (Object[] tc : testCases) {
            ScalarOperator scaleOp = (ScalarOperator) tc[0];
            ColumnRefSet partColumnIds = (ColumnRefSet) tc[1];
            Boolean expect = (Boolean) tc[2];
            Op op = opConverter.apply(scaleOp);
            Assert.assertEquals(opToSql.apply(op),
                    OpUtil.getPartitionByTimeGranule(op, partColumnIds).isPresent(), expect);
        }
    }

    private void testTimeGranuleHelper(Object[][] testCases, ColumnRefOperator column, boolean debug) {
        for (Object[] tc : testCases) {
            String unit = (String) tc[0];
            TimeGranule.Unit unit2 = TimeGranule.Unit.valueOf((String) tc[1]);

            Op op = opConverter.apply(dateTrunc(unit.toLowerCase(), column));
            TimeGranule granule = Objects.requireNonNull(TimeGranule.of(op));
            String textualGranule = opToSql.apply(granule.getOp());
            boolean fineGrained = granule.isFineGrained(unit2);
            TimeGranule coarseGranule = granule.toCoarse(unit2);
            String textualCoarseGranule = opToSql.apply(coarseGranule.getOp());
            String textualWellDefinedGranule = opToSql.apply(granule.toWellFormed().getOp());
            if (debug) {
                PrettyPrinter printer = new PrettyPrinter();
                System.out.println(
                        printer.add("{")
                                .addEscapedDoubleQuoted(unit).add(", ")
                                .addEscapedDoubleQuoted(unit2.name()).add(", ")
                                .addEscapedDoubleQuoted(textualGranule).add(", ")
                                .add(fineGrained).add(", ")
                                .addEscapedDoubleQuoted(textualCoarseGranule).add(", ")
                                .addEscapedDoubleQuoted(textualWellDefinedGranule).add("},").getResult());
            } else {
                String expectTextualGranule = (String) tc[2];
                Boolean expectFineGrained = (Boolean) tc[3];
                String expectTextualCoarseGranule = (String) tc[4];
                String expectTextualWellDefinedGranule = (String) tc[5];
                Assert.assertEquals(textualGranule, textualGranule, expectTextualGranule);
                Assert.assertEquals(fineGrained, expectFineGrained);
                Assert.assertEquals(textualCoarseGranule, textualCoarseGranule, expectTextualCoarseGranule);
                Assert.assertEquals(textualWellDefinedGranule, textualWellDefinedGranule,
                        expectTextualWellDefinedGranule);
            }

        }
    }

    @Test
    public void testDateTypedTimeGranule() {
        Object[][] testCases = new Object[][] {
                {"MICROSECOND", "MICROSECOND", "date_trunc(\"microsecond\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MICROSECOND", "MILLISECOND", "date_trunc(\"microsecond\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MICROSECOND", "SECOND", "date_trunc(\"microsecond\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MICROSECOND", "MINUTE", "date_trunc(\"microsecond\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MICROSECOND", "HOUR", "date_trunc(\"microsecond\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MICROSECOND", "DAY", "date_trunc(\"microsecond\", `test_db`.`t0`.eventDate)", true,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MICROSECOND", "WEEK", "date_trunc(\"microsecond\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"MICROSECOND", "MONTH", "date_trunc(\"microsecond\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"MICROSECOND", "QUARTER", "date_trunc(\"microsecond\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"MICROSECOND", "YEAR", "date_trunc(\"microsecond\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"MILLISECOND", "MICROSECOND", "date_trunc(\"millisecond\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MILLISECOND", "MILLISECOND", "date_trunc(\"millisecond\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MILLISECOND", "SECOND", "date_trunc(\"millisecond\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MILLISECOND", "MINUTE", "date_trunc(\"millisecond\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MILLISECOND", "HOUR", "date_trunc(\"millisecond\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MILLISECOND", "DAY", "date_trunc(\"millisecond\", `test_db`.`t0`.eventDate)", true,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MILLISECOND", "WEEK", "date_trunc(\"millisecond\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"MILLISECOND", "MONTH", "date_trunc(\"millisecond\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"MILLISECOND", "QUARTER", "date_trunc(\"millisecond\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"MILLISECOND", "YEAR", "date_trunc(\"millisecond\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"SECOND", "MICROSECOND", "date_trunc(\"second\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"SECOND", "MILLISECOND", "date_trunc(\"second\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"SECOND", "SECOND", "date_trunc(\"second\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"SECOND", "MINUTE", "date_trunc(\"second\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"SECOND", "HOUR", "date_trunc(\"second\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"SECOND", "DAY", "date_trunc(\"second\", `test_db`.`t0`.eventDate)", true, "`test_db`.`t0`.eventDate",
                        "`test_db`.`t0`.eventDate"},
                {"SECOND", "WEEK", "date_trunc(\"second\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"SECOND", "MONTH", "date_trunc(\"second\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"SECOND", "QUARTER", "date_trunc(\"second\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"SECOND", "YEAR", "date_trunc(\"second\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"MINUTE", "MICROSECOND", "date_trunc(\"minute\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MINUTE", "MILLISECOND", "date_trunc(\"minute\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MINUTE", "SECOND", "date_trunc(\"minute\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MINUTE", "MINUTE", "date_trunc(\"minute\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MINUTE", "HOUR", "date_trunc(\"minute\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"MINUTE", "DAY", "date_trunc(\"minute\", `test_db`.`t0`.eventDate)", true, "`test_db`.`t0`.eventDate",
                        "`test_db`.`t0`.eventDate"},
                {"MINUTE", "WEEK", "date_trunc(\"minute\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"MINUTE", "MONTH", "date_trunc(\"minute\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"MINUTE", "QUARTER", "date_trunc(\"minute\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"MINUTE", "YEAR", "date_trunc(\"minute\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"HOUR", "MICROSECOND", "date_trunc(\"hour\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"HOUR", "MILLISECOND", "date_trunc(\"hour\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"HOUR", "SECOND", "date_trunc(\"hour\", `test_db`.`t0`.eventDate)", false, "`test_db`.`t0`.eventDate",
                        "`test_db`.`t0`.eventDate"},
                {"HOUR", "MINUTE", "date_trunc(\"hour\", `test_db`.`t0`.eventDate)", false, "`test_db`.`t0`.eventDate",
                        "`test_db`.`t0`.eventDate"},
                {"HOUR", "HOUR", "date_trunc(\"hour\", `test_db`.`t0`.eventDate)", false, "`test_db`.`t0`.eventDate",
                        "`test_db`.`t0`.eventDate"},
                {"HOUR", "DAY", "date_trunc(\"hour\", `test_db`.`t0`.eventDate)", true, "`test_db`.`t0`.eventDate",
                        "`test_db`.`t0`.eventDate"},
                {"HOUR", "WEEK", "date_trunc(\"hour\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"HOUR", "MONTH", "date_trunc(\"hour\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"HOUR", "QUARTER", "date_trunc(\"hour\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"HOUR", "YEAR", "date_trunc(\"hour\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"DAY", "MICROSECOND", "date_trunc(\"day\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"DAY", "MILLISECOND", "date_trunc(\"day\", `test_db`.`t0`.eventDate)", false,
                        "`test_db`.`t0`.eventDate", "`test_db`.`t0`.eventDate"},
                {"DAY", "SECOND", "date_trunc(\"day\", `test_db`.`t0`.eventDate)", false, "`test_db`.`t0`.eventDate",
                        "`test_db`.`t0`.eventDate"},
                {"DAY", "MINUTE", "date_trunc(\"day\", `test_db`.`t0`.eventDate)", false, "`test_db`.`t0`.eventDate",
                        "`test_db`.`t0`.eventDate"},
                {"DAY", "HOUR", "date_trunc(\"day\", `test_db`.`t0`.eventDate)", false, "`test_db`.`t0`.eventDate",
                        "`test_db`.`t0`.eventDate"},
                {"DAY", "DAY", "date_trunc(\"day\", `test_db`.`t0`.eventDate)", true, "`test_db`.`t0`.eventDate",
                        "`test_db`.`t0`.eventDate"},
                {"DAY", "WEEK", "date_trunc(\"day\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"DAY", "MONTH", "date_trunc(\"day\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"DAY", "QUARTER", "date_trunc(\"day\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"DAY", "YEAR", "date_trunc(\"day\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)", "`test_db`.`t0`.eventDate"},
                {"WEEK", "MICROSECOND", "date_trunc(\"week\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)"},
                {"WEEK", "MILLISECOND", "date_trunc(\"week\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)"},
                {"WEEK", "SECOND", "date_trunc(\"week\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)"},
                {"WEEK", "MINUTE", "date_trunc(\"week\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)"},
                {"WEEK", "HOUR", "date_trunc(\"week\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)"},
                {"WEEK", "DAY", "date_trunc(\"week\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)"},
                {"WEEK", "WEEK", "date_trunc(\"week\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)"},
                {"WEEK", "MONTH", "date_trunc(\"week\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)"},
                {"WEEK", "QUARTER", "date_trunc(\"week\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)"},
                {"WEEK", "YEAR", "date_trunc(\"week\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventDate)"},
                {"MONTH", "MICROSECOND", "date_trunc(\"month\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)"},
                {"MONTH", "MILLISECOND", "date_trunc(\"month\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)"},
                {"MONTH", "SECOND", "date_trunc(\"month\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)"},
                {"MONTH", "MINUTE", "date_trunc(\"month\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)"},
                {"MONTH", "HOUR", "date_trunc(\"month\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)"},
                {"MONTH", "DAY", "date_trunc(\"month\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)"},
                {"MONTH", "WEEK", "date_trunc(\"month\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)"},
                {"MONTH", "MONTH", "date_trunc(\"month\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)"},
                {"MONTH", "QUARTER", "date_trunc(\"month\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)"},
                {"MONTH", "YEAR", "date_trunc(\"month\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventDate)"},
                {"QUARTER", "MICROSECOND", "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)"},
                {"QUARTER", "MILLISECOND", "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)"},
                {"QUARTER", "SECOND", "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)"},
                {"QUARTER", "MINUTE", "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)"},
                {"QUARTER", "HOUR", "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)"},
                {"QUARTER", "DAY", "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)"},
                {"QUARTER", "WEEK", "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)"},
                {"QUARTER", "MONTH", "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)"},
                {"QUARTER", "QUARTER", "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)"},
                {"QUARTER", "YEAR", "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventDate)"},
                {"YEAR", "MICROSECOND", "date_trunc(\"year\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)"},
                {"YEAR", "MILLISECOND", "date_trunc(\"year\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)"},
                {"YEAR", "SECOND", "date_trunc(\"year\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)"},
                {"YEAR", "MINUTE", "date_trunc(\"year\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)"},
                {"YEAR", "HOUR", "date_trunc(\"year\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)"},
                {"YEAR", "DAY", "date_trunc(\"year\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)"},
                {"YEAR", "WEEK", "date_trunc(\"year\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)"},
                {"YEAR", "MONTH", "date_trunc(\"year\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)"},
                {"YEAR", "QUARTER", "date_trunc(\"year\", `test_db`.`t0`.eventDate)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)"},
                {"YEAR", "YEAR", "date_trunc(\"year\", `test_db`.`t0`.eventDate)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventDate)"},
        };
        testTimeGranuleHelper(testCases, eventDate, false);
    }

    @Test
    public void testDateTimeTypedTimeGranule() {
        Object[][] testCases = new Object[][] {
                {"MICROSECOND", "MICROSECOND", "date_trunc(\"microsecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"microsecond\", `test_db`.`t0`.eventTime)", "`test_db`.`t0`.eventTime"},
                {"MICROSECOND", "MILLISECOND", "date_trunc(\"microsecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)", "`test_db`.`t0`.eventTime"},
                {"MICROSECOND", "SECOND", "date_trunc(\"microsecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)", "`test_db`.`t0`.eventTime"},
                {"MICROSECOND", "MINUTE", "date_trunc(\"microsecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)", "`test_db`.`t0`.eventTime"},
                {"MICROSECOND", "HOUR", "date_trunc(\"microsecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)", "`test_db`.`t0`.eventTime"},
                {"MICROSECOND", "DAY", "date_trunc(\"microsecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)", "`test_db`.`t0`.eventTime"},
                {"MICROSECOND", "WEEK", "date_trunc(\"microsecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)", "`test_db`.`t0`.eventTime"},
                {"MICROSECOND", "MONTH", "date_trunc(\"microsecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)", "`test_db`.`t0`.eventTime"},
                {"MICROSECOND", "QUARTER", "date_trunc(\"microsecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)", "`test_db`.`t0`.eventTime"},
                {"MICROSECOND", "YEAR", "date_trunc(\"microsecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)", "`test_db`.`t0`.eventTime"},
                {"MILLISECOND", "MICROSECOND", "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)"},
                {"MILLISECOND", "MILLISECOND", "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)"},
                {"MILLISECOND", "SECOND", "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)"},
                {"MILLISECOND", "MINUTE", "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)"},
                {"MILLISECOND", "HOUR", "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)"},
                {"MILLISECOND", "DAY", "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)"},
                {"MILLISECOND", "WEEK", "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)"},
                {"MILLISECOND", "MONTH", "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)"},
                {"MILLISECOND", "QUARTER", "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)"},
                {"MILLISECOND", "YEAR", "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"millisecond\", `test_db`.`t0`.eventTime)"},
                {"SECOND", "MICROSECOND", "date_trunc(\"second\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)"},
                {"SECOND", "MILLISECOND", "date_trunc(\"second\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)"},
                {"SECOND", "SECOND", "date_trunc(\"second\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)"},
                {"SECOND", "MINUTE", "date_trunc(\"second\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)"},
                {"SECOND", "HOUR", "date_trunc(\"second\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)"},
                {"SECOND", "DAY", "date_trunc(\"second\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)"},
                {"SECOND", "WEEK", "date_trunc(\"second\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)"},
                {"SECOND", "MONTH", "date_trunc(\"second\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)"},
                {"SECOND", "QUARTER", "date_trunc(\"second\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)"},
                {"SECOND", "YEAR", "date_trunc(\"second\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"second\", `test_db`.`t0`.eventTime)"},
                {"MINUTE", "MICROSECOND", "date_trunc(\"minute\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)"},
                {"MINUTE", "MILLISECOND", "date_trunc(\"minute\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)"},
                {"MINUTE", "SECOND", "date_trunc(\"minute\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)"},
                {"MINUTE", "MINUTE", "date_trunc(\"minute\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)"},
                {"MINUTE", "HOUR", "date_trunc(\"minute\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)"},
                {"MINUTE", "DAY", "date_trunc(\"minute\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)"},
                {"MINUTE", "WEEK", "date_trunc(\"minute\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)"},
                {"MINUTE", "MONTH", "date_trunc(\"minute\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)"},
                {"MINUTE", "QUARTER", "date_trunc(\"minute\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)"},
                {"MINUTE", "YEAR", "date_trunc(\"minute\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"minute\", `test_db`.`t0`.eventTime)"},
                {"HOUR", "MICROSECOND", "date_trunc(\"hour\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)"},
                {"HOUR", "MILLISECOND", "date_trunc(\"hour\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)"},
                {"HOUR", "SECOND", "date_trunc(\"hour\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)"},
                {"HOUR", "MINUTE", "date_trunc(\"hour\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)"},
                {"HOUR", "HOUR", "date_trunc(\"hour\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)"},
                {"HOUR", "DAY", "date_trunc(\"hour\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)"},
                {"HOUR", "WEEK", "date_trunc(\"hour\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)"},
                {"HOUR", "MONTH", "date_trunc(\"hour\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)"},
                {"HOUR", "QUARTER", "date_trunc(\"hour\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)"},
                {"HOUR", "YEAR", "date_trunc(\"hour\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"hour\", `test_db`.`t0`.eventTime)"},
                {"DAY", "MICROSECOND", "date_trunc(\"day\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)"},
                {"DAY", "MILLISECOND", "date_trunc(\"day\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)"},
                {"DAY", "SECOND", "date_trunc(\"day\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)"},
                {"DAY", "MINUTE", "date_trunc(\"day\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)"},
                {"DAY", "HOUR", "date_trunc(\"day\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)"},
                {"DAY", "DAY", "date_trunc(\"day\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)"},
                {"DAY", "WEEK", "date_trunc(\"day\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)"},
                {"DAY", "MONTH", "date_trunc(\"day\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)"},
                {"DAY", "QUARTER", "date_trunc(\"day\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)"},
                {"DAY", "YEAR", "date_trunc(\"day\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"day\", `test_db`.`t0`.eventTime)"},
                {"WEEK", "MICROSECOND", "date_trunc(\"week\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)"},
                {"WEEK", "MILLISECOND", "date_trunc(\"week\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)"},
                {"WEEK", "SECOND", "date_trunc(\"week\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)"},
                {"WEEK", "MINUTE", "date_trunc(\"week\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)"},
                {"WEEK", "HOUR", "date_trunc(\"week\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)"},
                {"WEEK", "DAY", "date_trunc(\"week\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)"},
                {"WEEK", "WEEK", "date_trunc(\"week\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)"},
                {"WEEK", "MONTH", "date_trunc(\"week\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)"},
                {"WEEK", "QUARTER", "date_trunc(\"week\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)"},
                {"WEEK", "YEAR", "date_trunc(\"week\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"week\", `test_db`.`t0`.eventTime)"},
                {"MONTH", "MICROSECOND", "date_trunc(\"month\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)"},
                {"MONTH", "MILLISECOND", "date_trunc(\"month\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)"},
                {"MONTH", "SECOND", "date_trunc(\"month\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)"},
                {"MONTH", "MINUTE", "date_trunc(\"month\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)"},
                {"MONTH", "HOUR", "date_trunc(\"month\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)"},
                {"MONTH", "DAY", "date_trunc(\"month\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)"},
                {"MONTH", "WEEK", "date_trunc(\"month\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)"},
                {"MONTH", "MONTH", "date_trunc(\"month\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)"},
                {"MONTH", "QUARTER", "date_trunc(\"month\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)"},
                {"MONTH", "YEAR", "date_trunc(\"month\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"month\", `test_db`.`t0`.eventTime)"},
                {"QUARTER", "MICROSECOND", "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)"},
                {"QUARTER", "MILLISECOND", "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)"},
                {"QUARTER", "SECOND", "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)"},
                {"QUARTER", "MINUTE", "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)"},
                {"QUARTER", "HOUR", "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)"},
                {"QUARTER", "DAY", "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)"},
                {"QUARTER", "WEEK", "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)"},
                {"QUARTER", "MONTH", "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)"},
                {"QUARTER", "QUARTER", "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)"},
                {"QUARTER", "YEAR", "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"quarter\", `test_db`.`t0`.eventTime)"},
                {"YEAR", "MICROSECOND", "date_trunc(\"year\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)"},
                {"YEAR", "MILLISECOND", "date_trunc(\"year\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)"},
                {"YEAR", "SECOND", "date_trunc(\"year\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)"},
                {"YEAR", "MINUTE", "date_trunc(\"year\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)"},
                {"YEAR", "HOUR", "date_trunc(\"year\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)"},
                {"YEAR", "DAY", "date_trunc(\"year\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)"},
                {"YEAR", "WEEK", "date_trunc(\"year\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)"},
                {"YEAR", "MONTH", "date_trunc(\"year\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)"},
                {"YEAR", "QUARTER", "date_trunc(\"year\", `test_db`.`t0`.eventTime)", false,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)"},
                {"YEAR", "YEAR", "date_trunc(\"year\", `test_db`.`t0`.eventTime)", true,
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)",
                        "date_trunc(\"year\", `test_db`.`t0`.eventTime)"},
        };
        testTimeGranuleHelper(testCases, eventTime, false);
    }

    @Test
    public void testVarcharTypedTimeGranule() {
        Op op = opConverter.apply(str2Date(eventTimeS, "%Y-%m-%d %H:%i:%s"));
        TimeGranule granule = Objects.requireNonNull(TimeGranule.of(op));
        String textualGranule = opToSql.apply(granule.getOp());
        Assert.assertEquals(textualGranule, textualGranule,
                "str2date(`test_db`.`t0`.eventTimeS, \"%Y-%m-%d %H:%i:%s\")");
        Assert.assertTrue(granule.isFineGrained(TimeGranule.Unit.DAY));
        Assert.assertTrue(granule.isFineGrained(TimeGranule.Unit.MONTH));
        Assert.assertFalse(granule.isFineGrained(TimeGranule.Unit.HOUR));

        String textualWellDefinedGranule = opToSql.apply(granule.toWellFormed().getOp());
        Assert.assertEquals(textualWellDefinedGranule, textualWellDefinedGranule,
                "str2date(`test_db`.`t0`.eventTimeS, \"%Y-%m-%d %H:%i:%s\")");
        for (TimeGranule.Unit unit : TimeGranule.Unit.values()) {
            TimeGranule coarseGranule = granule.toCoarse(unit);
            Assert.assertEquals(coarseGranule, granule);
        }
    }

    @Test
    public void testSortOfTimeGranule() {
        List<TimeGranule> granules = Stream.of(
                dateTrunc("month", eventDate),
                eventTime,
                dateTrunc("minute", eventTime),
                dateTrunc("year", eventTime),
                eventDate,
                str2Date(eventTimeS, "%Y-%m-%d %H:%i:%s")
        ).map(opConverter).map(TimeGranule::of).collect(Collectors.toList());
        for (int i = 0; i < 10; ++i) {
            Collections.shuffle(granules);
            Optional<TimeGranule> optMaxGranule = granules.stream().max(TimeGranule.getComparator());
            Optional<TimeGranule> optMinGranule = granules.stream().min(TimeGranule.getComparator());
            Assert.assertTrue(optMaxGranule.isPresent());
            Assert.assertTrue(optMinGranule.isPresent());
            String textualMaxGranule = opToSql.apply(optMaxGranule.get().getOp());
            String textualMinGranule = opToSql.apply(optMinGranule.get().getOp());
            Assert.assertEquals(textualMaxGranule, textualMaxGranule,
                    "date_trunc(\"year\", `test_db`.`t0`.eventTime)");
            Assert.assertEquals(textualMinGranule, textualMinGranule,
                    "`test_db`.`t0`.eventTime");
        }
    }
}
