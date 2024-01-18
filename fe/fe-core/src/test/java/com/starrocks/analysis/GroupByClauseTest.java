// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.starrocks.common.exception.AnalysisException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;

public class GroupByClauseTest {

    private Analyzer analyzer;

    @Before
    public void setUp() {
        Analyzer analyzerBase = AccessTestUtil.fetchTableAnalyzer();
        analyzer = new Analyzer(analyzerBase.getCatalog(), analyzerBase.getContext());
        try {
            Field f = analyzer.getClass().getDeclaredField("tupleByAlias");
            f.setAccessible(true);
            Multimap<String, TupleDescriptor> tupleByAlias = ArrayListMultimap.create();
            TupleDescriptor td = new TupleDescriptor(new TupleId(0));
            td.setTable(analyzerBase.getTable(new TableName("testdb", "t")));
            tupleByAlias.put("testdb.t", td);
            f.set(analyzer, tupleByAlias);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGroupBy() {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k2", "k2", "k3", "k1"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName("testdb", "t"), col);
            groupingExprs.add(expr);
        }

        GroupByClause groupByClause = new GroupByClause(Expr.cloneList(groupingExprs),
                GroupByClause.GroupingType.GROUP_BY);
        try {
            groupByClause.analyze(analyzer);
        } catch (AnalysisException execption) {
            Assert.assertTrue(false);
        }
        Assert.assertEquals("`testdb`.`t`.`k2`, `testdb`.`t`.`k2`, `testdb`.`t`.`k3`, `testdb`.`t`.`k1`",
                groupByClause.toSql());
        Assert.assertEquals(3, groupByClause.getGroupingExprs().size());
        groupingExprs.remove(0);
        Assert.assertEquals(groupByClause.getGroupingExprs(), groupingExprs);
    }

    @Test
    public void testReset() {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k2", "k2", "k3", "k1"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName("testdb", "t"), col);
            groupingExprs.add(expr);
        }

        GroupByClause groupByClause = new GroupByClause(Expr.cloneList(groupingExprs),
                GroupByClause.GroupingType.GROUP_BY);
        try {
            groupByClause.analyze(analyzer);
        } catch (AnalysisException execption) {
            Assert.assertTrue(false);
        }
        try {
            groupByClause.reset();
        } catch (Exception e) {
            Assert.fail("reset throw exceptions!" + e);
        }
    }

    @Test
    public void testGetTuple() throws AnalysisException {
        ArrayList<Expr> groupingExprs = new ArrayList<>();
        String[] cols = {"k1", "k2", "k3", "k1"};
        for (String col : cols) {
            Expr expr = new SlotRef(new TableName("testdb", "t"), col);
            groupingExprs.add(expr);
        }
        GroupByClause groupByClause = new GroupByClause(Expr.cloneList(groupingExprs),
                GroupByClause.GroupingType.GROUP_BY);
        try {
            groupByClause.analyze(analyzer);
        } catch (AnalysisException exception) {
            Assert.assertTrue(false);
        }
    }
}
