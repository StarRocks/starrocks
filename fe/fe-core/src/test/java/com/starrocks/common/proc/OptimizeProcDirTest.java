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


package com.starrocks.common.proc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Database;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.util.OrderByPair;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class OptimizeProcDirTest {
    private Database db;
    private OptimizeProcDir optimizeProcDir;

    @Before
    public void setUp() throws DdlException, AnalysisException {
        db = new Database(10000L, "db1");
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        optimizeProcDir = new OptimizeProcDir(schemaChangeHandler, db);

        List<List<Comparable>> infos = Lists.newArrayList();

        List<Comparable> info = Lists.newArrayList();
        info.add(1);
        info.add("tb1");
        info.add("2020-01-01");
        info.add("2020-01-01");
        info.add("ALTER");
        info.add(0);
        info.add("FINISHED");
        info.add("");
        info.add(100);
        info.add(10000);
        infos.add(info);

        List<Comparable> info2 = Lists.newArrayList();
        info2.add(2);
        info2.add("tb2");
        info2.add("2020-01-02");
        info2.add("2020-01-02");
        info2.add("ALTER");
        info2.add(0);
        info2.add("CANCELLED");
        info2.add("");
        info2.add(100);
        info2.add(10000);
        infos.add(info);

        new Expectations(optimizeProcDir) {
            {
                optimizeProcDir.getOptimizeJobInfos();
                minTimes = 0;
                result = infos;
            }
        };
    }

    @Test
    public void testFetchResult() throws AnalysisException {
        BaseProcResult result = (BaseProcResult) optimizeProcDir.fetchResult();
        List<List<String>> rows = result.getRows();
        List<String> list1 = rows.get(0);
        Assert.assertEquals(list1.size(), OptimizeProcDir.TITLE_NAMES.size());
        // JobId
        Assert.assertEquals("1", list1.get(0));
        // TableName
        Assert.assertEquals("tb1", list1.get(1));
        // CreateTime
        Assert.assertEquals("2020-01-01", list1.get(2));
        // FinishTime
        Assert.assertEquals("2020-01-01", list1.get(3));
        // Operation
        Assert.assertEquals("ALTER", list1.get(4));
        // TransactionId
        Assert.assertEquals("0", list1.get(5));
        // State
        Assert.assertEquals("FINISHED", list1.get(6));
        // Msg
        Assert.assertEquals("", list1.get(7));
        // Progress
        Assert.assertEquals("100", list1.get(8));
        // Timeout
        Assert.assertEquals("10000", list1.get(9));

        List<String> list2 = rows.get(1);
        Assert.assertEquals(list2.size(), OptimizeProcDir.TITLE_NAMES.size());
        // JobId
        Assert.assertEquals("1", list2.get(0));
        // TableName
        Assert.assertEquals("tb1", list2.get(1));
        // CreateTime
        Assert.assertEquals("2020-01-01", list2.get(2));
        // FinishTime
        Assert.assertEquals("2020-01-01", list2.get(3));
        // Operation
        Assert.assertEquals("ALTER", list2.get(4));
        // TransactionId
        Assert.assertEquals("0", list2.get(5));
        // State
        Assert.assertEquals("FINISHED", list2.get(6));
        // Msg
        Assert.assertEquals("", list2.get(7));
        // Progress
        Assert.assertEquals("100", list2.get(8));
        // Timeout
        Assert.assertEquals("10000", list2.get(9));

    }

    @Test(expected = AnalysisException.class)
    public void testLookup() throws AnalysisException {
        optimizeProcDir.lookup("");
    }

    @Test
    public void testRegister() {
        Assert.assertFalse(optimizeProcDir.register(null, null));
    }

    @Test(expected = AnalysisException.class)
    public void testAnalyzeColumn() throws AnalysisException {
        Assert.assertEquals(optimizeProcDir.analyzeColumn("jobId"), 0);
        optimizeProcDir.analyzeColumn("Database");
    }

    @Test
    public void testFetchResultByFilterNull() throws AnalysisException {
        BaseProcResult result = (BaseProcResult) optimizeProcDir.fetchResultByFilter(null, null, null);
        List<List<String>> rows = result.getRows();
        List<String> list1 = rows.get(0);
        Assert.assertEquals(list1.size(), OptimizeProcDir.TITLE_NAMES.size());
        // JobId
        Assert.assertEquals("1", list1.get(0));
        // TableName
        Assert.assertEquals("tb1", list1.get(1));
        // CreateTime
        Assert.assertEquals("2020-01-01", list1.get(2));
        // FinishTime
        Assert.assertEquals("2020-01-01", list1.get(3));
        // Operation
        Assert.assertEquals("ALTER", list1.get(4));
        // TransactionId
        Assert.assertEquals("0", list1.get(5));
        // State
        Assert.assertEquals("FINISHED", list1.get(6));
        // Msg
        Assert.assertEquals("", list1.get(7));
        // Progress
        Assert.assertEquals("100", list1.get(8));
        // Timeout
        Assert.assertEquals("10000", list1.get(9));

        List<String> list2 = rows.get(1);
        Assert.assertEquals(list2.size(), OptimizeProcDir.TITLE_NAMES.size());
        // JobId
        Assert.assertEquals("1", list2.get(0));
        // TableName
        Assert.assertEquals("tb1", list2.get(1));
        // CreateTime
        Assert.assertEquals("2020-01-01", list2.get(2));
        // FinishTime
        Assert.assertEquals("2020-01-01", list2.get(3));
        // Operation
        Assert.assertEquals("ALTER", list2.get(4));
        // TransactionId
        Assert.assertEquals("0", list2.get(5));
        // State
        Assert.assertEquals("FINISHED", list2.get(6));
        // Msg
        Assert.assertEquals("", list2.get(7));
        // Progress
        Assert.assertEquals("100", list2.get(8));
        // Timeout
        Assert.assertEquals("10000", list2.get(9));

    }

    @Test
    public void testFetchResultByFilter() throws AnalysisException {
        HashMap<String, Expr> filter = Maps.newHashMap();
        filter.put("jobId", new BinaryPredicate(BinaryType.EQ, new StringLiteral(), new StringLiteral("1")));

        ArrayList<OrderByPair> orderByPairs = Lists.newArrayList();
        orderByPairs.add(new OrderByPair(0));

        LimitElement limitElement = new LimitElement(1);

        BaseProcResult result = (BaseProcResult) optimizeProcDir.fetchResultByFilter(
                filter, orderByPairs, limitElement);

        List<List<String>> rows = result.getRows();
        List<String> list1 = rows.get(0);
        Assert.assertEquals(list1.size(), OptimizeProcDir.TITLE_NAMES.size());
        // JobId
        Assert.assertEquals("1", list1.get(0));
        // TableName
        Assert.assertEquals("tb1", list1.get(1));
        // CreateTime
        Assert.assertEquals("2020-01-01", list1.get(2));
        // FinishTime
        Assert.assertEquals("2020-01-01", list1.get(3));
        // Operation
        Assert.assertEquals("ALTER", list1.get(4));
        // TransactionId
        Assert.assertEquals("0", list1.get(5));
        // State
        Assert.assertEquals("FINISHED", list1.get(6));
        // Msg
        Assert.assertEquals("", list1.get(7));
        // Progress
        Assert.assertEquals("100", list1.get(8));
        // Timeout
        Assert.assertEquals("10000", list1.get(9));

    }
}
