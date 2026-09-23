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
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.OrderByPair;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

public class RollupProcDirTest {
    private Database db;
    private RollupProcDir rollupProcDir;

    private static List<Comparable> job(int jobId, String tableName, String state) {
        List<Comparable> info = Lists.newArrayList();
        info.add(jobId);           // JobId
        info.add(tableName);       // TableName
        info.add("2020-01-0" + jobId); // CreateTime
        info.add("2020-01-0" + jobId); // FinishedTime
        info.add("base");          // BaseIndexName
        info.add("rollup");        // RollupIndexName
        info.add(100 + jobId);     // RollupId
        info.add(0);               // TransactionId
        info.add(state);           // State
        info.add("");              // Msg
        info.add(100);             // Progress
        info.add(10000);           // Timeout
        return info;
    }

    @BeforeEach
    public void setUp() {
        db = new Database(10000L, "db1");
        MaterializedViewHandler handler = new MaterializedViewHandler();
        rollupProcDir = new RollupProcDir(handler, db);

        List<List<Comparable>> infos = Lists.newArrayList();
        infos.add(job(1, "tb1", "FINISHED"));
        infos.add(job(2, "tb2", "RUNNING"));
        infos.add(job(3, "tb1", "FINISHED"));

        // MaterializedViewHandler extends Thread (via FrontendDaemon), which JMockit refuses to
        // partially mock, so stub the method with a MockUp instead of Expectations.
        new MockUp<MaterializedViewHandler>() {
            @Mock
            public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
                return infos;
            }
        };
    }

    @Test
    public void testFetchResultByFilterNull() throws AnalysisException {
        BaseProcResult result = (BaseProcResult) rollupProcDir.fetchResultByFilter(null, null, null);
        List<List<String>> rows = result.getRows();
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals(RollupProcDir.TITLE_NAMES.size(), rows.get(0).size());
        Assertions.assertEquals("1", rows.get(0).get(0));
    }

    @Test
    public void testWhereIsApplied() throws AnalysisException {
        HashMap<String, Expr> filter = Maps.newHashMap();
        filter.put("tablename", new BinaryPredicate(BinaryType.EQ,
                new SlotRef(null, "TableName"), new StringLiteral("tb2")));

        BaseProcResult result = (BaseProcResult) rollupProcDir.fetchResultByFilter(filter, null, null);
        List<List<String>> rows = result.getRows();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("2", rows.get(0).get(0));
        Assertions.assertEquals("tb2", rows.get(0).get(1));
    }

    @Test
    public void testOrderByIsApplied() throws AnalysisException {
        // JobId descending: the natural order this dir returns is ascending, so an unapplied
        // ORDER BY would leave job 1 first.
        List<OrderByPair> orderByPairs = Lists.newArrayList(new OrderByPair(0, true));

        BaseProcResult result = (BaseProcResult) rollupProcDir.fetchResultByFilter(null, orderByPairs, null);
        List<List<String>> rows = result.getRows();
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals("3", rows.get(0).get(0));
        Assertions.assertEquals("1", rows.get(2).get(0));
    }

    @Test
    public void testLimitIsApplied() throws AnalysisException {
        List<OrderByPair> orderByPairs = Lists.newArrayList(new OrderByPair(0, true));

        BaseProcResult result = (BaseProcResult) rollupProcDir.fetchResultByFilter(
                null, orderByPairs, new LimitElement(0, 1));
        List<List<String>> rows = result.getRows();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("3", rows.get(0).get(0));
    }

    @Test
    public void testAnalyzeColumnUsesThisDirsLayout() throws AnalysisException {
        Assertions.assertEquals(0, RollupProcDir.analyzeColumn("JobId"));
        // State sits at 8 here and at 9 in SchemaChangeProcDir; resolving one against the other
        // is what made an ORDER BY State sort by the wrong column.
        Assertions.assertEquals(8, RollupProcDir.analyzeColumn("State"));
        Assertions.assertEquals(5, RollupProcDir.analyzeColumn("RollupIndexName"));
        Assertions.assertThrows(AnalysisException.class, () -> RollupProcDir.analyzeColumn("SchemaVersion"));
    }
}
