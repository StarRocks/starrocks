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
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SchemaChangeProcDirTest {
    private Database db;
    private SchemaChangeProcDir schemaChangeProcDir;

    private static List<Comparable> job(int jobId, String tableName, String state) {
        List<Comparable> info = Lists.newArrayList();
        info.add(jobId);          // JobId
        info.add(tableName);      // TableName
        info.add("2020-01-01");   // CreateTime
        info.add("2020-01-02");   // FinishTime
        info.add("index");        // IndexName
        info.add(200);            // IndexId
        info.add(100);            // OriginIndexId
        info.add(1);              // SchemaVersion
        info.add(0);              // TransactionId
        info.add(state);          // State
        info.add("");             // Msg
        info.add(100);            // Progress
        info.add(10000);          // Timeout
        // This layout's width is decided at class-load time - shared-data mode appends a Warehouse
        // column - so the fixture is built to whatever the list reports rather than to a fixed 13,
        // and the assertions below index by position into a row of that width.
        while (info.size() < SchemaChangeProcDir.TITLE_NAMES.size()) {
            info.add("default_warehouse");
        }
        return info;
    }

    @BeforeEach
    public void setUp() {
        db = new Database(10000L, "db1");
        SchemaChangeHandler handler = new SchemaChangeHandler();
        schemaChangeProcDir = new SchemaChangeProcDir(handler, db);

        List<List<Comparable>> infos = Lists.newArrayList();
        infos.add(job(1, "tb1", "FINISHED"));
        infos.add(job(2, "tb2", "RUNNING"));

        // SchemaChangeHandler extends Thread (via FrontendDaemon), which JMockit refuses to
        // partially mock, so stub the method with a MockUp instead of Expectations.
        new MockUp<SchemaChangeHandler>() {
            @Mock
            public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
                return infos;
            }
        };
    }

    @Test
    public void testFetchResultTakesTheSameRoad() throws AnalysisException {
        // SHOW PROC and the proc HTTP endpoints call fetchResult rather than fetchResultByFilter,
        // and it used to build its answer with its own copy of the loop. Both roads now go through
        // ProcUtils.toProcResult, and this is what keeps that from quietly coming apart.
        BaseProcResult result = (BaseProcResult) schemaChangeProcDir.fetchResult();
        List<List<String>> rows = result.getRows();

        Assertions.assertEquals(SchemaChangeProcDir.TITLE_NAMES, result.getColumnNames());
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(SchemaChangeProcDir.TITLE_NAMES.size(), rows.get(0).size());

        // Every cell arrives stringified, which is what the shared builder does.
        List<String> first = rows.get(0);
        Assertions.assertEquals("1", first.get(0));
        Assertions.assertEquals("tb1", first.get(1));
        Assertions.assertEquals("2020-01-01", first.get(2));
        Assertions.assertEquals("2020-01-02", first.get(3));
        Assertions.assertEquals("200", first.get(5));
        Assertions.assertEquals("FINISHED", first.get(9));
        Assertions.assertEquals("10000", first.get(12));
    }

    @Test
    public void testFetchResultAgreesWithTheFilteredRoad() throws AnalysisException {
        // With no predicate the two roads have nothing to differ about, so they must not.
        Assertions.assertEquals(
                ((BaseProcResult) schemaChangeProcDir.fetchResultByFilter(null, null, null)).getRows(),
                ((BaseProcResult) schemaChangeProcDir.fetchResult()).getRows());
    }
}
