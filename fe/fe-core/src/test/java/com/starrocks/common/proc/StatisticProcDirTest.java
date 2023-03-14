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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.SystemId;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StatisticProcDirTest {

    private Database db1;
    private Database db2;
    private Database infomation;
    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Before
    public void setUp() {
        db1 = new Database(10000L, "db1");
        db2 = new Database(10001L, "db2");
        infomation = new InfoSchemaDb();
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getDbIds();
                minTimes = 0;
                result = Lists.newArrayList(10000L, 10001L, SystemId.INFORMATION_SCHEMA_DB_ID);

                globalStateMgr.getDb(10000L);
                minTimes = 0;
                result = db1;

                globalStateMgr.getDb(10001L);
                minTimes = 0;
                result = db2;

                globalStateMgr.getDb(SystemId.INFORMATION_SCHEMA_DB_ID);
                minTimes = 0;
                result = infomation;

            }
        };
    }

    @After
    public void tearDown() {
        globalStateMgr = null;
    }

    @Test(expected = AnalysisException.class)
    public void testLookupInvalid() throws AnalysisException {
        new StatisticProcDir(GlobalStateMgr.getCurrentState()).lookup("12345");
    }

    @Test
    public void testFilterInformationSchemaDb() throws AnalysisException {
        StatisticProcDir statisticProcDir;
        statisticProcDir = new StatisticProcDir(globalStateMgr);
        ProcResult result = statisticProcDir.fetchResult();
        Assert.assertEquals(result.getRows().size(), 3);
    }
}
