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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class RoutineLoadsProcDirTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private RoutineLoadMgr routineLoadMgr;

    @Mocked
    private RoutineLoadJob routineLoadJob1;

    @Mocked
    private RoutineLoadJob routineLoadJob2;

    private RoutineLoadsProcDir routineLoadsProcDir;

    @Before
    public void setUp() {
        routineLoadsProcDir = new RoutineLoadsProcDir();

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getRoutineLoadMgr();
                minTimes = 0;
                result = routineLoadMgr;
            }
        };
    }

    @Test
    public void testRegister() {
        Assert.assertFalse(routineLoadsProcDir.register("test", new BaseProcDir()));
    }

    @Test
    public void testLookup() throws AnalysisException {
        ProcNodeInterface node = routineLoadsProcDir.lookup("test_job");
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof RoutineLoadsNameProcDir);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupWithEmptyName() throws AnalysisException {
        routineLoadsProcDir.lookup("");
    }

    @Test
    public void testFetchResultNormal() throws MetaNotFoundException, AnalysisException {
        new Expectations() {
            {
                routineLoadMgr.getJob(null, null, true);
                result = Lists.newArrayList(routineLoadJob1, routineLoadJob2);

                routineLoadJob1.getShowStatistic();
                result = Arrays.asList("job1", "1", "db1", "RUNNING", "Task");

                routineLoadJob2.getShowStatistic();
                result = Arrays.asList("job2", "2", "db2", "RUNNING", "Task");
            }
        };

        ProcResult result = routineLoadsProcDir.fetchResult();
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);

        List<String> expectedNames = ImmutableList.of("Name", "Id", "DbName", "Statistic", "TaskStatistic");
        Assert.assertEquals(expectedNames, result.getColumnNames());

        List<List<String>> rows = result.getRows();
        Assert.assertEquals(2, rows.size());

        Assert.assertEquals(Arrays.asList("job1", "1", "db1", "RUNNING", "Task"), rows.get(0));
        Assert.assertEquals(Arrays.asList("job2", "2", "db2", "RUNNING", "Task"), rows.get(1));
    }

    @Test(expected = AnalysisException.class)
    public void testFetchResultWithException() throws MetaNotFoundException, AnalysisException {
        new Expectations() {
            {
                routineLoadMgr.getJob(null, null, true);
                result = new MetaNotFoundException("test exception");
            }
        };

        routineLoadsProcDir.fetchResult();
    }
} 