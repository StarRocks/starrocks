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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @BeforeEach
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
        Assertions.assertFalse(routineLoadsProcDir.register("test", new BaseProcDir()));
    }

    @Test
    public void testLookup() throws AnalysisException {
        ProcNodeInterface node = routineLoadsProcDir.lookup("test_job");
        Assertions.assertNotNull(node);
        Assertions.assertTrue(node instanceof RoutineLoadsNameProcDir);
    }

    @Test
    public void testLookupWithEmptyName() {
        assertThrows(IllegalArgumentException.class, () -> routineLoadsProcDir.lookup(""));
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
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result instanceof BaseProcResult);

        List<String> expectedNames = ImmutableList.of("Name", "Id", "DbName", "Statistic", "TaskStatistic");
        Assertions.assertEquals(expectedNames, result.getColumnNames());

        List<List<String>> rows = result.getRows();
        Assertions.assertEquals(2, rows.size());

        Assertions.assertEquals(Arrays.asList("job1", "1", "db1", "RUNNING", "Task"), rows.get(0));
        Assertions.assertEquals(Arrays.asList("job2", "2", "db2", "RUNNING", "Task"), rows.get(1));
    }

    @Test
    public void testFetchResultWithException() {
        assertThrows(AnalysisException.class, () -> {
            new Expectations() {
                {
                    routineLoadMgr.getJob(null, null, true);
                    result = new MetaNotFoundException("test exception");
                }
            };

            routineLoadsProcDir.fetchResult();
        });
    }
} 