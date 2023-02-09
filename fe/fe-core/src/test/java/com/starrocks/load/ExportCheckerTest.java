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

package com.starrocks.load;

import com.starrocks.common.UserException;
import com.starrocks.load.ExportJob.JobState;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class ExportCheckerTest {
    
    @Test
    public void testCheckBeStatus() throws NoSuchFieldException, SecurityException, 
        IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        new MockUp<ExportJob>() {
            @Mock
            public synchronized void cancel(ExportFailMsg.CancelType type, String msg) throws UserException {
            }
        };

        Backend be = new Backend();
        
        new MockUp<SystemInfoService>() {
            @Mock
            public Backend getBackend(long backendId) {
                return be;
            }
        };

        ExportChecker.init(1000L);
        Field field = ExportChecker.class.getDeclaredField("checkers");
        field.setAccessible(true);
        Object obj = field.get(ExportChecker.class);
        
        Map<JobState, ExportChecker> map = (Map<JobState, ExportChecker>) obj;
        ExportChecker checker = map.get(JobState.EXPORTING);
        Method method = ExportChecker.class.getDeclaredMethod("checkJobNeedCancel", ExportJob.class);
        method.setAccessible(true);

        ExportJob job = new ExportJob();
        job.setBeStartTime(1, 1000L);
        boolean cancelled = (boolean) method.invoke(checker, job);
        Assert.assertTrue(cancelled);

        be.setAlive(true);
        be.setIsDecommissioned(true);

        be.setLastStartTime(1001L);

        cancelled = (boolean) method.invoke(checker, job);
        Assert.assertTrue(cancelled);

        be.setLastStartTime(999L);

        cancelled = (boolean) method.invoke(checker, job);
        Assert.assertTrue(!cancelled);

        new MockUp<SystemInfoService>() {
            @Mock
            public Backend getBackend(long backendId) {
                return null;
            }
        };

        cancelled = (boolean) method.invoke(checker, job);
        Assert.assertTrue(cancelled);
    }
}
