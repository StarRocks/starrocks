// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
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

        new MockUp<Backend>() {
            @Mock
            public boolean isAvailable() {
                return false;
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

        new MockUp<Backend>() {
            @Mock
            public boolean isAlive() {
                return true;
            }
        };

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
