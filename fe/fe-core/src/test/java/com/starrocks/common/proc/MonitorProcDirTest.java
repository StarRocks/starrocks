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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.ExceptionChecker;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MonitorProcDirTest {

    @Test
    public void testFetchResult() throws AnalysisException {
        MonitorProcDir dir = new MonitorProcDir();
        ProcResult result = dir.fetchResult();
        
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);

        Assert.assertEquals(MonitorProcDir.TITLE_NAMES, result.getColumnNames());

        List<List<String>> rows = result.getRows();
        Assert.assertEquals(1, rows.size());
        List<String> row = rows.get(0);
        Assert.assertEquals(2, row.size());
        Assert.assertEquals("jvm", row.get(0));
    }
    
    @Test
    public void testRegister() {
        MonitorProcDir dir = new MonitorProcDir();
        Assert.assertFalse(dir.register("test", new BaseProcDir()));
    }
    
    @Test
    public void testLookupNormal() throws AnalysisException {
        MonitorProcDir dir = new MonitorProcDir();
        ProcNodeInterface node = dir.lookup("jvm");
        
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof JvmMonitorProcDir);
    }
    
    @Test
    public void testLookupInvalid() {
        MonitorProcDir dir = new MonitorProcDir();

        ExceptionChecker.expectThrows(AnalysisException.class, () -> dir.lookup(null));
        ExceptionChecker.expectThrows(AnalysisException.class, () -> dir.lookup(""));

        ExceptionChecker.expectThrows(AnalysisException.class, () -> dir.lookup("unknown"));
    }
} 