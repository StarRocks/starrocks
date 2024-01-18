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

import com.starrocks.common.structure.Status;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.ExportPendingTask;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TStatusCode;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

public class ExportPendingTaskTest {

    @Test
    public void testMakeSnapshots(@Mocked ExportJob job) throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException {

        ComputeNode node = new ComputeNode(4L, "127.0.0.1", 12345);

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long backendId) {
                return node;
            }

            @Mock
            public ComputeNode getBackendOrComputeNodeWithBePort(String host, int bePort) {
                return node;
            }
        };

        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("127.0.0.1", 12346));
        scanRangeLocations.addToLocations(scanRangeLocation);
        TScanRange scanRange = new TScanRange();
        scanRange.setInternal_scan_range(new TInternalScanRange());
        scanRangeLocations.setScan_range(scanRange);

        new Expectations() {
            {
                job.getTabletLocations();
                result = Collections.singletonList(scanRangeLocations);
                minTimes = 0;

                job.exportLakeTable();
                result = true;
                minTimes = 0;
            }
        };

        ExportPendingTask task = new ExportPendingTask(job);

        Method method = ExportPendingTask.class.getDeclaredMethod("makeSnapshots");
        method.setAccessible(true);

        Status status = (Status) method.invoke(task, null);
        Assert.assertEquals(Status.CANCELLED, status);

        node.setAlive(true);
        status = (Status) method.invoke(task, null);
        Assert.assertEquals(TStatusCode.CANCELLED, status.getErrorCode());
    }
}

