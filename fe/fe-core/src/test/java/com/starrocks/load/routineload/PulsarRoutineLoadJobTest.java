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

package com.starrocks.load.routineload;

import com.starrocks.common.jmockit.Deencapsulation;
import org.junit.Assert;
import org.junit.Test;

public class PulsarRoutineLoadJobTest {

    @Test
    public void testGetStatistic() {
        RoutineLoadJob job = new PulsarRoutineLoadJob(1L, "routine_load", 1L, 1L, "127.0.0.1:9020", "topic1", "");
        Deencapsulation.setField(job, "receivedBytes", 10);
        Deencapsulation.setField(job, "totalRows", 20);
        Deencapsulation.setField(job, "errorRows", 2);
        Deencapsulation.setField(job, "unselectedRows", 2);
        Deencapsulation.setField(job, "totalTaskExcutionTimeMs", 1000);
        String statistic = job.getStatistic();
        Assert.assertTrue(statistic.contains("\"receivedBytesRate\":10"));
        Assert.assertTrue(statistic.contains("\"loadRowsRate\":16"));
    }


    @Test
    public void testGetSourceLagString() {
        RoutineLoadJob job = new PulsarRoutineLoadJob(1L, "routine_load", 1L, 1L, "127.0.0.1:9020", "topic1", "");
        String sourceLagString = job.getSourceLagString(null);
        Assert.assertTrue(sourceLagString.equals(""));
    }


}
