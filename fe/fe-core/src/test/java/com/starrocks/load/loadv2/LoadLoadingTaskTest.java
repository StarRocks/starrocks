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

package com.starrocks.load.loadv2;

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class LoadLoadingTaskTest {
    @Test
    public void testBuilder(
            @Mocked GlobalStateMgr globalStateMgr) {

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNextId();
                result = 1;
                times = 1;
            }};

        // json options
        LoadJob.JSONOptions jsonOptions = new LoadJob.JSONOptions();
        jsonOptions.jsonRoot = "\"$.data\"";
        jsonOptions.jsonPaths = "[\"$.key1\"]";
        jsonOptions.stripOuterArray = true;

        LoadLoadingTask task = new LoadLoadingTask.Builder()
                .setJSONOptions(jsonOptions)
                .build();


        LoadJob.JSONOptions realJSONOptions =  Deencapsulation.getField(task, "jsonOptions");
        Assert.assertEquals(jsonOptions, realJSONOptions);
    }
}
