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

package com.starrocks.qe.scheduler.plan;

import com.starrocks.qe.scheduler.SchedulerTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

@RunWith(Parameterized.class)
public class SchedulerTPCHTest extends SchedulerTestBase {
    private final String fileName;

    public SchedulerTPCHTest(String fileName) {
        this.fileName = fileName;
    }

    @Test
    public void testTPCH() {
        runFileUnitTest(fileName);
    }

    @Parameters(name = "{0}")
    public static Collection<String> getTPCHTestFileNames() {
        return listTestFileNames("scheduler/tpch/");
    }
}
