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

import com.starrocks.common.DdlException;
import com.starrocks.qe.scheduler.SchedulerConnectorTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;

public class SchedulerHiveTPCHTest extends SchedulerConnectorTestBase {
    private String fileName;

    public void initSchedulerHiveTPCHTest(String fileName) {
        this.fileName = fileName;
    }

    @BeforeEach
    public void before() throws DdlException {
        connectContext.changeCatalogDb("hive0.tpch");
    }

    @MethodSource("getTPCHTestFileNames")
    @ParameterizedTest(name = "{0}")
    public void testTPCH(String fileName) {
        initSchedulerHiveTPCHTest(fileName);
        runFileUnitTest(fileName);
    }

    public static Collection<String> getTPCHTestFileNames() {
        return listTestFileNames("scheduler/external/hive/tpch/");
    }
}
