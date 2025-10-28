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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Strings;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksTestBase;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

public class MVTraceExtension implements BeforeEachCallback, AfterEachCallback, TestWatcher {
    private long startTime;

    @Override
    public void beforeEach(ExtensionContext context) {
        startTime = System.currentTimeMillis();
        // Get the test instance if it's MVTestBase
        Object testInstance = context.getRequiredTestInstance();
        if (testInstance instanceof StarRocksTestBase) {
            StarRocksTestBase mvTest = (StarRocksTestBase) testInstance;
            ConnectContext connectContext = mvTest.starRocksAssert != null ? mvTest.starRocksAssert.getCtx() : null;
            if (connectContext == null) {
                connectContext = new ConnectContext();
            }
            Tracers.register(connectContext);
            Tracers.init(connectContext, Tracers.Mode.LOGS, "MV");
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        long endTime = System.currentTimeMillis();
        System.out.println("Test " + context.getDisplayName() + " failed in " + (endTime - startTime) + " ms");
        String pr = Tracers.printLogs();
        if (!Strings.isNullOrEmpty(pr)) {
            System.out.println(pr);
        }
        Tracers.close();
    }
}