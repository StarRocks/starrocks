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

package com.starrocks.server;

import com.starrocks.common.ErrorReportException;
import com.starrocks.common.ExceptionChecker;
import org.junit.Test;

public class WarehouseManagerTest {

    @Test
    public void testWarehouseNotExist() {
        WarehouseManager mgr = new WarehouseManager();
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse name: a not exist.",
                () -> mgr.getWarehouse("a"));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getWarehouse(1L));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse name: a not exist.",
                () -> mgr.getAllComputeNodeIds("a"));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getAllComputeNodeIds(1L));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse name: a not exist.",
                () -> mgr.getComputeNodeId("a", null));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getComputeNodeId(1L, null));
    }
}
