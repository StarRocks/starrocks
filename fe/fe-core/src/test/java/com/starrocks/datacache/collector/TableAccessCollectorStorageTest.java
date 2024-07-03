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

package com.starrocks.datacache.collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class TableAccessCollectorStorageTest {

    @Test
    public void testAddAccessLogs() {
        TableAccessCollectorStorage storage = new TableAccessCollectorStorage();
        List<AccessLog> accessLogs = new LinkedList<>();
        accessLogs.add(new AccessLog("catalog", "db", "table", "partition", "column1", 1));
        accessLogs.add(new AccessLog("catalog", "db", "table", "partition", "column1", 1, 2));
        accessLogs.add(new AccessLog("catalog", "db", "table", "partition", "column2", 1));
        accessLogs.add(new AccessLog("catalog", "db", "tbl", "par", "column", 2, 3));
        storage.addAccessLogs(accessLogs);
        Assert.assertEquals(97, storage.getEstimateMemorySize());
        List<AccessLog> export = storage.exportAccessLogs();
        Assert.assertEquals(3, export.size());
        for (int i = 0; i < export.size(); i++) {
            AccessLog accessLog = export.get(i);
            if (i == 0) {
                Assert.assertEquals(new AccessLog("catalog", "db", "table", "partition", "column1", 1, 3), accessLog);
            } else if (i == 1) {
                Assert.assertEquals(new AccessLog("catalog", "db", "table", "partition", "column2", 1, 1), accessLog);
            } else if (i == 2) {
                Assert.assertEquals(new AccessLog("catalog", "db", "tbl", "par", "column", 2, 3), accessLog);
            }
        }
    }
}
