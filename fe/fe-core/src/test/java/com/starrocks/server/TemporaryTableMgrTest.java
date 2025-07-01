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


import com.google.common.collect.Table;
import com.starrocks.common.util.UUIDUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class TemporaryTableMgrTest {

    @Test
    public void testBasicOperations() throws Exception {
        TemporaryTableMgr temporaryTableMgr = new TemporaryTableMgr();
        UUID sessionId1 = UUIDUtil.genUUID();
        UUID sessionId2 = UUIDUtil.genUUID();

        temporaryTableMgr.addTemporaryTable(sessionId1, 1L, "table1", 1L);
        temporaryTableMgr.addTemporaryTable(sessionId1, 2L, "table2", 2L);
        temporaryTableMgr.addTemporaryTable(sessionId2, 1L, "table1", 3L);
        temporaryTableMgr.addTemporaryTable(sessionId2, 1L, "table2", 4L);

        Assertions.assertTrue(temporaryTableMgr.tableExists(sessionId1, 1L, "table1"));
        long tableId = temporaryTableMgr.getTable(sessionId1, 1L, "table1");
        Assertions.assertEquals(tableId, 1L);

        Assertions.assertFalse(temporaryTableMgr.tableExists(sessionId1, 1L, "table2"));
        Assertions.assertEquals(temporaryTableMgr.getTable(sessionId1, 1L, "table2"), null);

        {
            List<String> tables = temporaryTableMgr.listTemporaryTables(sessionId1, 1L);
            List<String> expected = Arrays.asList("table1");
            Assertions.assertTrue(tables.size() == expected.size() && tables.containsAll(expected));

            tables = temporaryTableMgr.listTemporaryTables(UUIDUtil.genUUID(), 1L);
            Assertions.assertTrue(tables.isEmpty());
        }

        {
            List<String> tables = temporaryTableMgr.listTemporaryTables(sessionId1, 3L);
            Assertions.assertTrue(tables.isEmpty());
        }

        {
            Set<Long> dbIds = new HashSet<>(Arrays.asList(1L));
            Table<Long, Long, UUID> actual = temporaryTableMgr.getAllTemporaryTables(dbIds);

            Assertions.assertEquals(actual.size(), 3);
            Assertions.assertTrue(actual.containsRow(1L));
            Assertions.assertTrue(actual.row(1L).size() == 3);
            Assertions.assertTrue(actual.row(1L).get(1L) == sessionId1);
            Assertions.assertTrue(actual.row(1L).get(3L) == sessionId2);
            Assertions.assertTrue(actual.row(1L).get(4L) == sessionId2);
        }

        Assertions.assertEquals(temporaryTableMgr.listSessions().size(), 2);

        temporaryTableMgr.dropTemporaryTable(sessionId1, 1L, "table1");
        Assertions.assertFalse(temporaryTableMgr.tableExists(sessionId1, 1L, "table1"));

        temporaryTableMgr.dropTemporaryTable(UUIDUtil.genUUID(), 1L, "table1");

        temporaryTableMgr.removeTemporaryTables(sessionId1);

        Assertions.assertEquals(temporaryTableMgr.listSessions().size(), 1);

        temporaryTableMgr.clear();
    }
}
