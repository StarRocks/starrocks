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

public class TableAccessCollectorRepoTest {
    @Test
    public void testDeleteSQLBuilder() {
        String sql = TableAccessCollectorRepo.DeleteSQLBuilder.buildDeleteStatsSQL();
        Assert.assertTrue(sql.contains(
                "DELETE FROM `default_catalog`.`_statistics_`.`table_access_statistics` WHERE `access_time` <="));
    }

    @Test
    public void testInsertSQLBuilder() {
        List<AccessLog> accessLogs = new LinkedList<>();
        accessLogs.add(new AccessLog("c", "d", "t", "", "col1.a", 1719908852, 10));
        accessLogs.add(new AccessLog("c", "d", "t", "par", "col", 1719908862, 1));

        TableAccessCollectorRepo.InsertSQLBuilder builder = new TableAccessCollectorRepo.InsertSQLBuilder();
        for (AccessLog accessLog : accessLogs) {
            builder.addAccessLog(accessLog);
        }

        Assert.assertEquals(2, builder.size());
        Assert.assertEquals("INSERT INTO `default_catalog`.`_statistics_`.`table_access_statistics` " +
                "VALUES ('c', 'd', 't', '', 'col1.a', from_unixtime(1719908852, 'yyyy-MM-dd HH:mm:ss'), 10), " +
                "('c', 'd', 't', 'par', 'col', from_unixtime(1719908862, 'yyyy-MM-dd HH:mm:ss'), 1)", builder.build());
        builder.clear();
        Assert.assertEquals(0, builder.size());
        Assert.assertThrows(IllegalArgumentException.class, builder::build);
    }
}
