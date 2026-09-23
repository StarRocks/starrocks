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

package com.starrocks.persist;

import com.google.common.collect.Lists;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ModifyTableColumnOperationLogTest {
    @Test
    public void testDbNameIsStoredWithoutClusterPrefix() {
        ModifyTableColumnOperationLog log = new ModifyTableColumnOperationLog("default_cluster:db1", "t1",
                Lists.newArrayList());
        Assertions.assertEquals("db1", log.getDbName());
        Assertions.assertEquals("t1", log.getTableName());
        String json = GsonUtils.GSON.toJson(new ModifyTableColumnOperationLog("db2", "t2", Lists.newArrayList()));
        Assertions.assertTrue(json.contains("\"dbName\":\"db2\""), json);

        // written by an older version
        ModifyTableColumnOperationLog old = GsonUtils.GSON.fromJson(
                "{\"dbName\":\"default_cluster:db3\",\"tableName\":\"t3\",\"columns\":[]}",
                ModifyTableColumnOperationLog.class);
        Assertions.assertEquals("db3", old.getDbName());
        Assertions.assertEquals("t3", old.getTableName());
    }
}
