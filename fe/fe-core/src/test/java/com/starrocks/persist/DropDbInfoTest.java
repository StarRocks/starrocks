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

import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DropDbInfoTest {
    @Test
    public void testDbNameIsStoredWithoutClusterPrefix() {
        DropDbInfo info = new DropDbInfo("default_cluster:db1", true);
        Assertions.assertEquals("db1", info.getDbName());
        Assertions.assertTrue(info.isForceDrop());
        String json = GsonUtils.GSON.toJson(new DropDbInfo("db2", false));
        Assertions.assertTrue(json.contains("\"dbName\":\"db2\""), json);

        // written by an older version
        DropDbInfo old = GsonUtils.GSON.fromJson("{\"dbName\":\"default_cluster:db3\",\"forceDrop\":false}",
                DropDbInfo.class);
        Assertions.assertEquals("db3", old.getDbName());
    }
}
