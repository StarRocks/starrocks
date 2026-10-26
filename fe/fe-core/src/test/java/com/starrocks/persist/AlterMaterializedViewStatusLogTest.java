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

import com.google.gson.JsonObject;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AlterMaterializedViewStatusLogTest {

    @Test
    public void testClearVersionMapOfCurrentAndOldEntries() {
        AlterMaterializedViewStatusLog log = new AlterMaterializedViewStatusLog(1L, 2L, "INACTIVE", "reason");
        // Every entry written now carries the flag, false included.
        Assertions.assertFalse(log.isClearVersionMap());
        Assertions.assertFalse(log.isClearVersionMapUnset());
        AlterMaterializedViewStatusLog read = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(log),
                AlterMaterializedViewStatusLog.class);
        Assertions.assertFalse(read.isClearVersionMap());
        Assertions.assertFalse(read.isClearVersionMapUnset());

        log.setClearVersionMap(true);
        read = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(log), AlterMaterializedViewStatusLog.class);
        Assertions.assertTrue(read.isClearVersionMap());
        Assertions.assertFalse(read.isClearVersionMapUnset());

        // An entry written by a version that did not journal the clear.
        JsonObject old = GsonUtils.GSON.toJsonTree(log).getAsJsonObject();
        old.remove("clearVersionMap");
        read = GsonUtils.GSON.fromJson(old, AlterMaterializedViewStatusLog.class);
        Assertions.assertFalse(read.isClearVersionMap());
        Assertions.assertTrue(read.isClearVersionMapUnset());
        Assertions.assertEquals("reason", read.getReason());
    }
}
