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
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class DatabaseInfoTest {

    private void verifyDatabaseInfo(DatabaseInfo info, String dbName, String newDbName,
                                    long quota, AlterDatabaseQuotaStmt.QuotaType quotaType,
                                    String storageVolumeId) {
        Assertions.assertEquals(dbName, info.getDbName());
        Assertions.assertEquals(newDbName, info.getNewDbName());
        Assertions.assertEquals(quota, info.getQuota());
        Assertions.assertEquals(quotaType, info.getQuotaType());
        Assertions.assertEquals(storageVolumeId, info.getStorageVolumeId());
    }

    @Test
    public void testSerialization() throws IOException {
        {
            DatabaseInfo info = DatabaseInfo.newRenameInfo("db1", "new_db1");
            verifyDatabaseInfo(info, "db1", "new_db1", -1, AlterDatabaseQuotaStmt.QuotaType.NONE, "");

            String json = GsonUtils.GSON.toJson(info);
            DatabaseInfo readInfo = GsonUtils.GSON.fromJson(json, DatabaseInfo.class);
            verifyDatabaseInfo(readInfo, "db1", "new_db1", -1, AlterDatabaseQuotaStmt.QuotaType.NONE, "");
        }

        {
            DatabaseInfo info = DatabaseInfo.newQuotaUpdateInfo("db2", 1234L, AlterDatabaseQuotaStmt.QuotaType.DATA);
            verifyDatabaseInfo(info, "db2", "", 1234L, AlterDatabaseQuotaStmt.QuotaType.DATA, "");

            String json = GsonUtils.GSON.toJson(info);
            DatabaseInfo readInfo = GsonUtils.GSON.fromJson(json, DatabaseInfo.class);
            verifyDatabaseInfo(readInfo, "db2", "", 1234L, AlterDatabaseQuotaStmt.QuotaType.DATA, "");
        }

        {
            DatabaseInfo info = DatabaseInfo.newStorageVolumeUpdateInfo("db3", "sv_01");
            verifyDatabaseInfo(info, "db3", "", -1, AlterDatabaseQuotaStmt.QuotaType.NONE, "sv_01");

            String json = GsonUtils.GSON.toJson(info);
            DatabaseInfo readInfo = GsonUtils.GSON.fromJson(json, DatabaseInfo.class);
            verifyDatabaseInfo(readInfo, "db3", "", -1, AlterDatabaseQuotaStmt.QuotaType.NONE, "sv_01");
        }
    }
}

