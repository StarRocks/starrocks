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
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AlterMaterializedViewBaseTableInfosLogTest {

    // Mirrors the log as written and read by FEs from before baseTablesRedefined was added.
    private static class LegacyAlterMaterializedViewBaseTableInfosLog {
        @SerializedName(value = "dbId")
        private long dbId;
        @SerializedName(value = "mvId")
        private long mvId;
        @SerializedName(value = "baseTableInfos")
        private List<BaseTableInfo> baseTableInfos;
        @SerializedName(value = "remoteMvId")
        private MvId remoteMvId;
        @SerializedName("baseTableVisibleVersionMap")
        private Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap;
        @SerializedName("alterType")
        private int alterType;
        @SerializedName("baseTableInfoVisibleVersionMap")
        private Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> baseTableInfoVisibleVersionMap;
    }

    // Writes the entry as EditLog#logJsonObject does and reads it back as a replaying FE does.
    private static AlterMaterializedViewBaseTableInfosLog journalRoundTrip(String json) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Text.writeString(new DataOutputStream(bytes), json);
        return (AlterMaterializedViewBaseTableInfosLog) EditLogDeserializer.deserialize(
                OperationType.OP_ALTER_MATERIALIZED_VIEW_BASE_TABLE_INFOS,
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
    }

    private static final List<BaseTableInfo> BASE_TABLE_INFOS = Lists.newArrayList(
            new BaseTableInfo(100, "test", "t2", 1002));

    @Test
    public void testRedefinedBaseTablesRoundTrip() throws IOException {
        AlterMaterializedViewBaseTableInfosLog log =
                AlterMaterializedViewBaseTableInfosLog.ofRedefinedBaseTables(100, 1000, BASE_TABLE_INFOS);

        AlterMaterializedViewBaseTableInfosLog read = journalRoundTrip(GsonUtils.GSON.toJson(log));

        Assertions.assertTrue(read.isBaseTablesRedefined());
        Assertions.assertEquals(AlterMaterializedViewBaseTableInfosLog.AlterType.DEFAULT.ordinal(), read.getAlterType());
        Assertions.assertEquals(100, read.getDbId());
        Assertions.assertEquals(1000, read.getMvId());
        Assertions.assertEquals(BASE_TABLE_INFOS, read.getBaseTableInfos());
        Assertions.assertTrue(read.getBaseTableVisibleVersionMap().isEmpty());
    }

    @Test
    public void testLogFromOlderFeIsNotRedefined() throws IOException {
        // An entry written by an older FE has no baseTablesRedefined, so it must replay the existing way.
        String legacyJson = "{\"dbId\":100,\"mvId\":1000,\"baseTableInfos\":[]," +
                "\"baseTableVisibleVersionMap\":{\"1001\":{}}}";
        AlterMaterializedViewBaseTableInfosLog read = journalRoundTrip(legacyJson);

        Assertions.assertFalse(read.isBaseTablesRedefined());
        Assertions.assertEquals(100, read.getDbId());
        Assertions.assertEquals(1000, read.getMvId());
        Assertions.assertTrue(read.getBaseTableInfos().isEmpty());
        Assertions.assertTrue(read.getBaseTableVisibleVersionMap().containsKey(1001L));
    }

    @Test
    public void testOlderFeReadsRedefinedLog() {
        // An older FE has to read the entry despite the unknown field, falling back to the non-redefined fields.
        String json = GsonUtils.GSON.toJson(
                AlterMaterializedViewBaseTableInfosLog.ofRedefinedBaseTables(100, 1000, BASE_TABLE_INFOS));
        Assertions.assertTrue(json.contains("\"baseTablesRedefined\":true"), json);

        LegacyAlterMaterializedViewBaseTableInfosLog read =
                GsonUtils.GSON.fromJson(json, LegacyAlterMaterializedViewBaseTableInfosLog.class);

        Assertions.assertEquals(100, read.dbId);
        Assertions.assertEquals(1000, read.mvId);
        Assertions.assertNull(read.remoteMvId);
        Assertions.assertEquals(BASE_TABLE_INFOS, read.baseTableInfos);
        Assertions.assertTrue(read.baseTableVisibleVersionMap.isEmpty());
        Assertions.assertTrue(read.baseTableInfoVisibleVersionMap.isEmpty());
        // so the older FE replays it as a DEFAULT entry
        Assertions.assertEquals(AlterMaterializedViewBaseTableInfosLog.AlterType.DEFAULT.ordinal(), read.alterType);
    }
}
