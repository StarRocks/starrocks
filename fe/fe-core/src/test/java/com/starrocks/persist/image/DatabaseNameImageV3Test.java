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

package com.starrocks.persist.image;

import com.google.gson.JsonObject;
import com.starrocks.alter.OptimizeTask;
import com.starrocks.catalog.TableName;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.OptimizeTaskPB;
import com.starrocks.persist.proto.TableNamePB;
import com.starrocks.persist.proto.TaskCorePB;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies the private image converters after syncing the plain database-name storage contract. */
public class DatabaseNameImageV3Test {
    @Test
    public void testNewTaskWritesPlainNameAndPreservesPresence() throws Exception {
        for (String db : Arrays.asList(null, "", "analytics")) {
            Task source = new Task("task");
            source.setDbName(db);
            TaskCorePB pb = TaskCorePB.parseFrom(source.toTaskCoreProto().toByteArray());
            assertEquals(db != null, pb.hasDbName());
            assertEquals(db == null ? "" : db, pb.getDbName());
            Task loaded = new Task();
            loaded.fillFromTaskCoreProto(pb);
            assertEquals(db, source.getDbName());
            assertEquals(db, loaded.getDbName());
            GsonEquivalence.assertEquivalent(source, loaded);
        }
    }

    @Test
    public void testLegacyTaskJsonBecomesPlainBeforeProtobufWrite() throws Exception {
        for (String storedDb : List.of("default_cluster:analytics", "analytics")) {
            Task source = GsonUtils.GSON.fromJson("{\"name\":\"task\",\"dbName\":\"" + storedDb + "\"}", Task.class);
            assertEquals("analytics", source.getDbName());
            assertEquals(0, source.getConsecutiveFailCount());
            TaskCorePB pb = TaskCorePB.parseFrom(source.toTaskCoreProto().toByteArray());
            assertEquals("analytics", pb.getDbName());
            Task loaded = new Task();
            loaded.fillFromTaskCoreProto(pb);
            assertEquals("analytics", loaded.getDbName());
            JsonObject json = GsonUtils.GSON.toJsonTree(loaded).getAsJsonObject();
            assertEquals("analytics", json.get("dbName").getAsString());
            GsonEquivalence.assertEquivalent(source, loaded);
        }
    }

    @Test
    public void testOptimizeTaskUsesInheritedLegacyNameNormalization() throws Exception {
        OptimizeTask fixture = new OptimizeTask("optimize");
        fixture.setPartitionName("p1");
        fixture.setTempPartitionName("tp1");
        fixture.setOptimizeTaskState(Constants.TaskRunState.RUNNING);
        fixture.setLastVersion(42);
        JsonObject legacy = GsonUtils.GSON.toJsonTree(fixture).getAsJsonObject();
        legacy.addProperty("dbName", "default_cluster:analytics");
        OptimizeTask source = GsonUtils.GSON.fromJson(legacy, OptimizeTask.class);
        assertEquals("analytics", source.getDbName());
        assertEquals(0, source.getConsecutiveFailCount());
        OptimizeTaskPB pb = OptimizeTaskPB.parseFrom(source.toProto().toByteArray());
        assertEquals("analytics", pb.getTask().getDbName());
        OptimizeTask loaded = OptimizeTask.fromProto(pb);
        assertEquals("analytics", loaded.getDbName());
        assertEquals("p1", loaded.getPartitionName());
        assertEquals("tp1", loaded.getTempPartitionName());
        assertEquals(42, loaded.getLastVersion());
        GsonEquivalence.assertEquivalent(source, loaded);
    }

    @Test
    public void testTableNameJsonAndProtobufKeepPlainDatabase() throws Exception {
        for (String storedDb : List.of("default_cluster:analytics", "analytics")) {
            TableName source = GsonUtils.GSON.fromJson("{\"tbl\":\"events\",\"fullDb\":\"" + storedDb + "\"}",
                    TableName.class);
            TableNamePB pb = TableNamePB.parseFrom(source.toProto().toByteArray());
            assertEquals("analytics", pb.getDb());
            TableName loaded = TableName.fromProto(pb);
            assertEquals("analytics", loaded.getDb());
            assertEquals("events", loaded.getTbl());
            JsonObject json = GsonUtils.GSON.toJsonTree(loaded).getAsJsonObject();
            assertEquals("analytics", json.get("fullDb").getAsString());
            GsonEquivalence.assertEquivalent(source, loaded);
        }
    }
}
