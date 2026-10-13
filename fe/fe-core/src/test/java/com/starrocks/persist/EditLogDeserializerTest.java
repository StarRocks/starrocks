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

import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class EditLogDeserializerTest {

    @Test
    public void testEditLogDeserializerWarehouseInternalOpLog() throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();

        String warehouseName = "new_warehouse";
        String payload = "{'a':'b'}";
        WarehouseInternalOpLog log = new WarehouseInternalOpLog(warehouseName, payload);

        buffer.writeShort(OperationType.OP_WAREHOUSE_INTERNAL_OP);
        Text.writeString(buffer, GsonUtils.GSON.toJson(log, WarehouseInternalOpLog.class));

        DataInputStream inStream = new DataInputStream(new ByteArrayInputStream(buffer.getData()));
        buffer.flush();
        buffer.close();

        short opCode = inStream.readShort();
        Assertions.assertEquals(OperationType.OP_WAREHOUSE_INTERNAL_OP, opCode);
        Writable w = EditLogDeserializer.deserialize(opCode, inStream);
        Assertions.assertTrue(w instanceof WarehouseInternalOpLog);
        WarehouseInternalOpLog readLog = (WarehouseInternalOpLog) w;
        Assertions.assertEquals(warehouseName, readLog.getWarehouseName());
        Assertions.assertEquals(payload, readLog.getPayload());
    }
<<<<<<< HEAD
=======

    @Test
    public void testEditLogDeserializerReleaseAlterJobHistorySchemaLog() throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();
        buffer.writeShort(OperationType.OP_RELEASE_ALTER_JOB_HISTORY_SCHEMA);
        Text.writeString(buffer, GsonUtils.GSON.toJson(new ReleaseAlterJobHistorySchemaLog(101L, 102L, 103L)));
        DataInputStream inStream = new DataInputStream(new ByteArrayInputStream(buffer.getData()));
        buffer.close();

        short opCode = inStream.readShort();
        Assertions.assertEquals(OperationType.OP_RELEASE_ALTER_JOB_HISTORY_SCHEMA, opCode);
        Writable w = EditLogDeserializer.deserialize(opCode, inStream);
        Assertions.assertInstanceOf(ReleaseAlterJobHistorySchemaLog.class, w);
        ReleaseAlterJobHistorySchemaLog readLog = (ReleaseAlterJobHistorySchemaLog) w;
        Assertions.assertEquals(101L, readLog.getJobId());
        Assertions.assertEquals(102L, readLog.getDbId());
        Assertions.assertEquals(103L, readLog.getTableId());
    }

    private static Writable roundTrip(short opCode, Writable log) throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();
        buffer.writeShort(opCode);
        Text.writeString(buffer, GsonUtils.GSON.toJson(log));
        DataInputStream inStream = new DataInputStream(new ByteArrayInputStream(buffer.getData()));
        buffer.close();
        Assertions.assertEquals(opCode, inStream.readShort());
        return EditLogDeserializer.deserialize(opCode, inStream);
    }

    // The AgentBase extension writes and replays these codes, but the core deserializes them: a journal must stay
    // readable on an FE without the extension jar.
    @Test
    public void testEditLogDeserializerContextOpLog() throws IOException {
        List<Short> opCodes = List.of(
                OperationTypeEPack.OP_CREATE_CONTEXTBASE, OperationTypeEPack.OP_ALTER_CONTEXTBASE,
                OperationTypeEPack.OP_DROP_CONTEXTBASE, OperationTypeEPack.OP_RENAME_CONTEXTBASE,
                OperationTypeEPack.OP_CREATE_CONTEXT_COLLECTION, OperationTypeEPack.OP_ALTER_CONTEXT_COLLECTION,
                OperationTypeEPack.OP_DROP_CONTEXT_COLLECTION, OperationTypeEPack.OP_CREATE_CONTEXT_WORKSPACE,
                OperationTypeEPack.OP_DROP_CONTEXT_WORKSPACE, OperationTypeEPack.OP_CREATE_CONTEXT_RETRIEVAL_PROFILE,
                OperationTypeEPack.OP_DROP_CONTEXT_RETRIEVAL_PROFILE, OperationTypeEPack.OP_CREATE_CONTEXT_TYPE,
                OperationTypeEPack.OP_ALTER_CONTEXT_TYPE, OperationTypeEPack.OP_DROP_CONTEXT_TYPE);
        for (short opCode : opCodes) {
            Writable w = roundTrip(opCode, ContextOpLog.forContextBase(7L, "cb", Map.of("k", "v")));
            Assertions.assertInstanceOf(ContextOpLog.class, w, "op " + opCode);
            Assertions.assertEquals("cb", ((ContextOpLog) w).getName(), "op " + opCode);
            // Context metadata is auxiliary: a replay failure is logged and skipped rather than halting the FE.
            Assertions.assertTrue(OperationTypeEPack.IGNORABLE_OPERATIONS.contains(opCode), "op " + opCode);
        }

        ContextOpLog type = (ContextOpLog) roundTrip(OperationTypeEPack.OP_CREATE_CONTEXT_TYPE,
                ContextOpLog.forType(11L, 7L, "doc", "{\"fields\":[]}"));
        Assertions.assertEquals(11L, type.getId());
        Assertions.assertEquals(7L, type.getParentId());
        Assertions.assertEquals("doc", type.getName());
        Assertions.assertEquals("{\"fields\":[]}", type.getTypeJson());

        // Logged once by an AgentBase build that moved a pre-2026-10 image onto these ids; replay ignores it.
        Writable marker = roundTrip(OperationTypeEPack.OP_CONTEXT_LEGACY_IDS_MIGRATED,
                new ContextLegacyIdsMigratedLog(1234L));
        Assertions.assertEquals(1234L, ((ContextLegacyIdsMigratedLog) marker).getTimestampMs());
    }
>>>>>>> 0ef9a71f0d2... [Enhancement][RB-357] Journal the release of an alter job's history schema (#65086)
}
