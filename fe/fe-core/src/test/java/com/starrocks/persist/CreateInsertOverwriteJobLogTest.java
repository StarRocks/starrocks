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
import com.google.common.collect.Maps;
import com.starrocks.common.io.Writable;
import com.starrocks.journal.JournalEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CreateInsertOverwriteJobLogTest {
    @Test
    public void testBasic() throws IOException {
        List<Long> targetPartitionIds = Lists.newArrayList(10L, 20L);
        CreateInsertOverwriteJobLog jobInfo = new CreateInsertOverwriteJobLog(100L, 101L, 102L, targetPartitionIds, false);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        jobInfo.write(dataOutputStream);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        CreateInsertOverwriteJobLog newJobInfo = CreateInsertOverwriteJobLog.read(dataInputStream);
        Assertions.assertEquals(100L, newJobInfo.getJobId());
        Assertions.assertEquals(101L, newJobInfo.getDbId());
        Assertions.assertEquals(102L, newJobInfo.getTableId());
        Assertions.assertEquals(targetPartitionIds, newJobInfo.getTargetPartitionIds());

        JournalEntity journalEntity = new JournalEntity(OperationType.OP_CREATE_INSERT_OVERWRITE, jobInfo);
        ByteArrayOutputStream outputStream2 = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream2 = new DataOutputStream(outputStream2);
        dataOutputStream2.writeShort(journalEntity.opCode());
        journalEntity.data().write(dataOutputStream2);

        ByteArrayInputStream inputStream2 = new ByteArrayInputStream(outputStream2.toByteArray());
        DataInputStream dataInputStream2 = new DataInputStream(inputStream2);

        short opCode = dataInputStream2.readShort();
        Writable writable = EditLogDeserializer.deserialize(opCode, dataInputStream2);
        JournalEntity journalEntity2 = new JournalEntity(opCode, writable);

        Assertions.assertEquals(OperationType.OP_CREATE_INSERT_OVERWRITE, journalEntity2.opCode());
        Assertions.assertTrue(journalEntity2.data() instanceof CreateInsertOverwriteJobLog);
        CreateInsertOverwriteJobLog newJobInfo2 = (CreateInsertOverwriteJobLog) journalEntity2.data();
        Assertions.assertEquals(100L, newJobInfo2.getJobId());
        Assertions.assertEquals(101L, newJobInfo2.getDbId());
        Assertions.assertEquals(102L, newJobInfo2.getTableId());
        Assertions.assertEquals(targetPartitionIds, newJobInfo2.getTargetPartitionIds());
    }

    @Test
    public void testMap() {
        Map<String, String> map = Maps.newHashMap();
        String value = map.getOrDefault("hello", "");
        System.out.println("map size:" + map.size());
    }
}
