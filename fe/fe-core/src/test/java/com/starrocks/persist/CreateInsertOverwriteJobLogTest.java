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
import com.starrocks.journal.JournalEntity;
import org.junit.Assert;
import org.junit.Test;

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
        CreateInsertOverwriteJobLog jobInfo = new CreateInsertOverwriteJobLog(100L, 101L, 102L, targetPartitionIds);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        jobInfo.write(dataOutputStream);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        CreateInsertOverwriteJobLog newJobInfo = CreateInsertOverwriteJobLog.read(dataInputStream);
        Assert.assertEquals(100L, newJobInfo.getJobId());
        Assert.assertEquals(101L, newJobInfo.getDbId());
        Assert.assertEquals(102L, newJobInfo.getTableId());
        Assert.assertEquals(targetPartitionIds, newJobInfo.getTargetPartitionIds());


        JournalEntity journalEntity = new JournalEntity();
        journalEntity.setOpCode(OperationType.OP_CREATE_INSERT_OVERWRITE);
        journalEntity.setData(jobInfo);
        ByteArrayOutputStream outputStream2 = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream2 = new DataOutputStream(outputStream2);
        journalEntity.write(dataOutputStream2);

        ByteArrayInputStream inputStream2 = new ByteArrayInputStream(outputStream2.toByteArray());
        DataInputStream dataInputStream2 = new DataInputStream(inputStream2);
        JournalEntity journalEntity2 = new JournalEntity();
        journalEntity2.readFields(dataInputStream2);

        Assert.assertEquals(OperationType.OP_CREATE_INSERT_OVERWRITE, journalEntity2.getOpCode());
        Assert.assertTrue(journalEntity2.getData() instanceof CreateInsertOverwriteJobLog);
        CreateInsertOverwriteJobLog newJobInfo2 = (CreateInsertOverwriteJobLog) journalEntity2.getData();
        Assert.assertEquals(100L, newJobInfo2.getJobId());
        Assert.assertEquals(101L, newJobInfo2.getDbId());
        Assert.assertEquals(102L, newJobInfo2.getTableId());
        Assert.assertEquals(targetPartitionIds, newJobInfo2.getTargetPartitionIds());
    }

    @Test
    public void testMap() {
        Map<String, String> map = Maps.newHashMap();
        String value = map.getOrDefault("hello", "");
        System.out.println("map size:" + map.size());
    }
}
