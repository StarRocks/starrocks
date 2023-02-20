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
import com.starrocks.journal.JournalEntity;
import com.starrocks.load.InsertOverwriteJobState;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class InsertOverwriteStateChangeInfoTest {
    @Test
    public void testBasic() throws IOException {
        List<Long> sourcePartitionNames = Lists.newArrayList(100L, 101L);
        List<Long> newPartitionNames = Lists.newArrayList(1000L, 1001L);
        InsertOverwriteStateChangeInfo stateChangeInfo = new InsertOverwriteStateChangeInfo(100L,
                InsertOverwriteJobState.OVERWRITE_PENDING, InsertOverwriteJobState.OVERWRITE_RUNNING,
                sourcePartitionNames, newPartitionNames);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        stateChangeInfo.write(dataOutputStream);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        InsertOverwriteStateChangeInfo newStateChangeInfo = InsertOverwriteStateChangeInfo.read(dataInputStream);
        Assert.assertEquals(100L, newStateChangeInfo.getJobId());
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_PENDING, newStateChangeInfo.getFromState());
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_RUNNING, newStateChangeInfo.getToState());
        Assert.assertEquals(sourcePartitionNames, newStateChangeInfo.getSourcePartitionIds());
        Assert.assertEquals(newPartitionNames, newStateChangeInfo.getTmpPartitionIds());

        JournalEntity journalEntity = new JournalEntity();
        journalEntity.setOpCode(OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE);
        journalEntity.setData(stateChangeInfo);
        ByteArrayOutputStream outputStream2 = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream2 = new DataOutputStream(outputStream2);
        journalEntity.write(dataOutputStream2);

        ByteArrayInputStream inputStream2 = new ByteArrayInputStream(outputStream2.toByteArray());
        DataInputStream dataInputStream2 = new DataInputStream(inputStream2);
        JournalEntity journalEntity2 = new JournalEntity();
        journalEntity2.readFields(dataInputStream2);

        Assert.assertEquals(OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE, journalEntity2.getOpCode());
        Assert.assertTrue(journalEntity2.getData() instanceof InsertOverwriteStateChangeInfo);
        InsertOverwriteStateChangeInfo newStateChangeInfo2 = (InsertOverwriteStateChangeInfo) journalEntity2.getData();
        Assert.assertEquals(100L, newStateChangeInfo2.getJobId());
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_PENDING, newStateChangeInfo2.getFromState());
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_RUNNING, newStateChangeInfo2.getToState());
        Assert.assertEquals(sourcePartitionNames, newStateChangeInfo2.getSourcePartitionIds());
        Assert.assertEquals(newPartitionNames, newStateChangeInfo2.getTmpPartitionIds());
    }
}
