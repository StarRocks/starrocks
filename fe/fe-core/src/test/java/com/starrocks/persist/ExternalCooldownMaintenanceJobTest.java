// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.persist;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.io.Writable;
import com.starrocks.journal.JournalEntity;
import com.starrocks.scheduler.externalcooldown.ExternalCooldownMaintenanceJob;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.starrocks.persist.OperationType.OP_EXTERNAL_COOLDOWN_JOB_STATE;

public class ExternalCooldownMaintenanceJobTest {
    private String fileName = "./ExternalCooldownMaintenanceJobTest";

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        OlapTable table = new OlapTable(2000L, "tb2", null, null, null, null);
        ExternalCooldownMaintenanceJob job = new ExternalCooldownMaintenanceJob(table, 1001L);

        out.writeShort(OP_EXTERNAL_COOLDOWN_JOB_STATE);
        job.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        JournalEntity journalEntity = new JournalEntity();
        journalEntity.readFields(in);
        Writable data = journalEntity.getData();
        Assert.assertEquals(OP_EXTERNAL_COOLDOWN_JOB_STATE, journalEntity.getOpCode());
        Assert.assertTrue(data instanceof ExternalCooldownMaintenanceJob);

        ExternalCooldownMaintenanceJob recoverJob = (ExternalCooldownMaintenanceJob) data;
        Assert.assertEquals(1001L, recoverJob.getDbId());
        Assert.assertEquals(2000L, recoverJob.getTableId());
        Assert.assertEquals(2000L, recoverJob.getJobId());

        in.close();

        Assert.assertNotEquals(recoverJob, null);
        Assert.assertFalse(recoverJob.equals(0L));
    }
}
