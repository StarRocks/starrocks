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

package com.starrocks.load;

import com.starrocks.analysis.TableName;
import com.starrocks.common.Config;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.UUID;

public class ExportMgrTest {
    @Mocked
    GlobalStateMgr globalStateMgr;

    @Test
    public void testExpiredJob() throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        Config.history_job_keep_max_second = 10;
        ExportMgr mgr = new ExportMgr();

        // 1. create job 1
        ExportJob job1 = new ExportJob(1, new UUID(1, 1));
        job1.setTableName(new TableName("dummy", "dummy"));
        mgr.replayCreateExportJob(job1);
        Assert.assertEquals(1, mgr.getIdToJob().size());

        // 2. create job 2
        ExportJob job2 = new ExportJob(2, new UUID(2, 2));
        mgr.replayCreateExportJob(job2);
        Assert.assertEquals(2, mgr.getIdToJob().size());

        // 3. job 1 finished
        mgr.replayUpdateJobState(job1.getId(), ExportJob.JobState.FINISHED);
        Assert.assertEquals(2, mgr.getIdToJob().size());

        // 4. job 2 finished, but expired
        Config.history_job_keep_max_second = 1;
        Thread.sleep(2000);
        mgr.replayUpdateJobState(job2.getId(), ExportJob.JobState.FINISHED);
        Assert.assertEquals(1, mgr.getIdToJob().size());

        // 5. create job 3
        ExportJob job3 = new ExportJob(3, new UUID(3, 3));
        job3.setTableName(new TableName("dummy", "dummy"));
        mgr.replayCreateExportJob(job3);
        mgr.replayUpdateJobState(job3.getId(), ExportJob.JobState.FINISHED);
        Assert.assertEquals(2, mgr.getIdToJob().size());

        // 5. save image
        File tempFile = File.createTempFile("GlobalTransactionMgrTest", ".image");
        System.err.println("write image " + tempFile.getAbsolutePath());
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
        long checksum = 0;
        long saveChecksum = mgr.saveExportJob(dos, checksum);
        dos.close();

        // 6. clean expire
        mgr.removeOldExportJobs();
        Assert.assertEquals(1, mgr.getIdToJob().size());

        // 7. load image, will filter job1
        ExportMgr newMgr = new ExportMgr();
        Assert.assertEquals(0, newMgr.getIdToJob().size());
        DataInputStream dis = new DataInputStream(new FileInputStream(tempFile));
        checksum = 0;
        long loadChecksum = newMgr.loadExportJob(dis, checksum);
        Assert.assertEquals(1, newMgr.getIdToJob().size());
        Assert.assertEquals(saveChecksum, loadChecksum);

        tempFile.delete();
    }

    @Test
    public void testLoadSaveImageJsonFormat() throws Exception {
        ExportMgr leaderMgr = new ExportMgr();
        UtFrameUtils.setUpForPersistTest();
        ExportJob job = new ExportJob(3, new UUID(3, 3));
        job.setTableName(new TableName("dummy", "dummy"));
        leaderMgr.replayCreateExportJob(job);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        leaderMgr.saveExportJobV2(image.getDataOutputStream());

        ExportMgr followerMgr = new ExportMgr();
        SRMetaBlockReader reader = new SRMetaBlockReader(image.getDataInputStream());
        followerMgr.loadExportJobV2(reader);
        reader.close();

        Assert.assertEquals(1, followerMgr.getIdToJob().size());
    }
}
