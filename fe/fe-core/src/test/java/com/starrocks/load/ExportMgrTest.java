// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getCurrentStateJournalVersion();
                minTimes = 0;
                result = FeMetaVersion.VERSION_CURRENT;
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
    public void testShowExpiredJob() throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getCurrentStateJournalVersion();
                minTimes = 0;
                result = FeMetaVersion.VERSION_CURRENT;
            }
        };
        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.setThreadLocalInfo();
        GlobalStateMgr.getCurrentState().initAuth(true);
        ExportMgr mgr = new ExportMgr();
        int limit = 5;
        List<Integer> jobIds = Lists.newArrayList();
        jobIds.add(299948218);
        jobIds.add(299948214);
        jobIds.add(299948190);
        jobIds.add(299948188);
        jobIds.add(299948183);
        jobIds.add(299948087);
        jobIds.add(299943362);
        jobIds.add(299943118);
        jobIds.add(299943014);
        jobIds.add(299943012);
        jobIds.add(299942987);
        jobIds = jobIds.stream().sorted(Collections.reverseOrder()).collect(Collectors.toList()).subList(0, Math.min(limit, jobIds.size()));
        for (Integer jobId : jobIds) {
            ExportJob job1 = new ExportJob(jobId, new UUID(1, 1));
            job1.setTableName(new TableName("DUMMY" + jobId, "DUMMY" + jobId));
            job1.setBrokerDesc(new BrokerDesc("DUMMY", Maps.newHashMap()));
            mgr.replayCreateExportJob(job1);
        }
        ArrayList<OrderByPair> orderByPairs = new ArrayList<>();
        OrderByPair pair = new OrderByPair(0, true);
        orderByPairs.add(pair);
        List<List<String>> exportJobInfosByIdOrState = mgr.getExportJobInfosByIdOrState(-1, 0, null, null, orderByPairs, limit);
        List<Integer> resultJobIds = Lists.newArrayList();
        for (List<String> infos : exportJobInfosByIdOrState) {
            resultJobIds.add(Integer.valueOf(infos.get(0)));
        }
        Assert.assertArrayEquals(jobIds.toArray(new Integer[0]), resultJobIds.toArray(new Integer[0]));

        resultJobIds.clear();
        List<List<String>> exportJobInfosByIdOrState1 = mgr.getExportJobInfosByIdOrState(-1, 0, null, null, null, limit);
        for (List<String> infos : exportJobInfosByIdOrState1) {
            resultJobIds.add(Integer.valueOf(infos.get(0)));
        }
        Assert.assertEquals(limit, exportJobInfosByIdOrState1.size());

    }
}
