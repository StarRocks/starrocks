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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class ExportMgrTest {

    @Test
    public void testExpiredJob() throws Exception {
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

        // 6. get job by queryId
        ExportJob jobResult = mgr.getExportByQueryId(job3.getQueryId());
        Assert.assertNotNull(jobResult);
        Assert.assertEquals(3, jobResult.getId());
        ExportJob jobResultNull = mgr.getExportByQueryId(null);
        Assert.assertNull(jobResultNull);
        ExportJob jobResultNotExist = mgr.getExportByQueryId(new UUID(4, 4));
        Assert.assertNull(jobResultNotExist);
    }

    @Test
    public void testLoadSaveImageJsonFormat() throws Exception {
        ExportMgr leaderMgr = new ExportMgr();
        UtFrameUtils.setUpForPersistTest();
        ExportJob job = new ExportJob(3, new UUID(3, 3));
        job.setTableName(new TableName("dummy", "dummy"));
        leaderMgr.replayCreateExportJob(job);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        leaderMgr.saveExportJobV2(image.getImageWriter());

        ExportMgr followerMgr = new ExportMgr();
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        followerMgr.loadExportJobV2(reader);
        reader.close();

        Assert.assertEquals(1, followerMgr.getIdToJob().size());
    }

    @Test
    public void testShowExpiredJob() throws Exception {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.setThreadLocalInfo();
        GlobalStateMgr.getCurrentState();

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
        //Expected result set
        List<Integer> exceptJobIds = jobIds.stream().sorted(Collections.reverseOrder())
                .collect(Collectors.toList()).subList(0, Math.min(limit, jobIds.size()));
        for (Integer jobId : jobIds) {
            ExportJob job1 = new ExportJob(jobId, new UUID(1, 1));
            job1.setTableName(new TableName("*", "DUMMY" + jobId));
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
        //Compare the sorted and limited result sets
        Assert.assertArrayEquals(exceptJobIds.toArray(new Integer[0]), resultJobIds.toArray(new Integer[0]));
        resultJobIds.clear();


        List<List<String>> exportJobInfosByIdOrState1 = mgr.getExportJobInfosByIdOrState(-1, 0, null, null, null, limit);
        for (List<String> infos : exportJobInfosByIdOrState1) {
            resultJobIds.add(Integer.valueOf(infos.get(0)));
        }
        //Comparing the number of unordered but limited result sets
        Assert.assertEquals(exceptJobIds.size(), exportJobInfosByIdOrState1.size());

    }
}
