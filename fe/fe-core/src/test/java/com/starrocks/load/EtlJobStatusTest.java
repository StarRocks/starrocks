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

package com.starrocks.load;

import com.starrocks.thrift.TEtlState;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TUniqueId;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class EtlJobStatusTest {

    @Test
    public void testSerialization() throws Exception {
        File file = new File("./EtlJobStatusTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        EtlStatus etlJobStatus = new EtlStatus();

        TEtlState state = TEtlState.FINISHED;
        String trackingUrl = "http://localhost:9999/xxx?job_id=zzz";
        Map<String, String> counters = new TreeMap<String, String>();
        for (int count = 0; count < 5; ++count) {
            String countersKey = "countersKey" + count;
            String countersValue = "countersValue" + count;
            counters.put(countersKey, countersValue);
        }

        etlJobStatus.setState(state);
        etlJobStatus.setTrackingUrl(trackingUrl);
        etlJobStatus.setCounters(counters);
        etlJobStatus.setLoadFileInfo(10, 1000);
        TUniqueId loadId = new TUniqueId();
        loadId.setHi(1);
        loadId.setLo(2);
        TUniqueId fragId1 = new TUniqueId();
        fragId1.setHi(3);
        fragId1.setLo(4);
        TUniqueId fragId2 = new TUniqueId();
        fragId2.setHi(5);
        fragId2.setLo(6);
        Set<TUniqueId> fragIds = new HashSet<>();
        fragIds.add(fragId1);
        fragIds.add(fragId2);
        Long backendId = 90000L;
        List<Long> backendIds = Arrays.asList(backendId);

        etlJobStatus.getLoadStatistic().initLoad(loadId, fragIds, backendIds);
        TReportExecStatusParams params = new TReportExecStatusParams();
        params.setBackend_id(backendId);
        params.setQuery_id(loadId);
        params.setFragment_instance_id(fragId1);
        params.setSource_load_rows(1000);
        params.setDone(true);
        etlJobStatus.getLoadStatistic().updateLoadProgress(params);
        params.setFragment_instance_id(fragId2);
        params.setSource_load_rows(999);
        etlJobStatus.getLoadStatistic().updateLoadProgress(params);
        String showInfoStr = etlJobStatus.getLoadStatistic().toShowInfoStr();
        etlJobStatus.write(dos);
        // stats.clear();
        // counters.clear();

        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        EtlStatus etlJobStatus1 = new EtlStatus();
        etlJobStatus1.readFields(dis);
        counters = etlJobStatus1.getCounters();

        Assert.assertEquals(etlJobStatus1.getState().name(), "FINISHED");
        for (int count = 0; count < 5; ++count) {
            String countersKey = "countersKey" + count;
            String countersValue = "countersValue" + count;
            Assert.assertEquals(counters.get(countersKey), countersValue);
        }

        Assert.assertTrue(etlJobStatus.equals(etlJobStatus1));
        Assert.assertEquals(trackingUrl, etlJobStatus1.getTrackingUrl());
        Assert.assertEquals(showInfoStr, etlJobStatus.getLoadStatistic().toShowInfoStr());

        dis.close();
        file.delete();
    }

}
