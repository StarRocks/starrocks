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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/qe/QueryDetailQueueTest.java

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

package com.starrocks.qe;

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class QueryDetailQueueTest {
    @Test
    public void testQueryDetailQueue() {
        QueryDetail startQueryDetail = new QueryDetail("219a2d5443c542d4-8fc938db37c892e3", false, 1, "127.0.0.1",
                System.currentTimeMillis(), -1, -1, QueryDetail.QueryMemState.RUNNING,
                "testDb", "select * from table1 limit 1",
                "root", "");
        startQueryDetail.setScanRows(100);
        startQueryDetail.setScanBytes(10001);
        startQueryDetail.setReturnRows(1);
        startQueryDetail.setCpuCostNs(1002);
        startQueryDetail.setMemCostBytes(100003);
        QueryDetailQueue.addAndRemoveTimeoutQueryDetail(startQueryDetail);

        List<QueryDetail> queryDetails = QueryDetailQueue.getQueryDetailsAfterTime(startQueryDetail.getEventTime() - 1);
        Assert.assertEquals(1, queryDetails.size());

        Gson gson = new Gson();
        String jsonString = gson.toJson(queryDetails);
        String queryDetailString = "[{\"eventTime\":" + startQueryDetail.getEventTime() + ","
                + "\"queryId\":\"219a2d5443c542d4-8fc938db37c892e3\","
                + "\"isQuery\":false,"
                + "\"remoteIP\":\"127.0.0.1\","
                + "\"connId\":1,"
                + "\"startTime\":" + startQueryDetail.getStartTime() + ",\"endTime\":-1,\"latency\":-1,"
                + "\"state\":\"RUNNING\",\"database\":\"testDb\","
                + "\"sql\":\"select * from table1 limit 1\","
                + "\"user\":\"root\","
                + "\"scanRows\":100,"
                + "\"scanBytes\":10001,"
                + "\"returnRows\":1,"
                + "\"cpuCostNs\":1002,"
                + "\"memCostBytes\":100003}]";
        Assert.assertEquals(jsonString, queryDetailString);

        queryDetails = QueryDetailQueue.getQueryDetailsAfterTime(startQueryDetail.getEventTime());
        Assert.assertEquals(0, queryDetails.size());

        QueryDetail endQueryDetail = startQueryDetail.copy();
        endQueryDetail.setLatency(1);
        endQueryDetail.setState(QueryDetail.QueryMemState.FINISHED);
        QueryDetailQueue.addAndRemoveTimeoutQueryDetail(endQueryDetail);

        queryDetails = QueryDetailQueue.getQueryDetailsAfterTime(startQueryDetail.getEventTime() - 1);
        Assert.assertEquals(2, queryDetails.size());

        //set first element eventTime to 1min ago to simulate queryDetail timeout
        startQueryDetail.setEventTime(startQueryDetail.getEventTime() - 60000000000L);
        //add new queryDetail, this will trigger delete
        QueryDetailQueue.addAndRemoveTimeoutQueryDetail(new QueryDetail());
        queryDetails = QueryDetailQueue.getQueryDetailsAfterTime(startQueryDetail.getEventTime() - 1);
        Assert.assertEquals(2, queryDetails.size());
    }
}
