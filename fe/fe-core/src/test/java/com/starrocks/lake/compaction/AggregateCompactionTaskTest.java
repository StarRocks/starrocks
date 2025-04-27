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

package com.starrocks.lake.compaction;

import com.google.common.collect.Lists;
import com.starrocks.proto.AggregateCompactRequest;
import com.starrocks.proto.CompactRequest;
import com.starrocks.proto.CompactResponse;
import com.starrocks.proto.ComputeNodePB;
import com.starrocks.rpc.LakeService;
import com.starrocks.transaction.TabletCommitInfo;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

public class AggregateCompactionTaskTest {
    
    @Test
    public void testAbort(@Mocked LakeService lakeService) {
        CompactRequest request = new CompactRequest();
        request.tabletIds = Arrays.asList(1L);
        request.txnId = 1000L;
        request.version = 10L;
        ComputeNodePB nodePB = new ComputeNodePB();
        nodePB.setHost("127.0.0.1");
        nodePB.setBrpcPort(9030);
        nodePB.setId(1L);
        AggregateCompactRequest aggregateRequest = new AggregateCompactRequest();
        aggregateRequest.requests = Lists.newArrayList();
        aggregateRequest.computeNodes = Lists.newArrayList();
        aggregateRequest.requests.add(request);
        aggregateRequest.computeNodes.add(nodePB);

        CompactionTask task = new AggregateCompactionTask(10043, lakeService, aggregateRequest);
        task.abort();
    }

    @Test
    public void testGetSuccessCompactInputFileSizeSuccess(
            @Mocked Future<CompactResponse> mockFuture,
            @Mocked LakeService lakeService) throws Exception {

        CompactRequest request = new CompactRequest();
        request.tabletIds = Arrays.asList(1L);
        request.txnId = 1000L;
        request.version = 10L;
        ComputeNodePB nodePB = new ComputeNodePB();
        nodePB.setHost("127.0.0.1");
        nodePB.setBrpcPort(9030);
        nodePB.setId(1L);
        AggregateCompactRequest aggregateRequest = new AggregateCompactRequest();
        aggregateRequest.requests = Lists.newArrayList();
        aggregateRequest.computeNodes = Lists.newArrayList();
        aggregateRequest.requests.add(request);
        aggregateRequest.computeNodes.add(nodePB);

        CompactionTask task = new AggregateCompactionTask(10043, lakeService, aggregateRequest);
        Assert.assertEquals(1, task.tabletCount());
        List<TabletCommitInfo> tabletCommitInfo = task.buildTabletCommitInfo();
        Assert.assertEquals(1, tabletCommitInfo.size());

        CompactResponse mockResponse = new CompactResponse();
        mockResponse.successCompactionInputFileSize = 100L;
        Field field = task.getClass().getSuperclass().getDeclaredField("responseFuture");
        field.setAccessible(true);
        field.set(task, mockFuture);
        
        new Expectations() {
            {
                mockFuture.get(); 
                result = mockResponse;
                mockFuture.isDone();
                result = true;
            }
        };

        Assert.assertEquals(100L, task.getSuccessCompactInputFileSize());
    }
}
