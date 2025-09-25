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

package com.staros.manager;

import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.NotExistStarException;
import com.staros.exception.StarException;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerInfo;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpDispatcherTest {
    private static final Logger LOG = LogManager.getLogger(HttpDispatcherTest.class);
    private StarManager starManager;
    private HttpDispatcher httpDispatcher;
    ShardGroupInfo shardGroupInfo1;
    ShardInfo shardInfo1;
    List<ShardGroupInfo> groupInfosList1 = new ArrayList<>();
    List<ShardInfo> shardInfosList1 = new ArrayList<>();
    List<WorkerGroupDetailInfo> workerGroupList1 = new ArrayList<>();
    WorkerGroupDetailInfo workerGroupInfo1;
    WorkerInfo workerInfo;

    @Before
    public void prepare() {
        starManager = new StarManager();
        httpDispatcher = new HttpDispatcher(starManager);

        shardGroupInfo1 = ShardGroupInfo.newBuilder().setServiceId("serviceId")
                .setGroupId(1)
                .build();
        shardInfo1 = ShardInfo.newBuilder().setServiceId("serviceId")
                .setShardId(1)
                .build();
        workerInfo = WorkerInfo.newBuilder().setServiceId("serviceId")
                .setWorkerId(1)
                .build();
        workerGroupInfo1 = WorkerGroupDetailInfo.newBuilder().setServiceId("serviceId")
                .setGroupId(1)
                .build();
        groupInfosList1.add(shardGroupInfo1);
        shardInfosList1.add(shardInfo1);
        workerGroupList1.add(workerGroupInfo1);

        new MockUp<StarManager>() {
            @Mock
            Pair<List<ShardGroupInfo>, Long> listShardGroupInfo(String serviceId, boolean includeAnonymousGroup,
                                                                long startGroupId) throws StarException {
                List<ShardGroupInfo> shardGroupInfosList = new ArrayList<>();
                return Pair.of(shardGroupInfosList, 1L);
            }
            @Mock
            List<ShardGroupInfo> getShardGroupInfo(String serviceId, List<Long> shardGroupIds) throws StarException {
                List<ShardGroupInfo> shardGroupInfosList = new ArrayList<>();
                return groupInfosList1;
            }
            @Mock
            List<ShardInfo> getShardInfo(String serviceId, List<Long> shardIds, long workerGroupId) {
                return shardInfosList1;
            }
            @Mock
            List<WorkerGroupDetailInfo> listWorkerGroups(String serviceId, List<Long> groupIds,
                    Map<String, String> filterLabelsMap, boolean includeWorkersInfo) {
                return workerGroupList1;
            }
            @Mock
            WorkerInfo getWorkerInfo(String serviceId, long workerId) {
                return workerInfo;
            }
            @Mock
            void removeShardGroupReplicas(String serviceId, Long shardGroupId) {
            }
            @Mock
            String getServiceIdByIdOrName(String service) {
                return "starrocks";
            }
        };
    }

    @Test
    public void testGetObject() {
        Object object1 = httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/service/starrocks/listshardgrouP");
        Assert.assertTrue(object1 instanceof List);
        Object object2 = httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/Service/starrocks/shardgrouP/1");
        Assert.assertTrue(object2 instanceof List);
        Object object3 = httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/servicE/starrocks/sharD/10011");
        Assert.assertTrue(object3 instanceof List);
        Object object4 = httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/SERvice/starrocks/SHARDgroup/1/Removereplicas");
        Assert.assertNull(object4);

        Object object5 = httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/servicE/starrocks/listworkergrouP");
        Assert.assertTrue(object5 instanceof List);
        Object object6 = httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/Service/starrocks/workergrouP/0");
        Assert.assertTrue(object6 instanceof List);
        Object object7 = httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/SERVICE/starrocks/workeR/1");
        Assert.assertFalse(object7 instanceof List);

        { // no matching pattern mode
            NotExistStarException invalidArgument = Assert.assertThrows(
                    NotExistStarException.class,
                    () -> httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/service/abcd/1"));
            Assert.assertEquals(invalidArgument.getMessage(), "No matching pattern found");
        }

        { // shard id is invalid
            InvalidArgumentStarException invalidArgument = Assert.assertThrows(InvalidArgumentStarException.class,
                    () -> httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/service/starrocks/shard/test"));
            Assert.assertEquals(invalidArgument.getMessage(), "Shard id: test is not a long type number");
        }

        { // shard group id is invalid
            InvalidArgumentStarException invalidArgument = Assert.assertThrows(InvalidArgumentStarException.class,
                    () -> httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/service/starrocks/shardgroup/test"));
            Assert.assertEquals(invalidArgument.getMessage(), "Shard group id: test is not a long type number");
        }

        { // shard group id is invalid
            InvalidArgumentStarException invalidArgument = Assert.assertThrows(InvalidArgumentStarException.class,
                    () -> httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/service/starrocks/shardgroup/test/removereplicas"));
            Assert.assertEquals(invalidArgument.getMessage(), "Shard group id: test is not a long type number");
        }

        { // worker id is invalid
            InvalidArgumentStarException invalidArgument = Assert.assertThrows(InvalidArgumentStarException.class,
                    () -> httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/service/starrocks/worker/test"));
            Assert.assertEquals(invalidArgument.getMessage(), "Worker id: test is not a long type number");
        }

        { // worker group id is invalid
            InvalidArgumentStarException invalidArgument = Assert.assertThrows(InvalidArgumentStarException.class,
                    () -> httpDispatcher.getObject("http://127.0.0.1:8030/api/v1/starmgr/service/starrocks/workergroup/test"));
            Assert.assertEquals(invalidArgument.getMessage(), "Worker group id: test is not a long type number");
        }
    }
}
