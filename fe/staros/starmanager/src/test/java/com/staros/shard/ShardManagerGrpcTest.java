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

package com.staros.shard;

import com.staros.common.HijackConfig;
import com.staros.common.MockWorker;
import com.staros.common.TestUtils;
import com.staros.manager.StarManager;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.ReplicationType;
import com.staros.proto.ShardReportInfo;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupSpec;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.Config;
import com.staros.util.Constant;
import com.staros.worker.WorkerManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Specific case to cover the following scenario
 * - hundreds of thousands shards are allocated to a worker
 * - the worker is restarted, all the shards are required to add back again
 * - the shards' info are overwhelming the grpc channel, error with RESOURCE_EXHAUSTED
 */
public class ShardManagerGrpcTest {

    @BeforeClass
    public static void setUpForTestSuite() {
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.STARLET_AGENT;
    }

    @Test
    public void testGrpcPayLoadTooLargeFail() {
        // 500 shards, expect 600KB, failed to make addShard call and the await() check will fail with timeout exception.
        Assert.assertThrows(ConditionTimeoutException.class, () -> runTestGrpcPayload(500, 300 * 1024, 500));

        // Can make it work with no problem if Scheduler uses a much smaller batch size
        runTestGrpcPayload(500, 300 * 1024, 100);
    }

    @Test
    public void testGrpcPayLoadGood() {
        // 500 shards, expect 600KB
        runTestGrpcPayload(500, 1024 * 1024, 500);
    }

    private void runTestGrpcPayload(int numOfShards, int grpcMsgSize, int batchAddSize) {
        // setup Configuration
        Config.S3_BUCKET = "test-bucket";
        Config.S3_REGION = "test-region";
        Config.S3_ENDPOINT = "test-endpoint";
        Config.SIMPLE_CREDENTIAL_ACCESS_KEY_ID = RandomStringUtils.randomAlphabetic(20);
        Config.SIMPLE_CREDENTIAL_ACCESS_KEY_SECRET = RandomStringUtils.randomAlphabetic(100);

        HijackConfig disableShardChecker = new HijackConfig("DISABLE_BACKGROUND_SHARD_SCHEDULE_CHECK", "true");
        // change to 100KB, around 1k shard info
        HijackConfig grpcMaxMsgSize = new HijackConfig("GRPC_CHANNEL_MAX_MESSAGE_SIZE", String.valueOf(grpcMsgSize));
        HijackConfig heartbeatInterval = new HijackConfig("WORKER_HEARTBEAT_INTERVAL_SEC", "1");
        HijackConfig heartbeatRetry = new HijackConfig("WORKER_HEARTBEAT_RETRY_COUNT", "1");
        HijackConfig batchAddShardSize = new HijackConfig("SCHEDULER_MAX_BATCH_ADD_SHARD_SIZE", String.valueOf(batchAddSize));

        // setup StarManager
        StarManager starManager = new StarManager();
        starManager.becomeLeader();
        WorkerManager workerManager = starManager.getWorkerManager();

        // bootstrap test service
        String serviceTemplateName = "ShardManagerGrpcTest";
        String serviceName = "testGrpcPayLoadTooLargeError";
        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
        long workerGroupId = workerManager.createWorkerGroup(
                serviceId, "unittest", WorkerGroupSpec.newBuilder().setSize("x0").build(), null, null, 1 /* replicaNumber */,
                ReplicationType.SYNC, WarmupLevel.WARMUP_NOT_SET);

        // add MockWorker
        MockWorker mockWorker = new MockWorker();
        mockWorker.setDisableHeartbeat(true);
        mockWorker.start();
        String ipPort = String.format("127.0.0.1:%d", mockWorker.getRpcPort());
        long workerId = workerManager.addWorker(serviceId, workerGroupId, ipPort);
        // wait the worker state to ON
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> workerManager.getWorker(workerId).isAlive());
        // simulate the first heartbeat from mockWorker
        starManager.processWorkerHeartbeat(
                serviceId, workerId, System.currentTimeMillis(), Collections.emptyMap(), Collections.emptyList(),
                new ArrayList<ShardReportInfo>());
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> TestUtils.workerManagerExecutorsIdle(workerManager));

        try {
            FileCacheInfo cacheInfo = FileCacheInfo.newBuilder()
                    .setEnableCache(true)
                    .setAsyncWriteBack(true)
                    .setTtlSeconds(1234567890L)
                    .build();

            // build pathinfo, give random ak/sk to enlarge the shard info size.
            FilePathInfo pathInfo = FilePathInfo.newBuilder()
                    .setFsInfo(FileStoreInfo.newBuilder()
                            .setFsType(FileStoreType.S3)
                            .setFsKey(Constant.S3_FSKEY_FOR_CONFIG)
                            .setFsName(Constant.S3_FSNAME_FOR_CONFIG)
                            .build())

                    .setFullPath("s3://" + Config.S3_BUCKET + "/" + RandomStringUtils.randomAlphabetic(1024))
                    .build();
            CreateShardInfo createInfo = CreateShardInfo.newBuilder()
                    .setCacheInfo(cacheInfo)
                    .setPathInfo(pathInfo)
                    .setReplicaCount(1)
                    .setScheduleToWorkerGroup(workerGroupId)
                    .build();
            starManager.createShard(serviceId, Collections.nCopies(numOfShards, createInfo));
            Assert.assertEquals(numOfShards, mockWorker.getShardCount());

            mockWorker.simulateRestart();
            // simulate mockWorker heartbeat again after restart
            starManager.processWorkerHeartbeat(
                    serviceId, workerId, mockWorker.getStartTime(), Collections.emptyMap(), Collections.emptyList(),
                    new ArrayList<ShardReportInfo>());
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> mockWorker.getShardCount() == numOfShards);
        } finally {
            disableShardChecker.reset();
            grpcMaxMsgSize.reset();
            heartbeatInterval.reset();
            heartbeatRetry.reset();
            batchAddShardSize.reset();
            workerManager.removeWorker(serviceId, workerGroupId, workerId);

            mockWorker.stop();
            starManager.stopBackgroundThreads();
        }
    }
}
