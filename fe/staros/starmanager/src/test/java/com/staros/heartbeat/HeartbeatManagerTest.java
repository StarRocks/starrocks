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

package com.staros.heartbeat;

import com.staros.common.HijackConfig;
import com.staros.common.MemoryJournalSystem;
import com.staros.common.MockWorker;
import com.staros.common.TestUtils;
import com.staros.proto.OperationType;
import com.staros.proto.StatusCode;
import com.staros.proto.WorkerState;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.Config;
import com.staros.worker.Worker;
import com.staros.worker.WorkerManager;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HeartbeatManagerTest {
    private final String serviceId = "1";
    private MemoryJournalSystem journalSystem;
    private WorkerManager workerManager;
    private HeartbeatManager heartbeatManager;
    private long workerGroupId;
    HijackConfig heartbeatInterval;
    HijackConfig heartbeatRetry;

    @BeforeClass
    public static void prepare() {
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.STARLET_AGENT;
    }

    @Before
    public void prepareHeartbeatManagerTest() {
        heartbeatInterval = new HijackConfig("WORKER_HEARTBEAT_INTERVAL_SEC", "1");
        heartbeatRetry = new HijackConfig("WORKER_HEARTBEAT_RETRY_COUNT", "1");
        journalSystem = new MemoryJournalSystem();
        workerManager = WorkerManager.createWorkerManagerForTest(journalSystem);
        workerManager.bootstrapService(serviceId);
        workerGroupId = TestUtils.createWorkerGroupForTest(workerManager, serviceId, 1);
        heartbeatManager = new HeartbeatManager(workerManager);
        workerManager.start();
    }

    @After
    public void cleanHeartbeatManagerTest() {
        heartbeatManager.stop();
        heartbeatInterval.reset();
        heartbeatRetry.reset();
        workerManager.stop();
    }

    // start a mock worker server and add this worker to worker manager
    private MockWorker prepareWorker() {
        MockWorker worker = new MockWorker();
        worker.setDisableHeartbeat(true);
        worker.start();

        String ipPort = String.format("127.0.0.1:%d", worker.getRpcPort());
        workerManager.addWorker(serviceId, workerGroupId, ipPort);
        return worker;
    }

    @Test
    public void testHeartbeatManagerBasic() {
        MockWorker worker = prepareWorker();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> worker.getWorkerId() > 0);
        Assert.assertEquals(1, worker.getHeartbeatCount());
        heartbeatManager.start();

        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> worker.getHeartbeatCount() > 1);
        Assert.assertTrue(worker.getHeartbeatCount() > 1);

        workerManager.removeWorker(serviceId, workerGroupId, worker.getWorkerId());
        worker.stop();
    }

    @Test
    public void testHeartbeatManagerDetectWorkerStateChange() {
        MockWorker worker = prepareWorker();
        // Wait until MockWorker is assigned with a worker Id
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> worker.getWorkerId() > 0);
        // Wait until the MockWorker's heartbeat response is processed by workerManager
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> workerManager.getWorker(worker.getWorkerId()).isAlive());
        // Wait until the MockWorker's state has been persisted
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(
                () -> journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_WORKER) == 1);

        heartbeatManager.start();
        Assert.assertTrue(workerManager.getWorker(worker.getWorkerId()).isAlive());
        // heartbeat fail
        worker.setError(StatusCode.UNKNOWN);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_WORKER) == 2);
        Assert.assertFalse(workerManager.getWorker(worker.getWorkerId()).isAlive());
        Assert.assertEquals(2, journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_WORKER));

        // heartbeat success
        worker.setError(StatusCode.OK);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_WORKER) == 3);
        Assert.assertEquals(WorkerState.ON, workerManager.getWorker(worker.getWorkerId()).getState());
        Assert.assertEquals(3, journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_WORKER));

        cleanMockWorker(worker);
    }

    private void cleanMockWorker(MockWorker worker) {
        workerManager.removeWorker(serviceId, workerGroupId, worker.getWorkerId());
        worker.stop();
    }

    @Test
    public void testMassiveWorkersTimeoutHeartbeat() throws InterruptedException {
        // massive number of workers are timeout responding the workerManager's heartbeat check.
        // Only one is alive, check if the alive worker can still get the heartbeat in time.
        heartbeatManager.stop();
        HijackConfig heartbeatInterval = new HijackConfig("WORKER_HEARTBEAT_INTERVAL_SEC", "5");
        // recreate the heartbeatManager with the new heartbeat interval setting
        HeartbeatManager myHeartBeatMgr = new HeartbeatManager(workerManager);

        int nDeadWorkers = 20;
        List<MockWorker> deadWorkers = new ArrayList<>();
        for (int i = 0; i < nDeadWorkers; ++i) {
            MockWorker worker = prepareWorker();
            deadWorkers.add(worker);
        }
        MockWorker aliveWorker = prepareWorker();

        // wait for all workers online with the first heartbeat received
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> workerManager.getWorkerGroup(serviceId, workerGroupId).getAllWorkerIds(true).size() ==
                        nDeadWorkers + 1);
        // set deadWorkers as not responding
        deadWorkers.forEach(x -> x.setResponding(false));

        int nCount = aliveWorker.getHeartbeatCount();
        myHeartBeatMgr.start();
        Thread.sleep(Config.WORKER_HEARTBEAT_INTERVAL_SEC * 1000L * 2 + 1000L);
        Assert.assertTrue(aliveWorker.getHeartbeatCount() - nCount >= 2);

        for (MockWorker worker : deadWorkers) {
            worker.setResponding(true);
            cleanMockWorker(worker);
        }
        cleanMockWorker(aliveWorker);
        myHeartBeatMgr.stop();
        heartbeatInterval.reset();
    }

    @Test
    public void testHeartbeatManagerDetectWorkerStateChangeAlive_Shutdown_Down() {
        MockWorker worker = prepareWorker();
        // Wait until MockWorker is assigned with a worker Id
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> worker.getWorkerId() > 0);
        // Wait until the MockWorker's heartbeat response is processed by workerManager
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> workerManager.getWorker(worker.getWorkerId()).isAlive());
        // Wait until the MockWorker's state has been persisted
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(
                () -> journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_WORKER) == 1);

        heartbeatManager.start();
        Assert.assertTrue(workerManager.getWorker(worker.getWorkerId()).isAlive());

        long workerId = worker.getWorkerId();
        Worker serverWorkerObj = workerManager.getWorker(workerId);

        // mockWorker report SHUT_DOWN
        worker.setError(StatusCode.SHUT_DOWN);

        int journalCount = journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_WORKER);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_WORKER) > journalCount);
        Assert.assertFalse(serverWorkerObj.isAlive());
        // expect server side mark the worker as SHUTTING_DOWN
        Assert.assertEquals(WorkerState.SHUTTING_DOWN, serverWorkerObj.getState());
        Assert.assertEquals(2, journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_WORKER));

        // mockWorker report UNKNOWN error
        worker.setError(StatusCode.UNKNOWN);
        int journalCount2 = journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_WORKER);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_WORKER) > journalCount2);
        // expect server side mark the worker as DOWN/DEAD
        Assert.assertEquals(WorkerState.DOWN, serverWorkerObj.getState());

        cleanMockWorker(worker);
    }
}
