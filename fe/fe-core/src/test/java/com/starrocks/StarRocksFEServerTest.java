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

package com.starrocks;

import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.qe.QeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.GracefulExitFlag;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class StarRocksFEServerTest {

    private long originalMinGracefulExitSecond;

    @BeforeEach
    public void setUp() {
        // waitForDraining() exits once min_graceful_exit_time_second has elapsed after the
        // accept-new window closes; set it to 0 so the drain loop terminates promptly.
        originalMinGracefulExitSecond = Config.min_graceful_exit_time_second;
        Config.min_graceful_exit_time_second = 0;
    }

    @AfterEach
    public void tearDown() {
        Config.min_graceful_exit_time_second = originalMinGracefulExitSecond;
    }

    @Test
    @Timeout(10)
    public void testWaitForDrainingOnLeaderWaitsForTxnAndConnections(
            @Mocked ExecuteEnv executeEnv,
            @Mocked ConnectScheduler connectScheduler,
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked GlobalTransactionMgr globalTransactionMgr,
            @Mocked GracefulExitFlag gracefulExitFlag) throws Exception {
        // Leader drain: must wait for BOTH connections and running transactions to reach zero.
        new Expectations() {
            {
                ExecuteEnv.getInstance();
                result = executeEnv;
                executeEnv.getScheduler();
                result = connectScheduler;
                connectScheduler.isDrained();
                result = true;
                connectScheduler.getTotalConnCount();
                result = 0;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = true;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                globalTransactionMgr.getRunningTxnNums();
                result = 0;
                // Accept-new window is already over, so stopAccept is triggered on the first pass.
            }
        };

        // waitForDraining is private static; drive it through reflection. It must return (not hang)
        // because all drain conditions are met on the first pass.
        Deencapsulation.invoke(StarRocksFEServer.class, "waitForDraining");
    }

    @Test
    @Timeout(10)
    public void testWaitForDrainingOnFollowerIgnoresClusterWideTxns(
            @Mocked ExecuteEnv executeEnv,
            @Mocked ConnectScheduler connectScheduler,
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked GlobalTransactionMgr globalTransactionMgr,
            @Mocked GracefulExitFlag gracefulExitFlag) throws Exception {
        // Follower drain: runningTxnNums reflects the WHOLE cluster (replicated via BDB), so a
        // follower must exit once its own connections are gone even if cluster txns are still active.
        // This is the P1 fix -- otherwise continuous ingestion keeps runningTxnNums > 0 forever and
        // every follower times out and force-exits.
        new Expectations() {
            {
                ExecuteEnv.getInstance();
                result = executeEnv;
                executeEnv.getScheduler();
                result = connectScheduler;
                connectScheduler.isDrained();
                result = true;
                connectScheduler.getTotalConnCount();
                result = 0;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = false;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                // A very large cluster-wide txn count that must NOT block the follower's drain.
                globalTransactionMgr.getRunningTxnNums();
                result = 10_000;
            }
        };

        Deencapsulation.invoke(StarRocksFEServer.class, "waitForDraining");
    }

    @Test
    @Timeout(10)
    public void testWaitForDrainingStopsAcceptWhenWindowOver(
            @Mocked ExecuteEnv executeEnv,
            @Mocked ConnectScheduler connectScheduler,
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked GlobalTransactionMgr globalTransactionMgr,
            @Mocked GracefulExitFlag gracefulExitFlag,
            @Mocked QeService qeService) throws Exception {
        // Once the accept-new window is over, waitForDraining() must call stopAccept() on the QE
        // service so the MySQL listen socket closes and an L4 LB stops routing new connections.
        new Expectations() {
            {
                ExecuteEnv.getInstance();
                result = executeEnv;
                executeEnv.getScheduler();
                result = connectScheduler;
                connectScheduler.isDrained();
                result = true;
                connectScheduler.getTotalConnCount();
                result = 0;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = false;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                globalTransactionMgr.getRunningTxnNums();
                result = 0;
                qeService.stopAccept();
            }
        };

        QeService original = (QeService) getStaticField("QE_SERVICE");
        setStaticField(qeService);
        try {
            Deencapsulation.invoke(StarRocksFEServer.class, "waitForDraining");
        } finally {
            setStaticField(original);
        }
    }

    private static Object getStaticField(String name) throws Exception {
        java.lang.reflect.Field field = StarRocksFEServer.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(null);
    }

    private static void setStaticField(Object value) throws Exception {
        java.lang.reflect.Field field = StarRocksFEServer.class.getDeclaredField("QE_SERVICE");
        field.setAccessible(true);
        field.set(null, value);
    }

    @Test
    @Timeout(10)
    public void testWaitForDrainingLogsConnectionDrain(
            @Mocked ExecuteEnv executeEnv,
            @Mocked ConnectScheduler connectScheduler,
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked GlobalTransactionMgr globalTransactionMgr,
            @Mocked GracefulExitFlag gracefulExitFlag) throws Exception {
        // Leader with non-zero connections: drain loop must keep waiting and hit the
        // "waiting for N connections" log branch instead of exiting.
        new Expectations() {
            {
                ExecuteEnv.getInstance();
                result = executeEnv;
                executeEnv.getScheduler();
                result = connectScheduler;
                connectScheduler.isDrained();
                result = false;
                result = true;
                connectScheduler.getTotalConnCount();
                result = 3;
                result = 0;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = true;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                globalTransactionMgr.getRunningTxnNums();
                result = 0;
            }
        };

        Deencapsulation.invoke(StarRocksFEServer.class, "waitForDraining");
    }

    @Test
    @Timeout(10)
    public void testWaitForDrainingLogsRunningTxnDrain(
            @Mocked ExecuteEnv executeEnv,
            @Mocked ConnectScheduler connectScheduler,
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked GlobalTransactionMgr globalTransactionMgr,
            @Mocked GracefulExitFlag gracefulExitFlag) throws Exception {
        // Leader with running transactions but no connections: the drain loop must hit the
        // "waiting for N running transactions to drain" log branch before the txn count drops.
        new Expectations() {
            {
                ExecuteEnv.getInstance();
                result = executeEnv;
                executeEnv.getScheduler();
                result = connectScheduler;
                connectScheduler.isDrained();
                result = true;
                connectScheduler.getTotalConnCount();
                result = 0;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = true;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                globalTransactionMgr.getRunningTxnNums();
                result = 3;
                result = 0;
            }
        };

        Deencapsulation.invoke(StarRocksFEServer.class, "waitForDraining");
    }

    @Test
    @Timeout(10)
    public void testWaitForDrainingLogsDrainedBeforeMinWindowElapses(
            @Mocked ExecuteEnv executeEnv,
            @Mocked ConnectScheduler connectScheduler,
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked GlobalTransactionMgr globalTransactionMgr,
            @Mocked GracefulExitFlag gracefulExitFlag) throws Exception {
        // After connections and transactions drain, while min_graceful_exit_time_second has not yet
        // elapsed, the loop must log "drained, waiting for min_graceful_exit_time_second".
        Config.min_graceful_exit_time_second = 2;
        new Expectations() {
            {
                ExecuteEnv.getInstance();
                result = executeEnv;
                executeEnv.getScheduler();
                result = connectScheduler;
                connectScheduler.isDrained();
                result = true;
                connectScheduler.getTotalConnCount();
                result = 0;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.isLeader();
                result = false;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                globalTransactionMgr.getRunningTxnNums();
                result = 0;
            }
        };

        Deencapsulation.invoke(StarRocksFEServer.class, "waitForDraining");
    }
}
