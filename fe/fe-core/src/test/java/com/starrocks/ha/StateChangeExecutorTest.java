// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.ha;

import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

public class StateChangeExecutorTest {
    private class StateChangeExecutionTest implements StateChangeExecution {
        private FrontendNodeType type;
        @Override
        public void transferToLeader() {
            type = FrontendNodeType.LEADER;
        }
        @Override
        public void transferToNonLeader(FrontendNodeType newType) {
            type = newType;
        }
        public FrontendNodeType getType() {
            return type;
        }
        public void setType(FrontendNodeType newType) {
            type = newType;
        }
    }

    private void runOne(FrontendNodeType oldType, FrontendNodeType newType) {
        StateChangeExecutionTest execution = new StateChangeExecutionTest();
        execution.setType(oldType);
        Assert.assertEquals(oldType, execution.getType());

        new MockUp<GlobalStateMgr>() {
            @Mock
            public FrontendNodeType getFeType() {
                return execution.getType();
            }
        };

        StateChangeExecutor executor = new StateChangeExecutor();
        executor.registerStateChangeExecution(execution);
        executor.start();

        executor.notifyNewFETypeTransfer(newType);
        int i = 0;
        for (; i < 4; ++i) {
            try {
                Thread.sleep(500 /* 0.5 second */);
            } catch (InterruptedException e) {
            }
            if (execution.getType() == newType) {
                break;
            }
        }
        if (i != 4) { // it's possible that consumer thread is too slow
            Assert.assertEquals(newType, execution.getType());
        }

        executor.exit();
    }

    @Test
    public void testStateChangeExecutor() {
        // INIT -> LEADER
        runOne(FrontendNodeType.INIT, FrontendNodeType.LEADER);

        // INIT -> FOLLOWER
        runOne(FrontendNodeType.INIT, FrontendNodeType.FOLLOWER);

        // UNKNOWN -> LEADER
        runOne(FrontendNodeType.UNKNOWN, FrontendNodeType.LEADER);

        // UNKNOWN -> FOLLOWER
        runOne(FrontendNodeType.UNKNOWN, FrontendNodeType.FOLLOWER);

        // FOLLOWER -> LEADER
        runOne(FrontendNodeType.FOLLOWER, FrontendNodeType.LEADER);

        // FOLLOWER -> UNKNOWN
        runOne(FrontendNodeType.FOLLOWER, FrontendNodeType.UNKNOWN);

        // OBSERVER -> UNKNOWN
        runOne(FrontendNodeType.OBSERVER, FrontendNodeType.UNKNOWN);
    }
}
