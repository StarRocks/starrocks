// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.ha;

import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StateChangeExecutorTest {
    private class StateChangeExecutionTest implements StateChangeExecution {
        private FrontendNodeType type;
        @Override
        public void transferToLeader(FrontendNodeType newType) {
            type = newType;
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

    private StateChangeExecutor executor = null;

    @Before
    public void init() {
        executor = new StateChangeExecutor();
    }

    @After
    public void cleanup() {
        executor.exit();
    }

    private void notifyAndCheck(FrontendNodeType newType, StateChangeExecutionTest execution) {
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
    }

    @Test
    public void testStateChangeExecutor() {
        StateChangeExecutionTest execution = new StateChangeExecutionTest();
        executor.registerStateChangeExecution(execution);

        executor.start();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public FrontendNodeType getFeType() {
                return execution.getType();
            }
        };
        // INIT -> MASTER
        execution.setType(FrontendNodeType.INIT);
        Assert.assertEquals(FrontendNodeType.INIT, execution.getType());
        notifyAndCheck(FrontendNodeType.MASTER, execution);

        // INIT -> FOLLOWER
        execution.setType(FrontendNodeType.INIT);
        notifyAndCheck(FrontendNodeType.FOLLOWER, execution);

        // UNKNOWN -> MASTER
        execution.setType(FrontendNodeType.UNKNOWN);
        notifyAndCheck(FrontendNodeType.MASTER, execution);

        // UNKNOWN -> FOLLOWER
        execution.setType(FrontendNodeType.UNKNOWN);
        notifyAndCheck(FrontendNodeType.FOLLOWER, execution);

        // FOLLOWER -> MASTER
        execution.setType(FrontendNodeType.FOLLOWER);
        notifyAndCheck(FrontendNodeType.MASTER, execution);

        // FOLLOWER -> UNKNOWN
        execution.setType(FrontendNodeType.FOLLOWER);
        notifyAndCheck(FrontendNodeType.UNKNOWN, execution);

        // OBSERVER -> UNKNOWN
        execution.setType(FrontendNodeType.OBSERVER);
        notifyAndCheck(FrontendNodeType.UNKNOWN, execution);
    }
}
