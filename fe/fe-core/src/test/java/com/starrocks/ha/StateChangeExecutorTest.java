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


package com.starrocks.ha;

import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class StateChangeExecutorTest {
    private static final Logger LOG = LogManager.getLogger(StateChangeExecutorTest.class);

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

    private void runOne(String name, FrontendNodeType oldType, FrontendNodeType newType) {
        StateChangeExecutionTest execution = new StateChangeExecutionTest();
        execution.setType(oldType);
        Assert.assertEquals(oldType, execution.getType());

        new MockUp<GlobalStateMgr>() {
            @Mock
            public FrontendNodeType getFeType() {
                LOG.info("{}: get mock fe type {}.", name, oldType);
                return oldType;
            }
        };

        StateChangeExecutor executor = new StateChangeExecutor(name);
        executor.registerStateChangeExecution(execution);
        executor.start();

        LOG.info("{}: notify new type {}.", name, newType);
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

        executor.setStop();
    }

    @Test
    public void testStateChangeExecutor() {
        // INIT -> LEADER
        runOne("StateChangeExecutor_initTOleader", FrontendNodeType.INIT, FrontendNodeType.LEADER);

        // INIT -> FOLLOWER
        runOne("StateChangeExecutor_initTOfollower", FrontendNodeType.INIT, FrontendNodeType.FOLLOWER);

        // UNKNOWN -> LEADER
        runOne("StateChangeExecutor_unknownTOleader", FrontendNodeType.UNKNOWN, FrontendNodeType.LEADER);

        // UNKNOWN -> FOLLOWER
        runOne("StateChangeExecutor_unknownTOfollower", FrontendNodeType.UNKNOWN, FrontendNodeType.FOLLOWER);

        // FOLLOWER -> LEADER
        runOne("StateChangeExecutor_followerTOleader", FrontendNodeType.FOLLOWER, FrontendNodeType.LEADER);

        // FOLLOWER -> UNKNOWN
        runOne("StateChangeExecutor_followerTOunknown", FrontendNodeType.FOLLOWER, FrontendNodeType.UNKNOWN);

        // OBSERVER -> UNKNOWN
        runOne("StateChangeExecutor_observerTOunknown", FrontendNodeType.OBSERVER, FrontendNodeType.UNKNOWN);
    }
}
