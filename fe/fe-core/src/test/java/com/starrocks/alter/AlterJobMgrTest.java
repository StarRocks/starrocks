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

package com.starrocks.alter;

import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class AlterJobMgrTest {

    @Test
    public void testGetRunningAlterJobCount() {
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        SystemHandler systemHandler = new SystemHandler();
        AlterJobMgr alterJobMgr = new AlterJobMgr(schemaChangeHandler, materializedViewHandler, systemHandler);

        // warehouse 0
        SchemaChangeJobV2 job1 = new SchemaChangeJobV2(1L, 20001, 30001, "test", 3600000);
        job1.setComputeResource(WarehouseComputeResource.of(0));
        job1.setJobState(AlterJobV2.JobState.FINISHED);
        schemaChangeHandler.addAlterJobV2(job1);
        SchemaChangeJobV2 job2 = new SchemaChangeJobV2(2L, 20001, 30001, "test", 3600000);
        job2.setComputeResource(WarehouseComputeResource.of(0));
        job2.setJobState(AlterJobV2.JobState.CANCELLED);
        schemaChangeHandler.addAlterJobV2(job2);
        SchemaChangeJobV2 job3 = new SchemaChangeJobV2(3L, 20001, 30001, "test", 3600000);
        job3.setComputeResource(WarehouseComputeResource.of(0));
        job3.setJobState(AlterJobV2.JobState.RUNNING);
        schemaChangeHandler.addAlterJobV2(job3);

        // warehouse 1
        SchemaChangeJobV2 job4 = new SchemaChangeJobV2(4L, 20001, 30001, "test", 3600000);
        job4.setComputeResource(WarehouseComputeResource.of(1));
        job4.setJobState(AlterJobV2.JobState.FINISHED);
        schemaChangeHandler.addAlterJobV2(job4);
        SchemaChangeJobV2 job5 = new SchemaChangeJobV2(5L, 20001, 30001, "test", 3600000);
        job5.setComputeResource(WarehouseComputeResource.of(1));
        job5.setJobState(AlterJobV2.JobState.CANCELLED);
        schemaChangeHandler.addAlterJobV2(job5);
        SchemaChangeJobV2 job6 = new SchemaChangeJobV2(6L, 20001, 30001, "test", 3600000);
        job6.setComputeResource(WarehouseComputeResource.of(1));
        job6.setJobState(AlterJobV2.JobState.RUNNING);
        schemaChangeHandler.addAlterJobV2(job6);
        RollupJobV2 job7 = buildRollupJob(7);
        job7.setComputeResource(WarehouseComputeResource.of(1));
        job7.setJobState(AlterJobV2.JobState.FINISHED);
        materializedViewHandler.addAlterJobV2(job7);
        RollupJobV2 job8 = buildRollupJob(8);
        job8.setComputeResource(WarehouseComputeResource.of(1));
        job8.setJobState(AlterJobV2.JobState.CANCELLED);
        materializedViewHandler.addAlterJobV2(job8);
        RollupJobV2 job9 = buildRollupJob(9);
        job9.setComputeResource(WarehouseComputeResource.of(1));
        job9.setJobState(AlterJobV2.JobState.RUNNING);
        materializedViewHandler.addAlterJobV2(job9);

        // warehouse 2
        RollupJobV2 job10 = buildRollupJob(10);
        job10.setComputeResource(WarehouseComputeResource.of(2));
        job10.setJobState(AlterJobV2.JobState.FINISHED);
        materializedViewHandler.addAlterJobV2(job10);
        RollupJobV2 job11 = buildRollupJob(11);
        job11.setComputeResource(WarehouseComputeResource.of(2));
        job11.setJobState(AlterJobV2.JobState.CANCELLED);
        materializedViewHandler.addAlterJobV2(job11);
        RollupJobV2 job12 = buildRollupJob(12);
        job12.setComputeResource(WarehouseComputeResource.of(2));
        job12.setJobState(AlterJobV2.JobState.RUNNING);
        materializedViewHandler.addAlterJobV2(job12);

        Map<Long, Long> result = alterJobMgr.getRunningAlterJobCount();
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals((Long) 1L, result.get(0L));
        Assertions.assertEquals((Long) 2L, result.get(1L));
        Assertions.assertEquals((Long) 1L, result.get(2L));
    }

    @Test
    public void testStopGracefullyDrainsAllHandlers() {
        // AlterJobMgr is an aggregator; stopGracefully(timeoutMs) must reach every wired
        // handler so each one's worker thread exits and onStopped() runs. LeaderDaemon's
        // stopGracefully is final, so we observe the contract via the onStopped() hook
        // it invokes for an unstarted daemon (worker=null path).
        boolean[] schemaChangeStopped = new boolean[1];
        boolean[] mvStopped = new boolean[1];
        boolean[] systemStopped = new boolean[1];

        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler() {
            @Override
            protected void onStopped() {
                schemaChangeStopped[0] = true;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler() {
            @Override
            protected void onStopped() {
                mvStopped[0] = true;
            }
        };
        SystemHandler systemHandler = new SystemHandler() {
            @Override
            protected void onStopped() {
                systemStopped[0] = true;
            }
        };
        AlterJobMgr alterJobMgr = new AlterJobMgr(schemaChangeHandler, materializedViewHandler, systemHandler);

        alterJobMgr.stopGracefully(1000L);

        Assertions.assertTrue(schemaChangeStopped[0], "schemaChangeHandler must be stopped");
        Assertions.assertTrue(mvStopped[0], "materializedViewHandler must be stopped");
        Assertions.assertTrue(systemStopped[0], "clusterHandler must be stopped");
    }

    @Test
    public void testStopGracefullyContinuesWhenOneHandlerThrows() {
        // A misbehaving handler must not abort the demotion drain; the remaining handlers
        // still need to be stopped. setStop() runs before stopGracefully's internal
        // try/catch around onStopped, so a throwing setStop propagates - AlterJobMgr's
        // own per-handler try/catch is the safety net we exercise here.
        boolean[] mvStopped = new boolean[1];
        boolean[] systemStopped = new boolean[1];

        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler() {
            @Override
            public void setStop() {
                throw new RuntimeException("simulated handler stop failure");
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler() {
            @Override
            protected void onStopped() {
                mvStopped[0] = true;
            }
        };
        SystemHandler systemHandler = new SystemHandler() {
            @Override
            protected void onStopped() {
                systemStopped[0] = true;
            }
        };
        AlterJobMgr alterJobMgr = new AlterJobMgr(schemaChangeHandler, materializedViewHandler, systemHandler);

        alterJobMgr.stopGracefully(1000L);

        Assertions.assertTrue(mvStopped[0], "materializedViewHandler must still be stopped");
        Assertions.assertTrue(systemStopped[0], "clusterHandler must still be stopped");
    }

    @Test
    public void testStopGracefullyContinuesWhenMaterializedViewHandlerThrows() {
        // Mirror of the previous test, but the middle handler is the one that misbehaves.
        // Covers the materializedViewHandler-specific try/catch arm in stopGracefully so a
        // failure there does not skip the SystemHandler drain.
        boolean[] schemaChangeStopped = new boolean[1];
        boolean[] systemStopped = new boolean[1];

        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler() {
            @Override
            protected void onStopped() {
                schemaChangeStopped[0] = true;
            }
        };
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler() {
            @Override
            public void setStop() {
                throw new RuntimeException("simulated materializedView stop failure");
            }
        };
        SystemHandler systemHandler = new SystemHandler() {
            @Override
            protected void onStopped() {
                systemStopped[0] = true;
            }
        };
        AlterJobMgr alterJobMgr = new AlterJobMgr(schemaChangeHandler, materializedViewHandler, systemHandler);

        alterJobMgr.stopGracefully(1000L);

        Assertions.assertTrue(schemaChangeStopped[0], "schemaChangeHandler must still be stopped");
        Assertions.assertTrue(systemStopped[0], "clusterHandler must still be stopped");
    }

    @Test
    public void testStopGracefullyToleratesClusterHandlerThrowing() {
        // The last handler in the fan-out is SystemHandler (a.k.a. clusterHandler). Its
        // try/catch arm has no successor to verify, so we just assert the call completes
        // without propagating the exception - the safety net must absorb it.
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        SystemHandler systemHandler = new SystemHandler() {
            @Override
            public void setStop() {
                throw new RuntimeException("simulated cluster stop failure");
            }
        };
        AlterJobMgr alterJobMgr = new AlterJobMgr(schemaChangeHandler, materializedViewHandler, systemHandler);

        // Should not throw - the SystemHandler-arm catch swallows.
        Assertions.assertDoesNotThrow(() -> alterJobMgr.stopGracefully(1000L));
    }

    private RollupJobV2 buildRollupJob(long id) {
        return new RollupJobV2(id, 20001, 30001, "test", 3600000,
                0, 0, "test", "test", 1,
                null, null, 1, 1, null, (short) 0,
                null, null, false);
    }
}
