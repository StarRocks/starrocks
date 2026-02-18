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
package com.starrocks.qe;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class SessionVariableTest {

    @Test
    public void testNonDefaultVariables() {
        SessionVariable sessionVariable = new SessionVariable();
        Map<String, SessionVariable.NonDefaultValue> nonDefaultVariables = sessionVariable.getNonDefaultVariables();
        Assertions.assertTrue(nonDefaultVariables.isEmpty());

        sessionVariable.setSqlDialect("test1");
        nonDefaultVariables = sessionVariable.getNonDefaultVariables();
        Assertions.assertEquals(1, nonDefaultVariables.size());
        Assertions.assertTrue(nonDefaultVariables.containsKey(SessionVariable.SQL_DIALECT));
        SessionVariable.NonDefaultValue kv = nonDefaultVariables.get(SessionVariable.SQL_DIALECT);
        Assertions.assertEquals(SessionVariable.DEFAULT_SESSION_VARIABLE.getSqlDialect(), kv.defaultValue);
        Assertions.assertEquals("test1", kv.actualValue);

        sessionVariable.setPipelineProfileLevel(100);
        nonDefaultVariables = sessionVariable.getNonDefaultVariables();
        Assertions.assertEquals(2, nonDefaultVariables.size());
        Assertions.assertTrue(nonDefaultVariables.containsKey(SessionVariable.PIPELINE_PROFILE_LEVEL));
        kv = nonDefaultVariables.get(SessionVariable.PIPELINE_PROFILE_LEVEL);
        Assertions.assertEquals(SessionVariable.DEFAULT_SESSION_VARIABLE.getPipelineProfileLevel(), kv.defaultValue);
        Assertions.assertEquals(100, kv.actualValue);
    }

    @Test
    public void testSetChooseMode() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setChooseExecuteInstancesMode("adaptive_increase");
        Assertions.assertTrue(sessionVariable.getChooseExecuteInstancesMode().enableIncreaseInstance());

        sessionVariable.setChooseExecuteInstancesMode("adaptive_decrease");
        Assertions.assertTrue(sessionVariable.getChooseExecuteInstancesMode().enableDecreaseInstance());

        sessionVariable.setChooseExecuteInstancesMode("auto");
        Assertions.assertTrue(sessionVariable.getChooseExecuteInstancesMode().enableIncreaseInstance());
        Assertions.assertTrue(sessionVariable.getChooseExecuteInstancesMode().enableDecreaseInstance());

        try {
            sessionVariable.setChooseExecuteInstancesMode("xxx");
            Assertions.fail("cannot set a invalid value");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Legal values of choose_execute_instances_mode are"),
                    e.getMessage());
        }
    }

    @Test
    public void testLakeBucketAssignMode() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setLakeBucketAssignMode("balance");
        Assertions.assertEquals(SessionVariableConstants.BALANCE, sessionVariable.getLakeBucketAssignMode());

        sessionVariable.setLakeBucketAssignMode("elastic");
        Assertions.assertEquals(SessionVariableConstants.ELASTIC, sessionVariable.getLakeBucketAssignMode());

        try {
            sessionVariable.setLakeBucketAssignMode("auto");
            Assertions.fail("cannot set a invalid value");
        } catch (Exception e) {
            Assertions.assertTrue(
                    e.getMessage().contains("Legal values of lake_bucket_assign_mode are elastic|balance"),
                    e.getMessage());
        }
    }

    @Test
    public void testSetEnableInsertPartialUpdate() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableInsertPartialUpdate(true);
        Assertions.assertTrue(sessionVariable.isEnableInsertPartialUpdate());

        sessionVariable.setEnableInsertPartialUpdate(false);
        Assertions.assertFalse(sessionVariable.isEnableInsertPartialUpdate());
    }

    @Test
    public void testConnectorSinkShuffleModeBackwardCompatibility() {
        SessionVariable sessionVariable = new SessionVariable();

        // Default mode is AUTO
        Assertions.assertEquals(com.starrocks.connector.ConnectorSinkShuffleMode.AUTO,
                sessionVariable.getConnectorSinkShuffleMode());

        // Backward compatibility: enableIcebergSinkGlobalShuffle implies FORCE when mode stays at default AUTO.
        com.starrocks.common.jmockit.Deencapsulation.setField(sessionVariable, "enableIcebergSinkGlobalShuffle", true);
        Assertions.assertEquals(com.starrocks.connector.ConnectorSinkShuffleMode.FORCE,
                sessionVariable.getConnectorSinkShuffleMode());

        // Explicitly set mode to NEVER should not be affected by legacy boolean.
        com.starrocks.common.jmockit.Deencapsulation.setField(sessionVariable, "connectorSinkShuffleMode", "never");
        Assertions.assertEquals(com.starrocks.connector.ConnectorSinkShuffleMode.NEVER,
                sessionVariable.getConnectorSinkShuffleMode());
    }

    @Test
    public void testEnableMVPlanner() {
        SessionVariable sessionVariable = new SessionVariable();

        // Test default value
        Assertions.assertFalse(sessionVariable.isMVPlanner());

        // Test setter and getter
        sessionVariable.setMVPlanner(true);
        Assertions.assertTrue(sessionVariable.isMVPlanner());

        sessionVariable.setMVPlanner(false);
        Assertions.assertFalse(sessionVariable.isMVPlanner());
    }
}
