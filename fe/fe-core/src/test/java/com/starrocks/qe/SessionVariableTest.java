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

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SessionVariableTest {

    @Test
    public void testNonDefaultVariables() {
        SessionVariable sessionVariable = new SessionVariable();
        Map<String, SessionVariable.NonDefaultValue> nonDefaultVariables = sessionVariable.getNonDefaultVariables();
        Assert.assertTrue(nonDefaultVariables.isEmpty());

        sessionVariable.setSqlDialect("test1");
        nonDefaultVariables = sessionVariable.getNonDefaultVariables();
        Assert.assertEquals(1, nonDefaultVariables.size());
        Assert.assertTrue(nonDefaultVariables.containsKey(SessionVariable.SQL_DIALECT));
        SessionVariable.NonDefaultValue kv = nonDefaultVariables.get(SessionVariable.SQL_DIALECT);
        Assert.assertEquals(SessionVariable.DEFAULT_SESSION_VARIABLE.getSqlDialect(), kv.defaultValue);
        Assert.assertEquals("test1", kv.actualValue);

        sessionVariable.setPipelineProfileLevel(100);
        nonDefaultVariables = sessionVariable.getNonDefaultVariables();
        Assert.assertEquals(2, nonDefaultVariables.size());
        Assert.assertTrue(nonDefaultVariables.containsKey(SessionVariable.PIPELINE_PROFILE_LEVEL));
        kv = nonDefaultVariables.get(SessionVariable.PIPELINE_PROFILE_LEVEL);
        Assert.assertEquals(SessionVariable.DEFAULT_SESSION_VARIABLE.getPipelineProfileLevel(), kv.defaultValue);
        Assert.assertEquals(100, kv.actualValue);
    }

    @Test
    public void testSetChooseMode() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setChooseExecuteInstancesMode("adaptive_increase");
        Assert.assertTrue(sessionVariable.getChooseExecuteInstancesMode().enableIncreaseInstance());

        sessionVariable.setChooseExecuteInstancesMode("adaptive_decrease");
        Assert.assertTrue(sessionVariable.getChooseExecuteInstancesMode().enableDecreaseInstance());

        sessionVariable.setChooseExecuteInstancesMode("auto");
        Assert.assertTrue(sessionVariable.getChooseExecuteInstancesMode().enableIncreaseInstance());
        Assert.assertTrue(sessionVariable.getChooseExecuteInstancesMode().enableDecreaseInstance());

        try {
            sessionVariable.setChooseExecuteInstancesMode("xxx");
            Assert.fail("cannot set a invalid value");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(),
                    e.getMessage().contains("Legal values of choose_execute_instances_mode are"));
        }
    }
}
