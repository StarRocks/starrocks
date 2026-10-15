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
package com.starrocks.authorization.ranger;

import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerRequestScriptEvaluator;
import org.apache.ranger.plugin.util.RangerRequestExprResolver;
import org.apache.ranger.plugin.util.ScriptEngineUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngine;

public class RangerScriptEngineTest {
    @Test
    public void testDynamicRowFilter() {
        RangerRequestExprResolver resolver =
                new RangerRequestExprResolver("owner = '${{ctx.getUser()}}'", "starrocks");
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setUser("alice");

        // Exercise Ranger's real engine discovery and bindings instead of mocking a policy result.
        Assertions.assertEquals("owner = 'alice'", resolver.resolveExpressions(request));
    }

    @Test
    public void testScriptCondition() {
        ScriptEngine engine = ScriptEngineUtil.createScriptEngine("starrocks");
        Assertions.assertNotNull(engine, "Ranger requires a JavaScript engine on the FE runtime classpath");
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setUser("alice");
        RangerRequestScriptEvaluator evaluator = new RangerRequestScriptEvaluator(request, engine, false);

        evaluator.evaluateConditionScript("ctx.getUser() == 'alice'");
        Assertions.assertTrue(evaluator.getResult());
        evaluator.evaluateConditionScript("ctx.getUser() == 'bob'");
        Assertions.assertFalse(evaluator.getResult());
    }
}
