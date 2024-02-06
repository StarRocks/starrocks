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

package com.starrocks.sql.parser;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.analysis.HintNode;
import com.starrocks.qe.SessionVariable;
import org.antlr.v4.runtime.CommonToken;
import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class HintFactoryTest {

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("generateValidHint")
    void buildHintNode(String hintStr, Map<String, String> expectMap) {
        CommonToken token = new CommonToken(1, hintStr);
        HintNode hint = HintFactory.buildHintNode(token, new SessionVariable());
        String message = "actual: " + hint.getValue() + ". expect: " + expectMap;
        for (Map.Entry<String, String> entry : hint.getValue().entrySet()) {
            Assert.assertEquals(message, expectMap.get(entry.getKey()), entry.getValue());
        }
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("generateInvalidHint")
    void invalidHintNode(String hintStr) {
        CommonToken token = new CommonToken(1, hintStr);
        try {
            HintNode hint = HintFactory.buildHintNode(token, new SessionVariable());
            Assert.assertEquals(null, hint);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Invalid hint value"));
        }
    }


    private static Stream<Arguments> generateValidHint() {
        List<Arguments> arguments = Lists.newArrayList();
        arguments.add(Arguments.of("/*+     \r \n \u3000 set_VAR (abc=abc) */",
                ImmutableMap.of("abc", "abc")));
        arguments.add(Arguments.of("/*+  set_VAR \r \n \u3000 (abc=abc) */",
                ImmutableMap.of("abc", "abc")));
        arguments.add(Arguments.of("/*+     \r \n \u3000 set_VAR ('abc'=abc) */",
                ImmutableMap.of("abc", "abc")));

        arguments.add(Arguments.of("/*+     \r \n \u3000 set_VAR ('abc'=abc,  ab     = '\r\na   b') */",
                ImmutableMap.of("abc", "abc", "ab", "\r\na   b")));
        arguments.add(Arguments.of("/*+     \r \n \u3000 set_user_variable (@a = 1) */",
                ImmutableMap.of("a", "1")));
        arguments.add(Arguments.of("/*+  set_user_variable \r \n \u3000 (@`a` = 1) */",
                ImmutableMap.of("a", "1")));
        arguments.add(Arguments.of("/*+     \r \n \u3000 SET_USER_VARIABLE (@`a` = 1) */",
                ImmutableMap.of("a", "1")));

        arguments.add(Arguments.of("/*+     \r \n \u3000 set_USER_VARIABLE (@`a` = 1,  @ab     = '\r\na   b') */",
                ImmutableMap.of("a", "1", "ab", "'\r\na   b'")));
        return arguments.stream();
    }

    private static Stream<Arguments> generateInvalidHint() {
        List<Arguments> arguments = Lists.newArrayList();
        arguments.add(Arguments.of("/*+ invalid_test */"));
        arguments.add(Arguments.of("/*+ set _VAR() */"));
        arguments.add(Arguments.of("/*+ set _VAR((abc = abc)) */"));
        arguments.add(Arguments.of("/*+     \r \n \u3000 set_VAR  (abc=abc,, abc = abc) */"));
        arguments.add(Arguments.of("/*+     \n\r \n \u3000 set_VAR  (abc==abc) */"));
        arguments.add(Arguments.of("/*+     \r \n \u3000 set_VAR  (('abc'='abc')) */"));
        arguments.add(Arguments.of("/*+ set_VAR('abc'='abc', \r\n 'ab' ='ab')) */"));
        arguments.add(Arguments.of("/* set_var('abc' = 'abc') /*+ set_var(abc =abc) */ */"));
        arguments.add(Arguments.of("/* set_var('abc' = 'abc') /*+ set_var(abc =abc) */ */"));
        arguments.add(Arguments.of("/* set_user_variable(@a = 'abc') /*+ set_var(abc =abc) */ */"));
        return arguments.stream();
    }
}