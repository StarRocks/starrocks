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
    @MethodSource("generateHint")
    void buildHintNode(String hintStr, Map<String, String> expectMap) {
        CommonToken token = new CommonToken(1, hintStr);
        HintNode hint = HintFactory.buildHintNode(token);
        if (hint == null) {
            Assert.assertEquals(0, expectMap.size());
        } else {
            String message = "actual: " + hint.getValue() + ". expect: " + expectMap;
            for (Map.Entry<String, String> entry : hint.getValue().entrySet()) {
                Assert.assertEquals(message, expectMap.get(entry.getKey()), entry.getValue());
            }
        }
    }


    private static Stream<Arguments> generateHint() {
        List<Arguments> arguments = Lists.newArrayList();
        arguments.add(Arguments.of("/*+ invalid_test */", ImmutableMap.of()));
        arguments.add(Arguments.of("/*+ set _VAR() */", ImmutableMap.of()));
        arguments.add(Arguments.of("/*+ set _VAR((abc = abc)) */", ImmutableMap.of()));
        arguments.add(Arguments.of("/*+     \r \n \u3000 set_VAR  (abc=abc,, abc = abc) */",
                ImmutableMap.of()));
        arguments.add(Arguments.of("/*+     \n\r \n \u3000 set_VAR  (abc==abc) */",
                ImmutableMap.of()));
        arguments.add(Arguments.of("/*+     \r \n \u3000 set_VAR  (('abc'='abc')) */",
                ImmutableMap.of()));
        arguments.add(Arguments.of("/*+ set_VAR('abc'='abc', \r\n 'ab' ='ab')) */",
                ImmutableMap.of()));

        arguments.add(Arguments.of("/*+     \r \n \u3000 set_VAR (abc=abc) */",
                ImmutableMap.of("abc", "abc")));
        arguments.add(Arguments.of("/*+  set_VAR \r \n \u3000 (abc=abc) */",
                ImmutableMap.of("abc", "abc")));
        arguments.add(Arguments.of("/*+     \r \n \u3000 set_VAR ('abc'=abc) */",
                ImmutableMap.of("abc", "abc")));

        arguments.add(Arguments.of("/*+     \r \n \u3000 set_VAR ('abc'=abc,  ab     = '\r\na   b') */",
                ImmutableMap.of("abc", "abc", "ab", "\r\na   b")));



        return arguments.stream();
    }
}