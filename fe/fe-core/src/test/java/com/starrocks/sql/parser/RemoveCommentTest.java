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

import com.clearspring.analytics.util.Lists;
import com.starrocks.common.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RemoveCommentTest {

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("sqlList")
    void testRemoveComment(Pair<String, String> test) {
        try {
            Class<?> clazz = SqlParser.class;
            String methodName = "removeComment";

            Class<?>[] parameterTypes = new Class<?>[] { String.class };

            Method method = clazz.getDeclaredMethod(methodName, parameterTypes);

            method.setAccessible(true);

            Object result = method.invoke(null, test.second);
            System.out.println(result);
            assertEquals(test.first, result);
        } catch (Exception e) {
            fail("sql: " + test.second + " failed");
        }
    }


    public static Stream<Arguments> sqlList() {
        List<Pair<String, String>> list = Lists.newArrayList();
        list.add(Pair.create("insert INTO tbl (col) VALUES ('a-\\\\'),('Q--8')",
                "insert INTO tbl (col) VALUES ('a-\\\\'),('Q--8')"));
        list.add(Pair.create("insert INTO tbl (col) VALUES ('a-\\\\\\''),('Q--8')",
                "insert INTO tbl (col) VALUES ('a-\\\\\\''),('Q--8')"));
        list.add(Pair.create("insert INTO tbl (col) VALUES ('a-\\\\')           ,('Q--8')",
                "insert INTO tbl (col) VALUES ('a-\\\\') -- '\'abcd'\n,('Q--8')"));
        list.add(Pair.create("select concat(\"\\\"abc--\\\\\\\\\\\\\", '\\'ac--abc\\\\\\\\\\'')",
                "select concat(\"\\\"abc--\\\\\\\\\\\\\", '\\'ac--abc\\\\\\\\\\'')"));
        list.add(Pair.create("select concat(\"\\\"abc\\\\\\\\\\\\\"           , '\\'abc\\\\\\\\\\'');",
                "select concat(\"\\\"abc\\\\\\\\\\\\\" /* \\\"abc\" */, '\\'abc\\\\\\\\\\'');"));
        return list.stream().map(e -> Arguments.of(e));
    }
}
