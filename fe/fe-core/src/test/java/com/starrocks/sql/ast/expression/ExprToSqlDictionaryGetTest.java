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

package com.starrocks.sql.ast.expression;

import com.google.common.collect.Lists;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Round-trip serialization of {@code dictionary_get}.
 *
 * <p>The serialized form is re-parsed and re-analyzed by features that rebuild a query from an
 * expression tree — {@code partition_retention_condition} is one, and it queries
 * {@code information_schema.partitions_meta} with the reconstructed predicate. If the printer
 * emits a different number of arguments than the analyzer accepts, that internal query fails
 * analysis and the feature silently stops working.
 *
 * <p>An analyzed {@link DictionaryGetExpr} carries {@code keySize} (the dictionary's key count)
 * and {@code nullIfNotExist} as fields, while the optional boolean argument also stays in the
 * children list. The printer must therefore take the key count from {@code keySize} rather than
 * guessing it from {@code children.size()}.
 */
public class ExprToSqlDictionaryGetTest {

    private static DictionaryGetExpr dictGet(int keySize, boolean nullIfNotExist, boolean explicitFlagChild) {
        List<Expr> params = Lists.newArrayList();
        params.add(new StringLiteral("dict"));
        for (int i = 1; i <= keySize; i++) {
            params.add(new StringLiteral("k" + i));
        }
        if (explicitFlagChild) {
            params.add(new BoolLiteral(nullIfNotExist));
        }
        DictionaryGetExpr expr = new DictionaryGetExpr(params, NodePosition.ZERO);
        expr.setKeySize(keySize);              // set by ExpressionAnalyzer from the dictionary metadata
        expr.setNullIfNotExist(nullIfNotExist);
        expr.setType(IntegerType.INT);
        expr.setOriginType(IntegerType.INT);
        return expr;
    }

    /** Top-level arguments of {@code NAME(a, b, c)}; every argument here is a literal. */
    private static List<String> args(String sql) {
        String inner = sql.substring(sql.indexOf('(') + 1, sql.lastIndexOf(')'));
        return Arrays.asList(inner.split(",\\s*"));
    }

    private static void assertArgs(int keySize, boolean nullIfNotExist, boolean explicitFlagChild) {
        String sql = ExprToSql.toSql(dictGet(keySize, nullIfNotExist, explicitFlagChild));
        List<String> args = args(sql);
        // dictionary name + keySize keys + exactly one null_if_not_exist flag
        Assertions.assertEquals(keySize + 2, args.size(),
                "wrong argument count in " + sql);
        for (int i = 1; i <= keySize; i++) {
            Assertions.assertTrue(args.get(i).contains("k" + i),
                    "key " + i + " missing or misplaced in " + sql);
        }
        Assertions.assertEquals(nullIfNotExist ? "true" : "false", args.get(args.size() - 1),
                "null_if_not_exist flag not serialized exactly once in " + sql);
    }

    // ---- control: single-key calls have always been serialized correctly ------------------

    @Test
    public void oneKeyWithExplicitFlag() {
        assertArgs(1, true, true);
    }

    @Test
    public void oneKeyWithoutExplicitFlag() {
        assertArgs(1, false, false);
    }

    // ---- trigger: multi-key calls ----------------------------------------------------------

    @Test
    public void twoKeysWithExplicitFlag() {
        // children = [dict, k1, k2, TRUE] -> printing all four and then appending the flag
        // yields five arguments, and re-analysis rejects it.
        assertArgs(2, true, true);
    }

    @Test
    public void twoKeysWithoutExplicitFlag() {
        // children = [dict, k1, k2] -> the size==3 heuristic mistakes the second key for the
        // optional flag and drops it. Silent, and worse than the loud failure above.
        assertArgs(2, false, false);
    }

    @Test
    public void threeKeysWithExplicitFlag() {
        assertArgs(3, false, true);
    }
}
