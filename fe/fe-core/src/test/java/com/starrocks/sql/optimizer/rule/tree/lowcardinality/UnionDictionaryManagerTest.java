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

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Config;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.type.StringType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

class UnionDictionaryManagerTest {

    private static final SessionVariable SESSION_VARIABLE = new SessionVariable();

    @BeforeAll
    static void setupSharedResource() {
        SESSION_VARIABLE.setEnableLowCardinalityOptimizeForUnionAll(true);
    }

    private static ColumnDict makeDict(Collection<String> values, int collectedVersion, int version) {
        ImmutableMap.Builder<ByteBuffer, Integer> builder = ImmutableMap.builder();
        List<ByteBuffer> sortedValues = values.stream().sorted().map(StandardCharsets.UTF_8::encode).toList();
        for (int i = 0; i < sortedValues.size(); ++i) {
            builder.put(sortedValues.get(i), i + 1);
        }
        return new ColumnDict(builder.build(), collectedVersion, version);
    }

    private static ColumnDict makeDict(Collection<String> values, int version) {
        return makeDict(values, version, version);
    }

    private static ColumnDict makeDict(Collection<String> values) {
        return makeDict(values, 0);
    }

    private static List<String> getDictValues(ColumnDict dict) {
        String[] array = new String[dict.getDictSize()]; // Array of size 3
        for (Map.Entry<ByteBuffer, Integer> entry : dict.getDict().entrySet()) {
            array[entry.getValue() - 1] = StandardCharsets.UTF_8.decode(entry.getKey().duplicate()).toString();
        }
        return Arrays.asList(array);
    }

    @Test
    public void testMergeDictionaries() throws Exception {
        Map<Integer, ColumnDict> globalDicts = new HashMap<>(Map.of(
                1, makeDict(List.of("a", "g")),
                2, makeDict(List.of("a", "c")),
                3, makeDict(List.of("b", "e", "f")),
                4, makeDict(List.of("z"))));
        UnionDictionaryManager unionDictionaryManager =
                new UnionDictionaryManager(SESSION_VARIABLE, Map.of(), globalDicts, Set.of());
        Assertions.assertTrue(unionDictionaryManager.mergeDictionaries(List.of(1, 2, 3)));
        unionDictionaryManager.finalizeColumnDictionaries();
        Assertions.assertEquals(getDictValues(globalDicts.get(1)), List.of("a", "b", "c", "e", "f", "g"));
        Assertions.assertEquals(getDictValues(globalDicts.get(2)), List.of("a", "b", "c", "e", "f", "g"));
        Assertions.assertEquals(getDictValues(globalDicts.get(3)), List.of("a", "b", "c", "e", "f", "g"));
        Assertions.assertEquals(getDictValues(globalDicts.get(4)), List.of("z"));
    }

    @Test
    public void testMergeDictionariesMultipleStages() throws Exception {
        Map<Integer, ColumnDict> globalDicts = new HashMap<>(Map.of(
                1, makeDict(List.of("a", "g")),
                2, makeDict(List.of("a", "c")),
                3, makeDict(List.of("t")),
                4, makeDict(List.of("z")),
                5, makeDict(List.of("b", "e", "f")),
                6, makeDict(List.of("h")),
                7, makeDict(List.of("x", "y"))));
        UnionDictionaryManager unionDictionaryManager =
                new UnionDictionaryManager(SESSION_VARIABLE, Map.of(), globalDicts, Set.of());
        Assertions.assertTrue(unionDictionaryManager.mergeDictionaries(List.of(1, 2)));
        Assertions.assertTrue(unionDictionaryManager.mergeDictionaries(List.of(5, 6)));
        Assertions.assertTrue(unionDictionaryManager.mergeDictionaries(List.of(1, 5)));
        Assertions.assertTrue(unionDictionaryManager.mergeDictionaries(List.of(4, 7)));
        unionDictionaryManager.finalizeColumnDictionaries();
        Assertions.assertEquals(globalDicts.get(1).getDict(), globalDicts.get(2).getDict());
        Assertions.assertEquals(globalDicts.get(1).getDict(), globalDicts.get(5).getDict());
        Assertions.assertEquals(globalDicts.get(1).getDict(), globalDicts.get(6).getDict());
        Assertions.assertEquals(globalDicts.get(4).getDict(), globalDicts.get(7).getDict());
        Assertions.assertEquals(getDictValues(globalDicts.get(1)), List.of("a", "b", "c", "e", "f", "g", "h"));
        Assertions.assertEquals(getDictValues(globalDicts.get(2)), List.of("a", "b", "c", "e", "f", "g", "h"));
        Assertions.assertEquals(getDictValues(globalDicts.get(3)), List.of("t"));
        Assertions.assertEquals(getDictValues(globalDicts.get(4)), List.of("x", "y", "z"));
        Assertions.assertEquals(getDictValues(globalDicts.get(5)), List.of("a", "b", "c", "e", "f", "g", "h"));
        Assertions.assertEquals(getDictValues(globalDicts.get(6)), List.of("a", "b", "c", "e", "f", "g", "h"));
        Assertions.assertEquals(getDictValues(globalDicts.get(7)), List.of("x", "y", "z"));

        Assertions.assertEquals(unionDictionaryManager.getMergedDictColumnIds(), Set.of(1, 2, 4, 5, 6, 7));

        Collection<Set<Integer>> columnGroups = unionDictionaryManager.getUnionColumnGroups();
        Assertions.assertEquals(2, columnGroups.size());
        Assertions.assertTrue(columnGroups.contains(Set.of(1, 2, 5, 6)));
        Assertions.assertTrue(columnGroups.contains(Set.of(4, 7)));

    }

    @Test
    void testMergeDictionaryTooManyElements() {
        List<String> bigList = new ArrayList<>(IntStream.range(0, Config.low_cardinality_threshold - 2)
                .mapToObj(String::valueOf).sorted().toList());
        Map<Integer, ColumnDict> globalDicts = new HashMap<>(Map.of(
                1, makeDict(List.of("a", "b")),
                2, makeDict(bigList),
                3, makeDict(List.of("z"))));
        UnionDictionaryManager unionDictionaryManager =
                new UnionDictionaryManager(SESSION_VARIABLE, Map.of(), globalDicts, Set.of());
        Assertions.assertTrue(unionDictionaryManager.mergeDictionaries(List.of(2, 3)));
        Assertions.assertFalse(unionDictionaryManager.mergeDictionaries(List.of(3, 1)));
        unionDictionaryManager.finalizeColumnDictionaries();
        Assertions.assertEquals(globalDicts.get(2).getDict(), globalDicts.get(3).getDict());

        Assertions.assertEquals(getDictValues(globalDicts.get(1)), List.of("a", "b"));
        bigList.add("z");
        Assertions.assertEquals(getDictValues(globalDicts.get(2)), bigList);
        Assertions.assertEquals(getDictValues(globalDicts.get(3)), bigList);

        Assertions.assertEquals(unionDictionaryManager.getMergedDictColumnIds(), Set.of(2, 3));
        Collection<Set<Integer>> columnGroups = unionDictionaryManager.getUnionColumnGroups();
        Assertions.assertEquals(1, columnGroups.size());
        Assertions.assertTrue(columnGroups.contains(Set.of(2, 3)));
    }

    @Test
    void testMergeDictionaryBigDictionarySize() {
        List<String> bigList = new ArrayList<>(List.of("a".repeat(1024 * 1024 - 9 - 32)));
        Map<Integer, ColumnDict> globalDicts = new HashMap<>(Map.of(
                1, makeDict(List.of("a", "b")),
                2, makeDict(bigList),
                3, makeDict(List.of("z"))));
        UnionDictionaryManager unionDictionaryManager =
                new UnionDictionaryManager(SESSION_VARIABLE, Map.of(), globalDicts, Set.of());
        Assertions.assertTrue(unionDictionaryManager.mergeDictionaries(List.of(2, 3)));
        Assertions.assertFalse(unionDictionaryManager.mergeDictionaries(List.of(3, 1)));
        unionDictionaryManager.finalizeColumnDictionaries();
        Assertions.assertEquals(globalDicts.get(2).getDict(), globalDicts.get(3).getDict());

        Assertions.assertEquals(getDictValues(globalDicts.get(1)), List.of("a", "b"));
        bigList.add("z");
        Assertions.assertEquals(getDictValues(globalDicts.get(2)), bigList);
        Assertions.assertEquals(getDictValues(globalDicts.get(3)), bigList);

        Assertions.assertEquals(unionDictionaryManager.getMergedDictColumnIds(), Set.of(2, 3));
        Collection<Set<Integer>> columnGroups = unionDictionaryManager.getUnionColumnGroups();
        Assertions.assertEquals(1, columnGroups.size());
        Assertions.assertTrue(columnGroups.contains(Set.of(2, 3)));
    }

    @Test
    void testConflictWithJoin() {
        Map<Integer, ColumnDict> globalDicts = new HashMap<>(Map.of(
                1, makeDict(List.of("a", "b")),
                2, makeDict(List.of("c", "d")),
                3, makeDict(List.of("e"))));
        UnionDictionaryManager unionDictionaryManager =
                new UnionDictionaryManager(SESSION_VARIABLE, Map.of(), globalDicts, Set.of(3));
        Assertions.assertTrue(unionDictionaryManager.mergeDictionaries(List.of(1, 2)));
        Assertions.assertFalse(unionDictionaryManager.mergeDictionaries(List.of(1, 3)));
        unionDictionaryManager.finalizeColumnDictionaries();
        Assertions.assertEquals(globalDicts.get(1).getDict(), globalDicts.get(2).getDict());

        Assertions.assertEquals(getDictValues(globalDicts.get(1)), List.of("a", "b", "c", "d"));
        Assertions.assertEquals(getDictValues(globalDicts.get(2)), List.of("a", "b", "c", "d"));
        Assertions.assertEquals(getDictValues(globalDicts.get(3)), List.of("e"));

        Assertions.assertEquals(unionDictionaryManager.getMergedDictColumnIds(), Set.of(1, 2));
        Collection<Set<Integer>> columnGroups = unionDictionaryManager.getUnionColumnGroups();
        Assertions.assertEquals(1, columnGroups.size());
        Assertions.assertTrue(columnGroups.contains(Set.of(1, 2)));
    }

    @Test
    void testUseDefineExpr() {
        Map<Integer, ColumnDict> globalDicts = new HashMap<>(Map.of(
                1, makeDict(List.of("a", "b")),
                2, makeDict(List.of("c", "d")),
                3, makeDict(List.of("e"))));
        Map<Integer, ScalarOperator> stringRefToDefineExprMap = Map.of(
                4, new ColumnRefOperator(1, StringType.STRING, "", true),
                5, new ColumnRefOperator(2, StringType.STRING, "", true),
                6, new ColumnRefOperator(3, StringType.STRING, "", true),
                7, new ColumnRefOperator(6, StringType.STRING, "", true)
        );
        UnionDictionaryManager unionDictionaryManager =
                new UnionDictionaryManager(SESSION_VARIABLE, stringRefToDefineExprMap, globalDicts, Set.of(2));
        Assertions.assertTrue(unionDictionaryManager.mergeDictionaries(List.of(4, 7)));
        Assertions.assertFalse(unionDictionaryManager.mergeDictionaries(List.of(4, 5)));
        unionDictionaryManager.finalizeColumnDictionaries();
        Assertions.assertEquals(globalDicts.get(1).getDict(), globalDicts.get(3).getDict());

        Assertions.assertEquals(getDictValues(globalDicts.get(1)), List.of("a", "b", "e"));
        Assertions.assertEquals(getDictValues(globalDicts.get(2)), List.of("c", "d"));
        Assertions.assertEquals(getDictValues(globalDicts.get(3)), List.of("a", "b", "e"));

        Assertions.assertEquals(unionDictionaryManager.getMergedDictColumnIds(), Set.of(1, 3));
        Collection<Set<Integer>> columnGroups = unionDictionaryManager.getUnionColumnGroups();
        Assertions.assertEquals(1, columnGroups.size());
        Assertions.assertTrue(columnGroups.contains(Set.of(1, 3)));
    }

}