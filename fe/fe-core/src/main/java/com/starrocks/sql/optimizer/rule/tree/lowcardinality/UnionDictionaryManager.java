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

import com.google.api.client.util.Maps;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.Sets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.util.UnionFind;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.type.IntegerType;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class UnionDictionaryManager {
    private final UnionFind<Integer> unionColumnGroups = new UnionFind<>();
    private final Map<Integer, ImmutableMap<ByteBuffer, Integer>> unionDictData = Maps.newHashMap();
    private final SessionVariable sessionVariable;
    private final Map<Integer, ScalarOperator> stringRefToDefineExprMap;
    private final Map<Integer, ColumnDict> globalDicts;
    private final Set<Integer> joinEqColumnGroupIds;
    private final Map<Integer, ByteBuffer> constantColumns = Maps.newHashMap();

    private static final int CONSTANT_ID = -1;

    public UnionDictionaryManager(SessionVariable sessionVariable,
                                  Map<Integer, ScalarOperator> stringRefToDefineExprMap,
                                  Map<Integer, ColumnDict> globalDicts,
                                  Set<Integer> joinEqColumnGroupIds) {
        this.sessionVariable = sessionVariable;
        this.stringRefToDefineExprMap = stringRefToDefineExprMap;
        this.globalDicts = globalDicts;
        this.joinEqColumnGroupIds = joinEqColumnGroupIds;
    }

    private static ImmutableMap<ByteBuffer, Integer> mergeDictionaryData(List<ImmutableMap<ByteBuffer, Integer>> dicts,
                                                                         Collection<ByteBuffer> constants) {
        Set<ByteBuffer> uniques = Sets.newHashSet();
        uniques.addAll(constants);
        dicts.forEach(d -> uniques.addAll(d.keySet()));
        int totalDictSize = uniques.stream().map(b -> b.remaining() + 4).reduce(0, Integer::sum);
        // TODO(farhad-celo): This constant is hardcoded in other places, refactor to a config.
        final int DICT_PAGE_MAX_SIZE = 1024 * 1024;
        if (uniques.size() > Config.low_cardinality_threshold || totalDictSize > DICT_PAGE_MAX_SIZE - 32) {
            return null;
        }
        List<ByteBuffer> sortedValues = uniques.stream().sorted().toList();
        ImmutableMap.Builder<ByteBuffer, Integer> builder = ImmutableMap.builder();
        for (int i = 0; i < sortedValues.size(); ++i) {
            builder.put(sortedValues.get(i), i + 1);
        }
        return builder.build();
    }

    private Integer getSourceDictionaryColumnId(Integer cid) {
        if (globalDicts.containsKey(cid)) {
            return cid;
        }
        if (constantColumns.containsKey(cid)) {
            return CONSTANT_ID;
        }
        ScalarOperator define = stringRefToDefineExprMap.get(cid);
        if (define == null || !define.isColumnRef()) {
            return null;
        }
        int newId = ((ColumnRefOperator) define).getId();
        if (newId == cid) {
            return null;
        }
        return getSourceDictionaryColumnId(newId);
    }

    Integer mergeDictionaries(List<Integer> columnIds) {
        if (!sessionVariable.isEnableLowCardinalityOptimizeForUnionAll()) {
            return null;
        }
        List<ByteBuffer> allConstantData =
                columnIds.stream().map(constantColumns::get).filter(Objects::nonNull).toList();
        List<Integer> nonConstantColumnIds = columnIds.stream().map(this::getSourceDictionaryColumnId)
                .filter(cid -> cid == null || cid != CONSTANT_ID).toList();
        if (nonConstantColumnIds.isEmpty()
                || nonConstantColumnIds.contains(null)
                || !nonConstantColumnIds.stream().allMatch(globalDicts::containsKey)
                || nonConstantColumnIds.stream().anyMatch(joinEqColumnGroupIds::contains)) {
            return null;
        }
        nonConstantColumnIds.forEach(cid -> {
            Integer groupId = unionColumnGroups.getGroupIdOrAdd(cid);
            if (!unionDictData.containsKey(groupId)) {
                Preconditions.checkState(globalDicts.containsKey(cid));
                unionDictData.put(groupId, globalDicts.get(cid).getDict());
            }
        });
        List<Integer> columnGroupIds = nonConstantColumnIds.stream().map(
                cid -> unionColumnGroups.getGroupId(cid).orElseThrow()).distinct().toList();
        List<ImmutableMap<ByteBuffer, Integer>> allDictData = columnGroupIds.stream().map(unionDictData::get).toList();
        ImmutableMap<ByteBuffer, Integer> mergedDictData = mergeDictionaryData(allDictData, allConstantData);
        if (mergedDictData == null) {
            return null;
        }
        final Integer firstElement = nonConstantColumnIds.get(0);
        nonConstantColumnIds.forEach(cid -> unionColumnGroups.union(cid, firstElement));
        Integer finalGroup = unionColumnGroups.getGroupId(firstElement).orElseThrow();
        unionDictData.put(finalGroup, mergedDictData);
        return columnIds.stream().filter(cid -> firstElement.equals(getSourceDictionaryColumnId(cid))).findAny()
                .orElseThrow();
    }

    void finalizeColumnDictionaries() {
        unionColumnGroups.getAllGroups().forEach(s -> {
            Integer groupId = unionColumnGroups.getGroupId(s.stream().findAny().orElseThrow()).orElseThrow();
            ImmutableMap<ByteBuffer, Integer> dictData = unionDictData.get(groupId);
            Preconditions.checkNotNull(dictData);
            s.forEach(cid -> {
                Preconditions.checkState(globalDicts.containsKey(cid));
                globalDicts.compute(cid, (k, oldDict) ->
                        new ColumnDict(dictData, oldDict.getCollectedVersion(), oldDict.getVersion()));
            });
        });
    }

    Collection<Set<Integer>> getUnionColumnGroups() {
        return unionColumnGroups.getAllGroups().stream().toList();
    }

    Set<Integer> getMergedDictColumnIds() {
        return unionColumnGroups.getAllGroups().stream().flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    void recordIfConstant(ColumnRefOperator key, ScalarOperator value) {
        if (value.isConstantRef() && (value.getType().isStringType() || value.getType().isNull())) {
            ConstantOperator constant = value.cast();
            ByteBuffer buffer = constant.isConstantNull() ? null : ByteBuffer.wrap(constant.getVarchar().getBytes());
            constantColumns.put(key.getId(), buffer);
        } else if (value.isColumnRef()) {
            ColumnRefOperator c = value.cast();
            if (constantColumns.containsKey(c.getId())) {
                constantColumns.put(key.getId(), constantColumns.get(c.getId()));
            }
        }
    }

    private ConstantOperator generateConstantOperator(ColumnRefOperator column, Map<ByteBuffer, Integer> dict) {
        ByteBuffer constantValue = constantColumns.get(column.getId());
        if (constantValue == null) {
            return ConstantOperator.createNull(IntegerType.INT);
        }
        Integer index = dict.get(constantValue);
        Preconditions.checkNotNull(index);
        return ConstantOperator.createInt(index);
    }

    List<Map<ColumnRefOperator, ConstantOperator>> generateConstantEncodingMap(List<ColumnRefOperator> outputColumns,
                                                                               List<List<ColumnRefOperator>> childColumns,
                                                                               Set<Integer> allStringColumns) {
        List<Map<ColumnRefOperator, ConstantOperator>> result = Lists.newArrayList();
        childColumns.forEach(c -> result.add(Maps.newHashMap()));
        for (int i = 0; i < outputColumns.size(); ++i) {
            if (!allStringColumns.contains(outputColumns.get(i).getId())) {
                continue;
            }
            Integer dictColumnId = getSourceDictionaryColumnId(outputColumns.get(i).getId());
            Preconditions.checkNotNull(dictColumnId);
            Map<ByteBuffer, Integer> dictData = unionDictData.get(
                    unionColumnGroups.getGroupId(dictColumnId).orElseThrow());
            Preconditions.checkNotNull(dictData);
            for (int j = 0; j < childColumns.size(); ++j) {
                ColumnRefOperator c = childColumns.get(j).get(i);
                if (constantColumns.containsKey(c.getId())) {
                    result.get(j).put(c, generateConstantOperator(c, dictData));
                }
            }
        }
        return result;

    }

    boolean isSupportedConstant(ColumnRefOperator c) {
        return constantColumns.containsKey(c.getId());
    }
}
