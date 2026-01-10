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
import com.starrocks.common.Config;
import com.starrocks.common.util.UnionFind;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class UnionDictionaryManager {
    private final UnionFind<Integer> unionColumnGroups = new UnionFind<>();
    private final Map<Integer, ImmutableMap<ByteBuffer, Integer>> unionDictData = Maps.newHashMap();
    private final SessionVariable sessionVariable;
    private final Map<Integer, ScalarOperator> stringRefToDefineExprMap;
    private final Map<Integer, ColumnDict> globalDicts;
    private final Set<Integer> joinEqColumnGroupIds;

    public UnionDictionaryManager(SessionVariable sessionVariable,
                                  Map<Integer, ScalarOperator> stringRefToDefineExprMap,
                                  Map<Integer, ColumnDict> globalDicts,
                                  Set<Integer> joinEqColumnGroupIds) {
        this.sessionVariable = sessionVariable;
        this.stringRefToDefineExprMap = stringRefToDefineExprMap;
        this.globalDicts = globalDicts;
        this.joinEqColumnGroupIds = joinEqColumnGroupIds;
    }

    private static ImmutableMap<ByteBuffer, Integer> mergeDictionaryData(List<ImmutableMap<ByteBuffer, Integer>> dicts) {
        Set<ByteBuffer> uniques = Sets.newHashSet();
        dicts.forEach(d -> uniques.addAll(d.keySet()));
        int totalDictSize = uniques.stream().map(b -> b.limit() - b.position() + 4).reduce(0, Integer::sum);
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

    boolean mergeDictionaries(List<Integer> columnIds) {
        if (!sessionVariable.isEnableLowCardinalityOptimizeForUnionAll()) {
            return false;
        }
        columnIds = columnIds.stream().map(this::getSourceDictionaryColumnId).toList();
        if (columnIds.contains(null) || !columnIds.stream().allMatch(globalDicts::containsKey)
                || columnIds.stream().anyMatch(joinEqColumnGroupIds::contains)) {
            return false;
        }
        columnIds.forEach(cid -> {
            Integer groupId = unionColumnGroups.getGroupIdOrAdd(cid);
            if (!unionDictData.containsKey(groupId)) {
                Preconditions.checkState(globalDicts.containsKey(cid));
                unionDictData.put(groupId, globalDicts.get(cid).getDict());
            }
        });
        List<Integer> columnGroupIds = columnIds.stream().map(
                cid -> unionColumnGroups.getGroupId(cid).orElseThrow()).distinct().toList();
        List<ImmutableMap<ByteBuffer, Integer>> allData = columnGroupIds.stream().map(unionDictData::get).toList();
        ImmutableMap<ByteBuffer, Integer> mergedDictData = mergeDictionaryData(allData);
        if (mergedDictData == null) {
            return false;
        }
        final Integer firstElement = columnIds.get(0);
        columnIds.forEach(cid -> unionColumnGroups.union(cid, firstElement));
        Integer finalGroup = unionColumnGroups.getGroupId(firstElement).orElseThrow();
        unionDictData.put(finalGroup, mergedDictData);
        return true;
    }

    void finalizeColumnDictionaries() {
        unionColumnGroups.getAllGroups().stream().filter(s -> s.size() > 1).forEach(s -> {
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
        return unionColumnGroups.getAllGroups().stream().filter(s -> s.size() > 1).toList();
    }

    Set<Integer> getMergedDictColumnIds() {
        return unionColumnGroups.getAllGroups().stream().filter(s -> s.size() > 1).flatMap(Set::stream)
                .collect(Collectors.toSet());
    }
}

