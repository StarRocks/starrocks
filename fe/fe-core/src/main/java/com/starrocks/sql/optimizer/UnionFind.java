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

package com.starrocks.sql.optimizer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

// UnionFind support equivalence inferring based on equivalence classes
// TODO(by satanson): There are many UnionFind utilities, in future, all of them will be
//  unified into one
public class UnionFind<T> {
    private final Map<T, Integer> element2Group = Maps.newHashMap();
    private final Map<Integer, Set<T>> eqGroupMap = Maps.newHashMap();

    public Map<T, Set<T>> getEquivGroups(Set<T> elements) {
        Map<T, Set<T>> elm2group = Maps.newHashMap();
        for (T elm : elements) {
            if (!element2Group.containsKey(elm)) {
                continue;
            }
            Set<T> eqGroup = eqGroupMap.get(element2Group.get(elm));
            if (eqGroup.size() > 1) {
                elm2group.put(elm, eqGroup);
            }
        }
        return elm2group;
    }

    public Set<T> getEquivGroup(T element) {
        if (element2Group.containsKey(element)) {
            return Collections.unmodifiableSet(eqGroupMap.get(element2Group.get(element)));
        } else {
            return Collections.emptySet();
        }
    }

    public void add(T... elements) {
        for (T elm : elements) {
            if (!find(elm)) {
                int groupIdx = element2Group.size();
                element2Group.put(elm, groupIdx);
                eqGroupMap.put(groupIdx, Sets.newHashSet(elm));
            }
        }
    }

    public int getGroupIdOrAdd(T elem) {
        add(elem);
        return element2Group.get(elem);
    }

    public Optional<Integer> getGroupId(T elem) {
        if (element2Group.containsKey(elem)) {
            return Optional.of(element2Group.get(elem));
        } else {
            return Optional.empty();
        }
    }

    public Set<T> getGroup(int groupId) {
        return eqGroupMap.getOrDefault(groupId, Collections.emptySet());
    }

    public void union(T lhs, T rhs) {
        add(lhs, rhs);
        Integer lhsGroupIdx = element2Group.get(lhs);
        Integer rhsGroupIdx = element2Group.get(rhs);
        if (!lhsGroupIdx.equals(rhsGroupIdx)) {
            Set<T> lhsGroup = eqGroupMap.get(lhsGroupIdx);
            Set<T> rhsGroup = eqGroupMap.get(rhsGroupIdx);
            lhsGroup.addAll(rhsGroup);
            rhsGroup.forEach(s -> element2Group.put(s, lhsGroupIdx));
            eqGroupMap.remove(rhsGroupIdx);
        }
    }

    public boolean find(T slotId) {
        return element2Group.containsKey(slotId);
    }

    public boolean containsAll(Collection<T> keys) {
        return element2Group.keySet().containsAll(keys);
    }

    public void removesAll(Collection<T> keys) {
        for (T key : keys) {
            if (!element2Group.containsKey(key)) {
                continue;
            }
            int groupIdx = element2Group.remove(key);
            eqGroupMap.get(groupIdx).remove(key);
        }
    }
}
