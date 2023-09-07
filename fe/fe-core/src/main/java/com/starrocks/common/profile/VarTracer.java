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

package com.starrocks.common.profile;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VarTracer {
    private final Map<String, Var<?>> allVars = Maps.newHashMap();

    public void record(long time, String name, String value) {
        StringVar v = new StringVar(time, name);
        v.update(value);
        allVars.put(name, v);
    }

    public void count(long time, String name, int value) {
        if (allVars.containsKey(name)) {
            allVars.get(name).add(value);
            return;
        }
        CounterVar v = new CounterVar(time, name);
        v.update(value);
        allVars.put(name, v);
    }

    public List<Var<?>> getAllVarsWithOrder() {
        return allVars.values().stream().sorted(Comparator.comparingLong(Var::getTimePoint))
                .collect(Collectors.toList());
    }

    public List<Var<?>> getAllVars() {
        return Lists.newArrayList(allVars.values());
    }

    private static class StringVar extends Var<String> {
        public StringVar(long timePoints, String name) {
            super(timePoints, name);
        }
    }

    private static class CounterVar extends Var<Integer> {
        public CounterVar(long timePoints, String name) {
            super(timePoints, name);
        }

        @Override
        public void add(int var) {
            value += var;
        }
    }
}
