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

package com.starrocks.udf;

import java.util.ArrayList;
import java.util.List;

public class FunctionStates<T> {
    private final int PROBE_MAX_TIMES = 16;
    private int probeIndex = 0;
    private int emptySlots = 0;
    public List<T> states = new ArrayList<>();

    public T get(int idx) {
        return states.get(idx);
    }

    Object[] batch_get(int[] idxs) {
        Object[] res = new Object[idxs.length];
        for (int i = 0; i < idxs.length; ++i) {
            if (idxs[i] != -1) {
                res[i] = states.get(idxs[i]);
            }
        }
        return res;
    }

    public int add(T state) throws Exception {
        // probe empty slot from
        if (emptySlots > states.size() / 2) {
            int probeTimes = 0;
            do {
                if (states.get(probeIndex) == null) {
                    states.set(probeIndex, state);
                    emptySlots--;
                    final int ret = probeIndex++;
                    if (probeIndex == states.size()) {
                        probeIndex = 0;
                    }
                    return ret;
                }
                probeIndex++;
                if (probeIndex == states.size()) {
                    probeIndex = 0;
                }
                probeTimes++;
            } while (probeTimes <= PROBE_MAX_TIMES);
        }
        states.add(state);
        return states.size() - 1;
    }

    public void remove(int idx) {
        emptySlots++;
        states.set(idx, null);
    }

    public void clear() {
        states.clear();
        emptySlots = 0;
        probeIndex = 0;
    }

    // used for test
    public int size() {
        return states.size();
    }

}
