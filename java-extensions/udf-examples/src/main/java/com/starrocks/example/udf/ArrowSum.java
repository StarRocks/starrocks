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

package com.starrocks.example.udf;

import org.apache.arrow.vector.IntVector;

import java.nio.ByteBuffer;

// Vectorized ("input"="arrow") UDAF: SUM over INT. Only update() receives the whole-batch
// FieldVector; create/merge/serialize/finalize keep the normal boxed signatures. Supported for
// global / sorted aggregation (a whole batch belongs to one state); hash GROUP BY is rejected.
public class ArrowSum {
    public static class State {
        long sum = 0;

        public int serializeLength() {
            return 8;
        }
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {}

    public void update(State state, IntVector v) {
        for (int i = 0; i < v.getValueCount(); i++) {
            if (!v.isNull(i)) {
                state.sum += v.get(i);
            }
        }
    }

    public void serialize(State state, ByteBuffer buf) {
        buf.putLong(state.sum);
    }

    public void merge(State state, ByteBuffer buf) {
        state.sum += buf.getLong();
    }

    public Long finalize(State state) {
        return state.sum;
    }
}
