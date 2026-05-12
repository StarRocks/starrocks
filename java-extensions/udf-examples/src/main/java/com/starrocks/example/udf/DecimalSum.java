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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

// Example UDAF: sum of a DECIMAL column, mapped to Java java.math.BigDecimal.
// Demonstrates the BigDecimal input/output contract for Java UDAFs.
public class DecimalSum {

    public static class State {
        BigDecimal sum = BigDecimal.ZERO;

        public int serializeLength() {
            byte[] bytes = sum.toPlainString().getBytes(StandardCharsets.UTF_8);
            return 4 + bytes.length;
        }
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public void update(State state, BigDecimal value) {
        if (value != null) {
            state.sum = state.sum.add(value);
        }
    }

    public void serialize(State state, ByteBuffer buffer) {
        byte[] bytes = state.sum.toPlainString().getBytes(StandardCharsets.UTF_8);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    public void merge(State state, ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        BigDecimal other = new BigDecimal(new String(bytes, StandardCharsets.UTF_8));
        state.sum = state.sum.add(other);
    }

    public BigDecimal finalize(State state) {
        return state.sum;
    }
}
