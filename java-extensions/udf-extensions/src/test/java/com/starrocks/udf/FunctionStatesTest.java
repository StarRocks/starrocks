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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class FunctionStatesTest {
    private static class State {

    }
    // Test Agg batch Call Single
    @Test
    public void testAddFunctionStates() throws Exception {
        FunctionStates<State> states = new FunctionStates<>();
        List<Integer> lst = new ArrayList<>();
        for (int i = 0; i < 4096; i++) {
            lst.add(states.add(new State()));
        }
        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < 4095; i++) {
                states.remove(lst.get(i));
            }
            lst.clear();
            for (int i = 0; i < 4095; i++) {
                lst.add(states.add(new State()));
            }
        }

        Assertions.assertEquals(states.size(), 8191);
    }
}
