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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class VarTracerTest {

    @Test
    public void testSameTimePointKeepsRecordOrder() {
        VarTracer tracer = new VarTracer();
        List<String> names = List.of("g.Table", "g.Requests", "g.IOTime", "g.Retries", "g.Slowest",
                "a", "z", "m", "b", "y");
        for (String name : names) {
            tracer.record(7, name, "v");
        }
        // recorded again later, all at one new time point: still in the order first recorded
        for (String name : names) {
            tracer.record(9, name, "v2");
        }
        Assertions.assertEquals(names,
                tracer.getAllVarsWithOrder().stream().map(v -> v.name).collect(Collectors.toList()));
    }
}
