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

package com.starrocks.sql.automv.boolalgebra;

import com.starrocks.common.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BoolAlgebraTest {

    @Test

    public void testTriBool() {
        List<Pair<TriBool, TriBool>> tbPairs = Stream.of(TriBool.values())
                .flatMap(a -> Stream.of(TriBool.values()).map(b -> Pair.create(a, b)))
                .collect(Collectors.toList());
        TriBool[][] results = new TriBool[][] {
                {TriBool.TB_FALSE, TriBool.TB_FALSE, TriBool.TB_TRUE, TriBool.TB_TRUE, TriBool.TB_TRUE},
                {TriBool.TB_FALSE, TriBool.TB_NULL, TriBool.TB_NULL, TriBool.TB_FALSE, TriBool.TB_TRUE},
                {TriBool.TB_FALSE, TriBool.TB_TRUE, TriBool.TB_FALSE, TriBool.TB_FALSE, TriBool.TB_TRUE},
                {TriBool.TB_FALSE, TriBool.TB_NULL, TriBool.TB_NULL, TriBool.TB_FALSE, TriBool.TB_NULL},
                {TriBool.TB_NULL, TriBool.TB_NULL, TriBool.TB_NULL, TriBool.TB_TRUE, TriBool.TB_NULL},
                {TriBool.TB_NULL, TriBool.TB_TRUE, TriBool.TB_NULL, TriBool.TB_FALSE, TriBool.TB_NULL},
                {TriBool.TB_FALSE, TriBool.TB_TRUE, TriBool.TB_FALSE, TriBool.TB_FALSE, TriBool.TB_FALSE},
                {TriBool.TB_NULL, TriBool.TB_TRUE, TriBool.TB_NULL, TriBool.TB_FALSE, TriBool.TB_FALSE},
                {TriBool.TB_TRUE, TriBool.TB_TRUE, TriBool.TB_TRUE, TriBool.TB_TRUE, TriBool.TB_FALSE},
        };
        for (int i = 0; i < tbPairs.size(); ++i) {
            TriBool a = tbPairs.get(i).first;
            TriBool b = tbPairs.get(i).second;
            TriBool andResult = a.and(b);
            TriBool orResult = a.or(b);
            TriBool eqResult = a.eq(b);
            TriBool nullSafeEqResult = a.nullSafeEq(b);
            TriBool notResult = a.not();
            TriBool[] expect = results[i];
            Assert.assertEquals(andResult, expect[0]);
            Assert.assertEquals(orResult, expect[1]);
            Assert.assertEquals(eqResult, expect[2]);
            Assert.assertEquals(nullSafeEqResult, expect[3]);
            Assert.assertEquals(notResult, expect[4]);
        }
    }

    @Test
    public void testPowerBool() {
        List<Pair<PowerBool, PowerBool>> pbPairs = Stream.of(PowerBool.values())
                .flatMap(a -> Stream.of(PowerBool.values()).map(b -> Pair.create(a, b)))
                .collect(Collectors.toList());
        PowerBool[][] results = {
                {PowerBool.PB_FALSE, PowerBool.PB_FALSE, PowerBool.PB_TRUE, PowerBool.PB_TRUE, PowerBool.PB_TRUE},
                {PowerBool.PB_FALSE, PowerBool.PB_NULL, PowerBool.PB_NULL, PowerBool.PB_FALSE, PowerBool.PB_TRUE},
                {PowerBool.PB_FALSE, PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_TRUE},
                {PowerBool.PB_FALSE, PowerBool.PB_TRUE, PowerBool.PB_FALSE, PowerBool.PB_FALSE, PowerBool.PB_TRUE},
                {PowerBool.PB_FALSE, PowerBool.PB_NOT_NULL, PowerBool.PB_NOT_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_TRUE},
                {PowerBool.PB_FALSE, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_FALSE,
                        PowerBool.PB_TRUE},
                {PowerBool.PB_FALSE, PowerBool.PB_UNKNOWN, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_TRUE},
                {PowerBool.PB_FALSE, PowerBool.PB_NULL, PowerBool.PB_NULL, PowerBool.PB_FALSE, PowerBool.PB_NULL},
                {PowerBool.PB_NULL, PowerBool.PB_NULL, PowerBool.PB_NULL, PowerBool.PB_TRUE, PowerBool.PB_NULL},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_NULL, PowerBool.PB_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_NULL},
                {PowerBool.PB_NULL, PowerBool.PB_TRUE, PowerBool.PB_NULL, PowerBool.PB_FALSE, PowerBool.PB_NULL},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_NULL, PowerBool.PB_FALSE,
                        PowerBool.PB_NULL},
                {PowerBool.PB_NULL, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_NULL},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_NULL},
                {PowerBool.PB_FALSE, PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_TRUE_OR_NULL},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_NULL, PowerBool.PB_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_TRUE_OR_NULL},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_TRUE_OR_NULL,
                        PowerBool.PB_NOT_NULL, PowerBool.PB_TRUE_OR_NULL},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_TRUE, PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_FALSE,
                        PowerBool.PB_TRUE_OR_NULL},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_UNKNOWN, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_TRUE_OR_NULL},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_FALSE_OR_NULL,
                        PowerBool.PB_NOT_NULL, PowerBool.PB_TRUE_OR_NULL},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_UNKNOWN, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_TRUE_OR_NULL},
                {PowerBool.PB_FALSE, PowerBool.PB_TRUE, PowerBool.PB_FALSE, PowerBool.PB_FALSE, PowerBool.PB_FALSE},
                {PowerBool.PB_NULL, PowerBool.PB_TRUE, PowerBool.PB_NULL, PowerBool.PB_FALSE, PowerBool.PB_FALSE},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_TRUE, PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_FALSE,
                        PowerBool.PB_FALSE},
                {PowerBool.PB_TRUE, PowerBool.PB_TRUE, PowerBool.PB_TRUE, PowerBool.PB_TRUE, PowerBool.PB_FALSE},
                {PowerBool.PB_NOT_NULL, PowerBool.PB_TRUE, PowerBool.PB_NOT_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_FALSE},
                {PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_TRUE, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_FALSE},
                {PowerBool.PB_UNKNOWN, PowerBool.PB_TRUE, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_FALSE},
                {PowerBool.PB_FALSE, PowerBool.PB_NOT_NULL, PowerBool.PB_NOT_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_NOT_NULL},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_NULL, PowerBool.PB_FALSE,
                        PowerBool.PB_NOT_NULL},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_UNKNOWN, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_NOT_NULL},
                {PowerBool.PB_NOT_NULL, PowerBool.PB_TRUE, PowerBool.PB_NOT_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_NOT_NULL},
                {PowerBool.PB_NOT_NULL, PowerBool.PB_NOT_NULL, PowerBool.PB_NOT_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_NOT_NULL},
                {PowerBool.PB_UNKNOWN, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_NOT_NULL},
                {PowerBool.PB_UNKNOWN, PowerBool.PB_UNKNOWN, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_NOT_NULL},
                {PowerBool.PB_FALSE, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_FALSE,
                        PowerBool.PB_FALSE_OR_NULL},
                {PowerBool.PB_NULL, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_FALSE_OR_NULL},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_FALSE_OR_NULL,
                        PowerBool.PB_NOT_NULL, PowerBool.PB_FALSE_OR_NULL},
                {PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_TRUE, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_FALSE_OR_NULL},
                {PowerBool.PB_UNKNOWN, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_FALSE_OR_NULL},
                {PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_FALSE_OR_NULL},
                {PowerBool.PB_UNKNOWN, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_FALSE_OR_NULL},
                {PowerBool.PB_FALSE, PowerBool.PB_UNKNOWN, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_UNKNOWN},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_NULL, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_UNKNOWN},
                {PowerBool.PB_FALSE_OR_NULL, PowerBool.PB_UNKNOWN, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_UNKNOWN},
                {PowerBool.PB_UNKNOWN, PowerBool.PB_TRUE, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_UNKNOWN},
                {PowerBool.PB_UNKNOWN, PowerBool.PB_UNKNOWN, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_UNKNOWN},
                {PowerBool.PB_UNKNOWN, PowerBool.PB_TRUE_OR_NULL, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_UNKNOWN},
                {PowerBool.PB_UNKNOWN, PowerBool.PB_UNKNOWN, PowerBool.PB_UNKNOWN, PowerBool.PB_NOT_NULL,
                        PowerBool.PB_UNKNOWN},
        };
        for (int i = 0; i < pbPairs.size(); ++i) {
            PowerBool a = pbPairs.get(i).first;
            PowerBool b = pbPairs.get(i).second;
            PowerBool[] expect = results[i];
            PowerBool andResult = a.and(b);
            PowerBool orResult = a.or(b);
            PowerBool eqResult = a.eq(b);
            PowerBool nullSafeEqResult = a.nullSafeEq(b);
            PowerBool notResult = a.not();
            Assert.assertEquals(andResult, expect[0]);
            Assert.assertEquals(orResult, expect[1]);
            Assert.assertEquals(eqResult, expect[2]);
            Assert.assertEquals(nullSafeEqResult, expect[3]);
            Assert.assertEquals(notResult, expect[4]);
        }
    }
}
