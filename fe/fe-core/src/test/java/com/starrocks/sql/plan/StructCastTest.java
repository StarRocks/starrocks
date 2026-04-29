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

package com.starrocks.sql.plan;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class StructCastTest extends PlanTestBase {
    @Test
    public void testPositionCast() throws Exception {
        final ConnectContext context = ConnectContext.get();
        final SessionVariable sv = context.getSessionVariable();
        final long prev = sv.getSqlMode();
        try {
            sv.setSqlMode(prev & ~SqlModeHelper.MODE_STRUCT_CAST_BY_NAME);

            String sql = "select cast(named_struct('key',1,'value',1) as struct<key1 int,value1 int>)";
            String plan = getFragmentPlan(sql);

            assertContains(plan,
                    "CAST(named_struct('key', 1, 'value', 1) AS struct<`key1` int(11), `value1` int(11)>)");
        } finally {
            sv.setSqlMode(prev);
        }
    }

    @Test
    public void testNamedCast() throws Exception {
        final ConnectContext context = ConnectContext.get();
        final SessionVariable sv = context.getSessionVariable();
        final long prev = sv.getSqlMode();
        try {
            sv.setSqlMode(prev | SqlModeHelper.MODE_STRUCT_CAST_BY_NAME);

            assertThrows(SemanticException.class, () -> {
                String sql = "select cast(named_struct('key',1,'value',1) as struct<key1 int,value1 int>)";
                getFragmentPlan(sql);
            });

            String sql = "select cast(named_struct('k',1,'v',1) as struct<v int,`k` int>)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "CAST(named_struct('k', 1, 'v', 1) AS struct<`v` int(11), `k` int(11)>)");
        } finally {
            sv.setSqlMode(prev);
        }
    }

}
