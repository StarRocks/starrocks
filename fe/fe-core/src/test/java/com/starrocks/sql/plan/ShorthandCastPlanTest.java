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

import com.starrocks.catalog.View;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShorthandCastPlanTest extends PlanTestBase {

    @Test
    public void testTypeCastEquivalentToCast() throws Exception {
        String[][] pairs = {
                {"select v1::int from t0", "select cast(v1 as int) from t0"},
                {"select v1::int::string from t0", "select cast(cast(v1 as int) as string) from t0"},
                {"select v1 + v2::int from t0", "select v1 + cast(v2 as int) from t0"},
                {"select (v1+v2)::decimal(10,2) from t0", "select cast((v1+v2) as decimal(10,2)) from t0"},
                {"select v1::varchar(20) from t0", "select cast(v1 as varchar(20)) from t0"},
                {"select v1::int < 5 from t0", "select cast(v1 as int) < 5 from t0"},
                {"select v1 < v2::int from t0", "select v1 < cast(v2 as int) from t0"},
                {"select '[1,2,3]'::array<int>[1]", "select cast('[1,2,3]' as array<int>)[1]"},
                {"select -2147483648::int", "select -cast(2147483648 as int)"},
        };
        for (String[] pair : pairs) {
            String shorthandPlan = getFragmentPlan(pair[0]);
            String classicPlan = getFragmentPlan(pair[1]);
            assertEquals(classicPlan, shorthandPlan, "plan mismatch for: " + pair[0]);
        }
    }

    @Test
    public void testDereferenceAfterScalarCastFailsCleanly() {
        Exception exception = Assertions.assertThrows(Exception.class, () -> getFragmentPlan("select v1::int.col from t0"));
        assertContains(exception.getMessage(), "must be a struct type");
    }

    @Test
    public void testCreateViewPersistsCanonicalCast() throws Exception {
        String viewName = "shorthand_cast_view";
        starRocksAssert.withView("create or replace view " + viewName + " as select v1::int as c1 from t0");

        View view = (View) starRocksAssert.getTable("test", viewName);
        Assertions.assertTrue(view.getInlineViewDef().contains("CAST("));
        Assertions.assertFalse(view.getInlineViewDef().contains("::"));
        Assertions.assertTrue(view.getDDLViewDef().contains("CAST("));
        Assertions.assertFalse(view.getDDLViewDef().contains("::"));

        List<List<String>> result = starRocksAssert.show("show create view " + viewName);
        assertEquals(1, result.size());
        String showCreateView = result.get(0).get(1);
        Assertions.assertTrue(showCreateView.contains("CAST("));
        Assertions.assertTrue(showCreateView.contains(" AS INT)"));
        Assertions.assertFalse(showCreateView.contains("::"));
    }
}
