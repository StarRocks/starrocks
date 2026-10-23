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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

// An equality join records `<key expression> IS NOT NULL` for its join keys when it does not push it down (the
// date_dim join pushes a predicate to the same side), and an outer join below may become an inner join on it. That is
// only right when the key expression is NULL for the NULL-extended rows: `if(s is null, 1, s)` is 1 there, so those
// rows still match web_site_sk = 1.
public class OuterJoinRecordedNotNullExpressionTest extends TPCDS1TTestBase {

    private static final String OUTER_JOIN = "select ws_web_site_sk s, wr_returned_date_sk d " +
            "from web_sales right join web_returns on ws_item_sk = wr_item_sk and ws_order_number = wr_order_number";

    private static String query(String key) {
        return "select * from (" + OUTER_JOIN + ") t join web_site on " + key + " = web_site_sk " +
                "join date_dim on t.d = d_date_sk where d_year = 2000";
    }

    @ParameterizedTest
    @ValueSource(strings = {"if(t.s is null, 1, t.s)", "coalesce(t.s, 1)", "ifnull(t.s, 1)"})
    public void testKeyExpressionKeepsNull(String key) throws Exception {
        assertContains(getFragmentPlan(query(key)), "RIGHT OUTER JOIN");
    }

    @Test
    public void testKeyExpressionRejectsNull() throws Exception {
        assertNotContains(getFragmentPlan(query("t.s + 1")), "OUTER JOIN");
    }
}
