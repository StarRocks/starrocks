// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.plan;

import org.junit.jupiter.api.Test;

public class ConvertTimeZonePredicateTest extends PlanTestBase {

    @Test
    public void testConvert_tz() throws Exception {
        String sql = "SELECT count(1)\n" +
                "FROM test_time\n" +
                "WHERE (convert_tz(__time, '+08:00', '+09:00') >= '2025-11-16 10:00:00')";
        String planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 1: __time >= '2025-11-16 09:00:00'");

        sql = "SELECT count(1)\n" +
                "FROM test_time\n" +
                "WHERE (convert_tz(__time, '+08:00', 'Asia/Yakutsk') >= '2025-11-16 10:00:00')";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 1: __time >= '2025-11-16 09:00:00'");
    }
}
