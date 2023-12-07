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
package com.starrocks.http;

import com.starrocks.http.action.QueryProfileAction;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class QueryProfileActionTest {

    @Test
    public void testEscapeHtmlInPreTag() throws Exception {
        Class<QueryProfileAction> clazz = QueryProfileAction.class;
        Method method = clazz.getDeclaredMethod("appendQueryProfile", StringBuilder.class, String.class);
        Assert.assertNotNull(method);
        method.setAccessible(true);

        StringBuilder buffer = new StringBuilder();
        String content = "Query:\n" +
                "  Summary:\n" +
                "     - Query ID: eaff21d2-3734-11ee-909f-8e20563011de\n" +
                "     - Start Time: 2023-08-10 12:18:11\n" +
                "     - End Time: 2023-08-10 12:18:11\n" +
                "     - Total: 150ms\n" +
                "     - Query Type: Query\n" +
                "     - Query State: Finished\n" +
                "     - StarRocks Version: bugfix2-0da335ff34\n" +
                "     - User: root\n" +
                "     - Test: a<b<c\n" +
                "     - Default Db: ssb\n" +
                "     - Sql Statement: select count(s_suppkey), count(s_name), count(s_address), count(s_city), " +
                "count(s_nation), count(s_region), count(s_phone), count(lo_revenue), count(lo_shipmode), " +
                "count(lo_quantity), count(lo_partkey), count(lo_discount) from lineorder join supplier on " +
                "lo_suppkey=s_suppkey and lo_partkey<s_suppkey\n" +
                "     - Variables: parallel_fragment_exec_instance_num=1,max_parallel_scan_instance_num=-1," +
                "pipeline_dop=0,enable_adaptive_sink_dop=true,enable_runtime_adaptive_dop=false," +
                "runtime_profile_report_interval=10\n" +
                "     - Collect Profile Time: 41ms";
        method.invoke(new QueryProfileAction(null), buffer, content);

        Assert.assertEquals("<pre id='profile'>" +
                "Query:\n" +
                "  Summary:\n" +
                "     - Query ID: eaff21d2-3734-11ee-909f-8e20563011de\n" +
                "     - Start Time: 2023-08-10 12:18:11\n" +
                "     - End Time: 2023-08-10 12:18:11\n" +
                "     - Total: 150ms\n" +
                "     - Query Type: Query\n" +
                "     - Query State: Finished\n" +
                "     - StarRocks Version: bugfix2-0da335ff34\n" +
                "     - User: root\n" +
                "     - Test: a<b<c\n" +
                "     - Default Db: ssb\n" +
                "     - Sql Statement: select count(s_suppkey), count(s_name), count(s_address), count(s_city), " +
                "count(s_nation), count(s_region), count(s_phone), count(lo_revenue), count(lo_shipmode), " +
                "count(lo_quantity), count(lo_partkey), count(lo_discount) from lineorder join supplier on " +
                "lo_suppkey=s_suppkey and lo_partkey&lt;s_suppkey\n" +
                "     - Variables: parallel_fragment_exec_instance_num=1,max_parallel_scan_instance_num=-1," +
                "pipeline_dop=0,enable_adaptive_sink_dop=true,enable_runtime_adaptive_dop=false," +
                "runtime_profile_report_interval=10\n" +
                "     - Collect Profile Time: 41ms\n" +
                "</pre>", buffer.toString());
    }
}
