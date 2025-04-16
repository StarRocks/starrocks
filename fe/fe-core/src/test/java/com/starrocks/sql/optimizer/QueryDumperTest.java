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

package com.starrocks.sql.optimizer;

import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumper;
import com.starrocks.sql.plan.PlanTestBase;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryDumperTest extends PlanTestBase {
    private DumpInfo prevDumpInfo = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Before
    public void before() {
        prevDumpInfo = connectContext.getDumpInfo();
        connectContext.setDumpInfo(null);
    }

    @After
    public void after() {
        // After dumping query, the connect context should be reset to the previous state.
        assertThat(connectContext.isHTTPQueryDump()).isFalse();
        assertThat(connectContext.getCurrentCatalog()).isEqualTo("default_catalog");
        assertThat(connectContext.getDatabase()).isEqualTo("test");

        connectContext.setDumpInfo(prevDumpInfo);
    }

    @Test
    public void testDumpQueryInvalidConnectContext() {
        ConnectContext context = ConnectContext.get();
        try {
            ConnectContext.remove();

            Pair<HttpResponseStatus, String> statusAndRes = QueryDumper.dumpQuery("catalog", "db", "", false);
            assertThat(statusAndRes.first).isEqualTo(HttpResponseStatus.BAD_REQUEST);
            assertThat(statusAndRes.second).contains("There is no ConnectContext for this thread:");

        } finally {
            context.setThreadLocalInfo();
        }
    }

    @Test
    public void testDumpQueryInvalidQuery() {
        {

            Pair<HttpResponseStatus, String> statusAndRes = QueryDumper.dumpQuery("default_catalog", "test", "", false);
            assertThat(statusAndRes.first).isEqualTo(HttpResponseStatus.BAD_REQUEST);
            assertThat(statusAndRes.second).isEqualTo("query is empty");
        }

        {
            Pair<HttpResponseStatus, String> statusAndRes = QueryDumper.dumpQuery("default_catalog", "test", "no-query", false);
            assertThat(statusAndRes.first).isEqualTo(HttpResponseStatus.BAD_REQUEST);
            assertThat(statusAndRes.second).contains("execute query failed. Getting syntax error");
        }
    }

    @Test
    public void testDumpQueryInvalidCatalog() {
        Pair<HttpResponseStatus, String> statusAndRes =
                QueryDumper.dumpQuery("no-catalog", "test", "select count(v1) from t0", false);
        assertThat(statusAndRes.first).isEqualTo(HttpResponseStatus.NOT_FOUND);
        assertThat(statusAndRes.second).isEqualTo("Database [no-catalog.test] does not exists");
    }

    @Test
    public void testDumpQueryInvalidDatabase() {
        Pair<HttpResponseStatus, String> statusAndRes =
                QueryDumper.dumpQuery("default_catalog", "no-db", "select count(v1) from t0", false);
        assertThat(statusAndRes.first).isEqualTo(HttpResponseStatus.NOT_FOUND);
        assertThat(statusAndRes.second).isEqualTo("Database [default_catalog.no-db] does not exists");
    }

    @Test
    public void testDumpQuerySuccess() {
        {
            Pair<HttpResponseStatus, String> statusAndRes =
                    QueryDumper.dumpQuery("default_catalog", "test", "select count(v1) from t0", false);
            assertThat(statusAndRes.first).isEqualTo(HttpResponseStatus.OK);
            // Non-empty column_statistics.
            assertThat(statusAndRes.second).containsPattern("\"column_statistics\":\\{.+\\}");
        }
        {
            Pair<HttpResponseStatus, String> statusAndRes =
                    QueryDumper.dumpQuery("", "", "select count(v1) from t0", false);
            assertThat(statusAndRes.first).isEqualTo(HttpResponseStatus.OK);
            // Non-empty column_statistics.
            assertThat(statusAndRes.second).containsPattern("\"column_statistics\":\\{.+\\}");
        }
        {
            Pair<HttpResponseStatus, String> statusAndRes =
                    QueryDumper.dumpQuery("default_catalog", "test", "select count(v1) from t0", true);
            assertThat(statusAndRes.first).isEqualTo(HttpResponseStatus.OK);
            // Non-empty column_statistics.
            assertThat(statusAndRes.second)
                    .containsPattern("\"column_statistics\":\\{.+\\}")
                            .contains("SELECT count(tbl_mock_001.mock_002)");
        }
    }
}
