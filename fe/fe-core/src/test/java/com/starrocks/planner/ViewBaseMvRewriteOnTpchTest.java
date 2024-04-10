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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.common.FeConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class ViewBaseMvRewriteOnTpchTest extends MaterializedViewTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        MaterializedViewTestBase.beforeClass();
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
        starRocksAssert.useDatabase("test");
    }

    @ParameterizedTest(name = "ViewBasedRewriteOnTpch.{0}")
    @MethodSource("tpchSource")
    public void testTPCH(String name, String sql) throws Exception {
        testMvRewrite(name, sql);
    }

    private void testMvRewrite(String name, String sql) throws Exception {
        String viewName = String.format("v_%s", name);
        String dropViewSql = "drop view if exists " + viewName;
        connectContext.executeSql(dropViewSql);
        String createViewSql = String.format("create view %s as %s", viewName, sql);
        starRocksAssert.withView(createViewSql);
        {
            String query = String.format("select * from %s", viewName);
            String mv = query;
            testRewriteOK(mv, query);
        }
    }

    private static Stream<Arguments> tpchSource() {
        List<Arguments> cases = Lists.newArrayList();
        for (Map.Entry<String, String> entry : TpchSQL.getAllSQL().entrySet()) {
            // these queries are not supported because the view has only one column, and the column is double type, which
            // can not be used to create mv
            Set<String> filteredQueries = Sets.newHashSet("q6", "q14", "q17", "q19");
            if (!filteredQueries.contains(entry.getKey())) {
                cases.add(Arguments.of(entry.getKey(), entry.getValue()));
            }
        }
        return cases.stream();
    }
}
