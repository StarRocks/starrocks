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

package com.starrocks.sql.automv.qe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.starrocks.catalog.TableName;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.automv.ast.ShowRecommendationsStmt;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.thrift.TRowFormat;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AsyncRecommendationsTaskTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(TestUtil.prepareTables("ssb", TestUtil::getSsbCreateTableSqlList));
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        getStarRocksAssert();
    }

    @Test
    public void testAsync() {
        ConnectContext ctx = STARROCKS_ASSERT.get().getCtx();
        Set<String> queryNames = Sets.newHashSet("Q3.3", "Q3.4");
        List<Pair<String, String>> queryList =
                TestUtil.getSsbLineorderFlatQueryList().stream().filter(p -> queryNames.contains(p.first))
                        .collect(Collectors.toList());
        AutoMVUtil.mockUpCustomizedQueryExecutor(queryList);
        TableName tableName = new TableName(null, "db", "_tunespace_");
        ShowRecommendationsStmt stmt = new ShowRecommendationsStmt(tableName, -1, -1);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioLWM(0);
        ctx.getSessionVariable().setAutoMVCardRowCountRatioHWM(1.0);
        ctx.getSessionVariable().setAutoMVUseHllCountDistinct(true);
        ShowResultSet showResultSet = TunespaceExecutor.execute(stmt, ctx);
        for (List<String> row : showResultSet.getResultRows()) {
            PrettyPrinter printer = new PrettyPrinter();
            printer.addItemsWithDelNl(";", row);
        }

        TablePlus resultTable = ShowRecommendationResult.getTable("result_table", 1, 1);

        List<ShowRecommendationResult> showResultList = showResultSet.getResultRows().stream()
                .map(row -> ShowRecommendationResult.of("task", row))
                .collect(Collectors.toList());

        List<String> insertSqlList = showResultList.stream().map(res -> resultTable.getInsertSql(ImmutableList.of(res)))
                .collect(Collectors.toList());
        Assert.assertFalse(insertSqlList.isEmpty());
        Assert.assertTrue(insertSqlList.get(0), insertSqlList.get(0).contains("CREATE MATERIALIZED VIEW _mv_"));

        TRowFormat rowFormat = ColumnPlus.pack(showResultList.get(0), resultTable.getColumnPluses());
        ShowRecommendationResult result =
                ColumnPlus.unpack(ShowRecommendationResult.class, resultTable.getColumnPluses(), rowFormat);
        result.setTs(showResultList.get(0).getTs());
        Assert.assertEquals(result, showResultList.get(0));
    }
}
