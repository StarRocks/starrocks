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

package com.starrocks.sql.automv.generator;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TableUsage;
import com.starrocks.sql.automv.qe.PartitionExtractor;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class OneOneMVGeneratorTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        if (STARROCKS_ASSERT.get() == null) {
            try {
                STARROCKS_ASSERT.set(TestUtil.prepareTables("tpcds", TestUtil::getTPCDSCreateTableSqlList));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return STARROCKS_ASSERT.get();
    }

    @Test
    public void test11MVGenerator() {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        String q = TestUtil.getTPCDSQuery("query01");
        List<Pair<String, PlanPiece>> pieces =
                AutoMVUtil.get11MVPieces(ctx, ImmutableList.of(Pair.create("query01", q)), name -> true);

        List<TableUsage> tableUsages = pieces.stream()
                .map(p -> p.second)
                .map(TableUsage::analyzeUsage)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        Assert.assertEquals(tableUsages.size(), 6);
        List<TableUsage> mergedTableUsages = TableUsage.mergeUsages(tableUsages);
        Assert.assertEquals(mergedTableUsages.size(), 4);
        String[] expectResults = new String[] {
                "SELECT\n" +
                        "  `tpcds`.`customer`.c_customer_sk\n" +
                        "  ,`tpcds`.`customer`.c_customer_id\n" +
                        "FROM\n" +
                        "  `tpcds`.`customer`",
                "SELECT\n" +
                        "  `tpcds`.`store`.s_state\n" +
                        "  ,`tpcds`.`store`.s_store_sk\n" +
                        "FROM\n" +
                        "  `tpcds`.`store`",

                "SELECT\n" +
                        "  `tpcds`.`date_dim`.d_year\n" +
                        "  ,`tpcds`.`date_dim`.d_date_sk\n" +
                        "FROM\n" +
                        "  `tpcds`.`date_dim`",
                "SELECT\n" +
                        "  `tpcds`.`store_returns`.sr_returned_date_sk\n" +
                        "  ,`tpcds`.`store_returns`.sr_customer_sk\n" +
                        "  ,`tpcds`.`store_returns`.sr_store_sk\n" +
                        "  ,`tpcds`.`store_returns`.sr_return_amt\n" +
                        "FROM\n" +
                        "  `tpcds`.`store_returns`\n" +
                        "WHERE\n" +
                        "  (`tpcds`.`store_returns`.sr_store_sk IS NOT NULL)"
        };

        for (int i = 0; i < mergedTableUsages.size(); ++i) {
            TableUsage tableUsage = mergedTableUsages.get(i);
            AutoMVOptions options = AutoMVOptions.of(new PartitionExtractor(), ctx.getSessionVariable());
            ColumnRefToIdConverter idConverter = tableUsage.getPiece().getCommonState().getIdConverter();
            MVGenerateContext mvGenerateContext = MVGenerateContext.builder()
                    .setMvNameGenerator(query -> MVName.generateFromQuery(query).toString())
                    .setNextId(idConverter::nextId)
                    .setOptions(options)
                    .build();
            Optional<QueryGenerateResult> result = OneOneMVGenerator.generate(tableUsage, mvGenerateContext);
            Assert.assertTrue(result.isPresent());
            String s = result.get().getSubquery().getResult();
            Assert.assertTrue(s, s.contains(expectResults[i]));
        }
    }
}
