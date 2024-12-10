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

package com.starrocks.sql.automv.pieces;

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.automv.policies.AggregatePolicy;
import com.starrocks.sql.automv.policies.EliminateSemiAntiJoinPolicy;
import com.starrocks.sql.automv.qe.QueryStatementPlus;
import com.starrocks.sql.automv.qe.RboOptimizer;
import com.starrocks.sql.automv.qe.TableInfo;
import com.starrocks.sql.automv.qe.TypePlus;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PlanPieceTest {
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

    @BeforeClass
    public static void setUp() throws Exception {
        getStarRocksAssert();
    }

    @Ignore
    @Test
    public void testCategorizePieces() {
        ConnectContext ctx = getStarRocksAssert().getCtx();
        Set<String> excludeQuerySet = ImmutableSet.of("query61");

        List<Pair<String, String>> queryList = TestUtil.getTPCDSQueryList()
                .stream()
                .filter(p -> !excludeQuerySet.contains(p.first))
                .collect(Collectors.toList());
        List<Pair<String, AggregatePiece>> pieces = AutoMVUtil.getPieces(ctx, queryList, name -> true);
        Assert.assertEquals(pieces.size(), 198);
        System.out.println("pieceSize=" + pieces.size());
        pieces.forEach(p -> p.second.assignPieceIds());
        Map<String, List<Pair<String, PlanPiece>>> pieceGroups = pieces.stream()
                .map(p -> Pair.create(p.first, PlanPieceNormalizer.normalize(p.second)))
                .collect(Collectors.groupingBy(p ->
                        p.second.mustCast(AggregatePiece.class).getFlatTable().getPiece().getAuxState().getNormHash()));
        System.out.println("groupSize=" + pieceGroups.size());
        Assert.assertEquals(pieceGroups.size(), 85);
        Object[][] expectResults = new Object[][] {
                {"af5577d0b0a576f719cc417bcc0a5b70", ImmutableSet.of("query78")},
                {"3a5b366721fa2837c6aeeb195523a7e5", ImmutableSet.of("query55", "query03", "query52", "query42")},
                {"fcb4faf9c5a17ae1fd02c529fd84457b", ImmutableSet.of("query78")},
                {"012c9bc8dbd5313f671197a88968a743", ImmutableSet.of("query13")},
                {"096450856629b3812d00feb3dc437409", ImmutableSet.of("query24-1", "query24-2", "query24")},
                {"0e525259e189400a0cae68b41f2d273e", ImmutableSet.of("query12")},
                {"2245cde91868562bd1c0db366a16388f", ImmutableSet.of("query64-2", "query64")},
                {"9106b56163bc428a312320003ed3fcea", ImmutableSet.of("query62")},
                {"7be2159247be2c17973a755a1955ff93", ImmutableSet.of("query91")},
                {"8b0c06179d21befcbfb9a6825df21cb4", ImmutableSet.of("query26")},
                {"c7b4c9190240dd966c6421a1c33248bd", ImmutableSet.of("query04")},
                {"cf6668a29c107395d6998d149c4f5fbc", ImmutableSet.of("query78")},
                {"8aa4242bef79782cd7c70f55c3184341", ImmutableSet.of("query96")},
                {"7e63e48e20bbb29acd876f2587544c81", ImmutableSet.of("query15")},
                {"6439c20afa0aabbe0903c90bda4ad84b", ImmutableSet.of("query04", "query11", "query74")},
                {"f0d4e878fd72fbb53eeda1cf57a134d4", ImmutableSet.of("query49")},
                {"f702b2508ded146b48391e3a4d668257", ImmutableSet.of("query85")},
                {"66c24787ec370d34f7c9c3591ebc3c83", ImmutableSet.of("query99")},
                {"cc5fb67952af4e770d240484293cb13d", ImmutableSet.of("query56")},
                {"a8f800967f274494cf6500c24020fc87", ImmutableSet.of("query82")},
                {"0e7b94197e7519ec968fb0c9a0847738", ImmutableSet.of("query43")},
                {"c6690cee54a3d0adf053d719439e0474", ImmutableSet.of("query16")},
                {"f6963adb1e8070394aa6e62953eae136", ImmutableSet.of("query33")},
                {"3fe2a1389e3d1eedcad5af18ece98456", ImmutableSet.of("query06", "query41")},
                {"b92d147f43ad7cb481db34f4c80b60a2", ImmutableSet.of("query48")},
                {"735f953bf0407319fb55bafe83eaa4f8",
                        ImmutableSet.of("query39", "query39-2-2", "query21", "query39-1-2", "query39-2", "query39-1")},
                {"8b8b4d8931597ca28d194899cd68f18f", ImmutableSet.of("query56")},
                {"b25cc343a8d4a7077e12c0c44b8ba338", ImmutableSet.of("query83")},
                {"43d4c08a71a76ce6d5d235c7d4d10272", ImmutableSet.of("query01")},
                {"9a72fa5bc45b987d6354c7ae9eff8aca", ImmutableSet.of("query80")},
                {"21a0571dd9ceb2e13b160dcd2364edad", ImmutableSet.of("query77")},
                {"7a0b94c8082ec939e4f34666a2fb97b5", ImmutableSet.of("query60")},
                {"5c867b29605d266afa70c4a44d6c9be7", ImmutableSet.of("query06", "query54")},
                {"937581ed5f5c509a0b334638261415dd", ImmutableSet.of("query93")},
                {"a17494ea842fa0d47f88a54c8d6d0b00", ImmutableSet.of("query38", "query87")},
                {"da725543f9af9f60dfee4329369257a8", ImmutableSet.of("query65")},
                {"bac2d8c56937f10c39aff93bf74297d5", ImmutableSet.of("query89", "query47", "query63", "query53")},
                {"67ca583de57252a5f5e7f58ade982f13", ImmutableSet.of("query97", "query32")},
                {"9774df2ee2807704e99fa598e18a9170", ImmutableSet.of("query49")},
                {"1c1eee8630e24e47ce3143b47ea3fb54", ImmutableSet.of("query83")},
                {"17ede490eff6e58105a946e89f2bf532", ImmutableSet.of("query37")},
                {"cbfdfee7b1b0998dea79ff81382e001d", ImmutableSet.of("query90")},
                {"a61826152d93f4d628478eb74609fc73", ImmutableSet.of("query66")},
                {"16a112dfbc381b83221bbef8dea766fc", ImmutableSet.of("query80")},
                {"b15c06d3fc4efc0c999901abaaa68479", ImmutableSet.of("query23-1", "query23-2", "query23", "query98")},
                {"6d19da0b7e96ace11f81a0db821b16c6", ImmutableSet.of("query69")},
                {"62207f53f0f8d0148480a48e3c215695", ImmutableSet.of("query07")},
                {"a3fce86e56cf7906f7e8722e91a079cc", ImmutableSet.of("query88")},
                {"74047c0150b2fe7feea1de4e8f8330e6", ImmutableSet.of("query04", "query11", "query74")},
                {"c8dee18ef54e1af87f888dd4dd415656", ImmutableSet.of("query77", "query70")},
                {"75f5d2094c30f0049049644c0fa3bf41", ImmutableSet.of("query23-1", "query23-2", "query23")},
                {"7133365cbec9dfead0b7f7dc46f90cde", ImmutableSet.of("query83")},
                {"7ffb28bb9a297d10131c0b2221be8e05", ImmutableSet.of("query08")},
                {"65afbf5ca3e1ae951fc8f15409644eb9", ImmutableSet.of("query49")},
                {"f7a1fb6b97a2d787ee3a59b0134b809b", ImmutableSet.of("query95")},
                {"edd429da5851f72af129c81c8202bd30", ImmutableSet.of("query20")},
                {"489a4c97083d37437c6498a7f8d212fa", ImmutableSet.of("query77")},
                {"d20ef95124ad616e75874b040e9d1415", ImmutableSet.of("query33")},
                {"88f0ab9caaf31e8193f31170e888c701", ImmutableSet.of("query57")},
                {"65ac84f4682e5cf50af737e1b24330ae", ImmutableSet.of("query81")},
                {"d9c376342d02a1db946a1d52c34b62b1", ImmutableSet.of("query77")},
                {"b25eca601af12418ab5ad9341b73c05b", ImmutableSet.of("query66")},
                {"cf18516b1429b5a19564402703ef2947", ImmutableSet.of("query56")},
                {"436e9268f7a2454194161c9e1b59b487", ImmutableSet.of("query59", "query51", "query97")},
                {"41b557f81fec11eafe8fa88a1a3a5201", ImmutableSet.of("query80")},
                {"1d2fa291fc0da90963c007febfc7baba", ImmutableSet.of("query30")},
                {"a75bdefd6972a0fdc8d63aa5982dedbd", ImmutableSet.of("query77")},
                {"3da02bbb666d81042ba7072eae0ff859", ImmutableSet.of("query33")},
                {"fd46cacfdefe7d79965f6ae148e477b7", ImmutableSet.of("query17", "query29", "query25")},
                {"9a6bfe4d92319cef516376b377effc6b", ImmutableSet.of("query50")},
                {"9d976ee563551c83db54dfad74219137", ImmutableSet.of("query94")},
                {"2c5f5a27704a351febe9496a333e0a0d", ImmutableSet.of("query31")},
                {"c064b282159f32a03cb11e244f10d728", ImmutableSet.of("query19")},
                {"da29972c3d1be068e77ceba1c3af0fac", ImmutableSet.of("query72")},
                {"db12f8238bc126e1fee022314c97fb42", ImmutableSet.of("query60")},
                {"b3aa8e581aecbcee98deb4d3a5b4331b", ImmutableSet.of("query28", "query44", "query09")},
                {"f289b096908e0624ee7f5630e6401f31", ImmutableSet.of("query46", "query68")},
                {"4c21dbca96204ba25b3e591a76c61501", ImmutableSet.of("query40")},
                {"59e4148b26c9432cd84bb2eb8afc28fb", ImmutableSet.of("query38", "query87")},
                {"fc6ab5c943e5327386e9b67df3fceb54", ImmutableSet.of("query34", "query79", "query73")},
                {"d0e805aa4f1b83517dfe91907b49cf97", ImmutableSet.of("query31")},
                {"5fff41f43c34e6b89f4b10f54f1a5b67", ImmutableSet.of("query51", "query92")},
                {"f589efb2a2bd711c8745dea9678a5a61",
                        ImmutableSet.of("query23-1", "query38", "query23-2", "query23", "query87")},
                {"1b7737d16dccaae3e0bdd8951742cb28", ImmutableSet.of("query60")},
                {"8a4fe61d7ae0b8e73f854e4973587249", ImmutableSet.of("query77")},
        };
        Map<String, Set<String>> categoryMap = Stream.of(expectResults)
                .collect(Collectors.toMap(r -> (String) r[0], r -> (ImmutableSet<String>) r[1]));
        pieceGroups.forEach((k, v) -> {
            Set<String> querySet = v.stream().map(p -> p.first).collect(Collectors.toSet());
            String querySetStr = querySet.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(", "));
            System.out.printf("{\"%s\", ImmutableSet.of(%s)},\n", k, querySetStr);
            //Assert.assertEquals(categoryMap.get(k), querySet);
        });
    }

    @Test
    public void testTableInfo() {
        String q0 = TestUtil.getTPCDSQuery("query01");
        QueryStatementPlus stmtPlus = RboOptimizer.getQueryStatement(getStarRocksAssert().getCtx(), q0);
        FQTable fqTable = stmtPlus.getFqTableMap().values().iterator().next();
        TableInfo tableInfo = TableInfo.from(fqTable);
        Assert.assertNull(tableInfo.getCatalogName());
        Assert.assertEquals(tableInfo.getDatabaseName(), "tpcds");
    }

    @Test
    public void testTypePlus() {
        Type type = ScalarType.createDecimalV3NarrowestType(21, 3);
        TypePlus typePlus = TypePlus.of(type, -1, 21, 3);
        Assert.assertEquals(typePlus.getDecayedType(), ScalarType.DECIMAL128);
        Assert.assertEquals(typePlus.getLen(), -1);
        Assert.assertEquals(typePlus.getPrecision(), 21);
        Assert.assertEquals(typePlus.getScale(), 3);
        Assert.assertEquals(typePlus.getType(), type);
    }

    @Test
    public void testPlanPiecePrinter() {
        String q0 = TestUtil.getTPCDSQuery("query01");
        List<PlanPiece> planPieces = RboOptimizer.getPlanPieces(q0, getStarRocksAssert().getCtx());
        Assert.assertFalse(planPieces.isEmpty());
        PlanPiece piece = planPieces.get(0);
        String s = PlanPiecePrinter.print(piece);
        String snippet0 = "[3]: AggregatePiece\n" +
                "  InputPieces: [2]\n" +
                "  Dimensions:\n" +
                "    Dimensions.tier#0\n" +
                "      {5} = O:`tpcds`.`store_returns`.`sr_customer_sk`\n" +
                "      {9} = O:`tpcds`.`store_returns`.`sr_store_sk`\n" +
                "  RollupDimensions:\n" +
                "    RollupDimensions.tier#0\n" +
                "      {27} = O:`tpcds`.`date_dim`.`d_year`\n" +
                "  Metrics:\n" +
                "    Metrics.tier#0\n" +
                "      {49} = D:(sum[decimal(38, 2)][O] (var[decimal(7, 2)] #12))\n" +
                "  DistinctMetrics:\n" +
                "  NonHoistConjuncts:\n" +
                "    NonHoistConjuncts.tier#0\n" +
                "      [0] = ($inRange[boolean][O] (var[int(11)] #27) ($setOf[int(11)][U] " +
                "($closedRangeOf[int(11)][O] (val[int(11)] 2000) (val[int(11)] 2000))))\n" +
                "  HoistConjuncts:\n" +
                "    HoistConjuncts.tier#0\n" +
                "      [0] = ($modify[boolean][O] (val[varchar] M_IS_NOT_NULL) (var[int(11)] #9))\n" +
                "  Columns:\n" +
                "    Columns.tier#0\n" +
                "      {5} = O:`tpcds`.`store_returns`.`sr_customer_sk`\n" +
                "      {9} = O:`tpcds`.`store_returns`.`sr_store_sk`\n" +
                "    Columns.tier#1\n" +
                "      {27} = O:`tpcds`.`date_dim`.`d_year`\n" +
                "    Columns.tier#2\n" +
                "      {49} = D:(sum[decimal(38, 2)][O] (var[decimal(7, 2)] #12))\n" +
                "  Conjuncts:";
        Assert.assertTrue(s, s.contains(snippet0));
        List<String> lines = Stream.of(s.split("\n")).collect(Collectors.toList());
        Assert.assertEquals(s, 1, lines.stream().filter(ln -> ln.contains("StarJoinPiece")).count());
        Assert.assertEquals(s, 2, lines.stream().filter(ln -> ln.contains("TablePiece")).count());
        String s1 = PlanPiecePrinter.print(piece, new PrettyPrinter(), 1).getResult();
        List<String> lines1 = Stream.of(s1.split("\n")).collect(Collectors.toList());
        Assert.assertTrue(s1, s1.contains(snippet0));
        Assert.assertEquals(s1, 0, lines1.stream().filter(ln -> ln.contains("StarJoinPiece")).count());
        Assert.assertEquals(s1, 0, lines1.stream().filter(ln -> ln.contains("TablePiece")).count());
        String s2 = PlanPiecePrinter.print(piece, new PrettyPrinter(), 2).getResult();
        List<String> lines2 = Stream.of(s2.split("\n")).collect(Collectors.toList());
        Assert.assertTrue(s2, s2.contains(snippet0));
        Assert.assertEquals(s2, 1, lines2.stream().filter(ln -> ln.contains("StarJoinPiece")).count());
        Assert.assertEquals(s2, 0, lines2.stream().filter(ln -> ln.contains("TablePiece")).count());
    }

    @Test
    public void testEliminateLeftSemiAntiJoinPolicy() {
        String q16 = TestUtil.getTPCDSQuery("query16");
        List<PlanPiece> planPieces = RboOptimizer.getPlanPieces(q16, getStarRocksAssert().getCtx());
        Assert.assertFalse(planPieces.isEmpty());
        AggregatePiece aggPiece = planPieces.get(0).mustCast(AggregatePiece.class);
        String[] lines = PlanPiecePrinter.print(aggPiece).split("\n");
        Optional<AggregatePiece> optAggPiece2 = EliminateSemiAntiJoinPolicy.INSTANCE.convert(aggPiece);
        Assert.assertTrue(optAggPiece2.isPresent());
        AggregatePiece aggPiece2 = optAggPiece2.get();
        String[] lines2 = PlanPiecePrinter.print(aggPiece2).split("\n");
        Assert.assertEquals(1, Stream.of(lines).filter(ln -> ln.contains("LEFT SEMI JOIN")).count());
        Assert.assertEquals(1, Stream.of(lines).filter(ln -> ln.contains("LEFT ANTI JOIN")).count());
        Assert.assertTrue(Stream.of(lines2).noneMatch(ln -> ln.contains("LEFT SEMI JOIN")));
        Assert.assertTrue(Stream.of(lines2).noneMatch(ln -> ln.contains("LEFT ANTI JOIN")));
    }

    @Test
    public void testNotPolicy() {
        String q0 = "select cc_company_name, count(1) from call_center group by cc_company_name";
        List<PlanPiece> pieces = RboOptimizer.getPlanPieces(q0, getStarRocksAssert().getCtx());
        Assert.assertFalse(pieces.isEmpty());
        AggregatePiece aggPiece = pieces.get(0).mustCast(AggregatePiece.class);
        {
            AggregatePolicy.AbstractAggregatePolicy policy = AggregatePolicy.IDENTITY_POLICY;
            AggregatePolicy notPolicy = AggregatePolicy.not(policy);
            Assert.assertTrue(policy.convert(aggPiece).isPresent());
            Assert.assertFalse(notPolicy.convert(aggPiece).isPresent());
        }
        {
            AggregatePolicy.AbstractAggregatePolicy policy = AggregatePolicy.NONE_POLICY;
            AggregatePolicy notPolicy = AggregatePolicy.not(policy);
            Assert.assertFalse(policy.convert(aggPiece).isPresent());
            Assert.assertTrue(notPolicy.convert(aggPiece).isPresent());
        }
    }
}
