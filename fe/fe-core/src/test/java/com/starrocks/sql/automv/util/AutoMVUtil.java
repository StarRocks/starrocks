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

package com.starrocks.sql.automv.util;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.automv.ast.ShowRecommendationsStmt;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.estimation.MultiColumnCards;
import com.starrocks.sql.automv.generator.AggregateMVGenerator;
import com.starrocks.sql.automv.generator.MVGenerateContext;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.generator.QueryGenerateResult;
import com.starrocks.sql.automv.lattice.MVRecommendation;
import com.starrocks.sql.automv.lifecycle.MVChangeLog;
import com.starrocks.sql.automv.lifecycle.MVHitCountEntry;
import com.starrocks.sql.automv.lifecycle.QueryAuditEntry;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pattern.PlanPiecePattern;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.pieces.PlanPieceBuilder;
import com.starrocks.sql.automv.pieces.PlanPieceNormalizer;
import com.starrocks.sql.automv.policies.AggregatePolicies;
import com.starrocks.sql.automv.policies.AggregatePolicy;
import com.starrocks.sql.automv.qe.ColumnPlus;
import com.starrocks.sql.automv.qe.CustomizedQueryExecutor;
import com.starrocks.sql.automv.qe.QueryStatementPlus;
import com.starrocks.sql.automv.qe.RboOptimizer;
import com.starrocks.sql.automv.qe.TunespaceExecutor;
import com.starrocks.sql.automv.tunespace.PlanPieceInfo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.DoesNothing;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AutoMVUtil {
    private static MockedStatic<Authorizer> authorizerMockedStatic;

    private static List<PlanPieceInfo> getPlanPlanInfosFromQueryList(ConnectContext ctx,
                                                                     List<Pair<String, String>> queryList) {
        List<PlanPieceInfo> allPieceInfoList = Lists.newArrayList();
        for (Pair<String, String> namedQuery : queryList) {
            String name = namedQuery.first;
            String query = namedQuery.second;
            Supplier<String> nameGenerator = Util.nextStringGenerator(name + ".part.", "");
            List<PlanPieceInfo> pieceInfoList = RboOptimizer.getPlanPieces(query, ctx).stream().map(piece -> {
                AggregatePolicy policy =
                        AggregatePolicies.defaultPolicies(AutoMVOptions.of(ctx.getSessionVariable()));
                PlanPieceInfo pieceInfo = PlanPieceInfo.from(piece, policy, piece.getCommonState().getFqTableMap());
                pieceInfo.getTraits().setName(nameGenerator.get());
                return pieceInfo;
            }).collect(Collectors.toList());
            allPieceInfoList.addAll(pieceInfoList);
        }
        return allPieceInfoList;
    }

    public static void mockUpCustomizedQueryExecutor(List<Pair<String, String>> queryList) {
        mockUpCustomizedQueryExecutor(queryList, null, null);
    }

    public static void mockUpCustomizedQueryExecutor(List<Pair<String, String>> queryList, String catalog, String db) {
        new MockUp<CustomizedQueryExecutor>() {
            @Mock
            public <T> List<T> query(Class<T> klass, List<ColumnPlus> columns, ConnectContext context, String sql) {
                if (PlanPieceInfo.class.equals(klass)) {
                    if (catalog != null && db != null) {
                        Result.wrap(() -> context.changeCatalogDb(catalog + "." + db));
                    } else if (db != null) {
                        Result.wrap(() -> context.changeCatalogDb(db));
                    }
                    return (List<T>) getPlanPlanInfosFromQueryList(context, queryList);
                } else if (MultiColumnCards.class.equals(klass)) {
                    QueryStatement stmt = (QueryStatement) RboOptimizer.parseAndAnalyze(context, sql);
                    SelectRelation selectRelation = (SelectRelation) stmt.getQueryRelation();
                    SelectList selectList = selectRelation.getSelectList();
                    int numCards = selectList.getItems().get(1).getExpr().getChildren().size();
                    MultiColumnCards mcCards = new MultiColumnCards();
                    long rowCount = 100_000_000_000L;
                    mcCards.setRowCount(rowCount);
                    List<Long> cards = IntStream.range(0, numCards)
                            .mapToLong(i -> (long) (rowCount * 0.5 / (1.1 + i * 0.2)))
                            .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
                    mcCards.setCards(cards);
                    return (List<T>) Collections.singletonList(mcCards);
                } else if (QueryAuditEntry.class.equals(klass)) {
                    return (List<T>) queryList.stream().map(q -> {
                        QueryAuditEntry entry = new QueryAuditEntry();
                        entry.setCatalog(Objects.requireNonNull(catalog));
                        entry.setDb(Objects.requireNonNull(db));
                        entry.setTimestamp(Timestamp.valueOf(LocalDateTime.of(2024, Month.JANUARY, 1, 12, 34, 56)));
                        entry.setStmt(q.second);
                        return entry;
                    }).collect(Collectors.toList());
                } else if (MVHitCountEntry.class.equals(klass)) {
                    return Collections.emptyList();
                } else {
                    throw new InternalError("Error");
                }
            }
        };
    }

    public static void mockUpTunespaceExecutor() {
        new MockUp<TunespaceExecutor.TunespaceExecuteVisitor>() {
            @Mock
            public void exec(String sql, Class<?> klass, ConnectContext context) throws Exception {
            }
        };
    }

    public static synchronized void mockUpAuthorizer() {
        if (authorizerMockedStatic == null) {
            authorizerMockedStatic = Mockito.mockStatic(Authorizer.class);
            authorizerMockedStatic.when(() -> Authorizer.check(Mockito.any(),
                    Mockito.any())).then(DoesNothing.doesNothing());
        }
    }

    public static void mockMVChangeLogPersistence() {
        new MockUp<MVChangeLog>() {
            @Mock
            public void persist() {
            }
        };
    }

    public static void configDefaultAutoMV(SessionVariable sv) {
        sv.setAutoMVUseCardinalityEstimation(true);
        sv.setAutoMVEnableComplexDerivedMetrics(true);
        sv.setAutoMVEnableComplexDerivedDimensions(true);
        sv.setAutoMVPruneRollupUnableAggregateWithConjuncts(false);
        sv.setAutoMVCardRowCountRatioHWM(1.0);
    }

    public static Void justPrintResult(List<List<String>> results) {
        ShowRecommendationsStmt showStmt = new ShowRecommendationsStmt(null, 1, 1);
        List<Column> columns = showStmt.getMetaData().getColumns();
        for (List<String> row : results) {
            PrettyPrinter printer = new PrettyPrinter();
            List<PrettyPrinter> items = IntStream.range(0, columns.size())
                    .mapToObj(i -> new PrettyPrinter().add(columns.get(i).getName()).add(": ").add(row.get(i)))
                    .collect(Collectors.toList());

            printer.addSuperStepsWithDelNl(";", items);
            System.out.println(printer.getResult());
            System.out.println();
        }
        return null;
    }

    public static Void defaultResultChecker(List<Pair<String, AggregatePiece>> pieces, List<List<String>> results) {
        Assert.assertTrue(Boolean.logicalXor(!pieces.isEmpty(), results.isEmpty()));
        return null;
    }

    public static void testHelper(ConnectContext ctx, List<Pair<String, String>> queryList,
                                  Consumer<SessionVariable> svSetter,
                                  Consumer<List<List<String>>> resultChecker) {
        testHelper(ctx, queryList, svSetter, (pieces, mvResults) -> {
            resultChecker.accept(mvResults);
            return null;
        });
    }

    public static void testSingleQueryHelper(StarRocksAssert starRocksAssert, String query,
                                             Consumer<SessionVariable> svSetter,
                                             Consumer<List<List<String>>> resultChecker) {
        List<Pair<String, String>> queryList = Collections.singletonList(Pair.create("query", query));
        testHelper(starRocksAssert.getCtx(), queryList, svSetter, (pieces, mvResults) -> {
            resultChecker.accept(mvResults);
            if (!mvResults.isEmpty()) {
                Assert.assertEquals(mvResults.size(), 1);
                String mvName = mvResults.get(0).get(1);
                String mv = mvResults.get(0).get(2);
                starRocksAssert.withMaterializedView(mv, () -> {
                    String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), query);
                    Assert.assertTrue(plan, plan.contains(mvName));
                });
            }
            return null;
        });
    }

    public static void testHelper(ConnectContext ctx, List<Pair<String, String>> queryList,
                                  Consumer<SessionVariable> svSetter,
                                  BiFunction<List<Pair<String, AggregatePiece>>, List<List<String>>, Void> resultChecker) {
        mockUpCustomizedQueryExecutor(queryList);
        TableName tableName = new TableName(null, "db", "_tunespace_");
        ShowRecommendationsStmt stmt = new ShowRecommendationsStmt(tableName, -1, -1);
        String savedSv = null;
        try {
            savedSv = ctx.getSessionVariable().getJsonString();
            svSetter.accept(ctx.getSessionVariable());
            List<Pair<String, AggregatePiece>> pieces = getPieces(ctx, queryList);
            ShowResultSet showResultSet = TunespaceExecutor.execute(stmt, ctx);
            resultChecker.apply(pieces, showResultSet.getResultRows());
        } catch (IOException e) {
            Assert.fail();
        } finally {
            try {
                ctx.getSessionVariable().replayFromJson(savedSv);
            } catch (IOException e) {
                Assert.fail();
            }
        }
    }

    public static void defaultTestHelper(ConnectContext ctx, List<Pair<String, String>> queryList) {
        testHelper(ctx, queryList, AutoMVUtil::configDefaultAutoMV, AutoMVUtil::defaultResultChecker);
    }

    public static List<Pair<String, AggregatePiece>> getPieces(ConnectContext ctx, List<Pair<String, String>> queryList,
                                                               java.util.function.Predicate<String> filter) {
        AutoMVOptions options = AutoMVOptions.of(ctx.getSessionVariable());
        AggregatePolicy policy = AggregatePolicies.defaultPolicies(options);
        return queryList.stream()
                .filter(p -> filter.test(p.first))
                .flatMap(p -> RboOptimizer.getPlanPieces(p.second, ctx).stream()
                        .map(piece -> piece.mustCast(AggregatePiece.class))
                        .map(piece -> policy.convert(piece).orElse(piece))
                        .map(AggregatePolicies::applyRollupOrPerfectMatch)
                        .map(piece -> Pair.create(p.first, piece)))
                .collect(Collectors.toList());
    }

    public static List<Pair<String, AggregatePiece>> getPieces(ConnectContext ctx,
                                                               List<Pair<String, String>> queryList) {
        return getPieces(ctx, queryList, name -> true);
    }

    public static Map<String, String> getMaterializedViews(ConnectContext ctx, String sql) {
        QueryStatementPlus stmt = RboOptimizer.getQueryStatement(ctx, sql);
        QueryStatement queryStmt = stmt.getQueryStatement();
        Map<String, FQTable> fqTableMap = stmt.getFqTableMap();
        Map<String, String> mvMap = Maps.newHashMap();
        List<OptExpression> subPlans = RboOptimizer.getSubPlans(queryStmt, ctx, PlanPiecePattern.getSPJG());
        Supplier<String> nameGenerator = Util.nextStringGenerator("Q.part.", "");
        for (OptExpression subPlan : subPlans) {
            ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
            Optional<AggregatePiece> optPlanPiece =
                    PlanPieceBuilder.createPlanPiece(nameGenerator.get(), subPlan, idConverter, fqTableMap)
                            .cast(AggregatePiece.class);
            Preconditions.checkArgument(optPlanPiece.isPresent());
            AggregatePiece planPiece = optPlanPiece.get();

            PlanPieceNormalizer.normalize(planPiece);
            AutoMVOptions options = AutoMVOptions.of(ctx.getSessionVariable());
            MVGenerateContext mvGenerateContext = MVGenerateContext.builder()
                    .setMvNameGenerator(query -> MVName.generateFromQuery(query).toString())
                    .setNextId(idConverter::nextId)
                    .setOptions(options)
                    .build();
            planPiece = AggregatePolicies.defaultPolicies(options).convert(planPiece).orElse(planPiece);
            planPiece = AggregatePolicies.applyRollupOrPerfectMatch(planPiece);
            Optional<QueryGenerateResult> optResult = AggregateMVGenerator.generate(planPiece, mvGenerateContext);
            Assert.assertTrue(optResult.isPresent());
            QueryGenerateResult result = optResult.get();
            mvMap.put(result.getMvName(), result.getSubquery().getResult());
        }
        return mvMap;
    }

    public static List<MVRecommendation> recommend(List<Pair<String, String>> queryList, ConnectContext context) {
        mockUpCustomizedQueryExecutor(queryList);
        return TunespaceExecutor.recommend("ts", context, 0, Integer.MAX_VALUE);
    }
}
