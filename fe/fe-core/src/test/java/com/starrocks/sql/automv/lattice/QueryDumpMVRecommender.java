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

package com.starrocks.sql.automv.lattice;

import com.google.api.client.util.Lists;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.connector.ConnectorTableInfo;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.MetaUtil;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.plan.ReplayWithMVFromDumpTest;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.sql.plan.ReplayFromDumpTestBase.getDumpInfoFromJson;

public class QueryDumpMVRecommender {
    private final StarRocksAssert starRocksAssert;

    private QueryDumpMVRecommender(StarRocksAssert starRocksAssert) {
        this.starRocksAssert = starRocksAssert;
    }

    private static String rectifyQueryDump(String dump) {
        dump = dump.replaceAll("(?i)varchar\\(\\d+\\)", "string");
        return dump;
    }

    public static QueryDumpMVRecommender of() throws Exception {
        Configurator.setRootLevel(org.apache.logging.log4j.Level.OFF);
        Config.sys_log_level = "ERROR";
        Config.proc_profile_cpu_enable = false;
        Config.proc_profile_mem_enable = false;
        FeConstants.runningUnitTest = true;
        FeConstants.isReplayFromQueryDump = true;
        ReplayWithMVFromDumpTest.beforeClass();
        StarRocksAssert starRocksAssert = ReplayWithMVFromDumpTest.starRocksAssert;
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        return new QueryDumpMVRecommender(ReplayWithMVFromDumpTest.starRocksAssert);
    }

    public static void associateBaseTablesWithMV(StarRocksAssert starRocksAssert, String mvName,
                                                 List<Pair<String, AggregatePiece>> pieces) {
        MetadataMgr mgr = starRocksAssert.getCtx().getGlobalStateMgr().getMetadataMgr();
        String currentCatalog = starRocksAssert.getCtx().getCurrentCatalog();
        String currentDatabase = starRocksAssert.getCtx().getDatabase();

        Set<MvId> mvIds = MetaUtil.listLegacyMVs(currentCatalog, currentDatabase).stream()
                .filter(mvp -> mvp.getFqName().getTbl().equals(mvName))
                .map(mvp -> mvp.getMv().getMvId())
                .collect(Collectors.toSet());

        ConnectorTableInfo tableInfo = new ConnectorTableInfo.Builder()
                .setRelatedMaterializedViews(mvIds)
                .build();

        pieces.stream()
                .map(p -> p.second)
                .forEach(piece -> piece.getCommonState().getFqTableMap().values().forEach(fqTable -> {
                    String catalog = fqTable.getFqTableName().getCatalog();
                    String db = fqTable.getFqTableName().getDb();
                    String tableId = fqTable.getTable().getTableIdentifier();
                    mgr.getConnectorTblMetaInfoMgr().addConnectorTableInfo(catalog, db, tableId, tableInfo);
                }));
    }

    public StarRocksAssert getStarRocksAssert() {
        return starRocksAssert;
    }

    private List<MVResultWithRewriteTraceInfo> recommendAndValidate(StarRocksAssert starRocksAssert, String query,
                                                                    Consumer<SessionVariable> svSetter) {
        List<MVResultWithRewriteTraceInfo> rewritableMVs = Lists.newArrayList();
        List<Pair<String, String>> queryList = Collections.singletonList(Pair.create("query", query));
        AutoMVUtil.testHelper(starRocksAssert.getCtx(), queryList, svSetter, (pieces, mvResults) -> {
            for (List<String> row : mvResults) {
                String mvName = row.get(1);
                String mv = row.get(2);
                mv = Stream.of(mv.split("\n"))
                        .filter(ln -> !ln.contains("PARTITION BY"))
                        .filter(ln -> !ln.contains("partition_refresh_number"))
                        .collect(Collectors.joining("\n"));
                starRocksAssert.withMaterializedView(mv, () -> {
                    associateBaseTablesWithMV(starRocksAssert, mvName, pieces);
                    Optional<Pair<String, String>> optFailMessages =
                            UtFrameUtils.checkMVRewriteWithTracing(starRocksAssert.getCtx(), query, mvName);
                    if (optFailMessages.isPresent()) {
                        Pair<String, String> failMessages = optFailMessages.get();
                        String reason = failMessages.first;
                        String verboseTraceLogs = failMessages.second;
                        rewritableMVs.add(new MVResultWithRewriteTraceInfo(row, reason, verboseTraceLogs));
                    } else {
                        rewritableMVs.add(new MVResultWithRewriteTraceInfo(row, null, null));
                    }
                });
                try {
                    starRocksAssert.dropMaterializedView(mvName);
                } catch (Exception ignored) {
                }
            }
            boolean hasExternalTables = !pieces.stream()
                    .map(p -> p.second)
                    .flatMap(piece -> PlanPiece.collect(piece, TablePiece.class).stream())
                    .map(TablePiece::getTable)
                    .map(FQTable::getTable)
                    .allMatch(Table::isNativeTableOrMaterializedView);
            if (hasExternalTables) {
                starRocksAssert.getCtx().getSessionVariable().setAutoMVRectifyTableName(true);
                AutoMVUtil.testHelper(starRocksAssert.getCtx(), queryList, svSetter, (pieces0, mvResults0) -> {
                    rewritableMVs.replaceAll(mvResult -> {
                        int idx = Integer.parseInt(mvResult.mvResult.get(0));
                        return new MVResultWithRewriteTraceInfo(mvResults0.get(idx),
                                mvResult.rewriteFailReason, mvResult.rewriteFailVerboseLogs);
                    });
                    return null;
                });
                starRocksAssert.getCtx().getSessionVariable().setAutoMVRectifyTableName(false);
            }
            return null;
        });
        return rewritableMVs;
    }

    private List<MVResultWithRewriteTraceInfo> recommendMV(StarRocksAssert starRocksAssert,
                                                           QueryDumpInfo dumpInfo) {
        String query = dumpInfo.getOriginStmt();
        return recommendAndValidate(starRocksAssert, query, sv -> {
        });
    }

    public List<MVResultWithRewriteTraceInfo> recommend(String dumpJson, Consumer<SessionVariable> svSetter)
            throws Exception {
        dumpJson = rectifyQueryDump(dumpJson);
        QueryDumpInfo dumpInfo = getDumpInfoFromJson(dumpJson);
        System.out.println(dumpInfo.getOriginStmt());
        return UtFrameUtils.execInMockedEnv(starRocksAssert, dumpInfo, svSetter, this::recommendMV);
    }

    public List<String> recommendNoTraceInfo(String dumpJson, Consumer<SessionVariable> svSetter)
            throws Exception {
        dumpJson = rectifyQueryDump(dumpJson);
        QueryDumpInfo dumpInfo = getDumpInfoFromJson(dumpJson);
        System.out.println(dumpInfo.getOriginStmt());
        return UtFrameUtils.execInMockedEnv(starRocksAssert, dumpInfo, svSetter, this::recommendMV)
                .stream()
                .filter(mvResult -> mvResult.rewriteFailReason == null && mvResult.rewriteFailVerboseLogs == null)
                .map(mvResult -> mvResult.mvResult.get(2))
                .collect(Collectors.toList());
    }

    public String recommendAndFormatOutput(String dumpJson, Consumer<SessionVariable> svSetter) throws Exception {
        Supplier<String> mvIdGen = Util.nextStringGenerator("Recommend MV ", "");
        PrettyPrinter printer = new PrettyPrinter();
        recommend(dumpJson, svSetter).forEach(mvResult -> {
            boolean rewriteOK = mvResult.rewriteFailReason == null && mvResult.rewriteFailVerboseLogs == null;
            String rewriteStatus = rewriteOK ? "OK" : "FAIL";
            printer.add(mvIdGen.get())
                    .add(" [").add("REWRITE ").add(rewriteStatus).add("]: ")
                    .add(mvResult.mvResult.get(1)).newLine();
            printer.add(mvResult.mvResult.get(2)).newLine().newLine();
            if (mvResult.rewriteFailReason != null) {
                printer.add("Rewrite Fail Reason").newLine().add(mvResult.rewriteFailReason).newLine();
            }
            if (mvResult.rewriteFailVerboseLogs != null) {
                String[] logs = mvResult.rewriteFailVerboseLogs.split("\n");
                String tail10Lns =
                        Stream.of(logs).skip(Math.max(0, logs.length - 10)).collect(Collectors.joining("\n"));
                printer.add("Rewrite Verbose Logs").newLine().add(tail10Lns).newLine();
            }
        });
        return printer.getResult();
    }

    public static class MVResultWithRewriteTraceInfo {
        public final List<String> mvResult;
        public final String rewriteFailReason;
        public final String rewriteFailVerboseLogs;

        public MVResultWithRewriteTraceInfo(List<String> mvResult, String rewriteFailReason,
                                            String rewriteFailVerboseLogs) {
            this.mvResult = mvResult;
            this.rewriteFailReason = rewriteFailReason;
            this.rewriteFailVerboseLogs = rewriteFailVerboseLogs;
        }
    }
}
