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
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.plan.ReplayWithMVFromDumpTest;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
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

    private void recommendAndValidate(StarRocksAssert starRocksAssert, String query,
                                      Consumer<SessionVariable> svSetter,
                                      Consumer<List<List<String>>> resultChecker) {
        List<Pair<String, String>> queryList = Collections.singletonList(Pair.create("query", query));
        AutoMVUtil.testHelper(starRocksAssert.getCtx(), queryList, svSetter, (pieces, mvResults) -> {
            List<List<String>> rewritableMVs = Lists.newArrayList();
            for (List<String> row : mvResults) {
                String mvName = row.get(1);
                String mv = row.get(2);
                mv = Stream.of(mv.split("\n"))
                        .filter(ln -> !ln.contains("PARTITION BY"))
                        .filter(ln -> !ln.contains("partition_refresh_number"))
                        .collect(Collectors.joining("\n"));
                starRocksAssert.withMaterializedView(mv, () -> {
                    associateBaseTablesWithMV(starRocksAssert, mvName, pieces);
                    String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), query);
                    if (plan.contains(mvName)) {
                        rewritableMVs.add(row);
                    }
                });
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
                    rewritableMVs.replaceAll(strings -> mvResults0.get(Integer.parseInt(strings.get(0))));
                    return null;
                });
                starRocksAssert.getCtx().getSessionVariable().setAutoMVRectifyTableName(false);
            }
            resultChecker.accept(rewritableMVs);
            return null;
        });
    }

    private List<String> recommendMV(StarRocksAssert starRocksAssert, QueryDumpInfo dumpInfo) {
        String query = dumpInfo.getOriginStmt();
        List<String> mvList = Lists.newArrayList();
        recommendAndValidate(starRocksAssert, query, AutoMVUtil::configDefaultAutoMV, results -> {
            results.forEach(row -> mvList.add(row.get(2)));
        });
        return mvList;
    }

    public String recommend(String dumpJson) throws Exception {
        dumpJson = rectifyQueryDump(dumpJson);
        QueryDumpInfo dumpInfo = getDumpInfoFromJson(dumpJson);
        System.out.println(dumpInfo.getOriginStmt());
        starRocksAssert.getCtx().getSessionVariable().setAutoMVEnableComplexDerivedMetrics(true);
        List<String> mvs = UtFrameUtils.execInMockedEnv(starRocksAssert, dumpInfo, this::recommendMV);
        if (!mvs.isEmpty()) {
            return mvs.get(0);
        } else {
            return null;
        }
    }
}
