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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.MvId;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.LogUtil;
import com.starrocks.connector.ConnectorTableInfo;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.automv.generator.QueryGenerateContext;
import com.starrocks.sql.automv.generator.QueryGenerator;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.qe.CollectAstVisitor;
import com.starrocks.sql.automv.qe.RboOptimizer;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.MetaUtil;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.Result;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ReplayWithMVFromDumpTest;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.arrow.util.Preconditions;
import org.apache.logging.log4j.core.config.Configurator;
import org.bouncycastle.util.Strings;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.sql.plan.ReplayFromDumpTestBase.getDumpInfoFromJson;

public class QueryDumpMVRecommender {
    private final StarRocksAssert starRocksAssert;

    private QueryDumpMVRecommender(StarRocksAssert starRocksAssert) {
        this.starRocksAssert = starRocksAssert;
    }

    private static String rectifyQueryDump(StarRocksAssert starRocksAssert, String dump) {
        dump = convertToOlapTable(starRocksAssert, dump);
        dump = dump.replaceAll("(?i)varchar\\(\\d+\\)", "string");
        return dump;
    }

    private static Optional<String> convertToOlapTable(CreateTableStmt stmt, String catalog) {
        String engineName = stmt.getEngineName().toUpperCase();
        Set<String> allowEngines = ImmutableSet.of("JDBC", "MYSQL", "ICEBERG");
        if (!allowEngines.contains(engineName)) {
            return Optional.empty();
        }
        PrettyPrinter printer = new PrettyPrinter();
        printer.add("CREATE TABLE ").addBacktickQuoted(stmt.getTableName()).add(" (").newLine();
        List<PrettyPrinter> columns = stmt.getColumnDefs().stream().map(def -> new PrettyPrinter()
                .addBacktickQuoted(def.getName()).spaces(1)
                .add(def.getType().toSql()).spaces(1)
                .add(def.isAllowNull() ? "NULL" : "NOT NULL").spaces(1)
        ).collect(Collectors.toList());
        printer.indentEnclose(2, () -> printer.addSuperStepsWithDelNl(",", columns));
        printer.newLine().add(") ENGINE=OLAP").newLine();
        printer.add("PROPERTIES (").newLine();
        List<PrettyPrinter> propertyItems = Lists.newArrayList();
        propertyItems.add(new PrettyPrinter().addDoubleQuoted("replication_num").add(" = ").addDoubleQuoted("1"));
        Preconditions.checkArgument(catalog != null || stmt.getProperties().containsKey("resource"));
        String resource = catalog != null ? catalog : stmt.getProperties().get("resource");
        propertyItems.add(new PrettyPrinter().addDoubleQuoted("resource").add(" = ").addDoubleQuoted(resource));
        printer.indentEnclose(2, () -> printer.addSuperStepsWithDelNl(",", propertyItems));
        printer.newLine().add(")");
        return Optional.of(printer.getResult());
    }

    private static String convertToOlapTable(StarRocksAssert starRocksAssert, String dump) {
        Gson gson = new Gson();
        Map<String, Object> dumpJson = gson.<Map<String, Object>>fromJson(dump, Map.class);
        if (!dumpJson.containsKey("table_meta")) {
            return dump;
        }
        Map<String, String> tableMataMap = (Map<String, String>) dumpJson.get("table_meta");
        String query = (String) dumpJson.get("statement");
        List<StatementBase> stmts = SqlParser.parse(query, starRocksAssert.getCtx().getSessionVariable());
        QueryStatement queryStatement;
        if (stmts.get(0) instanceof QueryStatement) {
            queryStatement = (QueryStatement) stmts.get(0);
        } else {
            throw new IllegalArgumentException("Not support " + stmts.get(0).getClass().getSimpleName());
        }
        List<TableName> tableNames = CollectAstVisitor.collectTableNames(queryStatement, starRocksAssert.getCtx());
        Map<String, TableName> tableNamesWithCatalog = Maps.newHashMap();
        tableNames.forEach(tableName -> {
            if (tableName.getDb() != null && tableName.getCatalog() != null) {
                String dbTableName = String.format("%s.%s", tableName.getDb(), tableName.getTbl());
                tableNamesWithCatalog.put(dbTableName, tableName);
            }
        });

        Map<String, String> dbTableNameToDbMap = tableMataMap.keySet().stream().map(dbTableName -> {
            String[] parts = Strings.split(dbTableName, '.');
            Preconditions.checkArgument(parts.length == 2);
            return Pair.create(dbTableName, parts[0]);
        }).collect(Collectors.toMap(p -> p.first, p -> p.second));
        Set<String> dbs = new HashSet<>(dbTableNameToDbMap.values());
        dbs.forEach(db -> {
            try {
                starRocksAssert.withDatabase(db);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Map<String, String> newTableMetaMap = Maps.newHashMap();
        List<Pair<TableName, String>> tableRefList = Lists.newArrayList();
        for (Map.Entry<String, String> entry : tableMataMap.entrySet()) {
            String dbTableName = entry.getKey();
            Preconditions.checkArgument(dbTableNameToDbMap.containsKey(dbTableName));
            String createTblSql = LogUtil.removeLineSeparator(entry.getValue());
            List<StatementBase> statements =
                    SqlParser.parse(createTblSql, starRocksAssert.getCtx().getSessionVariable());
            StatementBase statementBase = statements.get(0);
            if (statementBase instanceof CreateTableStmt) {
                CreateTableStmt createTableStmt = (CreateTableStmt) statementBase;
                if (tableNamesWithCatalog.containsKey(dbTableName)) {
                    String catalog = Objects.requireNonNull(tableNamesWithCatalog.get(dbTableName).getCatalog());
                    String createTable = convertToOlapTable(createTableStmt, catalog).orElse(entry.getValue());
                    newTableMetaMap.put(entry.getKey(), createTable);
                    tableRefList.add(Pair.create(tableNamesWithCatalog.get(dbTableName), dbTableName));
                } else if (!createTableStmt.getEngineName().equalsIgnoreCase("OLAP")) {
                    String createTable = convertToOlapTable(createTableStmt, null).orElse(entry.getValue());
                    newTableMetaMap.put(entry.getKey(), createTable);
                } else {
                    newTableMetaMap.put(entry.getKey(), entry.getValue());
                }
            } else {
                newTableMetaMap.put(entry.getKey(), entry.getValue());
            }
        }
        for (Pair<TableName, String> p : tableRefList) {
            TableName tableName = p.first;
            Pattern pat = Pattern.compile(String.format("(`?)%s(`?)\\.(`?)%s(`?)\\.(`?)%s(`?)",
                    tableName.getCatalog(), tableName.getDb(), tableName.getTbl()));
            query = query.replaceAll(pat.pattern(), p.second);
        }
        dumpJson.put("statement", query);
        dumpJson.put("table_meta", newTableMetaMap);
        return gson.toJson(dumpJson);
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

    private static String backtrace(Throwable err, int topN) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        err.printStackTrace(pw);
        List<String> lines = Stream.of(sw.toString().split("\n")).collect(Collectors.toList());
        return String.join("\n", lines.subList(0, Math.min(topN, lines.size())));
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
                    // Set materialized_view_rewrite_mode='force'
                    starRocksAssert.getCtx().getSessionVariable().setMaterializedViewRewriteMode("force");
                    Result<Optional<Pair<String, String>>> maybeOptFailMessages =
                            Result.wrap(() ->
                                    UtFrameUtils.checkMVRewriteWithTracing(starRocksAssert.getCtx(), query, mvName));
                    Optional<Pair<String, String>> optFailMessages;
                    if (maybeOptFailMessages.maybeError().isPresent()) {
                        Throwable err = maybeOptFailMessages.maybeError().get();
                        optFailMessages = Optional.of(Pair.create(err.getMessage(), backtrace(err, 10)));
                    } else {
                        optFailMessages = maybeOptFailMessages.mustUnwrap();
                    }
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
            boolean hasTablesWithResource = pieces.stream()
                    .map(p -> p.second)
                    .flatMap(piece -> PlanPiece.collect(piece, TablePiece.class).stream())
                    .map(TablePiece::getTable)
                    .map(FQTable::getTable)
                    .anyMatch(table -> MetaUtil.getResourceName(table).isPresent());
            if (hasTablesWithResource) {
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
        if (rewritableMVs.isEmpty()) {
            List<PlanPiece> pieces = RboOptimizer.getPlanPieces(query, starRocksAssert.getCtx());
            if (!pieces.isEmpty()) {
                Function<PlanPiece, PrettyPrinter> planPieceToQuery = piece -> {
                    QueryGenerateContext generateContext = QueryGenerateContext.of(false, true, true);
                    piece = piece.cast(AggregatePiece.class)
                            .map(AggregatePiece::toPerfect)
                            .map(aggPiece -> (PlanPiece) aggPiece).orElse(piece);
                    return QueryGenerator.generate(piece, generateContext).getSubquery();
                };
                List<PrettyPrinter> subQueryList = pieces.stream().map(planPieceToQuery).collect(Collectors.toList());
                PrettyPrinter printer = new PrettyPrinter();
                printer.add("Succeeds in extracting SPJG sub-query but fails to recommend MV").newLine();
                List<String> hints = ImmutableList.of(
                        "use_array_agg_count_distinct <arg>                   " +
                                "(default false)Use array_agg to compute count distinct",
                        "use_bitmap_count_distinct <arg>                      " +
                                "(default true)Use bitmap to compute count distinct",
                        "use_hll_count_distinct <arg>                         " +
                                "(default false)Use hll to compute count distinct",
                        "enable_complex_derived_dimensions <arg>              " +
                                "(default true)Allow derived dimensions",
                        "enable_complex_derived_metrics <arg>                 " +
                                "(default false)Allow derived metrics",
                        "prune_rollup_unable_aggregate_with_conjuncts <arg>   " +
                                "(default true)Do not recommend MV if the sub-plan " +
                                "contains rollup-unable aggregations and predicates",
                        "push_down_agg_below_semi_anti_join <arg>             " +
                                "(default true)Recommend MV after eliminate semi/anti join in the sub-plan",
                        "disable_semi_anti_join <arg>                         " +
                                "(default true)Do not recommend MV if sub-plan contains semi/anti join"
                );
                printer.add("Please toggle parameters as follows to re-try:").newLine();
                printer.indentEnclose(() -> printer.addItemsWithDelNl(";", hints));
                printer.newLine();
                printer.newLine();
                printer.add("SPJG sub-queries extracted").newLine();
                Supplier<String> ordinal = Util.nextStringGenerator("Sub-query#", ":");
                List<PrettyPrinter> subQueryList2 = subQueryList.stream().map(subQuery -> {
                    PrettyPrinter p = new PrettyPrinter();
                    p.add(ordinal.get()).newLine();
                    p.indentEnclose(2, () -> p.addSuperStep(subQuery));
                    p.newLine();
                    return p;
                }).collect(Collectors.toList());
                printer.indentEnclose(2, () -> printer.addSuperStepsWithDelNl("", subQueryList2));
                MVResultWithRewriteTraceInfo info = new MVResultWithRewriteTraceInfo(null, printer.getResult(), null);
                return Collections.singletonList(info);
            } else {
                PrettyPrinter printer = new PrettyPrinter();
                printer.add("Fail to extract SPJG sub-queries from the query").newLine();
                printer.add(RboOptimizer.getLogicalPlan(query, starRocksAssert.getCtx()));
                MVResultWithRewriteTraceInfo info = new MVResultWithRewriteTraceInfo(null, printer.getResult(), null);
                return Collections.singletonList(info);
            }
        }
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
        Result<String> maybeDumpJson = Result.wrap(() -> rectifyQueryDump(starRocksAssert, dumpJson));
        if (maybeDumpJson.maybeError().isPresent()) {
            Throwable err = maybeDumpJson.maybeError().get();
            PrettyPrinter printer = new PrettyPrinter();
            printer.add("Querydump can be processed!").newLine();
            printer.add(backtrace(err, 200));
            MVResultWithRewriteTraceInfo info = new MVResultWithRewriteTraceInfo(null, printer.getResult(), null);
            return Collections.singletonList(info);
        }
        String newQueryDump = maybeDumpJson.mustUnwrap();
        QueryDumpInfo dumpInfo = getDumpInfoFromJson(newQueryDump);
        // System.out.println(dumpInfo.getOriginStmt());
        Result<List<MVResultWithRewriteTraceInfo>> maybeMVLists = Result.wrap(() ->
                UtFrameUtils.execInMockedEnv(starRocksAssert, dumpInfo, svSetter, this::recommendMV));
        if (maybeMVLists.maybeError().isPresent()) {
            Throwable err = maybeMVLists.maybeError().get();
            PrettyPrinter printer = new PrettyPrinter();
            printer.add("Internal error happens!").newLine();
            printer.add(backtrace(err, 200));
            MVResultWithRewriteTraceInfo info = new MVResultWithRewriteTraceInfo(null, printer.getResult(), null);
            return Collections.singletonList(info);
        } else {
            return maybeMVLists.mustUnwrap();
        }
    }

    public List<String> recommendNoTraceInfo(String dumpJson, Consumer<SessionVariable> svSetter)
            throws Exception {
        return recommend(dumpJson, svSetter)
                .stream()
                .filter(mvResult -> mvResult.mvResult != null && mvResult.rewriteFailReason == null &&
                        mvResult.rewriteFailVerboseLogs == null)
                .map(mvResult -> mvResult.mvResult.get(2))
                .collect(Collectors.toList());
    }

    public String recommendAndFormatOutput(String dumpJson, Consumer<SessionVariable> svSetter) throws Exception {
        Supplier<String> mvIdGen = Util.nextStringGenerator("Recommend MV ", "");
        List<MVResultWithRewriteTraceInfo> mvResultList = recommend(dumpJson, svSetter);

        PrettyPrinter printer = new PrettyPrinter();
        if (mvResultList.size() == 1 && mvResultList.get(0).mvResult == null) {
            return mvResultList.get(0).rewriteFailReason;
        }
        mvResultList.forEach(mvResult -> {
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
