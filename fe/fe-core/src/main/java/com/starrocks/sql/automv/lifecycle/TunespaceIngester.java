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

package com.starrocks.sql.automv.lifecycle;

import com.starrocks.analysis.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.automv.ast.ShowRecommendationsStmt;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.qe.CustomizedQueryExecutor;
import com.starrocks.sql.automv.qe.TunespaceExecutor;
import com.starrocks.sql.automv.tunespace.TuneSpace;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.Result;
import com.starrocks.sql.automv.util.TieredMap;
import jline.internal.Log;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class TunespaceIngester {

    private static final Logger LOG = LogManager.getLogger(TunespaceIngester.class);
    private final ConnectContext ctx;
    private final QueryAuditSource queryAuditSource;
    private final TuneSpace tuneSpace;
    private final String autoMVDb;

    private TunespaceIngester(ConnectContext ctx, QueryAuditSource queryAuditSource, TuneSpace tuneSpace,
                              String autoMVdb) {
        this.ctx = Objects.requireNonNull(ctx);
        this.queryAuditSource = Objects.requireNonNull(queryAuditSource);
        this.tuneSpace = Objects.requireNonNull(tuneSpace);
        this.autoMVDb = Objects.requireNonNull(autoMVdb);
    }

    public static TunespaceIngester of(ConnectContext ctx, String auditDb, String auditTbl, String tsDb, String tsTbl,
                                       String autoMVDb) {
        QueryAuditSource auditSource = new QueryAuditSource(auditDb, auditTbl);
        TuneSpace ts = TuneSpace.of(tsDb, tsTbl, 1, 1);
        return new TunespaceIngester(ctx, auditSource, ts, autoMVDb);
    }

    public void prepare() throws Throwable {
        String createDbSql = "CREATE DATABASE IF NOT EXISTS " + tuneSpace.getDb();

        String createSql = new PrettyPrinter().add("CREATE TUNESPACE IF NOT EXISTS ")
                .addBacktickQuoted(tuneSpace.getDb())
                .add(".")
                .addBacktickQuoted(tuneSpace.getTableName()).getResult();

        CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
        Result.wrap(() -> executor.exec(ctx, createDbSql))
                .ifError(ex -> LOG.error("Fail to create db '{}'", tuneSpace.getDb(), ex))
                .bind(ignored -> {
                    executor.exec(ctx, createSql);
                })
                .ifError(ex -> LOG.error("Fail to create tunespace '{}'", tuneSpace.getTableName(), ex))
                .unwrapOrThrowError();
    }

    public void ingest() {
        List<QueryAuditEntry> auditInfoList = queryAuditSource.getQueryAuditInfoList(ctx, Collections::emptyList);
        List<Optional<Result.Unit>> results = auditInfoList.stream()
                .map(this::ingest)
                .collect(Collectors.toList());
        long numSuccess = results.stream().filter(Optional::isPresent).count();
        LOG.info("Ingest QueryAuditEntries: total={} success={}", results.size(), numSuccess);
    }

    private Optional<Result.Unit> ingest(QueryAuditEntry info) {
        String catalogAndDb = info.getCatalog() + "." + info.getDb();

        String appendStmt = new PrettyPrinter().add("ALTER TUNESPACE ")
                .addBacktickQuoted(tuneSpace.getDb()).add(".").addBacktickQuoted(tuneSpace.getTableName())
                .add(" APPEND ").add(info.getStmt())
                .getResult();

        return Result.wrap(() -> ctx.changeCatalogDb(catalogAndDb))
                .ifError(ex -> LOG.error("Fail to switch to " + catalogAndDb, ex))
                .bind(ignored -> {
                    CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
                    executor.exec(ctx, appendStmt);
                }).ifError(ex -> LOG.error("Fail to execute: " + appendStmt, ex))
                .unwrap();
    }

    public TieredMap<String, String> recommendMVs() {
        TableName tsTableName = new TableName(tuneSpace.getDb(), tuneSpace.getTableName());
        ShowRecommendationsStmt recommendationsStmt = new ShowRecommendationsStmt(tsTableName, -1, -1);
        ShowResultSet resultSet = TunespaceExecutor.execute(recommendationsStmt, ctx);
        return resultSet.getResultRows()
                .stream()
                .collect(TieredMap.toMap(row -> row.get(1), row -> row.get(2)));
    }

    public void createMVs(TieredMap<String, String> mvMap) throws Throwable {
        String createDbSql = "CREATE DATABASE IF NOT EXISTS " + autoMVDb;
        CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
        Result.wrap(() -> executor.exec(ctx, createDbSql))
                .ifError(ex -> LOG.error("Fail to create db '{}'", autoMVDb, ex))
                .bind(ignored -> {
                    ctx.changeCatalogDb(autoMVDb);
                })
                .bind(ignored -> {
                    List<Optional<Result.Unit>> results = mvMap.entrySet()
                            .stream()
                            .map(e -> createMv(e.getKey(), e.getValue()))
                            .collect(Collectors.toList());
                    long numSuccess = results.stream().filter(Optional::isPresent).count();
                    LOG.info("Create MVs: total={}, success={}", results.size(), numSuccess);
                }).ifError(ex -> Log.error("Fail to create MV", ex))
                .unwrapOrThrowError();

    }

    public Optional<Result.Unit> createMv(String mvName, String mvSchema) {
        String fqMvName = String.format("`%s`.`%s`", autoMVDb, mvName);
        String newMVSchema = mvSchema.replace(mvName, fqMvName);
        CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
        return Result.wrap(() -> executor.exec(ctx, newMVSchema))
                .ifError((ex) -> Log.error("Failed to create MV '{}', schema={}", fqMvName, newMVSchema, ex))
                .unwrap();
    }

    public List<MVName> listLegacyMVs() {
        Result<Result.Unit> switchDbResult = Result.wrap(() -> ctx.changeCatalogDb(autoMVDb));
        if (switchDbResult.maybeError().isPresent()) {
            LOG.error("Fail to change current database '{}'", autoMVDb, switchDbResult.maybeError().get());
            return Collections.emptyList();
        }
        CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
        String showMVSql = "SHOW TABLES like '_mv_%'";

        return Result.wrap(() -> executor.show(ctx, showMVSql))
                .ifError(ex -> LOG.error("Fail to execute '{}'", showMVSql, ex))
                .unwrap()
                .map(showResultSet ->
                        showResultSet.getResultRows()
                                .stream()
                                .map(row -> row.get(0))
                                .map(MVName::parse)
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
    }
}
