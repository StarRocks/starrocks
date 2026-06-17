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

package com.starrocks.scheduler.mv;

import com.google.common.base.Strings;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.mv.IvmRefreshDefinition;
import com.starrocks.sql.analyzer.mv.IvmSchemaCompat;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.statistic.StatisticUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

/**
 * Detect schema drift between a still-active MV and its external (iceberg / hive / hudi / delta)
 * base tables, and mark the MV inactive when the current base schemas no longer match what the
 * MV captured at CREATE time. Invoked from the refresh pipeline after the connector metadata
 * cache has been refreshed; deliberately opposite to {@code MVActiveChecker}, which is the
 * background daemon that tries to take inactive MVs back to active.
 */
public final class MVRefreshSchemaChecker {

    private static final Logger LOG = LogManager.getLogger(MVRefreshSchemaChecker.class);

    private MVRefreshSchemaChecker() {
    }

    /**
     * Detect schema drift on a still-active MV against its external (iceberg / hive / hudi / delta)
     * base tables and mark it inactive on mismatch. Must be called <b>after</b>
     * {@code refreshExternalTable} so the analyzer reads the current connector schema.
     */
    public static void checkExternalBaseSchemaCompat(MaterializedView mv) {
        if (!mv.isActive()) {
            return;
        }
        boolean hasExternalBase = false;
        for (BaseTableInfo bti : mv.getBaseTableInfos()) {
            Optional<Table> baseTableOpt = MvUtils.getTable(bti);
            boolean isExternalCatalog = !CatalogMgr.isInternalCatalog(bti.getCatalogName());

            if (!baseTableOpt.isPresent() && isExternalCatalog) {
                mv.setInactiveAndReason(
                        MaterializedViewExceptions.inactiveReasonForBaseTableNotExists(bti.getTableName()));
                return;
            }
            if (baseTableOpt.isPresent() && !baseTableOpt.get().isNativeTableOrMaterializedView()) {
                hasExternalBase = true;
            }
        }
        if (!hasExternalBase) {
            return;
        }

        // IVM derives its rewritten query (with hidden __ROW_ID__ / __AGG_STATE_* columns) at
        // refresh time, so the schema check must compare against that same rewrite, not the
        // original user query. PCT compares against viewDefineSql directly.
        String selectSql = mv.getViewDefineSql();
        if (Strings.isNullOrEmpty(selectSql)) {
            return;
        }

        ConnectContext context = StatisticUtils.buildConnectContext();
        context.setStatisticsContext(false);
        // bindScope save+restore protects the outer TaskRun's threadlocal — a plain
        // ConnectContext.remove() in finally would null whatever was bound before us
        // (StatisticUtils.buildConnectContext does not install on the threadlocal).
        try (ConnectContext.ScopeGuard guard = context.bindScope()) {
            analyzeAndCompareSchema(mv, context, selectSql);
        } catch (SemanticException e) {
            // SemanticException covers both real drift and transient connector wrappers
            // (e.g. DeltaLake's getLatestSnapshot wraps IO failures). Only inactivate on
            // known drift signatures so a transient catalog blip doesn't brick the MV.
            if (isLikelyDriftException(e)) {
                if (mv.isActive()) {
                    mv.setInactiveAndReason(MaterializedViewExceptions.inactiveReasonForSchemaCheckFailed(
                            mv.getName(), e.getMessage()));
                }
                LOG.warn("[MVRefreshSchemaChecker] schema drift detected for MV {}: {}", mv.getName(), e.getMessage());
            } else {
                LOG.warn("[MVRefreshSchemaChecker] transient analyzer failure for MV {} (will retry on next refresh): {}",
                        mv.getName(), e.getMessage());
                throw e;
            }
        }
    }

    private static boolean isLikelyDriftException(SemanticException e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        // Stable analyzer / our-throw markers; everything else is treated as retryable.
        // "No matching function with signature" covers FunctionAnalyzer rejections after a
        // base column type changes (e.g. bitand(int,int) → bitand(bigint,int) no longer resolves).
        return msg.contains("cannot be resolved")
                || msg.contains("column schema not compatible")
                || msg.contains("base table schema changed for columns")
                || msg.contains("No matching function with signature")
                // IVM re-derive incompatibility (row-id strategy flip / no longer IVM-rewritable) is a
                // drop-and-recreate condition -- inactivate with the reason, don't retry forever.
                || msg.contains("row-id strategy")
                || msg.contains("is not an IVM query");
    }

    private static void analyzeAndCompareSchema(MaterializedView mv, ConnectContext context, String selectSql) {
        Optional<Database> mvDb = GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetDb(mv.getDbId());
        mvDb.map(Database::getFullName).ifPresent(context::setDatabase);

        QueryStatement queryStmt;
        // Gate on getRowIdStrategy()!=null (== has __ROW_ID__ = IVM schema), not the refresh mode: an
        // IVM MV needs the re-derived comparison, but a non-IVM-eligible AUTO MV (plain PCT schema) must
        // take the viewDefineSql branch -- else derive() throws and the healthy MV is falsely inactivated.
        if (mv.getRowIdStrategy() != null) {
            queryStmt = IvmRefreshDefinition.deriveRewrittenQuery(context, mv);
        } else {
            List<StatementBase> statements = SqlParser.parse(selectSql, context.getSessionVariable());
            if (statements.isEmpty() || !(statements.get(0) instanceof QueryStatement)) {
                return;
            }
            queryStmt = (QueryStatement) statements.get(0);
            Analyzer.analyze(queryStmt, context);
        }

        List<Field> derivedFields = queryStmt.getQueryRelation().getRelationFields().getAllFields();
        IvmSchemaCompat.compare(derivedFields, mv);
    }
}
