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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
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

        // Use the same query refresh actually runs: ivmDefineSql for IVM (post-rewrite, with
        // synthetic state-column expressions like sum_state_merge(...)), and viewDefineSql
        // for PCT via the fallback in getMVQueryDefinedSql.
        String selectSql = mv.getMVQueryDefinedSql();
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
                || msg.contains("No matching function with signature");
    }

    private static void analyzeAndCompareSchema(MaterializedView mv, ConnectContext context, String selectSql) {
        Optional<Database> mvDb = GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetDb(mv.getDbId());
        mvDb.map(Database::getFullName).ifPresent(context::setDatabase);

        List<StatementBase> statements = SqlParser.parse(selectSql, context.getSessionVariable());
        if (statements.isEmpty() || !(statements.get(0) instanceof QueryStatement)) {
            return;
        }
        QueryStatement queryStmt = (QueryStatement) statements.get(0);
        Analyzer.analyze(queryStmt, context);

        // Align by position, not name: stored MV cols and analyzer Fields are both in the
        // SELECT-list output order, so position i pairs them. Name lookup would break MVs
        // created with explicit column aliases — CREATE MV (x, y) AS SELECT k, v — where the
        // stored column is named x but the analyzer Field is named k. Position is the only
        // stable correspondence.
        // Hidden stored cols (IVM __ROW_ID__, __AGG_STATE_<func>) keep their position but are
        // skipped — their analyzer-derived nullability diverges from stored NOT NULL on
        // unchanged schemas. Drift that affects the user-visible projection is caught via
        // "cannot be resolved"; a base widening that keeps the user-visible output type
        // unchanged but mutates the hidden state type is not detected here.
        List<Field> derivedFields = queryStmt.getQueryRelation().getRelationFields().getAllFields();
        List<Column> orderedAll = mv.getOrderedOutputColumns(true);
        if (derivedFields.size() != orderedAll.size()) {
            // Stored col count must match analyzer output of the persisted SELECT. A mismatch
            // implies base schema changed in a way that altered the SELECT's output arity
            // (DROP of a SELECT * column expanded at CREATE time would normally throw
            // "cannot be resolved" first; this is a defensive catch).
            throw new SemanticException(MaterializedViewExceptions.inactiveReasonForColumnChanged(
                    Collections.singleton("column count " + orderedAll.size() + " vs " + derivedFields.size())));
        }
        for (int i = 0; i < orderedAll.size(); i++) {
            Column existed = orderedAll.get(i);
            if (existed.isHidden()) {
                continue;
            }
            Field derivedField = derivedFields.get(i);
            Type derivedNormalized = AnalyzerUtils.transformTableColumnType(derivedField.getType(), false);
            if (!isColumnCompatible(existed, derivedNormalized)) {
                // Render derived as a Column only for the error message — gives the canonical
                // OLAP `(name type NULL/NOT NULL ...)` format.
                Column derived = new Column(existed.getName(), derivedNormalized, derivedField.isNullable());
                String reason = MaterializedViewExceptions.inactiveReasonForColumnNotCompatible(
                        existed.toString(), derived.toString());
                throw new SemanticException(reason);
            }
        }
    }

    // matchesType recurses through ARRAY / MAP / STRUCT — catches inner-type drift like
    // STRUCT<a INT> → STRUCT<a BIGINT> (Spark/Trino ALTER on iceberg) that PrimitiveType.equals
    // misses because non-scalars all return INVALID_TYPE. CHAR/VARCHAR widths stay
    // interchangeable to avoid false positives on iceberg `string`.
    // Nullability skipped: analyzer's NULL on rewritten IVM SELECT diverges from stored NOT NULL.
    // Decimal precision: matchesType only buckets by primitive (DECIMAL32/64/128/256), so
    // DECIMAL(10,2) → DECIMAL(18,2) (both DECIMAL64, same scale) would pass — overflow at
    // refresh INSERT instead. Explicit precision check guards against this.
    // Visible for MVRefreshSchemaCheckerTest.
    static boolean isColumnCompatible(Column existed, Type derivedType) {
        if (!existed.getType().matchesType(derivedType)) {
            return false;
        }
        if (existed.getType().isDecimalOfAnyVersion() && derivedType.isDecimalOfAnyVersion()) {
            ScalarType existedScalar = (ScalarType) existed.getType();
            ScalarType derivedScalar = (ScalarType) derivedType;
            if (existedScalar.getScalarPrecision() != derivedScalar.getScalarPrecision()
                    || existedScalar.getScalarScale() != derivedScalar.getScalarScale()) {
                return false;
            }
        }
        return true;
    }
}
