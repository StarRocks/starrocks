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

package com.starrocks.sql.analyzer.mv;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Re-derives the IVM maintenance query (the rewritten SELECT producing hidden __ROW_ID__ /
 * __AGG_STATE_* columns) from the MV's original user query at refresh time, instead of reading
 * a frozen persisted string.
 */
public final class IvmRefreshDefinition {
    private static final Logger LOG = LogManager.getLogger(IvmRefreshDefinition.class);

    private IvmRefreshDefinition() {
    }

    /**
     * Re-runs the IVM rewrite with the MV's pinned encode version (no trial) and enforces the
     * row-id strategy gate. Returns the analyzed rewritten query; shared by {@link #derive} and
     * the refresh-time schema checker.
     */
    public static QueryStatement deriveRewrittenQuery(ConnectContext ctx, MaterializedView mv) {
        QueryStatement qs = (QueryStatement) SqlParser.parse(mv.getViewDefineSql(),
                ctx.getSessionVariable()).get(0);
        Analyzer.analyze(qs, ctx);

        IVMAnalyzer analyzer = new IVMAnalyzer(ctx, null, qs);
        Optional<IVMAnalyzer.IVMAnalyzeResult> result =
                analyzer.rewriteForRefresh(mv.getCurrentRefreshMode(), mv.getEncodeRowIdVersion());
        if (result.isEmpty()) {
            throw new SemanticException("MV %s definition is not an IVM query at refresh", mv.getName());
        }

        QueryStatement rewritten = result.get().queryStatement();
        // The rewrite prepends __ROW_ID__ / appends __AGG_STATE columns whose output names lag until
        // re-analysis; buildSimple and getRelationFields below both read those names.
        Analyzer.analyze(rewritten, ctx);

        if (result.get().rowIdStrategy() != mv.getRowIdStrategy()) {
            throw new SemanticException(
                    "IVM MV %s: re-derived row-id strategy %s does not match stored %s; "
                            + "drop and recreate the MV",
                    mv.getName(), result.get().rowIdStrategy(), mv.getRowIdStrategy());
        }
        return rewritten;
    }

    /** Derive the maintenance SELECT SQL the incremental refresh will run. */
    public static String derive(ConnectContext ctx, MaterializedView mv) {
        QueryStatement rewritten = deriveRewrittenQuery(ctx, mv);
        IvmSchemaCompat.compare(rewritten.getQueryRelation().getRelationFields().getAllFields(), mv);
        String derivedSql = AstToSQLBuilder.buildSimple(rewritten);
        warnIfDiffersFromFrozen(mv, ctx, derivedSql);
        return derivedSql;
    }

    private static void warnIfDiffersFromFrozen(MaterializedView mv, ConnectContext ctx, String derivedSql) {
        String frozen = mv.getIvmDefineSql();
        if (frozen == null || frozen.isEmpty()) {
            return;
        }
        // Normalize the frozen text through the same serializer before comparing: it was produced by
        // a different serializer than derivedSql, so a raw compare false-positives.
        try {
            QueryStatement frozenQs = (QueryStatement) SqlParser.parse(frozen,
                    ctx.getSessionVariable()).get(0);
            Analyzer.analyze(frozenQs, ctx);
            String normalizedFrozen = AstToSQLBuilder.buildSimple(frozenQs);
            if (!normalizedFrozen.equals(derivedSql)) {
                LOG.warn("[IvmRefreshDefinition] MV {} was built by an older rewrite; re-derived "
                        + "definition differs from the frozen one (verify data, consider rebuild)",
                        mv.getName());
            }
        } catch (Exception e) {
            LOG.warn("[IvmRefreshDefinition] MV {} frozen-text diff check skipped: {}",
                    mv.getName(), e.getMessage());
        }
    }
}
