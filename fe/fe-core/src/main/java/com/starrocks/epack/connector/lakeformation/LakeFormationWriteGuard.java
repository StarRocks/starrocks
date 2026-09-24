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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.TruncateTableStmt;

/**
 * Refuses statements whose mutation target lives in a Lake Formation catalog.
 *
 * The rule is about the target, not about the statement kind: reading a Lake Formation table and writing the
 * result into an internal table is fine, and so is CTAS whose source is a Lake Formation table. Only the
 * thing being modified matters.
 *
 * It runs ahead of the privilege check, because "this version does not support it" is a capability
 * statement and reporting it as a permission problem would send an administrator looking for a grant that
 * would not have helped. Analysis has already run by this point, so an INSERT's target table is resolved.
 *
 * Nothing here recurses into a statement's query block. That is deliberate: recursing would turn every
 * Lake Formation table used as a source into a refusal.
 */
public final class LakeFormationWriteGuard {

    private LakeFormationWriteGuard() {
    }

    public static void check(StatementBase statement, ConnectContext context) {
        if (statement == null) {
            return;
        }
        new Guard().visit(statement, context);
    }

    /**
     * @param catalogName may be null or empty for a statement the analyzer did not normalize - a CTAS nested
     *                    inside SUBMIT TASK is the case that actually happens - so it falls back to the
     *                    session's current catalog rather than reading an absent name as "not Lake Formation"
     */
    private static void refuseIfLakeFormation(String catalogName, ConnectContext context, String what) {
        String resolved = catalogName;
        if ((resolved == null || resolved.isEmpty()) && context != null) {
            resolved = context.getCurrentCatalog();
        }
        if (LakeFormationCatalogs.isLakeFormationCatalog(resolved)) {
            throw new LakeFormationTableAccessException(what + " is not supported on Lake Formation catalog "
                    + resolved + " in this version.");
        }
    }

    private static class Guard implements AstVisitorExtendInterface<Void, ConnectContext> {

        @Override
        public Void visitStatement(StatementBase statement, ConnectContext context) {
            // Anything not listed below is none of this guard's business.
            return null;
        }

        @Override
        public Void visitInsertStatement(InsertStmt statement, ConnectContext context) {
            if (statement.getTargetTable() instanceof LakeFormationGovernedTable lfTable) {
                throw new LakeFormationTableAccessException("Writing to "
                        + lfTable.getLakeFormationIdentity() + " is not supported in this version."
                        + " Reading a Lake Formation table and inserting the result elsewhere is allowed.");
            }
            return null;
        }

        /**
         * Only the table being created is checked. The SELECT that feeds it is left alone, so CTAS into an
         * internal catalog reading a Lake Formation table keeps working.
         */
        @Override
        public Void visitCreateTableAsSelectStatement(CreateTableAsSelectStmt statement, ConnectContext context) {
            if (statement.getCreateTableStmt() != null) {
                refuseIfLakeFormation(statement.getCreateTableStmt().getCatalogName(), context, "CREATE TABLE AS SELECT");
            }
            return null;
        }

        @Override
        public Void visitCreateTableStatement(CreateTableStmt statement, ConnectContext context) {
            refuseIfLakeFormation(statement.getCatalogName(), context, "CREATE TABLE");
            return null;
        }

        @Override
        public Void visitDropTableStatement(DropTableStmt statement, ConnectContext context) {
            refuseIfLakeFormation(statement.getCatalogName(), context, "DROP TABLE");
            return null;
        }

        @Override
        public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext context) {
            refuseIfLakeFormation(statement.getCatalogName(), context, "ALTER TABLE");
            return null;
        }

        @Override
        public Void visitTruncateTableStatement(TruncateTableStmt statement, ConnectContext context) {
            refuseIfLakeFormation(statement.getCatalogName(), context, "TRUNCATE TABLE");
            return null;
        }

        @Override
        public Void visitCreateDbStatement(CreateDbStmt statement, ConnectContext context) {
            refuseIfLakeFormation(statement.getCatalogName(), context, "CREATE DATABASE");
            return null;
        }

        @Override
        public Void visitDropDbStatement(DropDbStmt statement, ConnectContext context) {
            refuseIfLakeFormation(statement.getCatalogName(), context, "DROP DATABASE");
            return null;
        }

        /**
         * Not just a cache drop. The chain ends in an unconditional bare-Glue loadTable whose result is put
         * into the cross-query table cache, where the next principal would find an untrimmed physical schema.
         */
        @Override
        public Void visitRefreshTableStatement(RefreshTableStmt statement, ConnectContext context) {
            refuseIfLakeFormation(statement.getCatalogName(), context, "REFRESH EXTERNAL TABLE");
            return null;
        }

        /**
         * The metadata level guard cannot cover this one: MetadataMgr returns early for IF NOT EXISTS when
         * the table already exists, so createTableLike is never reached and the statement reports success.
         */
        @Override
        public Void visitCreateTableLikeStatement(CreateTableLikeStmt statement, ConnectContext context) {
            refuseIfLakeFormation(statement.getCatalogName(), context, "CREATE TABLE LIKE");
            return null;
        }

        /**
         * Checked at submit time, not only when the task runs. The task is persisted first and re-parsed
         * later, so without this the user gets a successful SUBMIT and a task that fails forever after.
         *
         * Only the embedded mutation target is inspected; the query that feeds it is left alone, exactly as
         * for a plain CTAS or INSERT.
         */
        @Override
        public Void visitSubmitTaskStatement(SubmitTaskStmt statement, ConnectContext context) {
            if (statement.getCreateTableAsSelectStmt() != null
                    && statement.getCreateTableAsSelectStmt().getCreateTableStmt() != null) {
                // Nested CTAS is not normalized by the analyzer, so its catalog name may be absent.
                refuseIfLakeFormation(statement.getCreateTableAsSelectStmt().getCreateTableStmt().getCatalogName(),
                        context, "SUBMIT TASK ... CREATE TABLE AS SELECT");
            }
            if (statement.getInsertStmt() != null
                    && statement.getInsertStmt().getTargetTable() instanceof LakeFormationGovernedTable lfTable) {
                throw new LakeFormationTableAccessException("SUBMIT TASK writing to "
                        + lfTable.getLakeFormationIdentity() + " is not supported in this version.");
            }
            // A submitted CACHE SELECT is judged by what it reads: it runs unattended on a schedule and leaves governed
            // data in a cache whose later reads cannot be refused. One the user runs now is an ordinary authorized
            // read.
            if (statement.getDataCacheSelectStmt() != null) {
                String what = "SUBMIT TASK ... CACHE SELECT";
                // The analyzer may not have reached a nested statement, so the named tables are checked too.
                refuseIfLakeFormation(statement.getDataCacheSelectStmt().getCatalog(), context, what);
                for (TableRelation relation : AnalyzerUtils.collectAllTableRelation(
                        statement.getDataCacheSelectStmt().getInsertStmt().getQueryStatement()).values()) {
                    refuseIfLakeFormation(relation.getName().getCatalog(), context, what);
                }
            }
            // Deliberately not checked: SubmitTaskStmt.getCatalogName(). The analyzer fills it from the
            // session's current catalog when the task is unqualified (TaskAnalyzer:33-37), so it names the
            // namespace the task is stored under, not what the task writes to. Reading it as a target would
            // refuse a task that merely runs while a Lake Formation catalog is current - the opposite of the
            // rule the two checks above implement.
            return null;
        }

        /**
         * A scheduled job runs later in a daemon over every table it enumerates; refused where it is created.
         */
        @Override
        public Void visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, ConnectContext context) {
            refuseIfLakeFormation(statement.getCatalogName(), context, "CREATE ANALYZE JOB");
            return null;
        }
    }
}
