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

package com.starrocks.connector.iceberg.procedure;

import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

/**
 * Carries everything a table procedure needs for one invocation.
 * <p>
 * This is a plain class rather than a record because {@link #storageConfiguration()} is a derived
 * value that has to be computed once and reused: procedures that walk storage ask for it per file,
 * and rebuilding it each time would re-run the Hadoop configuration rewrite for every file.
 */
public final class IcebergTableProcedureContext {
    private final IcebergCatalog icebergCatalog;
    private final Table table;
    private final ConnectContext context;
    private final Transaction transaction;
    private final HdfsEnvironment hdfsEnvironment;
    private final AlterTableStmt stmt;
    private final AlterTableOperationClause clause;

    // Derived from table + catalog + hdfsEnvironment; see storageConfiguration().
    private volatile Configuration storageConfiguration;

    public IcebergTableProcedureContext(IcebergCatalog icebergCatalog,
                                        Table table,
                                        ConnectContext context,
                                        Transaction transaction,
                                        HdfsEnvironment hdfsEnvironment,
                                        AlterTableStmt stmt,
                                        AlterTableOperationClause clause) {
        this.icebergCatalog = icebergCatalog;
        this.table = table;
        this.context = context;
        this.transaction = transaction;
        this.hdfsEnvironment = hdfsEnvironment;
        this.stmt = stmt;
        this.clause = clause;
    }

    public IcebergCatalog icebergCatalog() {
        return icebergCatalog;
    }

    public Table table() {
        return table;
    }

    public ConnectContext context() {
        return context;
    }

    public Transaction transaction() {
        return transaction;
    }

    public HdfsEnvironment hdfsEnvironment() {
        return hdfsEnvironment;
    }

    public AlterTableStmt stmt() {
        return stmt;
    }

    public AlterTableOperationClause clause() {
        return clause;
    }

    /**
     * The Hadoop configuration to use when a procedure reaches the table's storage directly.
     * <p>
     * ★Use this, not {@code hdfsEnvironment().getConfiguration()}★: the latter carries only the
     * static credentials from the catalog properties, so on a catalog that vends credentials per
     * table it authenticates with nothing at all.
     */
    public Configuration storageConfiguration() {
        Configuration local = storageConfiguration;
        if (local == null) {
            synchronized (this) {
                local = storageConfiguration;
                if (local == null) {
                    local = IcebergUtil.buildStorageConfiguration(table, icebergCatalog, hdfsEnvironment);
                    storageConfiguration = local;
                }
            }
        }
        return local;
    }
}
