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
package com.starrocks.catalog.system.information;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TGetRunningTxnsParams;
import com.starrocks.thrift.TGetRunningTxnsResult;
import com.starrocks.thrift.TRunningTxnInfo;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;
import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

// information_schema.running_transactions: one row per currently running (non-final:
// PREPARE/PREPARED/COMMITTED) transaction across all databases. It is the SQL-queryable diagnostic surface
// for a publish stall - the headline column PENDING_PUBLISH_MS is how long a COMMITTED txn has waited to be
// published to VISIBLE. Row production lives entirely on the FE (query()); a thin BE scanner issues the
// getRunningTransactions RPC back to the leader (the only node whose running set is authoritative), which
// is why the table name is registered in SystemTable.QUERY_FROM_LEADER_TABLES.
public class RunningTransactionsSystemTable {
    public static final String NAME = "running_transactions";
    private static final Logger LOG = LogManager.getLogger(RunningTransactionsSystemTable.class);

    public static SystemTable create() {
        return new SystemTable(SystemId.RUNNING_TRANSACTIONS_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        .column("TXN_ID", IntegerType.BIGINT)
                        .column("GLOBAL_TXN_ID", IntegerType.BIGINT)
                        .column("LABEL", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("DATABASE_ID", IntegerType.BIGINT)
                        .column("DATABASE_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TABLE_IDS", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("TABLE_NAMES", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("STATE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("COORDINATOR", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("SOURCE_TYPE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("WAREHOUSE_ID", IntegerType.BIGINT)
                        .column("PREPARE_TIME", DateType.DATETIME)
                        .column("PREPARED_TIME", DateType.DATETIME)
                        .column("COMMIT_TIME", DateType.DATETIME)
                        .column("PUBLISH_TIME", DateType.DATETIME)
                        .column("FINISH_TIME", DateType.DATETIME)
                        .column("PENDING_PUBLISH_MS", IntegerType.BIGINT)
                        .column("TIMEOUT_MS", IntegerType.BIGINT)
                        .column("PREPARED_TIMEOUT_MS", IntegerType.BIGINT)
                        .column("ERROR_REPLICA_NUM", IntegerType.BIGINT)
                        .column("REASON", TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .column("ERROR_MSG", TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .column("IS_NO_OP_PUBLISH", BooleanType.BOOLEAN)
                        .column("NO_OP_PUBLISH_REASON", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .build(), TSchemaTableType.SCH_RUNNING_TRANSACTIONS);
    }

    // Produce the rows on the FE. Called from FrontendServiceImpl.getRunningTransactions via the BE scanner.
    // The running-transaction snapshot is taken under each database manager's read lock (in
    // DatabaseTransactionMgr.getRunningTransactions); here we apply the pushed-down db/label filters, resolve
    // db/table NAMES off-lock (metastore lookups), and filter each row by the querying user's database
    // privileges. Ids that no longer resolve (dropped mid-flight) keep their raw id rather than throwing.
    public static TGetRunningTxnsResult query(TGetRunningTxnsParams params, ConnectContext authContext) {
        TGetRunningTxnsResult result = new TGetRunningTxnsResult();
        List<TRunningTxnInfo> txns = Lists.newArrayList();
        try {
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            LocalMetastore metastore = globalStateMgr.getLocalMetastore();

            // Fail closed. The querying user is forwarded from the scan node (SchemaScanNode always sets
            // current_user_ident) and turned into authContext by the RPC handler, so the leader can authorize
            // even though the scan is routed here from another FE. Without an identity we cannot authorize any
            // row, so we expose nothing rather than everything: getRunningTransactions is a FrontendService RPC
            // on the unauthenticated rpc_port, so returning unfiltered rows to an identity-less caller would let
            // a direct thrift client bypass the gate. In the normal SQL path an identity is always present.
            if (authContext == null || authContext.getCurrentUserIdentity() == null) {
                result.setTxns(txns);
                return result;
            }

            Long filterDbId = null;
            if (params.isSetDb() && !Strings.isNullOrEmpty(params.getDb())) {
                Database db = metastore.getDb(params.getDb());
                if (db == null) {
                    // Unknown database in the pushed-down filter -> no rows (rather than every database).
                    result.setTxns(txns);
                    return result;
                }
                filterDbId = db.getId();
            }
            // Only db and label are pushed down (TXN_ID is BIGINT, which the BE scanner cannot push down;
            // a WHERE TXN_ID = <n> predicate is applied by the BE as a residual filter on the returned rows).
            String labelFilter = params.isSetLabel() ? params.getLabel() : null;

            // Cache the per-database visibility decision so N running txns on one database cost one privilege
            // check, not N.
            Map<Long, Boolean> dbVisible = new HashMap<>();

            List<TRunningTxnInfo> rows = globalStateMgr.getGlobalTransactionMgr().getRunningTransactions(filterDbId);
            for (TRunningTxnInfo row : rows) {
                if (labelFilter != null && !labelFilter.equals(row.getLabel())) {
                    continue;
                }
                // Resolve and authorize the database first; only resolve table names for rows the user is
                // allowed to see, so denied databases do not pay for table-name lookups.
                Database db = metastore.getDb(row.getDatabase_id());
                if (db != null) {
                    row.setDatabase_name(db.getFullName());
                }
                if (!isDbVisible(authContext, row, dbVisible)) {
                    continue;
                }
                resolveTableNames(db, row);
                txns.add(row);
            }
            result.setTxns(txns);
        } catch (Exception e) {
            LOG.warn("Failed to query information_schema.running_transactions", e);
            throw e;
        }
        return result;
    }

    // A row is visible only if the querying user holds some privilege on, or within, its database (the same
    // db-level check information_schema.tables / tasks / task_runs use). A database that no longer resolves
    // (dropped mid-flight, so database_name is unset) cannot be authorized and is hidden from every user,
    // including admins - a deliberate fail-closed choice, documented on the DATABASE_NAME column. Decisions
    // are cached per database id by the caller.
    private static boolean isDbVisible(ConnectContext authContext, TRunningTxnInfo row, Map<Long, Boolean> cache) {
        Boolean cached = cache.get(row.getDatabase_id());
        if (cached != null) {
            return cached;
        }
        boolean visible;
        if (!row.isSetDatabase_name() || Strings.isNullOrEmpty(row.getDatabase_name())) {
            visible = false;
        } else {
            try {
                Authorizer.checkAnyActionOnOrInDb(authContext, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        row.getDatabase_name());
                visible = true;
            } catch (AccessDeniedException e) {
                visible = false;
            }
        }
        cache.put(row.getDatabase_id(), visible);
        return visible;
    }

    // Off-lock resolution of the comma-joined table ids into TABLE_NAMES, done only for rows the caller may
    // see. Best-effort - an id whose table was dropped mid-flight keeps its raw id.
    private static void resolveTableNames(Database db, TRunningTxnInfo row) {
        if (!row.isSetTable_ids() || Strings.isNullOrEmpty(row.getTable_ids())) {
            return;
        }
        List<String> names = Lists.newArrayList();
        for (String idStr : row.getTable_ids().split(",")) {
            if (idStr.isEmpty()) {
                continue;
            }
            String name = idStr;
            if (db != null) {
                try {
                    Table table = db.getTable(Long.parseLong(idStr));
                    if (table != null) {
                        name = table.getName();
                    }
                } catch (NumberFormatException ignored) {
                    // keep the raw id string
                }
            }
            names.add(name);
        }
        row.setTable_names(String.join(",", names));
    }
}
