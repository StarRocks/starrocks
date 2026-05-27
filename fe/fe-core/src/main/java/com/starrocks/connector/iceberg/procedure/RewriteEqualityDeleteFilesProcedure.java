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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergConvertEqualityDeleteJob;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

// `ALTER TABLE catalog.db.tbl EXECUTE 'rewrite_equality_delete_files'()`: converts the live equality
// delete files of the current snapshot into position delete files (reusing the StarRocks join engine
// for the match), then atomically removes the equality deletes and adds the position deletes.
// v1 converts the whole table (no arguments / WHERE filter).
public class RewriteEqualityDeleteFilesProcedure extends IcebergTableProcedure {
    private static final Logger LOGGER = LoggerFactory.getLogger(RewriteEqualityDeleteFilesProcedure.class);

    private static final String PROCEDURE_NAME = "rewrite_equality_delete_files";

    private static final RewriteEqualityDeleteFilesProcedure INSTANCE = new RewriteEqualityDeleteFilesProcedure();

    public static RewriteEqualityDeleteFilesProcedure getInstance() {
        return INSTANCE;
    }

    private RewriteEqualityDeleteFilesProcedure() {
        super(PROCEDURE_NAME, List.of(), IcebergTableOperation.REWRITE_EQUALITY_DELETE_FILES);
    }

    @Override
    public ShowResultSet execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        if (!args.isEmpty()) {
            throw new StarRocksConnectorException(
                    "rewrite_equality_delete_files does not accept arguments; it converts the whole table");
        }
        // The analyzer only binds a WHERE clause for REWRITE_DATA_FILES; for any other operation it
        // passes through. Reject it explicitly so a user filter is never silently ignored.
        if (context.clause().getWhere() != null) {
            throw new StarRocksConnectorException(
                    "rewrite_equality_delete_files does not support a WHERE clause; it converts the whole table");
        }

        org.apache.iceberg.Table nativeTable = context.table();
        if (nativeTable.currentSnapshot() == null) {
            return buildResult(context, IcebergConvertEqualityDeleteJob.ConvertMetrics.EMPTY, 0);
        }
        long snapshotId = nativeTable.currentSnapshot().snapshotId();

        String catalogName = context.stmt().getCatalogName();
        String dbName = context.stmt().getDbName();
        String tableName = context.stmt().getTableName();
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(context.context(), catalogName, dbName, tableName);
        if (!(table instanceof IcebergTable icebergTable)) {
            throw new StarRocksConnectorException("table %s.%s.%s is not an iceberg table",
                    catalogName, dbName, tableName);
        }

        long startMs = System.currentTimeMillis();
        IcebergConvertEqualityDeleteJob.ConvertMetrics metrics;
        try {
            IcebergConvertEqualityDeleteJob job = new IcebergConvertEqualityDeleteJob(
                    context.context(), icebergTable, snapshotId, context.stmt().getTableRef());
            metrics = job.execute();
        } catch (Exception e) {
            LOGGER.error("failed to rewrite equality delete files for iceberg table {}.{}", dbName, tableName, e);
            throw new StarRocksConnectorException(
                    "execute rewrite equality delete files for iceberg table %s.%s failed: %s",
                    dbName, tableName, e.getMessage(), e);
        }
        long durationMs = Math.max(0, System.currentTimeMillis() - startMs);
        return buildResult(context, metrics, durationMs);
    }

    private ShowResultSet buildResult(IcebergTableProcedureContext context,
                                      IcebergConvertEqualityDeleteJob.ConvertMetrics metrics, long durationMs) {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .column("db_name", TypeFactory.createVarcharType(128))
                .column("table_name", TypeFactory.createVarcharType(128))
                .column("removed_equality_delete_files", TypeFactory.createType(PrimitiveType.BIGINT))
                .column("added_position_delete_files", TypeFactory.createType(PrimitiveType.BIGINT))
                .column("equality_delete_rows", TypeFactory.createType(PrimitiveType.BIGINT))
                .column("position_delete_rows", TypeFactory.createType(PrimitiveType.BIGINT))
                .column("duration_ms", TypeFactory.createType(PrimitiveType.BIGINT))
                .column("status", TypeFactory.createVarcharType(128))
                .build();
        List<List<String>> rows = new ArrayList<>();
        rows.add(Arrays.asList(
                context.stmt().getDbName(),
                context.stmt().getTableName(),
                String.valueOf(metrics.removedEqualityDeleteFiles()),
                String.valueOf(metrics.addedPositionDeleteFiles()),
                String.valueOf(metrics.equalityDeleteRows()),
                String.valueOf(metrics.positionDeleteRows()),
                String.valueOf(durationMs),
                "success"));
        context.context().getState().setOk(0, 0, String.format(
                "rewrite equality delete files finished [%s.%s]: removed_equality_delete_files=%d, " +
                        "added_position_delete_files=%d, equality_delete_rows=%d, position_delete_rows=%d, " +
                        "duration_ms=%d",
                context.stmt().getDbName(), context.stmt().getTableName(),
                metrics.removedEqualityDeleteFiles(), metrics.addedPositionDeleteFiles(),
                metrics.equalityDeleteRows(), metrics.positionDeleteRows(), durationMs));
        return new ShowResultSet(metaData, rows);
    }
}
