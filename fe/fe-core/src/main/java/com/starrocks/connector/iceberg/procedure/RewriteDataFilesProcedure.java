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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergRewriteDataJob;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RewriteDataFilesProcedure extends IcebergTableProcedure {
    private static final Logger LOGGER = LoggerFactory.getLogger(RewriteDataFilesProcedure.class);

    private static final String PROCEDURE_NAME = "rewrite_data_files";

    private static final String REWRITE_ALL = "rewrite_all";
    private static final String MIN_FILE_SIZE_BYTES = "min_file_size_bytes";
    private static final String BATCH_SIZE = "batch_size";

    private static final long DEFAULT_MIN_FILE_SIZE_BYTES = 256L * 1024 * 1024;
    private static final long DEFAULT_BATCH_SIZE = 10L * 1024 * 1024 * 1024;

    private static final RewriteDataFilesProcedure INSTANCE = new RewriteDataFilesProcedure();

    public static RewriteDataFilesProcedure getInstance() {
        return INSTANCE;
    }

    private RewriteDataFilesProcedure() {
        super(
                PROCEDURE_NAME,
                Arrays.asList(
                        new NamedArgument(REWRITE_ALL, Type.BOOLEAN, false),
                        new NamedArgument(MIN_FILE_SIZE_BYTES, Type.BIGINT, false),
                        new NamedArgument(BATCH_SIZE, Type.BIGINT, false)
                ),
                IcebergTableOperation.REWRITE_DATA_FILES
        );
    }

    @Override
    public void execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        if (args.size() > 3) {
            throw new StarRocksConnectorException("invalid args. only support `rewrite_all`, " +
                    "`min_file_size_bytes` and `batch_size` in the rewrite data files operation");
        }

        boolean rewriteAll = args.get(REWRITE_ALL) != null && args.get(REWRITE_ALL)
                .castTo(Type.BOOLEAN)
                .map(ConstantOperator::getBoolean)
                .orElseThrow(() -> new StarRocksConnectorException("invalid argument type for %s, expected BOOLEAN",
                        REWRITE_ALL));
        long minFileSizeBytes = args.get(MIN_FILE_SIZE_BYTES) != null ? args.get(MIN_FILE_SIZE_BYTES)
                .castTo(Type.BIGINT)
                .map(ConstantOperator::getBigint)
                .filter(v -> v > 0)
                .orElseThrow(() -> new StarRocksConnectorException("invalid argument type for %s, expected positive BIGINT",
                        MIN_FILE_SIZE_BYTES))
                : DEFAULT_MIN_FILE_SIZE_BYTES;
        long batchSize = args.get(BATCH_SIZE) != null ? args.get(BATCH_SIZE)
                .castTo(Type.BIGINT)
                .map(ConstantOperator::getBigint)
                .filter(v -> v > 0)
                .orElseThrow(() -> new StarRocksConnectorException("invalid argument type for %s, expected positive BIGINT",
                        BATCH_SIZE))
                : DEFAULT_BATCH_SIZE;

        AlterTableStmt stmt = context.stmt();
        Expr partitionFilter = context.clause().getWhere();
        String catalogName = context.stmt().getCatalogName();
        String dbName = context.stmt().getDbName();
        String tableName = context.stmt().getTableName();

        VelocityContext velCtx = new VelocityContext();
        velCtx.put("catalogName", catalogName);
        velCtx.put("dbName", dbName);
        velCtx.put("tableName", tableName);

        String partitionFilterSql = null;
        if (partitionFilter != null) {
            List<SlotRef> slots = new ArrayList<>();
            partitionFilter.collect(SlotRef.class, slots);
            for (SlotRef slot : slots) {
                slot.setTblName(new TableName(dbName, tableName));
            }
            partitionFilterSql = partitionFilter.toSql();
        }
        velCtx.put("partitionFilterSql", partitionFilterSql);
        VelocityEngine defaultVelocityEngine = new VelocityEngine();
        defaultVelocityEngine.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
        StringWriter writer = new StringWriter();
        defaultVelocityEngine.evaluate(velCtx, writer, "InsertSelectTemplate",
                "INSERT INTO $catalogName.$dbName.$tableName" +
                        " SELECT * FROM $catalogName.$dbName.$tableName" +
                        " #if ($partitionFilterSql)" +
                        " WHERE $partitionFilterSql" +
                        " #end"
        );
        String executeStmt = writer.toString();
        IcebergRewriteDataJob job = new IcebergRewriteDataJob(executeStmt, rewriteAll,
                minFileSizeBytes, batchSize, context.context(), stmt);
        try {
            job.prepare();
            job.execute();
        } catch (Exception e) {
            LOGGER.error("failed to rewrite data files for iceberg table {}.{}",
                    stmt.getDbName(), stmt.getTableName(), e);
            throw new StarRocksConnectorException("execute rewrite data files for iceberg table %s.%s failed: %s",
                    stmt.getDbName(), stmt.getTableName(), e.getMessage(), e);
        }
    }
}