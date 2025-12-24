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

package com.starrocks.service;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Status;
import com.starrocks.common.StatusOr;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetTableSchemaRequest;
import com.starrocks.thrift.TGetTableSchemaResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTableSchemaMeta;
import com.starrocks.thrift.TTableSchemaRequestSource;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Service for retrieving table schema information.
 * <p>
 * This service provides functionality to retrieve olap table schema information by schema ID, supporting
 * Fast Schema Evolution scenarios in shared-data mode where schema changes don't propagate schema metas
 * to backends, and the query/load execution on backends need to get schema information from Frontend.
 * </p>
 * <p>
 * The service supports two request sources:
 * <ul>
 *   <li><b>SCAN</b>: Schema requests from query scan. The service first searches in the query coordinator's
 *       cached schema, then falls back to catalog and history schemas maintained by schema change jobs.</li>
 *   <li><b>LOAD</b>: Schema requests from data loading. The service searches in catalog and history schemas.</li>
 * </ul>
 * </p>
 */
public class TableSchemaService {

    private static final Logger LOG = LoggerFactory.getLogger(TableSchemaService.class);

    /**
     * Retrieves table schema information based on the provided request.
     *
     * @param request the schema request containing schema ID, database ID, table ID, and request source
     * @return a result containing the schema information if found, or an error status otherwise
     */
    public static TGetTableSchemaResponse getTableSchema(TGetTableSchemaRequest request) {
        TGetTableSchemaResponse response = new TGetTableSchemaResponse();
        Status validationStatus = validateParameters(request);
        if (!validationStatus.ok()) {
            response.setStatus(validationStatus.toThrift());
            return response;
        }

        long schemaId = request.getSchema_meta().getSchema_id();
        long dbId = request.getSchema_meta().getDb_id();
        long tableId = request.getSchema_meta().getTable_id();
        TTableSchemaRequestSource requestSource = request.getSource();

        try {
            SchemaStatusOr coordinatorResult = null;
            if (requestSource == TTableSchemaRequestSource.SCAN) {
                coordinatorResult = findSchemaInQueryCoordinator(schemaId, request.getQuery_id());
                if (coordinatorResult.isOk()) {
                    response.setStatus(new TStatus(TStatusCode.OK));
                    response.setSchema(coordinatorResult.getValue().toTabletSchema());
                    return logAndReturn(request, response);
                }
            }

            // For SCAN requests, even if the schema is not found in the query coordinator,
            // we still attempt to search in the catalog and history schemas as a fallback.
            // This provides resilience in cases where the coordinator's cache may be incomplete
            // or the query has already finished but the schema is still needed. The latter case
            // can occur when backends batch multiple schema requests from different queries or loads
            // into a single request, so the current request may represent a different task that
            // is requesting the schema.
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
            if (table == null) {
                TStatus status = new TStatus(TStatusCode.TABLE_NOT_EXIST);
                status.addToError_msgs(String.format(
                        "table not found, may be dropped, db id: %s, table id: %s", dbId, tableId));
                response.setStatus(status);
                return logAndReturn(request, response);
            }

            if (!table.isNativeTableOrMaterializedView()) {
                TStatus status = new TStatus(TStatusCode.NOT_IMPLEMENTED_ERROR);
                status.addToError_msgs("schema service not supports table type: " + table.getType());
                response.setStatus(status);
                return logAndReturn(request, response);
            }

            Optional<SchemaInfo> schemaInfo = findSchemaInCatalog(dbId, (OlapTable) table, schemaId);
            if (schemaInfo.isEmpty()) {
                schemaInfo = findSchemaInHistory(dbId, tableId, schemaId);
            }

            if (schemaInfo.isPresent()) {
                response.setStatus(new TStatus(TStatusCode.OK));
                response.setSchema(schemaInfo.get().toTabletSchema());
            } else if (requestSource == TTableSchemaRequestSource.SCAN) {
                // For SCAN requests, if the schema is also not found in catalog or history,
                // return the original response from the coordinator. The coordinator is considered
                // the source of truth since it contains the exact schema used during query plan
                // generation, while catalog and history are fallback sources that may not always
                // have the complete schema information.
                response.setStatus(coordinatorResult.getStatus());
            } else if (requestSource == TTableSchemaRequestSource.LOAD) {
                TStatus status = new TStatus();
                long txnId = request.getTxn_id();
                TransactionState txnState = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .getTransactionState(dbId, txnId);
                if (txnState == null) {
                    status.setStatus_code(TStatusCode.TXN_NOT_EXISTS);
                    status.addToError_msgs(String.format("transaction %s not found, maybe have been cleaned", txnId));
                } else if (txnState.getTransactionStatus().isFinalStatus()) {
                    status.setStatus_code(TStatusCode.TXN_NOT_EXISTS);
                    status.addToError_msgs(String.format("transaction %s has finished, status: %s, " +
                            "schema maybe have been cleaned", txnId, txnState.getTransactionStatus().toString()));
                } else {
                    status.setStatus_code(TStatusCode.INTERNAL_ERROR);
                    status.addToError_msgs(String.format("schema for load not found which should not happen, " +
                            "please contact developers. db id: %s, table id: %s, schema id: %s, txn id: %s",
                            dbId, tableId, schemaId, txnId));
                }
                response.setStatus(status);
            } else {
                TStatus status = new TStatus(TStatusCode.NOT_IMPLEMENTED_ERROR);
                status.addToError_msgs(String.format("no implementation for request source: %s, " +
                                "db id: %s, table id: %s, schema id: %s", requestSource.name(), dbId, tableId, schemaId));
                response.setStatus(status);
            }
        } catch (Exception e) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.addToError_msgs(String.format("exception happened when getting table schema, " +
                    "db id: %s, table id: %s, schema id: %s, request source: %s, error: %s",
                    dbId, tableId, schemaId, requestSource.name(), e.getMessage()));
            response.setStatus(status);
            return logAndReturn(request, response, e);
        }
        return logAndReturn(request, response);
    }

    /**
     * Searches for schema information in the query coordinator's cached scan nodes.
     * <p>
     * This method looks up the query coordinator by query ID and searches through all
     * OlapScanNode instances to find a matching schema by schema ID. This is the primary
     * source of truth for SCAN requests, as the coordinator contains the exact schema
     * used during query plan generation.
     * </p>
     *
     * @param schemaId the schema ID to search for
     * @param queryId the unique identifier of the query
     * @return a SchemaStatusOr containing the schema if found, or an error status if the
     *         query doesn't exist or the schema is not found in the query plan
     */
    private static SchemaStatusOr findSchemaInQueryCoordinator(long schemaId, TUniqueId queryId) {
        Coordinator coordinator = QeProcessorImpl.INSTANCE.getCoordinator(queryId);
        if (coordinator == null) {
            return SchemaStatusOr.error(TStatusCode.QUERY_NOT_EXIST,
                    "query not found, maybe has finished, query id: " + DebugUtil.printId(queryId));
        }
        Optional<SchemaInfo> schemaInfo = Optional.ofNullable(coordinator.getScanNodes())
                .orElse(Collections.emptyList()).stream()
                .filter(OlapScanNode.class::isInstance)
                .map(node -> ((OlapScanNode) node).getSchema())
                .filter(schema -> schema.isPresent() && schema.get().getId() == schemaId)
                .map(Optional::get)
                .findFirst();
        return schemaInfo.map(SchemaStatusOr::value).orElseGet(() -> SchemaStatusOr.error(TStatusCode.INTERNAL_ERROR,
                String.format("schema not found in query plan which should not happen, please contact developers. " +
                        "schema id: %s, query id: %s", schemaId, DebugUtil.printId(queryId))));
    }

    /**
     * Searches for schema information in the catalog's current metadata.
     */
    private static Optional<SchemaInfo> findSchemaInCatalog(long dbId, OlapTable table, long schemaId) {
        SchemaInfo schemaInfo = null;
        try (AutoCloseableLock ignore =
                new AutoCloseableLock(new Locker(), dbId, Collections.singletonList(table.getId()), LockType.READ)) {
            for (Map.Entry<Long, MaterializedIndexMeta> entry : table.getIndexMetaIdToMeta().entrySet()) {
                MaterializedIndexMeta indexMeta = entry.getValue();
                if (indexMeta.getSchemaId() == schemaId) {
                    schemaInfo = SchemaInfo.fromMaterializedIndex(table, entry.getKey(), indexMeta);
                    break;
                }
            }
        }
        return Optional.ofNullable(schemaInfo);
    }

    /**
     * Searches for schema information in historical schemas from alter jobs.
     */
    private static Optional<SchemaInfo> findSchemaInHistory(long dbId, long tableId, long schemaId) {
        return GlobalStateMgr.getCurrentState().getAlterJobMgr()
                .getSchemaChangeHandler().getHistorySchema(dbId, tableId, schemaId);
    }

    /**
     * Validates the parameters of a table schema request.
     */
    private static Status validateParameters(TGetTableSchemaRequest request) {
        try {
            Preconditions.checkArgument(request.isSetSchema_meta(), "schema meta not set");
            TTableSchemaMeta schemaMeta = request.getSchema_meta();
            Preconditions.checkArgument(schemaMeta.isSetSchema_id(), "schema id not set");
            Preconditions.checkArgument(schemaMeta.isSetDb_id(), "db id not set");
            Preconditions.checkArgument(schemaMeta.isSetTable_id(), "table id meta not set");
            Preconditions.checkArgument(request.isSetSource(), "request source not set");
            TTableSchemaRequestSource requestSource = request.getSource();
            switch (requestSource) {
                case SCAN -> Preconditions.checkArgument(request.isSetQuery_id(), "query id not set for scan");
                case LOAD -> Preconditions.checkArgument(request.isSetTxn_id(), "txn id not set for load");
            }
        } catch (Exception e) {
            return new Status(TStatusCode.INTERNAL_ERROR, e.getMessage());
        }
        return new Status();
    }

    private static TGetTableSchemaResponse logAndReturn(TGetTableSchemaRequest request,
                                                        TGetTableSchemaResponse response) {
        return logAndReturn(request, response, null);
    }

    private static TGetTableSchemaResponse logAndReturn(TGetTableSchemaRequest request,
                                                        TGetTableSchemaResponse response,
                                                        Exception exception) {
        TStatus status = response.getStatus();
        if (status != null && status.getStatus_code() == TStatusCode.OK && !LOG.isDebugEnabled()) {
            // only log OK if debug enabled
            return response;
        }
        TTableSchemaMeta schemaMeta = request.getSchema_meta();
        StringBuilder sb = new StringBuilder();
        sb.append("table schema retrieval");
        sb.append(", db: ").append(schemaMeta.getDb_id());
        sb.append(", table: ").append(schemaMeta.getTable_id());
        sb.append(", schema: ").append(schemaMeta.getSchema_id());
        sb.append(", tablet: ").append(request.getTablet_id());
        if (request.getSource() == TTableSchemaRequestSource.SCAN) {
            sb.append(", query: ").append(DebugUtil.printId(request.getQuery_id()));
        } else if (request.getSource() == TTableSchemaRequestSource.LOAD) {
            sb.append(", txn: ").append(request.getTxn_id());
        }
        if (status == null) {
            sb.append(", status: null");
        } else {
            sb.append(", status: ").append(status.getStatus_code().name())
                .append(", errors: ").append(status.getError_msgs());
        }
        if (status != null && status.getStatus_code() == TStatusCode.OK) {
            LOG.debug(sb.toString());
        } else {
            LOG.error(sb.toString(), exception);
        }
        return response;
    }

    /**
     * A wrapper class that combines a Thrift status with an optional schema information.
     */
    private static class SchemaStatusOr extends StatusOr<SchemaInfo> {

        private SchemaStatusOr(TStatus status, SchemaInfo schemaInfo) {
            super(status, schemaInfo);
        }

        /**
         * Creates an error result with the specified status code and error message.
         */
        public static SchemaStatusOr error(TStatusCode statusCode, String errorMsg) {
            TStatus status = new TStatus(statusCode);
            status.addToError_msgs(errorMsg);
            return new SchemaStatusOr(status, null);
        }

        /**
         * Creates a successful result with the specified schema information.
         */
        public static SchemaStatusOr value(SchemaInfo schema) {
            return new SchemaStatusOr(new TStatus(TStatusCode.OK), schema);
        }
    }
}

