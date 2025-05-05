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

package com.starrocks.service.arrow.flight.sql;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.starrocks.catalog.Column;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.ArrowUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.ast.StatementBase;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

// one connection will create one ArrowFlightSqlConnectContext
public class ArrowFlightSqlConnectContext extends ConnectContext {
    private final BufferAllocator allocator;

    private final String token;

    private StmtExecutor stmtExecutor;

    private StatementBase statement;

    private String query;

    private CompletableFuture<Coordinator> coordinatorFuture;

    private boolean returnResultFromFE;

    // - Only contains the execution result of the most recent query.
    // - When the result of a query is taken away, the result will be cleared.
    // - When the result is not taken away for 10 minutes, the result will be cleared.
    // - When a new query arrives, the previous result will be cleared.
    private final Cache<String, ArrowSchemaRootWrapper> resultCache = CacheBuilder.newBuilder()
            .maximumSize(128)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .removalListener((RemovalNotification<String, ArrowSchemaRootWrapper> notification) -> {
                ArrowSchemaRootWrapper wrapper = notification.getValue();
                if (wrapper != null) {
                    wrapper.close();
                }
            })
            .build();

    public ArrowFlightSqlConnectContext(String token) {
        super();
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.token = token;
        this.statement = null;
        this.query = "";
        this.coordinatorFuture = new CompletableFuture<>();
        this.returnResultFromFE = true;
    }

    public void reset(String query) {
        removeAllResults();
        statement = null;
        coordinatorFuture.complete(null);
        coordinatorFuture = new CompletableFuture<>();
        returnResultFromFE = true;
        this.query = query;
        this.setQueryId(UUIDUtil.genUUID());
        this.setExecutionId(UUIDUtil.toTUniqueId(this.getQueryId()));
    }

    public StatementBase getStatement() {
        return statement;
    }

    public void setStatement(StatementBase statement) {
        this.statement = statement;
    }

    public Coordinator waitForDeploymentFinished(long timeoutMs)
            throws ExecutionException, InterruptedException, TimeoutException, CancellationException {
        return coordinatorFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public void setDeploymentFinished(Coordinator coordinator) {
        this.coordinatorFuture.complete(coordinator);
    }

    public VectorSchemaRoot getResult(String queryId) {
        ArrowSchemaRootWrapper wrapper = resultCache.getIfPresent(queryId);
        return wrapper != null ? wrapper.getSchemaRoot() : null;
    }

    public String getQuery() {
        return query;
    }

    public boolean returnFromFE() {
        return returnResultFromFE;
    }

    public void setReturnResultFromFE(boolean returnResultFromFE) {
        this.returnResultFromFE = returnResultFromFE;
    }

    public String getToken() {
        return token;
    }

    public void removeAllResults() {
        resultCache.invalidateAll();
    }

    public void removeResult(String queryId) {
        resultCache.invalidate(queryId);
    }

    public void setStmtExecutor(StmtExecutor stmtExecutor) {
        this.stmtExecutor = stmtExecutor;
    }

    public void cancelQuery() {
        if (stmtExecutor != null) {
            stmtExecutor.cancel("Arrow Flight SQL client disconnected");
        }
    }

    public void setEmptyResultIfNotExist(String queryId) {
        if (resultCache.getIfPresent(queryId) == null) {
            VectorSchemaRoot schemaRoot = ArrowUtil.createSingleSchemaRoot("StatusResult", "0");
            resultCache.put(queryId, new ArrowSchemaRootWrapper(schemaRoot));
        }
    }

    @Override
    public void kill(boolean isKillConnection, String cancelledMessage) {
        StmtExecutor executorRef = executor;
        if (executorRef != null) {
            executorRef.cancel(cancelledMessage);
        }

        if (coordinatorFuture != null && coordinatorFuture.isDone()) {
            try {
                Coordinator coordinator = coordinatorFuture.getNow(null);
                if (coordinator != null) {
                    coordinator.cancel(cancelledMessage);
                }
            } catch (Exception e) {
                // Do nothing.
            }
        }

        if (isKillConnection) {
            isKilled = true;
            this.cleanup();
            ExecuteEnv.getInstance().getScheduler().unregisterConnection(this);
        }

        removeAllResults();

        if (allocator != null) {
            allocator.close();
        }
    }


    // Converts the result of the SHOW statement to Arrow's VectorSchemaRoot.
    public void addShowResult(String queryId, ShowResultSet showResultSet) {
        List<Field> schemaFields = new ArrayList<>();
        List<FieldVector> dataFields = new ArrayList<>();
        List<List<String>> resultData = showResultSet.getResultRows();
        ShowResultSetMetaData metaData = showResultSet.getMetaData();

        for (Column col : metaData.getColumns()) {
            schemaFields.add(new Field(col.getName(), FieldType.nullable(new Utf8()), null));
            VarCharVector varCharVector = new VarCharVector(col.getName(), allocator);
            varCharVector.allocateNew();
            varCharVector.setValueCount(resultData.size());
            dataFields.add(varCharVector);
        }

        for (int i = 0; i < resultData.size(); i++) {
            List<String> row = resultData.get(i);
            for (int j = 0; j < row.size(); j++) {
                String item = row.get(j);
                if (item == null || item.equals(FeConstants.NULL_STRING)) {
                    dataFields.get(j).setNull(i);
                } else {
                    ((VarCharVector) dataFields.get(j)).setSafe(i, item.getBytes());
                }
            }
        }

        resultCache.put(queryId, new ArrowSchemaRootWrapper(new VectorSchemaRoot(schemaFields, dataFields)));
    }

    @Override
    public boolean isArrowFlightSQL() {
        return true;
    }

    public boolean isFromFECoordinator() {
        Coordinator coordinator = coordinatorFuture.getNow(null);
        return !(coordinator instanceof DefaultCoordinator);
    }
}
