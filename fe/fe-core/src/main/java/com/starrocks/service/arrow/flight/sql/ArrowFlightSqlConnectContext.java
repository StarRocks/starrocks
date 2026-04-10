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
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.thrift.TResultSinkType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

// one connection will create one ArrowFlightSqlConnectContext
public class ArrowFlightSqlConnectContext extends ConnectContext {
    private static final Logger LOG = LogManager.getLogger(ArrowFlightSqlConnectContext.class);

    private final BufferAllocator allocator;

    private final String arrowFlightSqlToken;

    private final RunningToken runningToken = new RunningToken();

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

    private final Map<String, String> preparedStatements = new ConcurrentHashMap<>();

    public ArrowFlightSqlConnectContext(String arrowFlightSqlToken) {
        super();
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.arrowFlightSqlToken = arrowFlightSqlToken;
    }

    public boolean acquireRunningToken(long timeoutMs) throws InterruptedException {
        return runningToken.acquire(timeoutMs);
    }

    public void releaseRunningToken() {
        runningToken.release();
    }

    public void resetForStatement() {
        this.setQueryId(UUIDUtil.genUUID());
        this.setExecutionId(UUIDUtil.toTUniqueId(this.getQueryId()));
    }

    public VectorSchemaRoot getResult(String queryId) {
        ArrowSchemaRootWrapper wrapper = resultCache.getIfPresent(queryId);
        return wrapper != null ? wrapper.getSchemaRoot() : null;
    }

    public String getArrowFlightSqlToken() {
        return arrowFlightSqlToken;
    }

    public void removeAllResults() {
        resultCache.invalidateAll();
    }

    public void removeResult(String queryId) {
        resultCache.invalidate(queryId);
    }

    public String addPreparedStatement(String query) {
        String preparedStmtId = UUIDUtil.genUUID().toString();
        preparedStatements.put(preparedStmtId, query);
        return preparedStmtId;
    }

    public String getPreparedStatement(String preparedStmtId) {
        return preparedStatements.get(preparedStmtId);
    }

    public void removePreparedStatement(String preparedStmtId) {
        preparedStatements.remove(preparedStmtId);
    }

    public void setStmtExecutor(StmtExecutor stmtExecutor) {
        this.executor = stmtExecutor;
    }

    public void cancelQuery() {
        if (executor != null) {
            executor.cancel("Arrow Flight SQL client disconnected");
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
        if (hasPendingForwardRequest()) {
            String killSQL = "KILL " + getConnectionId();
            SimpleExecutor executor = new SimpleExecutor("ArrowFlightSQLCloseSession", TResultSinkType.MYSQL_PROTOCAL);
            try {
                executor.executeControl(killSQL);
            } catch (Exception e) {
                LOG.warn("Failed to kill the Arrow Flight SQL connection from the proxy to the leader.", e);
            }
        }

        StmtExecutor executorRef = executor;
        if (executorRef != null) {
            executorRef.cancel(cancelledMessage);
        }

        if (isKillConnection) {
            isKilled = true;
            cleanup();
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
            VarCharVector varCharVector = ArrowUtil.createVarCharVector(col.getName(), allocator, resultData.size());
            dataFields.add(varCharVector);
        }

        for (int rowIdx = 0; rowIdx < resultData.size(); rowIdx++) {
            List<String> row = resultData.get(rowIdx);
            for (int colIdx = 0; colIdx < row.size(); colIdx++) {
                String item = row.get(colIdx);
                if (item == null || item.equals(FeConstants.NULL_STRING)) {
                    dataFields.get(colIdx).setNull(rowIdx);
                } else {
                    ((VarCharVector) dataFields.get(colIdx)).setSafe(rowIdx, item.getBytes());
                }
            }
        }

        VectorSchemaRoot root = new VectorSchemaRoot(schemaFields, dataFields);
        root.setRowCount(resultData.size());

        resultCache.put(queryId, new ArrowSchemaRootWrapper(root));
    }

    public void addExplainResult(String queryId, String explainString) {
        List<Field> schemaFields = new ArrayList<>();
        List<FieldVector> dataFields = new ArrayList<>();

        schemaFields.add(new Field("Explain String", FieldType.nullable(new Utf8()), null));
        VarCharVector varCharVector = ArrowUtil.createVarCharVector("Explain", allocator, 1);
        dataFields.add(varCharVector);

        int rowIdx = 0;
        for (String item : explainString.split("\n")) {
            varCharVector.setSafe(rowIdx, item.getBytes());
            rowIdx++;
        }

        VectorSchemaRoot root = new VectorSchemaRoot(schemaFields, dataFields);
        root.setRowCount(rowIdx);

        resultCache.put(queryId, new ArrowSchemaRootWrapper(root));
    }

    @Override
    public String getCommandStr() {
        return "ARROW_FLIGHT_SQL.Query";
    }

    private static class RunningToken {
        private final Semaphore semaphore = new Semaphore(1);

        public boolean acquire(long timeoutMs) throws InterruptedException {
            return semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
        }

        public void release() {
            semaphore.release();
        }
    }
}
