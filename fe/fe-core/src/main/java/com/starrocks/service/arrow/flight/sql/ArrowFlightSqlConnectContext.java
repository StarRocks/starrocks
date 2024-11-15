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

import com.starrocks.catalog.Column;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.ArrowUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TUniqueId;
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

// one connection will create one ArrowFlightSqlConnectContext
public class ArrowFlightSqlConnectContext extends ConnectContext {
    private final BufferAllocator allocator;

    private StatementBase statement;

    private boolean initialized;

    private ExecPlan execPlan;

    private Coordinator coordinator;

    private String preparedQuery;

    private TUniqueId finstId;

    private VectorSchemaRoot result;

    private boolean isShowResult;

    private boolean returnFromFE;

    private String token;

    public ArrowFlightSqlConnectContext() {
        super();
        initialized = false;
        allocator = new RootAllocator(Long.MAX_VALUE);
        isShowResult = false;
        returnFromFE = true;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }

    public StatementBase getStatement() {
        return statement;
    }

    public void setStatement(StatementBase statement) {
        this.statement = statement;
    }

    public ExecPlan getExecPlan() {
        return execPlan;
    }

    public void setExecPlan(ExecPlan execPlan) {
        this.execPlan = execPlan;
    }

    public Coordinator getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public void setFinstId(TUniqueId finstId) {
        this.finstId = finstId;
    }

    public TUniqueId getFinstId() {
        return finstId;
    }

    public VectorSchemaRoot getResult() {
        return result;
    }

    public void setResult(VectorSchemaRoot result) {
        this.result = result;
    }

    public String getPreparedQuery() {
        return preparedQuery;
    }

    public void setPreparedQuery(String preparedQuery) {
        this.preparedQuery = preparedQuery;
    }

    public void addOKResult() {
        result = ArrowUtil.createSingleSchemaRoot("StatusResult", "0");
    }

    public boolean isShowResult() {
        return isShowResult;
    }

    public void setShowResult(boolean showResult) {
        isShowResult = showResult;
    }

    public boolean returnFromFE() {
        return returnFromFE;
    }

    public void setReturnFromFE(boolean returnFromFE) {
        this.returnFromFE = returnFromFE;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    @Override
    public void kill(boolean killConnection, String cancelledMessage) {
        StmtExecutor executorRef = executor;
        if (killConnection) {
            isKilled = true;
        }
        if (executorRef != null) {
            executorRef.cancel(cancelledMessage);
        }
        if (killConnection) {
            connectScheduler.unregisterConnection(this);
        }
    }

    public void addShowResult(ShowResultSet showResultSet) {
        result = null;
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

        result = new VectorSchemaRoot(schemaFields, dataFields);
        isShowResult = true;
    }
}
