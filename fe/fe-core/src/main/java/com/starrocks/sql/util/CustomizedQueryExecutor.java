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

package com.starrocks.sql.util;

import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TRowFormat;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CustomizedQueryExecutor {
    private List<TResultBatch> executeDQL(ConnectContext context, String sql) {
        StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
        ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.CUSTOMIZED);
        StmtExecutor executor = new StmtExecutor(context, parsedStmt);
        context.setExecutor(executor);
        context.setQueryId(UUIDUtil.genUUID());
        Pair<List<TResultBatch>, Status> sqlResult = executor.executeStmtWithExecPlan(context, execPlan);
        if (!sqlResult.second.ok()) {
            throw new SemanticException("Execute query fail | Error Message [%s] | QueryId [%s] | SQL [%s]",
                    context.getState().getErrorMessage(), DebugUtil.printId(context.getQueryId()), sql);
        } else {
            return sqlResult.first;
        }
    }

    private Function<ByteBuffer, TRowFormat> getDeserializer() {
        try {
            TDeserializer deserializer = new TDeserializer(TCompactProtocol::new);
            return byteRow -> {
                TRowFormat row = new TRowFormat();
                try {
                    deserializer.deserialize(row, byteRow.array(), byteRow.position(), byteRow.remaining());
                    return row;
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            };
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    }

    private List<TRowFormat> deserialize(List<TResultBatch> sqlResult) {
        return sqlResult.stream().flatMap(batch -> batch.getRows().stream())
                .map(getDeserializer())
                .collect(Collectors.toList());
    }

    public List<TRowFormat> query(ConnectContext context, String sql) {
        return deserialize(executeDQL(context, sql));
    }

    public <T> List<T> query(Class<T> klass, List<ColumnPlus> columns, ConnectContext context, String sql) {
        Function<TRowFormat, T> unpacker = row -> ColumnPlus.unpack(klass, columns, row);
        return query(context, sql).stream().map(unpacker).collect(Collectors.toList());
    }
}
