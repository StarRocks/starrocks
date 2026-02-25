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

package com.starrocks.sql.optimizer.dump;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.catalog.Database;
import com.starrocks.common.Pair;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryDumper {
    private static final Logger LOG = LogManager.getLogger(QueryDumper.class);

    private static final Gson GSON = new GsonBuilder()
            .addSerializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
            .addDeserializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
            .enableComplexMapKeySerialization()
            .disableHtmlEscaping()
            .registerTypeAdapter(QueryDumpInfo.class, new QueryDumpSerializer())
            .registerTypeAdapter(QueryDumpInfo.class, new QueryDumpDeserializer())
            .create();

    public static Pair<HttpResponseStatus, String> dumpQuery(String catalogName, String dbName, String query,
                                                             boolean enableMock) {
        ConnectContext context = ConnectContext.get();
        if (context == null) {
            return Pair.create(HttpResponseStatus.BAD_REQUEST,
                    "There is no ConnectContext for this thread: " + Thread.currentThread().getName());
        }

        final String prevCatalog = context.getCurrentCatalog();
        final String prevDb = context.getDatabase();
        final boolean prevIsHTTPQueryDump = context.isHTTPQueryDump();

        try {
            if (StringUtils.isEmpty(query)) {
                return Pair.create(HttpResponseStatus.BAD_REQUEST, "query is empty");
            }

            if (!StringUtils.isEmpty(catalogName)) {
                context.setCurrentCatalog(catalogName);
            }

            if (!StringUtils.isEmpty(dbName)) {
                Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, catalogName, dbName);
                if (db == null) {
                    return Pair.create(HttpResponseStatus.NOT_FOUND,
                            String.format("Database [%s.%s] does not exists", catalogName, dbName));
                }
                context.setDatabase(db.getFullName());
            }

            context.setIsHTTPQueryDump(true);

            StatementBase parsedStmt;
            try {
                parsedStmt = SqlParser.parse(query, context.getSessionVariable()).get(0);
                StmtExecutor executor = new StmtExecutor(context, parsedStmt);
                executor.execute();
            } catch (Exception e) {
                LOG.warn("execute query failed. ", e);
                return Pair.create(HttpResponseStatus.BAD_REQUEST, "execute query failed. " + e.getMessage());
            }

            DumpInfo dumpInfo = context.getDumpInfo();
            if (dumpInfo != null) {
                dumpInfo.setDesensitizedInfo(enableMock);
                String dumpStr = GSON.toJson(dumpInfo, QueryDumpInfo.class);
                return Pair.create(HttpResponseStatus.OK, dumpStr);
            } else {
                return Pair.create(HttpResponseStatus.BAD_REQUEST, "not use cbo planner, try again.");
            }
        } finally {
            context.setCurrentCatalog(prevCatalog);
            context.setDatabase(prevDb);
            context.setIsHTTPQueryDump(prevIsHTTPQueryDump);
        }
    }
}
