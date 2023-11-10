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

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Get meta info with privilege checking
 */
public class MetaInfoService {

    public static class GetDatabasesAction extends RestBaseAction {

        public GetDatabasesAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            GetDatabasesAction action = new GetDatabasesAction(controller);
            controller.registerHandler(HttpMethod.GET, "/api/v1/databases", action);
            controller.registerHandler(HttpMethod.GET, "/api/v1/catalogs/{" + CATALOG_KEY + "}/databases", action);
        }

        @Override
        public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
            String catalogName = request.getSingleParameter(CATALOG_KEY);
            catalogName = Optional.ofNullable(catalogName).orElse(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            if (!catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                        "only support default_catalog right now");
            }
            List<String> dbNames = GlobalStateMgr.getCurrentState().getDbNames();

            // handle limit offset
            Pair<Integer, Integer> fromToIndex = getFromToIndex(request);
            String finalCatalogName = catalogName;
            List<String> resultDbNameSet = dbNames.stream()
                    .map(fullName -> {
                        final String db = ClusterNamespace.getNameFromFullName(fullName);
                        try {
                            Authorizer.checkAnyActionOnOrInDb(ConnectContext.get().getCurrentUserIdentity(),
                                    ConnectContext.get().getCurrentRoleIds(), finalCatalogName, db);
                        } catch (Exception e) {
                            return null;
                        }
                        return db;
                    })
                    .filter(Objects::nonNull)
                    .sorted()
                    .skip(fromToIndex.first)
                    .limit(fromToIndex.second)
                    .collect(Collectors.toList());

            sendResult(request, response, new DatabasesResult(resultDbNameSet, resultDbNameSet.size()));
        }
    }

    public static class GetTablesAction extends RestBaseAction {

        public GetTablesAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            GetTablesAction action = new GetTablesAction(controller);
            controller.registerHandler(HttpMethod.GET, "/api/v1/databases/{" + DB_KEY + "}/tables", action);
            controller.registerHandler(HttpMethod.GET,
                    "/api/v1/catalogs/{" + CATALOG_KEY + "}/databases/{" + DB_KEY + "}/tables", action);

        }

        @Override
        public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
            String catalogName = request.getSingleParameter(CATALOG_KEY);
            catalogName = Optional.ofNullable(catalogName).orElse(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            if (!catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                        "only support default_catalog right now");
            }
            String dbName = request.getSingleParameter(DB_KEY);

            if (Strings.isNullOrEmpty(dbName)) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "{database} must be selected");
            }
            Authorizer.checkAnyActionOnOrInDb(ConnectContext.get().getCurrentUserIdentity(), ConnectContext.get()
                    .getCurrentRoleIds(), catalogName, dbName);
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db == null) {
                throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                        "Database [" + dbName + "] " + "does not exists");
            }

            // handle limit offset
            Pair<Integer, Integer> fromToIndex = getFromToIndex(request);
            List<String> resultTblNames = db.getTables().stream()
                    .map(table -> {
                        try {
                            Authorizer.checkAnyActionOnTable(ConnectContext.get().getCurrentUserIdentity(),
                                    ConnectContext.get().getCurrentRoleIds(),
                                    new TableName(db.getFullName(), table.getName()));
                        } catch (AccessDeniedException e) {
                            return null;
                        }
                        return table.getName();
                    })
                    .filter(Objects::nonNull)
                    .skip(fromToIndex.first)
                    .limit(fromToIndex.second)
                    .collect(Collectors.toList());

            sendResult(request, response, new TablesResult(dbName, resultTblNames, resultTblNames.size()));
        }

    }

    // get limit and offset from query parameter
    // and return fromIndex and toIndex of a list
    private static Pair<Integer, Integer> getFromToIndex(BaseRequest request) {
        String limitStr = request.getSingleParameter("limit");
        String offsetStr = request.getSingleParameter("offset");

        int offset = 0;
        int limit = Integer.MAX_VALUE;

        limit = !StringUtils.isEmpty(limitStr) ? Integer.parseInt(limitStr) : limit;
        offset = !StringUtils.isEmpty(offsetStr) ? Integer.parseInt(offsetStr) : offset;

        if (limit < 0) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "Param limit should >= 0");
        }
        if (offset < 0) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "Param offset should >= 0");
        }
        if (offset > 0 && limit == Integer.MAX_VALUE) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                    "Param offset should be set with param limit");
        }
        return Pair.create(offset, limit + offset);
    }

    private static class DatabasesResult extends RestBaseResult {
        private List<String> databases;
        private Integer count;

        public DatabasesResult(List<String> databases, Integer count) {
            this.databases = databases;
            this.count = count;
        }
    }

    private static class TablesResult extends RestBaseResult {
        private String database;
        private List<String> tables;
        private Integer tableCount;

        public TablesResult(String database, List<String> tables, Integer tableCount) {
            this.database = database;
            this.tables = tables;
            this.tableCount = tableCount;
        }
    }
}
