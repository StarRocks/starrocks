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
package com.starrocks.http.rest.v2;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.function.Function;

import static com.starrocks.catalog.InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
import static com.starrocks.sql.analyzer.Authorizer.checkTableAction;

public abstract class TableBaseAction extends RestBaseAction {

    protected static final Logger LOG = LogManager.getLogger(TableBaseAction.class);

    public TableBaseAction(ActionController controller) {
        super(controller);
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request,
                                          BaseResponse response) throws DdlException, AccessDeniedException {
        try {
            this.doExecuteWithoutPassword(request, response);
        } catch (StarRocksHttpException e) {
            HttpResponseStatus status = e.getCode();
            sendResult(request, response, status, new RestBaseResultV2<>(status.code(), e.getMessage()));
        } catch (Exception e) {
            LOG.error("Get table[{}/{}/{}] meta info error.",
                    request.getSingleParameter(CATALOG_KEY),
                    request.getSingleParameter(DB_KEY),
                    request.getSingleParameter(TABLE_KEY),
                    e);
            HttpResponseStatus status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            sendResult(request, response, status, new RestBaseResultV2<>(status.code(), e.getMessage()));
        }
    }

    protected abstract void doExecuteWithoutPassword(
            BaseRequest request, BaseResponse response) throws AccessDeniedException;

    /**
     * Get {@link OlapTable} object by name, and apply it with {@code tableFunction}
     *
     * @throws AccessDeniedException If the user does not have {@code SELECT} permission on the table.
     */
    protected <T> T getAndApplyOlapTable(String catalogName,
                                         String dbName,
                                         String tableName,
                                         Function<OlapTable, T> tableFunction) throws AccessDeniedException {
        // check privilege for select, otherwise return 401 HTTP status
        checkTableAction(
                ConnectContext.get().getCurrentUserIdentity(),
                ConnectContext.get().getCurrentRoleIds(),
                catalogName,
                dbName,
                tableName,
                PrivilegeType.SELECT
        );

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (null == db || !Optional.ofNullable(db.getCatalogName())
                .orElse(DEFAULT_INTERNAL_CATALOG_NAME).equalsIgnoreCase(catalogName)) {
            throw new StarRocksHttpException(
                    HttpResponseStatus.NOT_FOUND, ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(dbName)
            );
        }

        final Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
            if (null == table) {
                throw new StarRocksHttpException(
                        HttpResponseStatus.NOT_FOUND, ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(tableName)
                );
            }

            // just only support OlapTable currently
            if (!(table instanceof OlapTable)) {
                throw new StarRocksHttpException(
                        HttpResponseStatus.FORBIDDEN, ErrorCode.ERR_NOT_OLAP_TABLE.formatErrorMsg(tableName)
                );
            }

            return tableFunction.apply((OlapTable) table);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
    }

}
