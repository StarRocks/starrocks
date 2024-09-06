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

package com.starrocks.catalog.system.sys;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TObjectDependencyItem;
import com.starrocks.thrift.TObjectDependencyReq;
import com.starrocks.thrift.TObjectDependencyRes;
import com.starrocks.thrift.TSchemaTableType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Optional;

public class SysObjectDependencies {

    public static final String NAME = "object_dependencies";

    private static final Logger LOG = LogManager.getLogger(SysObjectDependencies.class);


    public static SystemTable create() {
        return new SystemTable(SystemId.OBJECT_DEPENDENCIES, NAME, Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("object_id", ScalarType.BIGINT)
                        .column("object_name", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("object_database", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("object_catalog", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("object_type", ScalarType.createVarcharType(64))

                        .column("ref_object_id", ScalarType.BIGINT)
                        .column("ref_object_name", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("ref_object_database", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("ref_object_catalog", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("ref_object_type", ScalarType.createVarcharType(64))
                        .build(),
                TSchemaTableType.STARROCKS_OBJECT_DEPENDENCIES);
    }

    public static TObjectDependencyRes listObjectDependencies(TObjectDependencyReq req) {
        TAuthInfo auth = req.getAuth_info();
        TObjectDependencyRes response = new TObjectDependencyRes();

        UserIdentity currentUser;
        if (auth.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(auth.getCurrent_user_ident());
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(auth.getUser(), auth.getUser_ip());
        }

        // list dependencies of mv
        Locker locker = new Locker();
        Collection<Database> dbs = GlobalStateMgr.getCurrentState().getLocalMetastore().getFullNameToDb().values();
        for (Database db : CollectionUtils.emptyIfNull(dbs)) {
            String catalog = Optional.ofNullable(db.getCatalogName())
                    .orElse(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                    // If it is not a materialized view, we do not need to verify permissions
                    if (!table.isMaterializedView()) {
                        continue;
                    }
                    // Only show tables with privilege
                    try {
                        Authorizer.checkAnyActionOnTableLikeObject(currentUser, null, db.getFullName(), table);
                    } catch (AccessDeniedException e) {
                        continue;
                    }

                    MaterializedView mv = (MaterializedView) table;
                    for (BaseTableInfo refObj : CollectionUtils.emptyIfNull(mv.getBaseTableInfos())) {
                        TObjectDependencyItem item = new TObjectDependencyItem();
                        item.setObject_id(mv.getId());
                        item.setObject_name(mv.getName());
                        item.setDatabase(db.getFullName());
                        item.setCatalog(catalog);
                        item.setObject_type(mv.getType().toString());

                        item.setRef_object_id(refObj.getTableId());
                        item.setRef_database(refObj.getDbName());
                        item.setRef_catalog(refObj.getCatalogName());
                        Optional<Table> refTable = MvUtils.getTableWithIdentifier(refObj);
                        item.setRef_object_type(getRefObjectType(refTable, mv.getName()));
                        // If the ref table is dropped/swapped/renamed, the actual info would be inconsistent with
                        // BaseTableInfo, so we use the source-of-truth information
                        if (refTable.isEmpty()) {
                            item.setRef_object_name(refObj.getTableName());
                        } else {
                            item.setRef_object_name(refTable.get().getName());
                        }

                        response.addToItems(item);
                    }
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }

        return response;
    }

    /**
     * We may not be able to obtain the base table information when external catalog is unavailable
     *
     * @param refTable Base table for materialized views
     * @param mvName materialized view name
     * @return base table type
     */
    private static String getRefObjectType(Optional<Table> refTable, String mvName) {
        String refObjType = "UNKNOWN";
        try {
            refObjType = refTable.map(x -> x.getType().toString())
                    .orElse("UNKNOWN");
        } catch (Exception e) {
            LOG.error("can not get table type error, mv name : {}, error-msg : {}",
                    mvName, e.getMessage(), e);
        }
        return refObjType;
    }

}
