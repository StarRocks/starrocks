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
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TObjectDependencyItem;
import com.starrocks.thrift.TObjectDependencyReq;
import com.starrocks.thrift.TObjectDependencyRes;
import com.starrocks.thrift.TSchemaTableType;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.Optional;

public class SysObjectDependencies {

    public static final String NAME = "object_dependencies";

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
        Collection<Database> dbs = GlobalStateMgr.getCurrentState().getFullNameToDb().values();
        for (Database db : CollectionUtils.emptyIfNull(dbs)) {
            String catalog = Optional.ofNullable(db.getCatalogName())
                    .orElse(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            for (Table table : db.getTables()) {
                // Only show tables with privilege
                try {
                    Authorizer.checkAnyActionOnTableLikeObject(currentUser, null, db.getFullName(), table);
                } catch (AccessDeniedException e) {
                    continue;
                }

                if (table.isMaterializedView()) {
                    MaterializedView mv = (MaterializedView) table;
                    for (BaseTableInfo refObj : CollectionUtils.emptyIfNull(mv.getBaseTableInfos())) {
                        TObjectDependencyItem item = new TObjectDependencyItem();
                        item.setObject_id(mv.getId());
                        item.setObject_name(mv.getName());
                        item.setDatabase(db.getFullName());
                        item.setCatalog(catalog);
                        item.setObject_type(mv.getType().toString());

                        item.setRef_object_id(refObj.getTableId());
                        item.setRef_object_name(refObj.getTableName());
                        item.setRef_database(refObj.getDbName());
                        item.setRef_catalog(refObj.getCatalogName());
                        item.setRef_object_type(Optional.ofNullable(refObj.getTable())
                                .map(x -> x.getType().toString())
                                .orElse("UNKNOWN"));

                        response.addToItems(item);
                    }
                }
            }
        }

        return response;
    }

}
