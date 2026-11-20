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

package com.starrocks.authorization.ranger.hive;

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.ranger.RangerAccessController;
import com.starrocks.authorization.ranger.RangerAccessResourceBuilder;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.Locale;

public class RangerHiveAccessController extends RangerAccessController {
    // Value of RangerHiveCatalogProperties.CATALOG_PREFIX_ENABLED on this catalog; null = follow the FE config.
    private final Boolean catalogPrefixEnabled;

    public RangerHiveAccessController(String serviceName, Boolean catalogPrefixEnabled) {
        super("hive", serviceName);
        this.catalogPrefixEnabled = catalogPrefixEnabled;
    }

    /**
     * Every check goes through here, so the catalog prefix applies to all of them or none.
     * The controller is registered per catalog, so {@code catalogName} is always this catalog.
     */
    private RangerAccessResourceBuilder resourceOf(String catalogName, String db) {
        return RangerHiveResource.builder()
                .setDatabase(RangerHiveCatalogProperties.resolveDatabase(catalogPrefixEnabled, catalogName, db));
    }

    @Override
    public void checkDbAction(ConnectContext context, String catalogName, String db,
                              PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(resourceOf(catalogName, db)
                        .build(),
                context.getCurrentUserIdentity(),
                context.getGroups(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnDb(ConnectContext context, String catalogName, String db)
            throws AccessDeniedException {
        hasPermission(resourceOf(catalogName, db)
                        .build(),
                context.getCurrentUserIdentity(),
                context.getGroups(),
                PrivilegeType.ANY);
    }

    @Override
    public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(resourceOf(tableName.getCatalog(), tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                context.getCurrentUserIdentity(),
                context.getGroups(),
                privilegeType);
    }

    @Override
    public void checkAnyActionOnTable(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        hasPermission(resourceOf(tableName.getCatalog(), tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                context.getCurrentUserIdentity(),
                context.getGroups(),
                PrivilegeType.ANY);
    }

    @Override
    public void checkColumnAction(ConnectContext context, TableName tableName,
                                  String column, PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(resourceOf(tableName.getCatalog(), tableName.getDb())
                        .setTable(tableName.getTbl())
                        .setColumn(column)
                        .build(),
                context.getCurrentUserIdentity(),
                context.getGroups(),
                privilegeType);
    }

    @Override
    protected RangerAccessResourceImpl tableResource(TableName tableName) {
        return resourceOf(tableName.getCatalog(), tableName.getDb())
                .setTable(tableName.getTbl())
                .build();
    }

    @Override
    protected RangerAccessResourceImpl columnResource(TableName tableName, String column) {
        return resourceOf(tableName.getCatalog(), tableName.getDb())
                .setTable(tableName.getTbl())
                .setColumn(column)
                .build();
    }

    @Override
    public String convertToAccessType(PrivilegeType privilegeType) {
        HiveAccessType hiveAccessType;
        if (privilegeType == PrivilegeType.SELECT) {
            hiveAccessType = HiveAccessType.SELECT;
        } else if (privilegeType == PrivilegeType.INSERT) {
            hiveAccessType = HiveAccessType.UPDATE;
        } else if (privilegeType == PrivilegeType.CREATE_DATABASE
                || privilegeType == PrivilegeType.CREATE_TABLE
                || privilegeType == PrivilegeType.CREATE_VIEW) {
            hiveAccessType = HiveAccessType.CREATE;
        } else if (privilegeType == PrivilegeType.DROP) {
            hiveAccessType = HiveAccessType.DROP;
        } else if (privilegeType == PrivilegeType.REFRESH) {
            hiveAccessType = HiveAccessType.REFRESH;
        } else if (privilegeType == PrivilegeType.ALTER) {
            hiveAccessType = HiveAccessType.ALTER;
        } else {
            hiveAccessType = HiveAccessType.NONE;
        }

        return hiveAccessType.name().toLowerCase(Locale.ENGLISH);
    }
}

