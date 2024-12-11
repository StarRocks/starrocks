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

package com.starrocks.privilege.ranger.hive;

import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.privilege.AccessDeniedException;
<<<<<<< HEAD
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.RangerAccessController;
=======
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.ranger.RangerAccessController;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class RangerHiveAccessController extends RangerAccessController {
<<<<<<< HEAD

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    public RangerHiveAccessController(String serviceName) {
        super("hive", serviceName);
    }

    @Override
    public void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
<<<<<<< HEAD
                              PrivilegeType privilegeType) {
        if (!hasPermission(RangerHiveResource.builder()
                        .setDatabase(db)
                        .build(),
                currentUser,
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.DATABASE, db);
        }
    }

    @Override
    public void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db) {
        if (!hasPermission(RangerHiveResource.builder()
                        .setDatabase(db)
                        .build(),
                currentUser,
                PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.DATABASE, db);
        }
    }

    @Override
    public void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType) {
        if (!hasPermission(RangerHiveResource.builder()
=======
                              PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(RangerHiveResource.builder()
                        .setDatabase(db)
                        .build(),
                currentUser,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db)
            throws AccessDeniedException {
        hasPermission(RangerHiveResource.builder()
                        .setDatabase(db)
                        .build(),
                currentUser,
                PrivilegeType.ANY);
    }

    @Override
    public void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(RangerHiveResource.builder()
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                currentUser,
<<<<<<< HEAD
                privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.TABLE, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        if (!hasPermission(RangerHiveResource.builder()
=======
                privilegeType);
    }

    @Override
    public void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName)
            throws AccessDeniedException {
        hasPermission(RangerHiveResource.builder()
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                currentUser,
<<<<<<< HEAD
                PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.TABLE, tableName.getTbl());
        }
=======
                PrivilegeType.ANY);
    }

    @Override
    public void checkColumnAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName,
                                  String column, PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(RangerHiveResource.builder()
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .setColumn(column)
                        .build(),
                currentUser, privilegeType);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName, List<Column> columns) {
        Map<String, Expr> maskingExprMap = Maps.newHashMap();
        for (Column column : columns) {
            Expr columnMaskingExpression = getColumnMaskingExpression(RangerHiveResource.builder()
                    .setDatabase(tableName.getDb())
                    .setTable(tableName.getTbl())
                    .setColumn(column.getName())
                    .build(), column, context);
            if (columnMaskingExpression != null) {
                maskingExprMap.put(column.getName(), columnMaskingExpression);
            }
        }

        return maskingExprMap;
    }

    @Override
    public Expr getRowAccessPolicy(ConnectContext context, TableName tableName) {
        return getRowAccessExpression(RangerHiveResource.builder()
                .setDatabase(tableName.getDb())
                .setTable(tableName.getTbl())
                .build(), context);
    }

    @Override
    public String convertToAccessType(PrivilegeType privilegeType) {
        HiveAccessType hiveAccessType;
        if (privilegeType == PrivilegeType.SELECT) {
            hiveAccessType = HiveAccessType.SELECT;
<<<<<<< HEAD
=======
        } else if (privilegeType == PrivilegeType.INSERT) {
            hiveAccessType = HiveAccessType.UPDATE;
        } else if (privilegeType == PrivilegeType.CREATE_DATABASE
                || privilegeType == PrivilegeType.CREATE_TABLE
                || privilegeType == PrivilegeType.CREATE_VIEW) {
            hiveAccessType = HiveAccessType.CREATE;
        } else if (privilegeType == PrivilegeType.DROP) {
            hiveAccessType = HiveAccessType.DROP;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        } else {
            hiveAccessType = HiveAccessType.NONE;
        }

        return hiveAccessType.name().toLowerCase(Locale.ENGLISH);
    }
}
<<<<<<< HEAD
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
