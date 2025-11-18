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

package com.starrocks.authorization;

import com.aliyun.datalake.auth.AuthClient;
import com.aliyun.datalake.auth.DlfAuth;
import com.aliyun.datalake.auth.Principal;
import com.aliyun.datalake.auth.request.CheckPermissionsRequest;
import com.aliyun.datalake.auth.resource.DatabaseResource;
import com.aliyun.datalake.auth.resource.MetaResource;
import com.aliyun.datalake.auth.resource.PrivilegeResource;
import com.aliyun.datalake.auth.resource.TableResource;
import com.aliyun.datalake.auth.result.CheckPermissionsResult;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.common.util.DlfUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static com.aliyun.datalake.core.constant.DataLakeConfig.CATALOG_ID;
import static com.aliyun.datalake.core.constant.DataLakeConfig.DLF_ENDPOINT;
import static com.aliyun.datalake.core.constant.DataLakeConfig.DLF_REGION;
import static com.aliyun.datalake.core.constant.DataLakeConfig.META_CREDENTIAL_PROVIDER;
import static com.aliyun.datalake.core.constant.DataLakeConfig.META_CREDENTIAL_PROVIDER_URL;

public class DlfAccessController extends ExternalAccessController implements AccessController {
    private static final Logger LOG = LogManager.getLogger(DlfAccessController.class);
    private final Map<String, String> options;
    
    public DlfAccessController(Map<String, String> options) {
        this.options = options;
    }

    @Override
    public void checkDbAction(ConnectContext context, String catalogName, String db,
                              PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(new PrivilegeResource()
                        .setAccess(convertToAccessType(privilegeType))
                        .setMetaResource(new MetaResource().setResourceType("DATABASE")
                                .setDatabaseResource(new DatabaseResource().setDatabaseName(db))),
                context.getCurrentUserIdentity());
    }

    @Override
    public void checkAnyActionOnDb(ConnectContext context, String catalogName, String db)
            throws AccessDeniedException {
        hasPermission(new PrivilegeResource()
                        .setAccess("DESCRIBE")
                        .setMetaResource(new MetaResource().setResourceType("DATABASE")
                                .setDatabaseResource(new DatabaseResource().setDatabaseName(db))),
                context.getCurrentUserIdentity());
    }

    @Override
    public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        tableName.setTbl(tableName.getTbl().replaceAll("\\$.*", ""));
        hasPermission(new PrivilegeResource()
                        .setAccess(convertToAccessType(privilegeType))
                        .setMetaResource(new MetaResource().setResourceType("TABLE")
                                .setTableResource(new TableResource()
                                        .setDatabaseName(tableName.getDb())
                                        .setTableName(tableName.getTbl()))),
                context.getCurrentUserIdentity());
    }

    @Override
    public void checkAnyActionOnTable(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        tableName.setTbl(tableName.getTbl().replaceAll("\\$.*", ""));
        hasPermission(new PrivilegeResource()
                        .setAccess("DESCRIBE")
                        .setMetaResource(new MetaResource().setResourceType("TABLE")
                                .setTableResource(new TableResource()
                                        .setDatabaseName(tableName.getDb())
                                        .setTableName(tableName.getTbl()))),
                context.getCurrentUserIdentity());
    }

    @Override
    public void checkColumnAction(ConnectContext context, TableName tableName,
                                  String column, PrivilegeType privilegeType) throws AccessDeniedException {
        // DLF does not support column level permission check
        checkTableAction(context, tableName, privilegeType);
    }

    // @Override
    // public Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName, List<Column> columns) {
    //     Map<String, Expr> maskingExprMap = Maps.newHashMap();
    //     for (Column column : columns) {
    //         Expr columnMaskingExpression = getColumnMaskingExpression(RangerHiveResource.builder()
    //                 .setDatabase(tableName.getDb())
    //                 .setTable(tableName.getTbl())
    //                 .setColumn(column.getName())
    //                 .build(), column, context);
    //         if (columnMaskingExpression != null) {
    //             maskingExprMap.put(column.getName(), columnMaskingExpression);
    //         }
    //     }
    //
    //     return maskingExprMap;
    // }

    @Override
    public Expr getRowAccessPolicy(ConnectContext context, TableName tableName) {
        return null;
    }


    private void hasPermission(PrivilegeResource privilegeResource, UserIdentity user)
            throws AccessDeniedException {
        Configuration conf = DlfUtil.readHadoopConf();

        Properties properties = new Properties();
        properties.put(DLF_ENDPOINT, options.containsKey(DLF_ENDPOINT)
                ? options.get(DLF_ENDPOINT) : conf.get(DLF_ENDPOINT));
        properties.put(DLF_REGION, options.containsKey(DLF_REGION)
                ? options.get(DLF_REGION) : conf.get(DLF_REGION));
        properties.put(META_CREDENTIAL_PROVIDER, options.containsKey(META_CREDENTIAL_PROVIDER)
                ? options.get(META_CREDENTIAL_PROVIDER) : conf.get(META_CREDENTIAL_PROVIDER));
        properties.put(META_CREDENTIAL_PROVIDER_URL, options.containsKey(META_CREDENTIAL_PROVIDER_URL)
                ? options.get(META_CREDENTIAL_PROVIDER_URL) : conf.get(META_CREDENTIAL_PROVIDER_URL));

        AuthClient dlfAuth = new DlfAuth(properties, "");
        String userName = DlfUtil.getRamUser(user.getUser());
        Principal principal = new Principal().setPrincipalArn(userName);

        CheckPermissionsRequest request = new CheckPermissionsRequest()
                .setExecUser(userName)
                .setCatalogId(this.options.get(CATALOG_ID))
                .setPrincipal(principal)
                .setPrivilegeResources(Collections.singletonList(privilegeResource));

        CheckPermissionsResult result = dlfAuth.checkPermission(request);
        LOG.debug("check acl permission success, result: " + result.getSuccess());
        if (!result.getSuccess()) {
            throw new AccessDeniedException();
        }
    }


    private static String convertToAccessType(PrivilegeType privilegeType) {
        String dlfAccessType;

        // Only select and insert need judge in StarRocks
        if (privilegeType.equals(PrivilegeType.SELECT)) {
            dlfAccessType = "Select";
        } else if (privilegeType.equals(PrivilegeType.INSERT)) {
            dlfAccessType = "Update";
        } else if (privilegeType.equals(PrivilegeType.CREATE_DATABASE)) {
            dlfAccessType = "CreateDatabase";
        } else if (privilegeType.equals(PrivilegeType.CREATE_TABLE)) {
            dlfAccessType = "CreateTable";
        } else if (privilegeType.equals(PrivilegeType.CREATE_FUNCTION)) {
            dlfAccessType = "CreateFunction";
        } else if (privilegeType.equals(PrivilegeType.DROP)) {
            dlfAccessType = "Drop";
        } else if (privilegeType.equals(PrivilegeType.ALTER)) {
            dlfAccessType = "Alter";
        } else {
            dlfAccessType = "Allowed";
        }
        return dlfAccessType;
    }

}
