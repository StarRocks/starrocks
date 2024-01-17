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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.common.Config;
import com.starrocks.privilege.AccessControl;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.ranger.RangerStarRocksAccessRequest;
import com.starrocks.privilege.ranger.starrocks.RangerStarRocksResource;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public class RangerHiveAccessControl implements AccessControl {
    private static final Logger LOG = LoggerFactory.getLogger(RangerHiveAccessControl.class);
    private final RangerBasePlugin rangerPlugin;

    public RangerHiveAccessControl(String serviceName) {
        String serviceType = "hive";

        String principal = Config.ranger_spnego_kerberos_principal;
        String keyTab = Config.ranger_spnego_kerberos_keytab;
        String krb5 = Config.ranger_kerberos_krb5_conf;

        if (!principal.isEmpty() && !keyTab.isEmpty()) {
            LOG.info("Interacting with Ranger Admin Server using Kerberos authentication");
            if (krb5 != null && !krb5.isEmpty()) {
                LOG.info("Load system property java.security.krb5.conf with path : " + krb5);
                System.setProperty("java.security.krb5.conf", krb5);
            }
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("hadoop.security.authorization", "true");
            hadoopConf.set("hadoop.security.auth_to_local", "DEFAULT");
            hadoopConf.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(hadoopConf);

            try {
                UserGroupInformation.loginUserFromKeytab(principal, keyTab);
            } catch (IOException ioe) {
                LOG.error("Performing kerberos login failed", ioe);
            }
        } else {
            LOG.info("Interacting with Ranger Admin Server using SIMPLE authentication");
        }
        RangerPluginConfig rangerPluginContext = new RangerPluginConfig(serviceType, serviceName, serviceType,
                null, null, null);
        /*
         * Because the jersey version currently used by ranger conflicts with the
         * jersey version used by other packages in starrocks, we temporarily turn
         * off the cookie authentication switch of Kerberos.
         * starrocks access to ranger is a low-frequency operation.
         * */
        rangerPluginContext.setBoolean("ranger.plugin." + serviceType + ".policy.rest.client.cookie.enabled", false);

        rangerPlugin = new RangerBasePlugin(rangerPluginContext);
        rangerPlugin.init(); // this will initialize policy engine and policy refresher
        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());
    }

    @Override
    public void checkDbAction(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db,
                              PrivilegeType privilegeType) {
        RangerHiveResource resource = new RangerHiveResource(ObjectType.DATABASE, Lists.newArrayList(db));
        if (!hasPermission(resource, currentUser, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.DATABASE, db);
        }
    }

    @Override
    public void checkAnyActionOnDb(UserIdentity currentUser, Set<Long> roleIds, String catalogName, String db) {
        RangerHiveResource resource = new RangerHiveResource(ObjectType.DATABASE, Lists.newArrayList(db));
        if (!hasPermission(resource, currentUser, PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.DATABASE, db);
        }
    }

    @Override
    public void checkTableAction(UserIdentity currentUser, Set<Long> roleIds, TableName tableName, PrivilegeType privilegeType) {
        RangerHiveResource resource = new RangerHiveResource(ObjectType.TABLE,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
        if (!hasPermission(resource, currentUser, privilegeType)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectType.TABLE, tableName.getTbl());
        }
    }

    @Override
    public void checkAnyActionOnTable(UserIdentity currentUser, Set<Long> roleIds, TableName tableName) {
        RangerHiveResource resource = new RangerHiveResource(ObjectType.TABLE,
                Lists.newArrayList(tableName.getDb(), tableName.getTbl()));
        if (!hasPermission(resource, currentUser, PrivilegeType.ANY)) {
            AccessDeniedException.reportAccessDenied(PrivilegeType.ANY.name(), ObjectType.TABLE, tableName.getTbl());
        }
    }

    @Override
    public Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName, List<Column> columns) {
        Map<String, Expr> maskingExprMap = Maps.newHashMap();
        for (Column column : columns) {
            RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(
                    new RangerStarRocksResource(tableName.getCatalog(), tableName.getDb(), tableName.getTbl(), column.getName()),
                    context.getCurrentUserIdentity(), PrivilegeType.SELECT.name().toLowerCase(ENGLISH));

            RangerAccessResult result = rangerPlugin.evalDataMaskPolicies(request, null);
            if (result.isMaskEnabled()) {
                String maskType = result.getMaskType();
                RangerServiceDef.RangerDataMaskTypeDef maskTypeDef = result.getMaskTypeDef();
                String transformer = null;

                if (maskTypeDef != null) {
                    transformer = maskTypeDef.getTransformer();
                }

                if (StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_NULL)) {
                    transformer = "NULL";
                } else if (StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_CUSTOM)) {
                    String maskedValue = result.getMaskedValue();

                    if (maskedValue == null) {
                        transformer = "NULL";
                    } else {
                        transformer = maskedValue;
                    }
                }

                if (StringUtils.isNotEmpty(transformer)) {
                    transformer = transformer.replace("{col}", column.getName())
                            .replace("{type}", column.getType().toSql());
                }

                maskingExprMap.put(column.getName(),
                        SqlParser.parseSqlToExpr(transformer, context.getSessionVariable().getSqlMode()));
            }
        }
        return maskingExprMap;
    }

    @Override
    public Expr getRowAccessPolicy(ConnectContext context, TableName tableName) {
        RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(
                new RangerStarRocksResource(ObjectType.TABLE,
                        Lists.newArrayList(tableName.getCatalog(), tableName.getDb(), tableName.getTbl())),
                context.getCurrentUserIdentity(), PrivilegeType.SELECT.name().toLowerCase(ENGLISH));
        RangerAccessResult result = rangerPlugin.evalRowFilterPolicies(request, null);
        if (result != null && result.isRowFilterEnabled()) {
            return SqlParser.parseSqlToExpr(result.getFilterExpr(), context.getSessionVariable().getSqlMode());
        } else {
            return null;
        }
    }

    public HiveAccessType convertToAccessType(PrivilegeType privilegeType) {
        if (privilegeType == PrivilegeType.SELECT) {
            return HiveAccessType.SELECT;
        } else {
            return HiveAccessType.NONE;
        }
    }

    private boolean hasPermission(RangerHiveResource resource, UserIdentity user, PrivilegeType privilegeType) {
        String accessType;
        if (privilegeType.equals(PrivilegeType.ANY)) {
            accessType = RangerPolicyEngine.ANY_ACCESS;
        } else {
            HiveAccessType hiveAccessType = convertToAccessType(privilegeType);
            accessType = hiveAccessType.name().toLowerCase(ENGLISH);
        }

        RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(resource, user, accessType);

        RangerAccessResult result = rangerPlugin.isAccessAllowed(request);
        if (result != null && result.getIsAllowed()) {
            return true;
        } else {
            return false;
        }
    }
}
