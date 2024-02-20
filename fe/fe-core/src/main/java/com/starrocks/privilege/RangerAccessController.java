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
package com.starrocks.privilege;

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.common.Config;
import com.starrocks.privilege.ranger.AccessTypeConverter;
import com.starrocks.privilege.ranger.RangerStarRocksAccessRequest;
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
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.util.Locale.ENGLISH;

public abstract class RangerAccessController implements AccessController, AccessTypeConverter {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAccessController.class);
    protected final RangerBasePlugin rangerPlugin;

    public RangerAccessController(String serviceType, String serviceName) {
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

        LOG.info("Start Ranger plugin ({} - {}) success",
                rangerPluginContext.getServiceType(), rangerPluginContext.getServiceName());
    }

    public Expr getColumnMaskingExpression(RangerAccessResourceImpl resource, Column column, ConnectContext context) {
        RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(
                resource, context.getCurrentUserIdentity(), PrivilegeType.SELECT.name().toLowerCase(ENGLISH));

        RangerAccessResult result = rangerPlugin.evalDataMaskPolicies(request, null);
        if (result != null && result.isMaskEnabled()) {
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

            return SqlParser.parseSqlToExpr(transformer, context.getSessionVariable().getSqlMode());
        }

        return null;
    }

    protected Expr getRowAccessExpression(RangerAccessResourceImpl resource, ConnectContext context) {
        RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(
                resource, context.getCurrentUserIdentity(), PrivilegeType.SELECT.name().toLowerCase(ENGLISH));
        RangerAccessResult result = rangerPlugin.evalRowFilterPolicies(request, null);
        if (result != null && result.isRowFilterEnabled()) {
            return SqlParser.parseSqlToExpr(result.getFilterExpr(), context.getSessionVariable().getSqlMode());
        } else {
            return null;
        }
    }

    protected boolean hasPermission(RangerAccessResourceImpl resource, UserIdentity user, PrivilegeType privilegeType)
            throws AccessDeniedException {
        String accessType;
        if (privilegeType.equals(PrivilegeType.ANY)) {
            accessType = RangerPolicyEngine.ANY_ACCESS;
        } else {
            accessType = convertToAccessType(privilegeType);
        }

        RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(resource, user, accessType);
        RangerAccessResult result = rangerPlugin.isAccessAllowed(request);
        if (result == null || !result.getIsAllowed()) {
            return false;
        } else {
            return true;
        }
    }
}
