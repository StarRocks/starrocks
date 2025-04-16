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
package com.starrocks.authorization.ranger;

import com.starrocks.analysis.Expr;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ExternalAccessController;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.lang.StringUtils;
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

import java.util.Objects;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public abstract class RangerAccessController extends ExternalAccessController implements AccessTypeConverter {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAccessController.class);
    protected final RangerBasePlugin rangerPlugin;

    public RangerAccessController(String serviceType, String serviceName) {
        RangerPluginConfig rangerPluginContext = buildRangerPluginContext(serviceType, serviceName);
        rangerPlugin = new RangerBasePlugin(rangerPluginContext);
        rangerPlugin.init(); // this will initialize policy engine and policy refresher
        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());

        LOG.info("Start Ranger plugin ({} - {}) success",
                rangerPluginContext.getServiceType(), rangerPluginContext.getServiceName());
    }

    protected RangerPluginConfig buildRangerPluginContext(String serviceType, String serviceName) {
        LOG.info("Interacting with Ranger Admin Server using SIMPLE authentication");
        return new RangerPluginConfig(serviceType, serviceName, serviceType,
                null, null, null);
    }

    public RangerBasePlugin getRangerPlugin() {
        return rangerPlugin;
    }

    public Expr getColumnMaskingExpression(RangerAccessResourceImpl resource, Column column, ConnectContext context) {
        RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(
                resource, context.getCurrentUserIdentity(), context.getGroups(),
                PrivilegeType.SELECT.name().toLowerCase(ENGLISH));

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
                transformer = Objects.requireNonNullElse(maskedValue, "NULL");
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
                resource, context.getCurrentUserIdentity(), context.getGroups(),
                PrivilegeType.SELECT.name().toLowerCase(ENGLISH));
        RangerAccessResult result = rangerPlugin.evalRowFilterPolicies(request, null);
        if (result != null && result.isRowFilterEnabled()) {
            return SqlParser.parseSqlToExpr(result.getFilterExpr(), context.getSessionVariable().getSqlMode());
        } else {
            return null;
        }
    }

    protected void hasPermission(RangerAccessResourceImpl resource, UserIdentity user, Set<String> groups,
                                 PrivilegeType privilegeType)
            throws AccessDeniedException {
        String accessType;
        if (privilegeType.equals(PrivilegeType.ANY)) {
            accessType = RangerPolicyEngine.ANY_ACCESS;
        } else {
            accessType = convertToAccessType(privilegeType);
        }

        RangerStarRocksAccessRequest request =
                RangerStarRocksAccessRequest.createAccessRequest(resource, user, groups, accessType);
        RangerAccessResult result = rangerPlugin.isAccessAllowed(request);
        if (result == null || !result.getIsAllowed()) {
            throw new AccessDeniedException();
        }
    }
}
