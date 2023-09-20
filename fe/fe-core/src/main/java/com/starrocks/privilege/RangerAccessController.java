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

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Type;
import com.starrocks.privilege.ranger.RangerStarRocksAccessRequest;
import com.starrocks.privilege.ranger.starrocks.RangerStarRocksResource;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import static java.util.Locale.ENGLISH;

public abstract class RangerAccessController extends ExternalAccessController {
    protected final RangerBasePlugin rangerPlugin;

    public RangerAccessController(String serviceType, String serviceName) {
        rangerPlugin = new RangerBasePlugin(serviceType, serviceName, serviceType);
        rangerPlugin.init(); // this will initialize policy engine and policy refresher
        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());
    }

    @Override
    public Expr getColumnMaskingPolicy(ConnectContext currentUser, TableName tableName, String columnName, Type type) {
        RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(
                new RangerStarRocksResource(tableName.getCatalog(), tableName.getDb(), tableName.getTbl(), columnName),
                currentUser.getCurrentUserIdentity(), PrivilegeType.SELECT.name().toLowerCase(ENGLISH));

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
                transformer = transformer.replace("{col}", columnName).replace("{type}", type.toSql());
            }

            return SqlParser.parseSqlToExpr(transformer, currentUser.getSessionVariable().getSqlMode());
        } else {
            return null;
        }
    }

    @Override
    public Expr getRowAccessPolicy(ConnectContext currentUser, TableName tableName) {
        RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(
                new RangerStarRocksResource(ObjectType.TABLE,
                        Lists.newArrayList(tableName.getCatalog(), tableName.getDb(), tableName.getTbl())),
                currentUser.getCurrentUserIdentity(), PrivilegeType.SELECT.name().toLowerCase(ENGLISH));
        RangerAccessResult result = rangerPlugin.evalRowFilterPolicies(request, null);
        if (result != null && result.isRowFilterEnabled()) {
            return SqlParser.parseSqlToExpr(result.getFilterExpr(), currentUser.getSessionVariable().getSqlMode());
        } else {
            return null;
        }
    }
}
