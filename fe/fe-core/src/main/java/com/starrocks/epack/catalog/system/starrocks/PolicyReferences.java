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

package com.starrocks.epack.catalog.system.starrocks;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.epack.catalog.system.SystemIdEPack;
import com.starrocks.epack.privilege.MaskingPolicyContext;
import com.starrocks.epack.privilege.MaskingPolicyContext;
import com.starrocks.epack.privilege.Policy;
import com.starrocks.epack.privilege.PolicyAppliedContext;
import com.starrocks.epack.privilege.RowAccessPolicyContext;
import com.starrocks.epack.privilege.SecurityPolicyMgr;
import com.starrocks.epack.privilege.TableUID;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetPolicyReferenceItem;
import com.starrocks.thrift.TGetPolicyReferenceResponse;
import com.starrocks.thrift.TGetPolicyReferencesRequest;
import com.starrocks.thrift.TSchemaTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class PolicyReferences {

    private static final Logger LOG = LogManager.getLogger(PolicyReferences.class);

    public static SystemTable createPolicyReferences() {
        return new SystemTable(SystemIdEPack.POLICY_REFERENCES_ID, "policy_references", Table.TableType.SCHEMA,
                builder()
                        .column("POLICY_DATABASE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("POLICY_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("POLICY_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))

                        .column("REF_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("REF_DATABASE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("REF_OBJECT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("REF_COLUMN", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(),
                TSchemaTableType.STARROCKS_POLICY_REFERENCES);
    }

    public static TGetPolicyReferenceResponse getPolicyReference(TGetPolicyReferencesRequest request) {
        SecurityPolicyMgr securityPolicyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        ConcurrentMap<TableUID, PolicyAppliedContext> policyContextConcurrentMap =
                securityPolicyManager.getPolicyContextMap();

        TGetPolicyReferenceResponse response = new TGetPolicyReferenceResponse();

        for (Map.Entry<TableUID, PolicyAppliedContext> entry : policyContextConcurrentMap.entrySet()) {
            TableName tableName = entry.getKey().toTableName();
            if (tableName == null) {
                continue;
            }
            
            PolicyAppliedContext policyContext = entry.getValue();
            for (Map.Entry<String, MaskingPolicyContext> maskingPolicyContextEntry
                    : policyContext.getMaskingPolicyApply().entrySet()) {

                MaskingPolicyContext withColumnMaskingPolicy = maskingPolicyContextEntry.getValue();
                Policy policy = securityPolicyManager.getPolicyById(withColumnMaskingPolicy.getPolicyId());
                TGetPolicyReferenceItem policyReferenceItem = new TGetPolicyReferenceItem();
                policyReferenceItem.setPolicy_database(policy.getDbUID().toDbName().getDb());
                policyReferenceItem.setPolicy_name(policy.getName());
                policyReferenceItem.setPolicy_type("Column Masking");

                policyReferenceItem.setRef_catalog(tableName.getCatalog());
                policyReferenceItem.setRef_database(tableName.getDb());
                policyReferenceItem.setRef_object_name(tableName.getTbl());
                policyReferenceItem.setRef_column(maskingPolicyContextEntry.getKey());
                response.addToPolicy_reference(policyReferenceItem);
            }

            for (RowAccessPolicyContext withRowAccessPolicy : policyContext.getRowAccessPolicyApply()) {
                Policy policy = securityPolicyManager.getPolicyById(withRowAccessPolicy.getPolicyId());
                TGetPolicyReferenceItem policyReferenceItem = new TGetPolicyReferenceItem();
                policyReferenceItem.setPolicy_database(policy.getDbUID().toDbName().getDb());
                policyReferenceItem.setPolicy_name(policy.getName());
                policyReferenceItem.setPolicy_type("Row Access");

                policyReferenceItem.setRef_catalog(tableName.getCatalog());
                policyReferenceItem.setRef_database(tableName.getDb());
                policyReferenceItem.setRef_object_name(tableName.getTbl());
                response.addToPolicy_reference(policyReferenceItem);
            }
        }

        return response;
    }
}
