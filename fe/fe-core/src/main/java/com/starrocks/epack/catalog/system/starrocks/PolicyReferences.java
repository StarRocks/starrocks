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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.epack.authorization.DbName;
import com.starrocks.epack.authorization.MaskingPolicyContext;
import com.starrocks.epack.authorization.Policy;
import com.starrocks.epack.authorization.PolicyAppliedContext;
import com.starrocks.epack.authorization.RowAccessPolicyContext;
import com.starrocks.epack.authorization.SecurityPolicyMgr;
import com.starrocks.epack.authorization.TableUID;
import com.starrocks.epack.catalog.system.SystemIdEPack;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetPolicyReferenceItem;
import com.starrocks.thrift.TGetPolicyReferenceResponse;
import com.starrocks.thrift.TGetPolicyReferencesRequest;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.type.TypeFactory;
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
                        .column("POLICY_DATABASE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("POLICY_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("POLICY_TYPE", TypeFactory.createVarcharType(NAME_CHAR_LEN))

                        .column("REF_CATALOG", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("REF_DATABASE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("REF_OBJECT_NAME", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("REF_COLUMN", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .build(),
                TSchemaTableType.STARROCKS_POLICY_REFERENCES);
    }

    public static TGetPolicyReferenceResponse getPolicyReference(TGetPolicyReferencesRequest request) {
        SecurityPolicyMgr securityPolicyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        ConcurrentMap<TableUID, PolicyAppliedContext> policyContextConcurrentMap =
                securityPolicyManager.getPolicyContextMap();

        TGetPolicyReferenceResponse response = new TGetPolicyReferenceResponse();

        for (Map.Entry<TableUID, PolicyAppliedContext> entry : policyContextConcurrentMap.entrySet()) {
            TableUID tableUID = entry.getKey();
            boolean external = tableUID.getCatalogId() != InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
            TableName tableName = null;
            Table table = null;
            try {
                tableName = tableUID.toTableName();
                if (tableName != null) {
                    table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(new ConnectContext(),
                            tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
                }
            } catch (Throwable err) {
                LOG.error("Fail to get table {}", tableUID, err);
            }

            // An external table that was dropped and recreated under the same name still resolves
            // by name to the live table (toTableName/getTableNameFromUUID ignore the create time),
            // but its UID embeds the create time, so the stored UID differs from the live table's UID.
            // Such an entry references a table that no longer exists -> treat it as dangling.
            boolean staleSynonym = external && table != null
                    && !tableUID.getTableUUID().equals(table.getUUID());

            boolean dangling = tableName == null || table == null || staleSynonym;
            String refCatalogName = dangling ? "[NULL]:" + tableUID.getCatalogId() : tableName.getCatalog();
            String refDatabaseName = dangling ? "[NULL]:" + tableUID.getDatabaseUUID() : tableName.getDb();
            // For a live external table, REF_OBJECT_NAME carries the full table UID
            // (catalog.db.table.createTime) so same-name tables stay distinguishable.
            String refObjectName;
            if (dangling) {
                refObjectName = "[NULL]:" + tableUID.getTableUUID();
            } else if (external) {
                refObjectName = tableUID.getTableUUID();
            } else {
                refObjectName = tableName.getTbl();
            }

            PolicyAppliedContext policyContext = entry.getValue();
            for (Map.Entry<ColumnId, MaskingPolicyContext> maskingPolicyContextEntry
                    : policyContext.getMaskingPolicyApply().entrySet()) {

                MaskingPolicyContext withColumnMaskingPolicy = maskingPolicyContextEntry.getValue();
                Policy policy = securityPolicyManager.getPolicyById(withColumnMaskingPolicy.getPolicyId());
                TGetPolicyReferenceItem policyReferenceItem = new TGetPolicyReferenceItem();
                fillPolicyIdentity(policyReferenceItem, policy, withColumnMaskingPolicy.getPolicyId());
                policyReferenceItem.setPolicy_type("Column Masking");

                policyReferenceItem.setRef_catalog(refCatalogName);
                policyReferenceItem.setRef_database(refDatabaseName);
                policyReferenceItem.setRef_object_name(refObjectName);
                ColumnId columnId = maskingPolicyContextEntry.getKey();
                Column col = dangling ? null : table.getColumn(columnId);
                String refColumn = (col != null) ? col.getName() : "[NULL]:" + columnId.getId();
                policyReferenceItem.setRef_column(refColumn);
                response.addToPolicy_reference(policyReferenceItem);
            }

            for (RowAccessPolicyContext withRowAccessPolicy : policyContext.getRowAccessPolicyApply()) {
                Policy policy = securityPolicyManager.getPolicyById(withRowAccessPolicy.getPolicyId());
                TGetPolicyReferenceItem policyReferenceItem = new TGetPolicyReferenceItem();
                fillPolicyIdentity(policyReferenceItem, policy, withRowAccessPolicy.getPolicyId());
                policyReferenceItem.setPolicy_type("Row Access");

                policyReferenceItem.setRef_catalog(refCatalogName);
                policyReferenceItem.setRef_database(refDatabaseName);
                policyReferenceItem.setRef_object_name(refObjectName);
                response.addToPolicy_reference(policyReferenceItem);
            }
        }

        return response;
    }

    // Resolves POLICY_DATABASE / POLICY_NAME defensively. The policy database is derived from external
    // catalog metadata (DbUID.toDbName()), which may return null or throw (e.g. the catalog/db was
    // dropped, or compute-resource acquisition fails) just like the table-resolution path above. Such a
    // failure must degrade to a "[NULL]:" dangling marker instead of escaping the thrift handler and
    // surfacing as "Internal error processing getPolicyReference".
    private static void fillPolicyIdentity(TGetPolicyReferenceItem item, Policy policy, long policyId) {
        if (policy == null) {
            item.setPolicy_database("[NULL]:" + policyId);
            item.setPolicy_name("[NULL]:" + policyId);
            return;
        }

        item.setPolicy_name(policy.getName());

        String policyDatabase = null;
        try {
            DbName dbName = policy.getDbUID().toDbName();
            if (dbName != null) {
                policyDatabase = dbName.getDb();
            }
        } catch (Throwable err) {
            LOG.error("Fail to resolve database for policy {}", policy.getName(), err);
        }
        if (policyDatabase == null) {
            policyDatabase = "[NULL]:" + policy.getDbUID().getUUID();
        }
        item.setPolicy_database(policyDatabase);
    }
}
