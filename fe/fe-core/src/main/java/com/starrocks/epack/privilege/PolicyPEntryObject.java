// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.PEntryObject;
import com.starrocks.privilege.PrivObjNotFoundException;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaNotFoundException;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Objects;

public class PolicyPEntryObject implements PEntryObject {
    @SerializedName(value = "ci")
    private long catalogId;
    @SerializedName(value = "d")
    protected String databaseUUID;
    @SerializedName(value = "t")
    protected PolicyType policyType;
    @SerializedName(value = "i")
    protected long policyId;

    protected PolicyPEntryObject(long catalogId, String databaseUUID, PolicyType policyType, long policyId) {
        this.catalogId = catalogId;
        this.databaseUUID = databaseUUID;
        this.policyType = policyType;
        this.policyId = policyId;
    }

    public String getDatabaseUUID() {
        return databaseUUID;
    }

    public long getPolicyId() {
        return policyId;
    }

    public long getCatalogId() {
        return catalogId;
    }

    public static PolicyPEntryObject generate(GlobalStateMgr mgr, PolicyType policyType, List<String> tokens)
            throws PrivilegeException {
        String catalogName = null;
        long catalogId;

        if (tokens.size() == 3) {
            // This is true only when we are initializing built-in roles like root and db_admin
            if (tokens.get(0).equals("*")) {
                return new PolicyPEntryObject(PrivilegeBuiltinConstants.ALL_CATALOGS_ID,
                        PrivilegeBuiltinConstants.ALL_DATABASES_UUID,
                        policyType,
                        PrivilegeBuiltinConstantsEPack.ALL_POLICY_ID);
            }
            catalogName = tokens.get(0);

            // It's very trick here.
            // Because of the historical legacy code, create external table belongs to internal catalog from the grammatical level
            // . In terms of code implementation, the catalog obtained from TableName turned out to be external catalog!
            // As a result, the authorization grant stmt is authorized according to the internal catalog,
            // but the external catalog is used for authentication.
            if (CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(catalogName)) {
                catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            }

            tokens = tokens.subList(1, tokens.size());
        } else if (tokens.size() != 2) {
            throw new PrivilegeException(
                    "invalid object tokens, should have two or three elements, current: " + tokens);
        }

        // Default to internal_catalog when no catalog explicitly selected.
        if (catalogName == null || CatalogMgr.isInternalCatalog(catalogName)) {
            catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            catalogId = InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
        } else {
            Catalog catalog = mgr.getCatalogMgr().getCatalogByName(catalogName);
            if (catalog == null) {
                throw new PrivObjNotFoundException("cannot find catalog: " + catalogName);
            }
            catalogId = catalog.getId();
        }

        String dbUUID;
        long policyId;

        if (tokens.get(0).equals("*")) {
            dbUUID = PrivilegeBuiltinConstants.ALL_DATABASES_UUID;
            policyId = PrivilegeBuiltinConstantsEPack.ALL_POLICY_ID;
        } else {
            Database database = mgr.getMetadataMgr().getDb(catalogName, tokens.get(0));
            if (database == null) {
                throw new PrivObjNotFoundException("cannot find db: " + tokens.get(0));
            }
            dbUUID = database.getUUID();

            if (tokens.get(1).equals("*")) {
                policyId = PrivilegeBuiltinConstantsEPack.ALL_POLICY_ID;
            } else {
                Policy policy = mgr.getSecurityPolicyManager().getPolicyByName(policyType,
                        new PolicyName(catalogName, database.getFullName(), tokens.get(1), NodePosition.ZERO));
                if (policy == null) {
                    throw new PrivObjNotFoundException("cannot find policy : " + tokens.get(1));
                }
                policyId = policy.getPolicyId();
            }
        }

        return new PolicyPEntryObject(catalogId, dbUUID, policyType, policyId);
    }

    /**
     * if the current table matches other table, including fuzzy matching.
     * <p>
     * this(db1.tbl1), other(db1.tbl1) -> true
     * this(db1.tbl1), other(db1.ALL) -> true
     * this(db1.ALL), other(db1.tbl1) -> false
     */
    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof PolicyPEntryObject)) {
            return false;
        }
        PolicyPEntryObject other = (PolicyPEntryObject) obj;

        if (!other.policyType.equals(this.policyType)) {
            return false;
        }

        if (other.catalogId == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
            return true;
        }
        if (Objects.equals(other.databaseUUID, PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
            return this.catalogId == other.catalogId;
        }
        if (Objects.equals(other.policyId, PrivilegeBuiltinConstantsEPack.ALL_POLICY_ID)) {
            return this.catalogId == other.catalogId && Objects.equals(this.databaseUUID, other.databaseUUID);
        }
        return this.catalogId == other.catalogId &&
                Objects.equals(other.databaseUUID, this.databaseUUID) &&
                Objects.equals(other.policyId, this.policyId);
    }

    @Override
    public boolean isFuzzyMatching() {
        return catalogId == PrivilegeBuiltinConstants.ALL_CATALOGS_ID ||
                Objects.equals(databaseUUID, PrivilegeBuiltinConstants.ALL_DATABASES_UUID) ||
                Objects.equals(policyId, PrivilegeBuiltinConstantsEPack.ALL_POLICY_ID);
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        return globalStateMgr.getSecurityPolicyManager().getPolicyById(this.policyId) != null;
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof PolicyPEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }

        PolicyPEntryObject o = (PolicyPEntryObject) obj;
        if (this.catalogId == o.catalogId) {
            // Always put the fuzzy matching object at the front of the privilege entry list
            // when sorting in ascendant order.
            if (Objects.equals(this.databaseUUID, o.databaseUUID)) {
                if (Objects.equals(this.policyId, o.policyId)) {
                    return 0;
                } else if (Objects.equals(this.policyId, PrivilegeBuiltinConstantsEPack.ALL_POLICY_ID)) {
                    return -1;
                } else if (Objects.equals(o.policyId, PrivilegeBuiltinConstantsEPack.ALL_POLICY_ID)) {
                    return 1;
                } else {
                    return Long.compare(this.policyId, o.policyId);
                }
            } else if (Objects.equals(this.databaseUUID, PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
                return -1;
            } else if (Objects.equals(o.databaseUUID, PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
                return 1;
            } else {
                return this.databaseUUID.compareTo(o.databaseUUID);
            }
        } else if (this.catalogId == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
            return -1;
        } else if (o.catalogId == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
            return 1;
        } else {
            return (int) (this.catalogId - o.catalogId);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PolicyPEntryObject that = (PolicyPEntryObject) o;
        return this.catalogId == that.catalogId &&
                Objects.equals(databaseUUID, that.databaseUUID) &&
                Objects.equals(policyId, that.policyId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogId, databaseUUID, policyId);
    }

    @Override
    public PEntryObject clone() {
        return new PolicyPEntryObject(catalogId, databaseUUID, policyType, policyId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (Objects.equals(databaseUUID, PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
            sb.append("ALL ");
            if (policyType.equals(PolicyType.MASKING)) {
                sb.append(ObjectTypeEPack.MASKING_POLICY.plural());
            } else {
                sb.append(ObjectTypeEPack.ROW_ACCESS_POLICY.plural());
            }
            sb.append(" IN ALL DATABASES");
        } else {
            String dbName;
            Database database;
            if (CatalogMgr.isInternalCatalog(catalogId)) {
                database = GlobalStateMgr.getCurrentState().getDb(Long.parseLong(databaseUUID));
                if (database == null) {
                    throw new MetaNotFoundException("Cannot find database : " + databaseUUID);
                }
                dbName = database.getFullName();
            } else {
                dbName = ExternalCatalog.getDbNameFromUUID(databaseUUID);
            }

            if (Objects.equals(policyId, PrivilegeBuiltinConstantsEPack.ALL_POLICY_ID)) {
                sb.append("ALL ");
                if (policyType.equals(PolicyType.MASKING)) {
                    sb.append(ObjectTypeEPack.MASKING_POLICY.plural());
                } else {
                    sb.append(ObjectTypeEPack.ROW_ACCESS_POLICY.plural());
                }
                sb.append(" IN DATABASE ").append(dbName);
            } else {
                Policy policy = GlobalStateMgr.getCurrentState().getSecurityPolicyManager().getPolicyById(policyId);
                sb.append(dbName).append(".").append(policy.getName());
            }
        }
        return sb.toString();
    }
}
