// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.common.DdlException;
import com.starrocks.epack.persist.CreatePolicyLog;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.server.GlobalStateMgr;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class SecurityPolicyManager {
    @SerializedName(value = "idToPolicy")
    private Map<Long, Policy> idToPolicy;
    private final Map<DbUID, Map<String, Policy>> nameToMaskingPolicy;
    private final Map<DbUID, Map<String, Policy>> nameToRowAccessPolicy;
    private final ReentrantReadWriteLock policyLock;

    public SecurityPolicyManager() {
        idToPolicy = new HashMap<>();
        nameToMaskingPolicy = new HashMap<>();
        nameToRowAccessPolicy = new HashMap<>();
        policyLock = new ReentrantReadWriteLock();
    }

    public void createMaskingPolicy(CreatePolicyStmt stmt) throws DdlException {
        long policyId = GlobalStateMgr.getCurrentState().getNextId();
        String policyName = stmt.getPolicyName().getName();

        Map<DbUID, Map<String, Policy>> nameToPolicy;
        if (stmt.getPolicyType().equals(PolicyType.COLUMN_MASKING)) {
            nameToPolicy = nameToMaskingPolicy;
        } else {
            nameToPolicy = nameToRowAccessPolicy;
        }

        policyLock.writeLock().lock();
        try {

            DbUID dbUID = new DbUID(stmt.getPolicyName().getCatalog(), stmt.getPolicyName().getDbName());
            Policy policy = new Policy(stmt.getPolicyType(),
                    policyId, policyName,
                    dbUID,
                    stmt.getArgNames(),
                    stmt.getArgTypeDefs().stream().map(TypeDef::getType).collect(Collectors.toList()),
                    stmt.getReturnType().getType(),
                    stmt.getExpression(),
                    stmt.getComment());

            if (nameToPolicy.containsKey(dbUID)) {
                Map<String, Policy> polices = nameToPolicy.get(dbUID);

                if (polices.containsKey(policyName)) {
                    Policy p = polices.get(policyName);
                    if (stmt.isReplaceIfExists()) {
                        //TODO: support drop policy
                        //doDropPolicyUnlock(stmt.getPolicyType(), dbPEntryObject, policyName, p.getPolicyId(), false);
                    } else if (!stmt.isIfNotExists()) {
                        throw new DdlException("Policy " + policyName + " has exist");
                    }
                }
                polices.put(stmt.getPolicyName().getName(), policy);
            } else {
                Map<String, Policy> polices = new HashMap<>();
                polices.put(stmt.getPolicyName().getName(), policy);
                nameToPolicy.put(dbUID, polices);
            }

            idToPolicy.put(policyId, policy);
            if (stmt.getPolicyType().equals(PolicyType.COLUMN_MASKING)) {
                GlobalStateMgr.getCurrentState().getEditLog().logCreateMaskingPolicy(policy);
            } else {
                GlobalStateMgr.getCurrentState().getEditLog().logCreateRowAccessPolicy(policy);
            }
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    public void replayCreatePolicy(CreatePolicyLog createPolicyInfo) {
        Map<DbUID, Map<String, Policy>> nameToPolicy;
        if (createPolicyInfo.getPolicyType().equals(PolicyType.COLUMN_MASKING)) {
            nameToPolicy = nameToMaskingPolicy;
        } else {
            nameToPolicy = nameToRowAccessPolicy;
        }

        policyLock.writeLock().lock();
        try {
            DbUID dbUID = createPolicyInfo.getDbPEntryObject();

            Policy policy = new Policy(
                    createPolicyInfo.getPolicyType(),
                    createPolicyInfo.getPolicyId(),
                    createPolicyInfo.getName(),
                    createPolicyInfo.getDbPEntryObject(),
                    createPolicyInfo.getArgNames(),
                    createPolicyInfo.getArgTypes(),
                    createPolicyInfo.getRetType(),
                    createPolicyInfo.getPolicyExpression(),
                    createPolicyInfo.getComment());

            idToPolicy.put(policy.getPolicyId(), policy);

            if (nameToPolicy.containsKey(dbUID)) {
                Map<String, Policy> polices = nameToPolicy.get(dbUID);

                if (polices.containsKey(createPolicyInfo.getName())) {
                    Policy p = polices.get(createPolicyInfo.getName());
                    idToPolicy.remove(p.getPolicyId());
                }

                polices.put(createPolicyInfo.getName(), policy);
            } else {
                Map<String, Policy> polices = new HashMap<>();
                polices.put(createPolicyInfo.getName(), policy);
                nameToPolicy.put(dbUID, polices);
            }

        } finally {
            policyLock.writeLock().unlock();
        }
    }

    public Policy getPolicyByName(PolicyType policyType, PolicyName policyName) {
        policyLock.readLock().lock();
        try {
            DbUID dbUID = new DbUID(policyName.getCatalog(), policyName.getDbName());
            if (policyType.equals(PolicyType.COLUMN_MASKING)) {
                Map<String, Policy> policies = nameToMaskingPolicy.get(dbUID);
                if (policies == null) {
                    return null;
                } else {
                    return policies.get(policyName.getName());
                }
            } else {
                Map<String, Policy> policies = nameToRowAccessPolicy.get(dbUID);
                if (policies == null) {
                    return null;
                } else {
                    return policies.get(policyName.getName());
                }
            }
        } finally {
            policyLock.readLock().unlock();
        }
    }

    public Map<String, Policy> getNameToPolicy(String catalog, String dbName, PolicyType policyType) {
        policyLock.readLock().lock();
        try {
            DbUID dbPEntryObject = new DbUID(catalog, dbName);
            if (policyType.equals(PolicyType.COLUMN_MASKING)) {
                return nameToMaskingPolicy.get(dbPEntryObject);
            } else {
                return nameToRowAccessPolicy.get(dbPEntryObject);
            }
        } finally {
            policyLock.readLock().unlock();
        }
    }
}
