// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TypeDef;
import com.starrocks.common.DdlException;
import com.starrocks.epack.persist.AlterPolicyLog;
import com.starrocks.epack.persist.CreatePolicyLog;
import com.starrocks.epack.persist.DropPolicyLog;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.SqlParser;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class SecurityPolicyMgr {
    @SerializedName(value = "idToPolicy")
    private Map<Long, Policy> idToPolicy;
    private final Map<DbUID, Map<String, Policy>> nameToMaskingPolicy;
    private final Map<DbUID, Map<String, Policy>> nameToRowAccessPolicy;
    private final ReentrantReadWriteLock policyLock;

    public SecurityPolicyMgr() {
        idToPolicy = new HashMap<>();
        nameToMaskingPolicy = new HashMap<>();
        nameToRowAccessPolicy = new HashMap<>();
        policyLock = new ReentrantReadWriteLock();
    }

    public void createMaskingPolicy(CreatePolicyStmt stmt) throws DdlException {
        String policyName = stmt.getPolicyName().getName();
        DbUID dbUID = DbUID.generate(stmt.getPolicyName().getCatalog(), stmt.getPolicyName().getDbName());

        policyLock.writeLock().lock();
        try {
            Policy existsPolicy = getPolicyByNameUnlocked(stmt.getPolicyType(), stmt.getPolicyName(), true);
            if (existsPolicy != null) {
                if (!stmt.isIfNotExists()) {
                    throw new DdlException("Policy " + policyName + " already exists");
                } else {
                    return;
                }
            }

            //Create Policy
            long policyId = GlobalStateMgr.getCurrentState().getNextId();
            Policy policy = new Policy(stmt.getPolicyType(),
                    policyId,
                    policyName,
                    dbUID,
                    stmt.getArgNames(),
                    stmt.getArgTypeDefs().stream().map(TypeDef::getType).collect(Collectors.toList()),
                    stmt.getReturnType().getType(),
                    stmt.getExpression(),
                    stmt.getComment());

            registerPolicy(policy);
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
        policyLock.writeLock().lock();
        try {
            Policy policy = new Policy(createPolicyInfo.getPolicyType(),
                    createPolicyInfo.getPolicyId(),
                    createPolicyInfo.getName(),
                    createPolicyInfo.getDbUID(),
                    createPolicyInfo.getArgNames(),
                    createPolicyInfo.getArgTypes(),
                    createPolicyInfo.getRetType(),
                    createPolicyInfo.getPolicyExpression(),
                    createPolicyInfo.getComment());

            registerPolicy(policy);
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    private void registerPolicy(Policy policy) {
        Map<String, Policy> nameToPolicy = getOrCreateNamePolicyMapByDBUIDUnlocked(policy.getDbUID(), policy.getPolicyType());
        nameToPolicy.put(policy.getName(), policy);
        idToPolicy.put(policy.getPolicyId(), policy);
    }

    public void dropPolicy(DropPolicyStmt stmt) {
        PolicyName policyName = stmt.getPolicyName();
        policyLock.writeLock().lock();
        try {
            Policy policy = getPolicyByNameUnlocked(stmt.getPolicyType(), stmt.getPolicyName(), stmt.isIfExists());
            // Return NULL means there is no policy but if exists is set
            if (policy == null) {
                return;
            }

            DbUID dbUID = DbUID.generate(stmt.getPolicyName().getCatalog(), stmt.getPolicyName().getDbName());
            doDropPolicyUnlocked(policy.getPolicyType(), dbUID, stmt.getPolicyName().getName(), policy.getPolicyId(),
                    stmt.isForce());

            GlobalStateMgr.getCurrentState().getEditLog().logDropPolicy(policyName, dbUID, policy);
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    public void replayDropPolicy(DropPolicyLog dropPolicyInfo) throws DdlException {
        policyLock.writeLock().lock();
        try {
            doDropPolicyUnlocked(dropPolicyInfo.getPolicyType(), dropPolicyInfo.getDb(), dropPolicyInfo.getName(),
                    dropPolicyInfo.getPolicyId(), true);
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    private void doDropPolicyUnlocked(PolicyType policyType, DbUID dbUID, String policyName, Long policyId,
                                      boolean force) {
        Map<String, Policy> nameToPolicy = getOrCreateNamePolicyMapByDBUIDUnlocked(dbUID, policyType);

        //TODO: support force drop
        //if (isPolicyHasApplied(policyType, policyId) && !force) {
        //    throw new DdlException("Can't drop policy which has be apply");
        //}

        nameToPolicy.remove(policyName);
        idToPolicy.remove(policyId);
    }

    public void alterPolicy(AlterPolicyStmt stmt) {
        policyLock.writeLock().lock();
        try {
            Policy policy = getPolicyByNameUnlocked(stmt.getPolicyType(), stmt.getPolicyName(), stmt.isIfExists());
            // Return NULL means there is no policy but if exists is set
            if (policy == null) {
                return;
            }

            if (stmt.getAlterPolicyClause() instanceof AlterPolicyStmt.PolicySetBody) {
                AlterPolicyStmt.PolicySetBody policySetBody =
                        (AlterPolicyStmt.PolicySetBody) stmt.getAlterPolicyClause();
                doAlterPolicySetBodyUnlocked(policy, policySetBody.getPolicyBody());
                GlobalStateMgr.getCurrentState().getEditLog().logAlterPolicySetBody(stmt.getPolicyName(),
                        stmt.getPolicyType(),
                        AstToSQLBuilder.toSQL(policySetBody.getPolicyBody()));
            } else if (stmt.getAlterPolicyClause() instanceof AlterPolicyStmt.PolicySetComment) {
                AlterPolicyStmt.PolicySetComment policySetComment =
                        (AlterPolicyStmt.PolicySetComment) stmt.getAlterPolicyClause();
                doAlterPolicySetCommentUnlocked(policy, policySetComment.getComment());
                GlobalStateMgr.getCurrentState().getEditLog().logAlterPolicySetComment(stmt.getPolicyName(),
                        stmt.getPolicyType(),
                        policySetComment.getComment());
            } else if (stmt.getAlterPolicyClause() instanceof AlterPolicyStmt.PolicyRename) {
                AlterPolicyStmt.PolicyRename policyRename = (AlterPolicyStmt.PolicyRename) stmt.getAlterPolicyClause();
                doAlterPolicyRenameUnlocked(policy, policyRename.getNewPolicyName());
                GlobalStateMgr.getCurrentState().getEditLog().logAlterPolicyRename(stmt.getPolicyName(),
                        policy.getPolicyType(),
                        policyRename.getNewPolicyName());
            }
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    public void replayAlterPolicy(AlterPolicyLog alterPolicyInfo) throws DdlException {
        policyLock.writeLock().lock();
        try {
            Policy policy = getPolicyByNameUnlocked(alterPolicyInfo.getPolicyType(), alterPolicyInfo.getPolicyName(), false);
            // Return NULL means there is no policy but if exists is set
            if (policy == null) {
                throw new DdlException("Policy " + alterPolicyInfo.getPolicyName() + " not exists");
            }

            if (alterPolicyInfo.getAlterPolicyClauseInfo() instanceof AlterPolicyLog.PolicySetBodyInfo) {
                AlterPolicyLog.PolicySetBodyInfo policySetBodyObject =
                        (AlterPolicyLog.PolicySetBodyInfo) alterPolicyInfo.getAlterPolicyClauseInfo();
                Expr expression = SqlParser.parseSqlToExpr(policySetBodyObject.getPolicyBody(),
                        SqlModeHelper.MODE_DEFAULT);
                doAlterPolicySetBodyUnlocked(policy, expression);
            } else if (alterPolicyInfo.getAlterPolicyClauseInfo() instanceof AlterPolicyLog.PolicySetCommentInfo) {
                AlterPolicyLog.PolicySetCommentInfo setCommentInfo =
                        (AlterPolicyLog.PolicySetCommentInfo) alterPolicyInfo.getAlterPolicyClauseInfo();
                doAlterPolicySetCommentUnlocked(policy, setCommentInfo.getComment());
            } else if (alterPolicyInfo.getAlterPolicyClauseInfo() instanceof AlterPolicyLog.PolicyRenameInfo) {
                AlterPolicyLog.PolicyRenameInfo policyRenameObject =
                        (AlterPolicyLog.PolicyRenameInfo) alterPolicyInfo.getAlterPolicyClauseInfo();
                doAlterPolicyRenameUnlocked(policy, policyRenameObject.getNewPolicyName());
            }
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    private void doAlterPolicySetBodyUnlocked(Policy policy, Expr policyBody) {
        policy.setPolicyExpression(policyBody);
    }

    private void doAlterPolicySetCommentUnlocked(Policy policy, String comment) {
        policy.setComment(comment);
    }

    private void doAlterPolicyRenameUnlocked(Policy policy, String newName) {
        Map<String, Policy> nameToPolicy = getOrCreateNamePolicyMapByDBUIDUnlocked(policy.getDbUID(), policy.getPolicyType());
        nameToPolicy.remove(policy.getName());
        policy.setName(newName);
        nameToPolicy.put(newName, policy);
    }

    public Policy getPolicyByName(PolicyType policyType, PolicyName policyName, boolean isSetIfExists) {
        policyLock.readLock().lock();
        try {
            return getPolicyByNameUnlocked(policyType, policyName, isSetIfExists);
        } finally {
            policyLock.readLock().unlock();
        }
    }

    private Policy getPolicyByNameUnlocked(PolicyType policyType, PolicyName policyName, boolean isSetIfExists) {
        Map<String, Policy> nameToPolicy = getOrCreateNamePolicyMapByDBUIDUnlocked(
                DbUID.generate(policyName.getCatalog(), policyName.getDbName()), policyType);
        Policy policy = nameToPolicy.get(policyName.getName());
        if (policy == null) {
            if (!isSetIfExists) {
                throw new SemanticException("Can't find policy " + policyName);
            } else {
                return null;
            }
        }

        return policy;
    }

    public Map<String, Policy> getOrCreateNamePolicyMapByDBUID(DbUID dbUID, PolicyType policyType) {
        policyLock.readLock().lock();
        try {
            return getOrCreateNamePolicyMapByDBUIDUnlocked(dbUID, policyType);
        } finally {
            policyLock.readLock().unlock();
        }
    }

    private Map<String, Policy> getOrCreateNamePolicyMapByDBUIDUnlocked(DbUID dbUID, PolicyType policyType) {
        Map<DbUID, Map<String, Policy>> nameToPolicy;
        if (policyType.equals(PolicyType.COLUMN_MASKING)) {
            nameToPolicy = nameToMaskingPolicy;
        } else {
            nameToPolicy = nameToRowAccessPolicy;
        }

        if (!nameToPolicy.containsKey(dbUID)) {
            nameToPolicy.put(dbUID, new HashMap<>());
        }
        return nameToPolicy.get(dbUID);
    }
}
