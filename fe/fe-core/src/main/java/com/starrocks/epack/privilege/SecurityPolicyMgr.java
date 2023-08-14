// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.epack.persist.AlterPolicyLog;
import com.starrocks.epack.persist.ApplyOrRevokeMaskingPolicyLog;
import com.starrocks.epack.persist.ApplyOrRevokeRowAccessPolicyLog;
import com.starrocks.epack.persist.CreatePolicyLog;
import com.starrocks.epack.persist.DropPolicyLog;
import com.starrocks.epack.persist.SRMetaBlockIDEPack;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.SqlParser;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SecurityPolicyMgr {

    private final Map<Long, Policy> idToPolicy;
    private final Map<DbUID, Map<String, Policy>> nameToMaskingPolicy;
    private final Map<DbUID, Map<String, Policy>> nameToRowAccessPolicy;
    private final ConcurrentMap<TableUID, PolicyAppliedContext> policyContextMap;
    private final ReentrantReadWriteLock policyLock;

    public SecurityPolicyMgr() {
        idToPolicy = new HashMap<>();
        nameToMaskingPolicy = new HashMap<>();
        nameToRowAccessPolicy = new HashMap<>();
        policyContextMap = new ConcurrentHashMap<>();
        policyLock = new ReentrantReadWriteLock();
    }

    public boolean hasTableAppliedPolicy(TableUID tablePEntryObject) {
        return policyContextMap.containsKey(tablePEntryObject);
    }

    public PolicyAppliedContext getTableAppliedPolicyInfo(TableUID tableId) {
        return policyContextMap.get(tableId);
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
                    stmt.getArgTypeDefs(),
                    stmt.getReturnType().getType(),
                    stmt.getExpression(),
                    stmt.getComment());

            registerPolicy(policy);
            if (stmt.getPolicyType().equals(PolicyType.MASKING)) {
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
        Map<String, Policy> nameToPolicy =
                getOrCreateNamePolicyMapByDBUIDUnlocked(policy.getDbUID(), policy.getPolicyType());
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

    public void replayDropPolicy(DropPolicyLog dropPolicyInfo) {
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

        if (policyContextMap.values().stream().anyMatch(policyContext -> policyContext.hasApplyPolicy(policyType, policyId))
                && !force) {
            throw new SemanticException("Can't drop policy which has be apply");
        }

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
            Policy policy =
                    getPolicyByNameUnlocked(alterPolicyInfo.getPolicyType(), alterPolicyInfo.getPolicyName(), false);
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
        Map<String, Policy> nameToPolicy =
                getOrCreateNamePolicyMapByDBUIDUnlocked(policy.getDbUID(), policy.getPolicyType());
        nameToPolicy.remove(policy.getName());
        policy.setName(newName);
        nameToPolicy.put(newName, policy);
    }

    public Policy getPolicyById(Long policyId) {
        policyLock.readLock().lock();
        try {
            return idToPolicy.get(policyId);
        } finally {
            policyLock.readLock().unlock();
        }
    }

    public Policy getPolicyByName(PolicyType policyType, PolicyName policyName, boolean isSetIfExists) {
        policyLock.readLock().lock();
        try {
            return getPolicyByNameUnlocked(policyType, policyName, isSetIfExists);
        } finally {
            policyLock.readLock().unlock();
        }
    }

    public Policy getPolicyByName(PolicyType policyType, PolicyName policyName) {
        policyLock.readLock().lock();
        try {
            DbUID dbUID = DbUID.generate(policyName.getCatalog(), policyName.getDbName());
            if (policyType.equals(PolicyType.MASKING)) {
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
        if (policyType.equals(PolicyType.MASKING)) {
            nameToPolicy = nameToMaskingPolicy;
        } else {
            nameToPolicy = nameToRowAccessPolicy;
        }

        if (!nameToPolicy.containsKey(dbUID)) {
            nameToPolicy.put(dbUID, new HashMap<>());
        }
        return nameToPolicy.get(dbUID);
    }

    public void removeInvalidObject() {
        policyLock.readLock().lock();
        try {
            nameToMaskingPolicy.entrySet().removeIf(entry -> !entry.getKey().validate());
            nameToRowAccessPolicy.entrySet().removeIf(entry -> !entry.getKey().validate());

            Iterator<Map.Entry<TableUID, PolicyAppliedContext>> iterator = policyContextMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<TableUID, PolicyAppliedContext> entry = iterator.next();
                if (!entry.getKey().validate()) {
                    iterator.remove();
                } else {
                    PolicyAppliedContext policyContext = entry.getValue();
                    Map<String, MaskingPolicyContext> m = policyContext.getMaskingPolicyApply();
                    for (MaskingPolicyContext context : m.values()) {
                        if (!idToPolicy.containsKey(context.getPolicyId())) {
                            policyContext.revokeMaskingPolicy(context.getPolicyId());
                        }
                    }

                    List<RowAccessPolicyContext> r = policyContext.getRowAccessPolicyApply();
                    for (RowAccessPolicyContext context : r) {
                        if (!idToPolicy.containsKey(context.getPolicyId())) {
                            policyContext.revokeRowAccessPolicy(context.getPolicyId());
                        }
                    }
                }
            }
        } finally {
            policyLock.readLock().unlock();
        }
    }

    public void save(DataOutputStream dos) throws IOException {
        try {
            // 1 json for idToPolicy size, 1 json for policyContextMap size, others for each map key and value
            int cnt = 1 + 1 + idToPolicy.size() + policyContextMap.size() * 2;
            SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockIDEPack.SECURITY_POLICY_MGR, cnt);

            writer.writeJson(idToPolicy.size());
            for (Policy policy : idToPolicy.values()) {
                writer.writeJson(new CreatePolicyLog(policy));
            }

            writer.writeJson(policyContextMap.size());
            for (Map.Entry<TableUID, PolicyAppliedContext> entry : policyContextMap.entrySet()) {
                writer.writeJson(entry.getKey());
                writer.writeJson(entry.getValue());
            }

            writer.close();
        } catch (SRMetaBlockException e) {
            throw new IOException("failed to save SecurityPolicyManager", e);
        }
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockEOFException, SRMetaBlockException {
        int policySize = reader.readJson(int.class);
        for (int i = 0; i < policySize; ++i) {
            CreatePolicyLog createPolicyInfo = reader.readJson(CreatePolicyLog.class);

            Policy policy = new Policy(createPolicyInfo.getPolicyType(), createPolicyInfo.getPolicyId(),
                    createPolicyInfo.getName(), createPolicyInfo.getDbUID(),
                    createPolicyInfo.getArgNames(), createPolicyInfo.getArgTypes(),
                    createPolicyInfo.getRetType(), createPolicyInfo.getPolicyExpression(),
                    createPolicyInfo.getComment());

            registerPolicy(policy);
        }

        int policyContextSize = reader.readJson(int.class);
        for (int i = 0; i < policyContextSize; ++i) {
            TableUID tablePEntryObject = reader.readJson(TableUID.class);
            PolicyAppliedContext policyContext = reader.readJson(PolicyAppliedContext.class);
            policyContextMap.put(tablePEntryObject, policyContext);
        }
    }

    public void applyMaskingPolicyContext(TableName tableName, String columnName,
                                          WithColumnMaskingPolicy withColumnMaskingPolicy) {
        TableUID tableUID = TableUID.generate(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());

        doApplyMaskingPolicyContext(tableUID, columnName, new MaskingPolicyContext(withColumnMaskingPolicy.getPolicyId(),
                withColumnMaskingPolicy.getUsingColumns()));
        GlobalStateMgr.getCurrentState().getEditLog().logApplyMaskingPolicy(new ApplyOrRevokeMaskingPolicyLog(
                tableUID, columnName, new MaskingPolicyContext(withColumnMaskingPolicy.getPolicyId(),
                withColumnMaskingPolicy.getUsingColumns())));
    }

    public void registerMaskingPolicyContext(TableName tableName, String columnName,
                                             WithColumnMaskingPolicy withColumnMaskingPolicy) {
        TableUID tableUID = TableUID.generate(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        doApplyMaskingPolicyContext(tableUID, columnName, new MaskingPolicyContext(withColumnMaskingPolicy.getPolicyId(),
                withColumnMaskingPolicy.getUsingColumns()));
    }

    public void registerMaskingPolicyContext(ApplyOrRevokeMaskingPolicyLog applyMaskingPolicyInfo) {
        doApplyMaskingPolicyContext(applyMaskingPolicyInfo.getTable(), applyMaskingPolicyInfo.getColumnName(),
                applyMaskingPolicyInfo.getColumnMaskingPolicyContext());
    }

    private void doApplyMaskingPolicyContext(TableUID tableUID, String columnName,
                                             MaskingPolicyContext columnMaskingPolicyContext) {
        policyLock.writeLock().lock();
        try {
            if (policyContextMap.containsKey(tableUID)) {
                PolicyAppliedContext tableAppliedPolicyInfo = policyContextMap.get(tableUID);
                if (tableAppliedPolicyInfo.getMaskingPolicyApply().containsKey(columnName)) {
                    throw new SemanticException("A masking policy already exists in the current column[" + columnName
                            + "], and only supports applying a masking policy to a specific column");
                }

                tableAppliedPolicyInfo.applyMaskingPolicy(columnName, columnMaskingPolicyContext);
            } else {
                PolicyAppliedContext tableAppliedPolicyInfo = new PolicyAppliedContext();
                tableAppliedPolicyInfo.applyMaskingPolicy(columnName, columnMaskingPolicyContext);
                policyContextMap.put(tableUID, tableAppliedPolicyInfo);
            }
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    public void revokeMaskingPolicyContext(String catalog, String dbName, String tblName, String columnName) {
        TableUID tableUID = TableUID.generate(catalog, dbName, tblName);
        doRevokeMaskingPolicyContext(tableUID, columnName);
        GlobalStateMgr.getCurrentState().getEditLog().logRevokeMaskingPolicy(
                new ApplyOrRevokeMaskingPolicyLog(tableUID, columnName, null));
    }

    public void replayRevokeMaskingPolicyContext(ApplyOrRevokeMaskingPolicyLog maskingPolicyInfo) {
        doRevokeMaskingPolicyContext(maskingPolicyInfo.getTable(), maskingPolicyInfo.getColumnName());
    }

    private void doRevokeMaskingPolicyContext(TableUID tableUID, String columnName) {
        policyContextMap.computeIfPresent(tableUID, (k, v) -> {
            v.revokeMaskingPolicy(columnName);
            return v;
        });
    }

    public void applyRowAccessPolicyContext(TableName tableName, WithRowAccessPolicy withRowAccessPolicy) {
        TableUID tableUID = TableUID.generate(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        RowAccessPolicyContext rowAccessPolicyContext =
                new RowAccessPolicyContext(withRowAccessPolicy.getPolicyId(), withRowAccessPolicy.getOnColumns());

        doApplyRowAccessPolicyContext(tableUID, rowAccessPolicyContext);
        GlobalStateMgr.getCurrentState().getEditLog()
                .logApplyRowAccessPolicy(new ApplyOrRevokeRowAccessPolicyLog(tableUID, rowAccessPolicyContext));
    }

    public void registerRowAccessPolicyContext(TableName tableName, WithRowAccessPolicy withRowAccessPolicy) {
        TableUID tableUID = TableUID.generate(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        doApplyRowAccessPolicyContext(tableUID,
                new RowAccessPolicyContext(withRowAccessPolicy.getPolicyId(), withRowAccessPolicy.getOnColumns()));
    }

    public void registerRowAccessPolicyContext(ApplyOrRevokeRowAccessPolicyLog applyRowAccessPolicyInfo) {
        doApplyRowAccessPolicyContext(applyRowAccessPolicyInfo.getTable(),
                applyRowAccessPolicyInfo.getRowAccessPolicyContext());
    }

    private void doApplyRowAccessPolicyContext(TableUID tableUID,
                                               RowAccessPolicyContext rowAccessPolicyContext) {
        policyLock.writeLock().lock();
        try {
            if (policyContextMap.containsKey(tableUID)) {
                PolicyAppliedContext tableAppliedPolicyInfo = policyContextMap.get(tableUID);
                for (RowAccessPolicyContext rp : tableAppliedPolicyInfo.getRowAccessPolicyApply()) {
                    if (rp.policyId.equals(rowAccessPolicyContext.policyId)
                            && rp.onColumns.equals(rowAccessPolicyContext.onColumns)) {
                        throw new SemanticException("The same Policy has already been applied to this table");
                    }
                }

                tableAppliedPolicyInfo.addRowAccessPolicy(rowAccessPolicyContext);
            } else {
                PolicyAppliedContext tableAppliedPolicyInfo = new PolicyAppliedContext();
                tableAppliedPolicyInfo.addRowAccessPolicy(rowAccessPolicyContext);
                policyContextMap.put(tableUID, tableAppliedPolicyInfo);
            }
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    public void revokeRowAccessPolicyContext(String catalog, String dbName, String tblName, PolicyName policyName) {
        SecurityPolicyMgr securityPolicyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        Policy policy = securityPolicyManager.getPolicyByName(PolicyType.ROW_ACCESS, policyName);
        if (policy == null) {
            throw new SemanticException("Can't find masking policy : " + policyName.getName());
        }
        long policyId = policy.getPolicyId();

        TableUID tableUID = TableUID.generate(catalog, dbName, tblName);
        policyContextMap.computeIfPresent(tableUID, (k, v) -> {
            v.revokeRowAccessPolicy(policyId);
            return v;
        });

        GlobalStateMgr.getCurrentState().getEditLog().logRevokeRowAccessPolicy(
                new ApplyOrRevokeRowAccessPolicyLog(tableUID, new RowAccessPolicyContext(policyId, null)));
    }

    public void revokeALLRowAccessPolicyContext(String catalog, String dbName, String tblName) {
        TableUID tableUID = TableUID.generate(catalog, dbName, tblName);
        policyContextMap.computeIfPresent(tableUID, (k, v) -> {
            v.clearRowAccessPolicy();
            return v;
        });

        GlobalStateMgr.getCurrentState().getEditLog().logRevokeRowAccessPolicy(
                new ApplyOrRevokeRowAccessPolicyLog(tableUID, new RowAccessPolicyContext(null, null)));
    }

    public void replayRevokeRowAccessPolicyContext(ApplyOrRevokeRowAccessPolicyLog applyRowAccessPolicyInfo) {
        TableUID tableUID = applyRowAccessPolicyInfo.getTable();
        if (applyRowAccessPolicyInfo.getRowAccessPolicyContext() == null) {
            policyContextMap.computeIfPresent(tableUID, (k, v) -> {
                v.clearRowAccessPolicy();
                return v;
            });
        } else {
            RowAccessPolicyContext rowAccessPolicyContext = applyRowAccessPolicyInfo.getRowAccessPolicyContext();
            policyContextMap.computeIfPresent(tableUID, (k, v) -> {
                v.revokeRowAccessPolicy(rowAccessPolicyContext.getPolicyId());
                return v;
            });
        }
    }

    public ConcurrentMap<TableUID, PolicyAppliedContext> getPolicyContextMap() {
        return policyContextMap;
    }
}