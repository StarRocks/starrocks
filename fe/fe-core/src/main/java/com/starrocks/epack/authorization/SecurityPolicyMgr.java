// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.google.common.collect.Maps;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.epack.persist.AlterPolicyLog;
import com.starrocks.epack.persist.ApplyOrRevokeMaskingPolicyLog;
import com.starrocks.epack.persist.ApplyOrRevokeRowAccessPolicyLog;
import com.starrocks.epack.persist.CreatePasswordPolicyLog;
import com.starrocks.epack.persist.CreatePolicyLog;
import com.starrocks.epack.persist.DropPasswordPolicyLog;
import com.starrocks.epack.persist.DropPolicyLog;
import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.epack.persist.SRMetaBlockIDEPack;
import com.starrocks.epack.persist.SetPasswordPolicyLog;
import com.starrocks.epack.persist.UnsetPasswordPolicyLog;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.CreatePasswordPolicyStmt;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.DropPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.PolicyName;
import com.starrocks.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.SqlParser;

import java.io.IOException;
import java.util.ArrayList;
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

    // Password Policy
    private final Map<Long, PasswordPolicy> passwordPolicyMap;
    private final Map<String, Long> passwordPolicyNameToId;
    private long globalPasswordPolicy;
    private final ReentrantReadWriteLock passwordPolicyLock;

    public SecurityPolicyMgr() {
        idToPolicy = new HashMap<>();
        nameToMaskingPolicy = new HashMap<>();
        nameToRowAccessPolicy = new HashMap<>();
        policyContextMap = new ConcurrentHashMap<>();
        policyLock = new ReentrantReadWriteLock();

        passwordPolicyMap = new HashMap<>();
        passwordPolicyNameToId = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        globalPasswordPolicy = -1;
        passwordPolicyLock = new ReentrantReadWriteLock();
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
                    AstToSQLBuilder.toSQL(stmt.getExpression()),
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
                    createPolicyInfo.getPolicyExpressionSQL(),
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

    private boolean isTableAlive(TableUID tableUID) {
        try {
            return tableUID.toTableName() != null;
        } catch (Throwable e) {
            return false;
        }
    }

    private void doDropPolicyUnlocked(PolicyType policyType, DbUID dbUID, String policyName, Long policyId,
                                      boolean force) {
        Map<String, Policy> nameToPolicy = getOrCreateNamePolicyMapByDBUIDUnlocked(dbUID, policyType);

        if (!force) {
            boolean hasLiveReference = policyContextMap.entrySet().stream()
                    .filter(entry -> entry.getValue().hasApplyPolicy(policyType, policyId))
                    .anyMatch(entry -> isTableAlive(entry.getKey()));
            if (hasLiveReference) {
                throw new SemanticException("Can't drop policy which has be apply");
            }
        }

        Iterator<Map.Entry<TableUID, PolicyAppliedContext>> it = policyContextMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<TableUID, PolicyAppliedContext> entry = it.next();
            PolicyAppliedContext ctx = entry.getValue();
            if (ctx.hasApplyPolicy(policyType, policyId)) {
                if (policyType == PolicyType.MASKING) {
                    ctx.revokeMaskingPolicy(policyId);
                } else {
                    ctx.revokeRowAccessPolicy(policyId);
                }
                if (ctx.isEmpty()) {
                    it.remove();
                }
            }
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
        policy.setPolicyExpressionSQL(AstToSQLBuilder.toSQL(policyBody));
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

    private boolean isValid(TableUID tableUID) {
        try {
            return tableUID.validate();
        } catch (Throwable ignored) {
        }
        return true;
    }

    public void removeInvalidObject() {
        policyLock.readLock().lock();
        try {
            nameToMaskingPolicy.entrySet().removeIf(entry -> !entry.getKey().validate());
            nameToRowAccessPolicy.entrySet().removeIf(entry -> !entry.getKey().validate());

            Iterator<Map.Entry<TableUID, PolicyAppliedContext>> iterator = policyContextMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<TableUID, PolicyAppliedContext> entry = iterator.next();
                if (!isValid(entry.getKey())) {
                    iterator.remove();
                } else {
                    PolicyAppliedContext policyContext = entry.getValue();
                    Map<ColumnId, MaskingPolicyContext> m = policyContext.getMaskingPolicyApply();
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

    public void save(ImageWriter imageWriter) throws IOException {
        try {
            // 1 json for idToPolicy size, 1 json for policyContextMap size, others for each map key and value
            int cnt = 1 + 1 + idToPolicy.size() + policyContextMap.size() * 2 + passwordPolicyMap.size() + 2;
            SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockIDEPack.SECURITY_POLICY_MGR, cnt);

            writer.writeInt(idToPolicy.size());
            for (Policy policy : idToPolicy.values()) {
                writer.writeJson(new CreatePolicyLog(policy));
            }

            writer.writeInt(policyContextMap.size());
            for (Map.Entry<TableUID, PolicyAppliedContext> entry : policyContextMap.entrySet()) {
                writer.writeJson(entry.getKey());
                writer.writeJson(entry.getValue());
            }

            writer.writeInt(passwordPolicyMap.size());
            for (Map.Entry<Long, PasswordPolicy> entry : passwordPolicyMap.entrySet()) {
                PasswordPolicy passwordPolicy = entry.getValue();
                writer.writeJson(new CreatePasswordPolicyLog(passwordPolicy.getPolicyId(),
                        passwordPolicy.getPolicyName(), passwordPolicy.getComment(), passwordPolicy.getProperties()));
            }
            writer.writeLong(globalPasswordPolicy);

            writer.close();
        } catch (SRMetaBlockException e) {
            throw new IOException("failed to save SecurityPolicyManager", e);
        }
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockEOFException, SRMetaBlockException {
        reader.readCollection(CreatePolicyLog.class, createPolicyInfo -> {
            Policy policy = new Policy(createPolicyInfo.getPolicyType(), createPolicyInfo.getPolicyId(),
                    createPolicyInfo.getName(), createPolicyInfo.getDbUID(),
                    createPolicyInfo.getArgNames(), createPolicyInfo.getArgTypes(),
                    createPolicyInfo.getRetType(), createPolicyInfo.getPolicyExpressionSQL(),
                    createPolicyInfo.getComment());
            registerPolicy(policy);
        });

        reader.readMap(TableUID.class, PolicyAppliedContext.class, policyContextMap::put);

        reader.readCollection(CreatePasswordPolicyLog.class, createPasswordPolicyLog -> {
            passwordPolicyMap.put(createPasswordPolicyLog.getPolicyId(),
                    new PasswordPolicy(
                            createPasswordPolicyLog.getPolicyId(),
                            createPasswordPolicyLog.getPolicyName(),
                            createPasswordPolicyLog.getComment(),
                            createPasswordPolicyLog.getProperties()));

            passwordPolicyNameToId.put(createPasswordPolicyLog.getPolicyName(), createPasswordPolicyLog.getPolicyId());
        });

        this.globalPasswordPolicy = reader.readLong();
    }

    public void applyMaskingPolicyContext(ConnectContext ctx, TableName tableName, String columnName,
                                          WithColumnMaskingPolicy withColumnMaskingPolicy) {
        TableUID tableUID = TableUID.generate(ctx, tableName.getCatalog(), tableName.getDb(), tableName.getTbl());

        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, tableName.getCatalog(),
                tableName.getDb(), tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName);
        }

        ColumnId columnId = table.getColumn(columnName).getColumnId();

        doApplyMaskingPolicyContext(tableUID, columnId, columnName,
                new MaskingPolicyContext(withColumnMaskingPolicy.getPolicyId(),
                        MetaUtils.getColumnIdsByColumnNames(table, withColumnMaskingPolicy.getUsingColumns())));
        GlobalStateMgr.getCurrentState().getEditLog().logApplyMaskingPolicy(
                new ApplyOrRevokeMaskingPolicyLog(tableUID, columnId,
                        new MaskingPolicyContext(withColumnMaskingPolicy.getPolicyId(),
                                MetaUtils.getColumnIdsByColumnNames(table, withColumnMaskingPolicy.getUsingColumns()))));
    }

    public void registerMaskingPolicyContext(ConnectContext ctx, TableName tableName, Table table, String columnName,
                                             WithColumnMaskingPolicy withColumnMaskingPolicy) {
        TableUID tableUID = TableUID.generate(ctx, tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        doApplyMaskingPolicyContext(tableUID, table.getColumn(columnName).getColumnId(), columnName,
                new MaskingPolicyContext(withColumnMaskingPolicy.getPolicyId(),
                        MetaUtils.getColumnIdsByColumnNames(table, withColumnMaskingPolicy.getUsingColumns())));
    }

    public void registerMaskingPolicyContext(ApplyOrRevokeMaskingPolicyLog applyMaskingPolicyInfo) {
        doForceApplyMaskingPolicyContext(applyMaskingPolicyInfo.getTable(), applyMaskingPolicyInfo.getColumnId(),
                applyMaskingPolicyInfo.getColumnMaskingPolicyContext());
    }

    // Replay-path upsert: overwrites any existing masking policy for the column without throwing.
    // Used during journal replay where the live-path duplicate check must not apply.
    private void doForceApplyMaskingPolicyContext(TableUID tableUID, ColumnId columnId,
                                                  MaskingPolicyContext columnMaskingPolicyContext) {
        policyLock.writeLock().lock();
        try {
            policyContextMap.computeIfAbsent(tableUID, k -> new PolicyAppliedContext())
                    .applyMaskingPolicy(columnId, columnMaskingPolicyContext);
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    private void doApplyMaskingPolicyContext(TableUID tableUID, ColumnId columnId, String columnName,
                                             MaskingPolicyContext columnMaskingPolicyContext) {
        policyLock.writeLock().lock();
        try {
            if (policyContextMap.containsKey(tableUID)) {
                PolicyAppliedContext tableAppliedPolicyInfo = policyContextMap.get(tableUID);
                if (tableAppliedPolicyInfo.getMaskingPolicyApply().containsKey(columnId)) {
                    throw new SemanticException("A masking policy already exists in the current column["
                            + (columnName != null ? columnName : columnId) +
                            "], and only supports applying a masking policy to a specific column");
                }

                tableAppliedPolicyInfo.applyMaskingPolicy(columnId, columnMaskingPolicyContext);
            } else {
                PolicyAppliedContext tableAppliedPolicyInfo = new PolicyAppliedContext();
                tableAppliedPolicyInfo.applyMaskingPolicy(columnId, columnMaskingPolicyContext);
                policyContextMap.put(tableUID, tableAppliedPolicyInfo);
            }
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    public void revokeMaskingPolicyContext(ConnectContext ctx, String catalog, String dbName, String tblName, String columnName) {
        TableUID tableUID = TableUID.generate(ctx, catalog, dbName, tblName);
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, catalog, dbName, tblName);
        if (table == null) {
            throw new SemanticException("Table %s is not found", tblName);
        }
        ColumnId columnId = table.getColumn(columnName).getColumnId();
        doRevokeMaskingPolicyContext(tableUID, columnId);
        GlobalStateMgr.getCurrentState().getEditLog().logRevokeMaskingPolicy(
                new ApplyOrRevokeMaskingPolicyLog(tableUID, columnId, null));
    }

    public void replayRevokeMaskingPolicyContext(ApplyOrRevokeMaskingPolicyLog maskingPolicyInfo) {
        doRevokeMaskingPolicyContext(maskingPolicyInfo.getTable(), maskingPolicyInfo.getColumnId());
    }

    private void doRevokeMaskingPolicyContext(TableUID tableUID, ColumnId columnId) {
        policyLock.writeLock().lock();
        try {
            policyContextMap.computeIfPresent(tableUID, (k, v) -> {
                v.revokeMaskingPolicy(columnId);
                return v;
            });
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    public void applyRowAccessPolicyContext(ConnectContext ctx, TableName tableName, WithRowAccessPolicy withRowAccessPolicy) {
        TableUID tableUID = TableUID.generate(ctx, tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(ctx, tableName.getCatalog(),
                tableName.getDb(), tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName);
        }

        RowAccessPolicyContext rowAccessPolicyContext =
                new RowAccessPolicyContext(withRowAccessPolicy.getPolicyId(),
                        MetaUtils.getColumnIdsByColumnNames(table, withRowAccessPolicy.getOnColumns()));

        doApplyRowAccessPolicyContext(tableUID, rowAccessPolicyContext);
        GlobalStateMgr.getCurrentState().getEditLog()
                .logApplyRowAccessPolicy(new ApplyOrRevokeRowAccessPolicyLog(tableUID, rowAccessPolicyContext));
    }

    public void registerRowAccessPolicyContext(ConnectContext ctx, TableName tableName, Table table,
                                               WithRowAccessPolicy withRowAccessPolicy) {
        TableUID tableUID = TableUID.generate(ctx, tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        doApplyRowAccessPolicyContext(tableUID,
                new RowAccessPolicyContext(withRowAccessPolicy.getPolicyId(),
                        MetaUtils.getColumnIdsByColumnNames(table, withRowAccessPolicy.getOnColumns())));
    }

    public void registerRowAccessPolicyContext(ApplyOrRevokeRowAccessPolicyLog applyRowAccessPolicyInfo) {
        doIdempotentApplyRowAccessPolicyContext(applyRowAccessPolicyInfo.getTable(),
                applyRowAccessPolicyInfo.getRowAccessPolicyContext());
    }

    // Replay-path apply: skips silently if the exact same (policyId, onColumns) already exists.
    // Used during journal replay where the live-path duplicate check must not apply.
    private void doIdempotentApplyRowAccessPolicyContext(TableUID tableUID,
                                                        RowAccessPolicyContext rowAccessPolicyContext) {
        policyLock.writeLock().lock();
        try {
            PolicyAppliedContext tableAppliedPolicyInfo =
                    policyContextMap.computeIfAbsent(tableUID, k -> new PolicyAppliedContext());
            for (RowAccessPolicyContext rp : tableAppliedPolicyInfo.getRowAccessPolicyApply()) {
                if (rp.policyId.equals(rowAccessPolicyContext.policyId)
                        && rp.onColumns.equals(rowAccessPolicyContext.onColumns)) {
                    return;
                }
            }
            tableAppliedPolicyInfo.addRowAccessPolicy(rowAccessPolicyContext);
        } finally {
            policyLock.writeLock().unlock();
        }
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

    public void revokeRowAccessPolicyContext(ConnectContext ctx,
                                             String catalog, String dbName, String tblName, PolicyName policyName) {
        SecurityPolicyMgr securityPolicyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        policyLock.writeLock().lock();
        try {
            Policy policy = securityPolicyManager.getPolicyByName(PolicyType.ROW_ACCESS, policyName);
            if (policy == null) {
                throw new SemanticException("Can't find masking policy : " + policyName.getName());
            }
            long policyId = policy.getPolicyId();

            TableUID tableUID = TableUID.generate(ctx, catalog, dbName, tblName);
            policyContextMap.computeIfPresent(tableUID, (k, v) -> {
                v.revokeRowAccessPolicy(policyId);
                return v;
            });

            GlobalStateMgr.getCurrentState().getEditLog().logRevokeRowAccessPolicy(
                    new ApplyOrRevokeRowAccessPolicyLog(tableUID, new RowAccessPolicyContext(policyId, null)));
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    public void revokeALLRowAccessPolicyContext(ConnectContext ctx, String catalog, String dbName, String tblName) {
        TableUID tableUID = TableUID.generate(ctx, catalog, dbName, tblName);
        policyLock.writeLock().lock();
        try {
            policyContextMap.computeIfPresent(tableUID, (k, v) -> {
                v.clearRowAccessPolicy();
                return v;
            });

            GlobalStateMgr.getCurrentState().getEditLog().logRevokeRowAccessPolicy(
                    new ApplyOrRevokeRowAccessPolicyLog(tableUID, new RowAccessPolicyContext(null, null)));
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    public void replayRevokeRowAccessPolicyContext(ApplyOrRevokeRowAccessPolicyLog applyRowAccessPolicyInfo) {
        policyLock.writeLock().lock();
        try {
            TableUID tableUID = applyRowAccessPolicyInfo.getTable();
            if (applyRowAccessPolicyInfo.getRowAccessPolicyContext().policyId == null) {
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
        } finally {
            policyLock.writeLock().unlock();
        }
    }

    public ConcurrentMap<TableUID, PolicyAppliedContext> getPolicyContextMap() {
        return policyContextMap;
    }

    public void createPasswordPolicy(CreatePasswordPolicyStmt stmt) throws DdlException {
        long policyId = GlobalStateMgr.getCurrentState().getNextId();
        CreatePasswordPolicyLog createPasswordPolicyLog =
                new CreatePasswordPolicyLog(policyId, stmt.getPolicyName(), stmt.getComment(), stmt.getProperties());
        doCreatePasswordPolicy(createPasswordPolicyLog);

        EditLogEPack editLogEPack = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
        editLogEPack.logCreatePasswordPolicy(createPasswordPolicyLog);
    }

    public void doCreatePasswordPolicy(CreatePasswordPolicyLog log) throws DdlException {
        passwordPolicyLock.writeLock().lock();
        try {
            if (passwordPolicyNameToId.containsKey(log.getPolicyName())) {
                throw new DdlException("Policy " + log.getPolicyName() + " has exist");
            }
            PasswordPolicy passwordPolicy = new PasswordPolicy(
                    log.getPolicyId(), log.getPolicyName(), log.getComment(), log.getProperties());

            passwordPolicyNameToId.put(log.getPolicyName(), log.getPolicyId());
            passwordPolicyMap.put(log.getPolicyId(), passwordPolicy);
        } finally {
            passwordPolicyLock.writeLock().unlock();
        }
    }

    public void dropPasswordPolicy(DropPasswordPolicyStmt stmt) throws DdlException {
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        final DropPasswordPolicyLog[] dropPasswordPolicyLogHolder = new DropPasswordPolicyLog[1];
        authenticationMgr.withReadLock(() -> {
            passwordPolicyLock.writeLock().lock();
            try {
                String policyName = stmt.getPolicyName();
                Long passwordPolicyId = passwordPolicyNameToId.get(policyName);
                if (passwordPolicyId == null) {
                    throw new DdlException("policy " + policyName + " not exists");
                }

                List<String> boundUsers = authenticationMgr.getUserNamesByPasswordPolicy(policyName);
                if (!boundUsers.isEmpty()) {
                    throw new DdlException("Policy " + policyName + " cannot be dropped as it is associated with users: "
                            + String.join(", ", boundUsers));
                }

                DropPasswordPolicyLog dropPasswordPolicyLog = new DropPasswordPolicyLog(passwordPolicyId, policyName);
                doDropPasswordPolicyUnlocked(dropPasswordPolicyLog);
                dropPasswordPolicyLogHolder[0] = dropPasswordPolicyLog;
            } finally {
                passwordPolicyLock.writeLock().unlock();
            }
        });
        EditLogEPack editLogEPack = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
        editLogEPack.logDropPasswordPolicy(dropPasswordPolicyLogHolder[0]);
    }

    public void doDropPasswordPolicy(DropPasswordPolicyLog log) throws DdlException {
        passwordPolicyLock.writeLock().lock();
        try {
            doDropPasswordPolicyUnlocked(log);
        } finally {
            passwordPolicyLock.writeLock().unlock();
        }
    }

    private void doDropPasswordPolicyUnlocked(DropPasswordPolicyLog log) throws DdlException {
        Long passwordPolicyId = log.getPolicyId();
        PasswordPolicy passwordPolicy = passwordPolicyMap.get(passwordPolicyId);
        if (passwordPolicy == null) {
            throw new DdlException("Password Policy " + log.getPolicyName() + " is not exist");
        }

        if (passwordPolicyId == globalPasswordPolicy) {
            throw new DdlException("Policy " + passwordPolicy.getPolicyName() +
                    " cannot be dropped as it is associated with the current system.");
        }

        passwordPolicyMap.remove(passwordPolicyId);
        passwordPolicyNameToId.remove(passwordPolicy.getPolicyName());
    }

    public List<PasswordPolicy> getAllPasswordPolicies() {
        passwordPolicyLock.readLock().lock();
        try {
            return new ArrayList<>(passwordPolicyMap.values());
        } finally {
            passwordPolicyLock.readLock().unlock();
        }
    }

    public PasswordPolicy getPasswordPolicy(String policyName) {
        passwordPolicyLock.readLock().lock();
        try {
            Long passwordPolicyId = passwordPolicyNameToId.get(policyName);
            if (passwordPolicyId == null) {
                return null;
            }
            return passwordPolicyMap.get(passwordPolicyId);
        } finally {
            passwordPolicyLock.readLock().unlock();
        }
    }

    public void setGlobalPasswordPolicy(String passwordPolicyName) throws DdlException {
        passwordPolicyLock.writeLock().lock();
        try {
            if (!passwordPolicyNameToId.containsKey(passwordPolicyName)) {
                throw new DdlException("Password Policy " + passwordPolicyName + " not exist");
            }

            this.globalPasswordPolicy = passwordPolicyNameToId.get(passwordPolicyName);
        } finally {
            passwordPolicyLock.writeLock().unlock();
        }
        SetPasswordPolicyLog setPasswordPolicyLog = new SetPasswordPolicyLog(this.globalPasswordPolicy);
        EditLogEPack editLogEPack = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
        editLogEPack.logSetGlobalPasswordPolicy(setPasswordPolicyLog);
    }

    public void setGlobalPasswordPolicy(long globalPasswordPolicy) {
        passwordPolicyLock.writeLock().lock();
        try {
            this.globalPasswordPolicy = globalPasswordPolicy;
        } finally {
            passwordPolicyLock.writeLock().unlock();
        }
    }

    public void unsetGlobalPasswordPolicy() {
        passwordPolicyLock.writeLock().lock();
        try {
            this.globalPasswordPolicy = -1;
        } finally {
            passwordPolicyLock.writeLock().unlock();
        }
        EditLogEPack editLogEPack = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
        editLogEPack.logUnsetGlobalPasswordPolicy(new UnsetPasswordPolicyLog());
    }

    public PasswordPolicy getGlobalPasswordPolicy() {
        passwordPolicyLock.readLock().lock();
        try {
            return passwordPolicyMap.get(globalPasswordPolicy);
        } finally {
            passwordPolicyLock.readLock().unlock();
        }
    }
}
