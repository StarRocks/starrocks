// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Type;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.NativeAccessControl;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.UserIdentity;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NativeAccessControlEPack extends NativeAccessControl implements AccessControlEPack {
    @Override
    public void checkPolicyAction(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                  String db, String policy, PrivilegeType privilegeType) {
        List<String> objectTokens = Lists.newArrayList(catalogName, db, policy);
        ObjectType objectType = policyType.equals(PolicyType.MASKING) ? ObjectTypeEPack.MASKING_POLICY :
                ObjectTypeEPack.ROW_ACCESS_POLICY;
        if (!checkObjectTypeAction(currentUser, roleIds, privilegeType, objectType, objectTokens)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), objectType, policy);
        }
    }

    @Override
    public void checkAnyActionOnPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                       String db, String policy) {
        List<String> objectTokens = Lists.newArrayList(catalogName, db, policy);
        ObjectType objectType = policyType.equals(PolicyType.MASKING) ? ObjectTypeEPack.MASKING_POLICY :
                ObjectTypeEPack.ROW_ACCESS_POLICY;
        if (!checkAnyActionOnObject(currentUser, roleIds, objectType, objectTokens)) {
            AccessDeniedException.reportAccessDenied("ANY", objectType, policy);
        }
    }

    @Override
    public void checkAnyActionOnAnyPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                          String db) {
        checkAnyActionOnPolicy(currentUser, roleIds, policyType, catalogName, db, "*");
    }


    @Override
    public void checkWarehouseAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {
        if (!checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectTypeEPack.WAREHOUSE,
                Collections.singletonList(name))) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectTypeEPack.WAREHOUSE, name);
        }
    }

    @Override
    public void checkAnyActionOnWarehouse(UserIdentity currentUser, Set<Long> roleIds, String name) {
        if (!checkAnyActionOnObject(currentUser, roleIds, ObjectTypeEPack.WAREHOUSE, Collections.singletonList(name))) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectTypeEPack.WAREHOUSE, name);
        }
    }

    @Override
    public Expr getColumnMaskingPolicy(ConnectContext currentUser, TableName tableName, String columnName, Type type) {
        SecurityPolicyMgr policyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        TableUID tableUID = TableUID.generate(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        if (!policyManager.hasTableAppliedPolicy(tableUID)) {
            return null;
        }

        PolicyAppliedContext tableAppliedPolicyInfo = policyManager.getTableAppliedPolicyInfo(tableUID);
        Map<String, MaskingPolicyContext> maskingPolicyApply = tableAppliedPolicyInfo.getMaskingPolicyApply();
        MaskingPolicyContext maskingPolicyContext = maskingPolicyApply.get(columnName);

        if (maskingPolicyContext != null) {
            Policy maskingPolicy = policyManager.getPolicyById(maskingPolicyContext.getPolicyId());
            Map<SlotRef, SlotRef> onColumnsMap = new HashMap<>();
            List<String> usingColumns = maskingPolicyContext.getUsingColumns();
            List<String> argNames = maskingPolicy.getArgNames();

            for (int i = 0; i < maskingPolicyContext.getUsingColumns().size(); ++i) {
                onColumnsMap.put(new SlotRef(null, argNames.get(i)), new SlotRef(tableName, usingColumns.get(i)));
            }

            RewriteAliasVisitor r = new RewriteAliasVisitor(onColumnsMap);
            return r.visit(maskingPolicy.getPolicyExpression().clone());
        } else {
            return null;
        }
    }

    @Override
    public Expr getRowAccessPolicy(ConnectContext currentUser, TableName tableName) {
        SecurityPolicyMgr policyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        TableUID tableUID = TableUID.generate(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        if (!policyManager.hasTableAppliedPolicy(tableUID)) {
            return null;
        }

        PolicyAppliedContext tableAppliedPolicyInfo = policyManager.getTableAppliedPolicyInfo(tableUID);

        Expr rewriteExpr = null;
        for (RowAccessPolicyContext rowAccessPolicyInfo : tableAppliedPolicyInfo.getRowAccessPolicyApply()) {
            Policy rowAccessPolicy = policyManager.getPolicyById(rowAccessPolicyInfo.getPolicyId());

            if (!rowAccessPolicyInfo.getOnColumns().isEmpty()) {
                Map<SlotRef, SlotRef> onColumnsMap = new HashMap<>();
                List<String> onColumns = rowAccessPolicyInfo.getOnColumns();
                List<String> argNames = rowAccessPolicy.getArgNames();

                for (int i = 0; i < rowAccessPolicyInfo.getOnColumns().size(); ++i) {
                    onColumnsMap.put(new SlotRef(null, argNames.get(i)), new SlotRef(tableName, onColumns.get(i)));
                }

                RewriteAliasVisitor r = new RewriteAliasVisitor(onColumnsMap);
                if (rewriteExpr == null) {
                    rewriteExpr = r.visit(rowAccessPolicy.getPolicyExpression().clone());
                } else {
                    rewriteExpr = Expr.compoundAnd(Lists.newArrayList(
                            r.visit(rowAccessPolicy.getPolicyExpression().clone()), rewriteExpr));
                }
            } else {
                rewriteExpr = rowAccessPolicy.getPolicyExpression();
            }
        }
        return rewriteExpr;
    }

    private static class RewriteAliasVisitor extends AstVisitor<Expr, Void> {
        Map<SlotRef, SlotRef> map;

        public RewriteAliasVisitor(Map<SlotRef, SlotRef> map) {
            this.map = map;
        }

        @Override
        public Expr visit(ParseNode expr) {
            return visit(expr, null);
        }

        @Override
        public Expr visitExpression(Expr expr, Void context) {
            for (int i = 0; i < expr.getChildren().size(); ++i) {
                expr.setChild(i, visit(expr.getChild(i)));
            }
            return expr;
        }

        @Override
        public Expr visitSlot(SlotRef slotRef, Void context) {
            return map.getOrDefault(slotRef, slotRef);
        }
    }
}
