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
import com.starrocks.sql.ast.AstRewriter;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;

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
    public Expr getColumnMaskingPolicy(ConnectContext context, TableName tableName, String columnName, Type type) {
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
            Expr policyExpr = SqlParser.parseSqlToExpr(maskingPolicy.getPolicyExpressionSQL(),
                    context.getSessionVariable().getSqlMode());
            return (Expr) r.visit(policyExpr);
        } else {
            return null;
        }
    }

    @Override
    public Expr getRowAccessPolicy(ConnectContext context, TableName tableName) {
        SecurityPolicyMgr policyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        TableUID tableUID = TableUID.generate(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        if (!policyManager.hasTableAppliedPolicy(tableUID)) {
            return null;
        }

        PolicyAppliedContext tableAppliedPolicyInfo = policyManager.getTableAppliedPolicyInfo(tableUID);

        Expr rewriteExpr = null;
        for (RowAccessPolicyContext rowAccessPolicyInfo : tableAppliedPolicyInfo.getRowAccessPolicyApply()) {
            Policy rowAccessPolicy = policyManager.getPolicyById(rowAccessPolicyInfo.getPolicyId());
            Expr policyExpr = SqlParser.parseSqlToExpr(rowAccessPolicy.getPolicyExpressionSQL(),
                    context.getSessionVariable().getSqlMode());

            if (!rowAccessPolicyInfo.getOnColumns().isEmpty()) {
                Map<SlotRef, SlotRef> onColumnsMap = new HashMap<>();
                List<String> onColumns = rowAccessPolicyInfo.getOnColumns();
                List<String> argNames = rowAccessPolicy.getArgNames();

                for (int i = 0; i < rowAccessPolicyInfo.getOnColumns().size(); ++i) {
                    onColumnsMap.put(new SlotRef(null, argNames.get(i), argNames.get(i)),
                            new SlotRef(tableName, onColumns.get(i)));
                }

                RewriteAliasVisitor r = new RewriteAliasVisitor(onColumnsMap);

                if (rewriteExpr == null) {
                    rewriteExpr = (Expr) r.visit(policyExpr);
                } else {
                    rewriteExpr = Expr.compoundAnd(Lists.newArrayList((Expr) r.visit(policyExpr), rewriteExpr));
                }
            } else {
                rewriteExpr = policyExpr;
            }
        }
        return rewriteExpr;
    }

    private static class RewriteAliasVisitor extends AstRewriter<Void> {
        Map<SlotRef, SlotRef> map;

        public RewriteAliasVisitor(Map<SlotRef, SlotRef> map) {
            this.map = map;
        }

        @Override
        public ParseNode visit(ParseNode node) {
            return visit(node, null);
        }

        @Override
        public ParseNode visitExpression(Expr expr, Void context) {
            for (int i = 0; i < expr.getChildren().size(); ++i) {
                expr.setChild(i, (Expr) visit(expr.getChild(i)));
            }
            return expr;
        }

        @Override
        public ParseNode visitSlot(SlotRef slotRef, Void context) {
            return map.getOrDefault(slotRef, slotRef);
        }
    }
}
